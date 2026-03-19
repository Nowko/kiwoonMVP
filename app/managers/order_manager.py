# -*- coding: utf-8 -*-
import datetime
import json
import math
import uuid

from PyQt5.QtCore import QObject, QTimer, pyqtSignal


class OrderManager(QObject):
    positions_changed = pyqtSignal()
    trade_cycles_changed = pyqtSignal()
    summaries_changed = pyqtSignal()
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, kiwoom_client, telegram_router, account_manager, position_state_manager=None, strategy_manager=None, realtime_market_state_manager=None, parent=None):
        super(OrderManager, self).__init__(parent)
        self.persistence = persistence
        self.kiwoom_client = kiwoom_client
        self.telegram_router = telegram_router
        self.account_manager = account_manager
        self.position_state_manager = position_state_manager
        self.strategy_manager = strategy_manager
        self.realtime_market_state_manager = realtime_market_state_manager
        self.execution_mode = "live"
        self.buy_reject_retry_cooldown_sec = 60
        self.kiwoom_client.chejan_received.connect(self._on_chejan_received)
        self.kiwoom_client.api_message_received.connect(self._on_api_message_received)
        if hasattr(self.kiwoom_client, "account_cash_received"):
            self.kiwoom_client.account_cash_received.connect(self._on_account_cash_received)
        if hasattr(self.kiwoom_client, "account_positions_received"):
            self.kiwoom_client.account_positions_received.connect(self._on_account_positions_received)
        if hasattr(self.kiwoom_client, "account_realized_received"):
            self.kiwoom_client.account_realized_received.connect(self._on_account_realized_received)
        if hasattr(self.kiwoom_client, "outstanding_orders_received"):
            self.kiwoom_client.outstanding_orders_received.connect(self._on_outstanding_orders_received)
        if hasattr(self.kiwoom_client, "real_price_received"):
            self.kiwoom_client.real_price_received.connect(self._on_real_price_received)
        self._daily_review_snapshot_timer = QTimer(self)
        self._daily_review_snapshot_timer.timeout.connect(self.save_daily_review_snapshot)
        self._daily_review_snapshot_timer.start(60000)
        self._finalize_stale_daily_review_rows()

    def set_execution_mode(self, mode):
        if mode not in ["simulated", "live"]:
            raise ValueError("invalid execution mode")
        self.execution_mode = mode
        self.log_emitted.emit("🔧 Trade Mode: {0}".format(self._execution_mode_label(mode)))

    def _execution_mode_label(self, mode=None):
        mode = str(mode or self.execution_mode or "live")
        return "Trade OFF" if mode == "simulated" else "Trade ON"

    def _is_regular_market_hours(self, now_dt=None):
        now_dt = now_dt or __import__("datetime").datetime.now()
        if now_dt.weekday() >= 5:
            return False
        hhmm = int(now_dt.strftime("%H%M"))
        return 900 <= hhmm < 1530

    def _normalize_symbol_row(self, symbol_row):
        if isinstance(symbol_row, dict):
            return dict(symbol_row)
        try:
            return dict(symbol_row or {})
        except Exception:
            return {}

    def _mark_after_hours_buy_blocked(self, symbol_row, trigger_type, detail):
        symbol_row = self._normalize_symbol_row(symbol_row)
        code = str(symbol_row.get("code") or "").strip()
        if not code:
            return
        now = self.persistence.now_ts()
        extra = {}
        try:
            extra = json.loads(symbol_row.get("extra_json") or "{}")
        except Exception:
            extra = {}
        extra["buy_block_reason"] = "after_hours"
        extra["buy_blocked_at"] = now
        extra["buy_block_trigger"] = str(trigger_type or "")
        extra["buy_block_detail"] = str(detail or "")
        self.persistence.execute(
            "UPDATE tracked_symbols SET current_state='BUY_BLOCKED', extra_json=?, updated_at=? WHERE code=?",
            (json.dumps(extra, ensure_ascii=False), now, code),
        )

    def _get_realtime_price_snapshot(self, code):
        code = str(code or "").strip()
        if not code:
            return {}
        if self.realtime_market_state_manager is not None and hasattr(self.realtime_market_state_manager, "get_snapshot"):
            try:
                snapshot = dict(self.realtime_market_state_manager.get_snapshot(code) or {})
                if snapshot:
                    return snapshot
            except Exception:
                pass
        if hasattr(self.kiwoom_client, "get_realtime_snapshot"):
            try:
                snapshot = dict(self.kiwoom_client.get_realtime_snapshot(code) or {})
                if snapshot:
                    return snapshot
            except Exception:
                pass
        return {}

    def _emit_trade_state_refresh(self):
        self.trade_cycles_changed.emit()
        self.positions_changed.emit()
        self.summaries_changed.emit()

    def _get_account_cash_settings(self, account_no):
        account_no = str(account_no or "").strip()
        if not account_no:
            return {"deposit_cash": 0.0, "orderable_cash": 0.0, "estimated_assets": 0.0}
        try:
            for row in self.account_manager.get_accounts():
                if str(row.get("account_no") or "").strip() != account_no:
                    continue
                settings = dict(row.get("settings") or {})
                return {
                    "deposit_cash": float(settings.get("deposit_cash", 0.0) or 0.0),
                    "orderable_cash": float(settings.get("orderable_cash", 0.0) or 0.0),
                    "estimated_assets": float(settings.get("estimated_assets", 0.0) or 0.0),
                }
        except Exception:
            pass
        return {"deposit_cash": 0.0, "orderable_cash": 0.0, "estimated_assets": 0.0}

    def _refresh_holding_realtime_watch(self):
        if self.realtime_market_state_manager is not None:
            self.realtime_market_state_manager.refresh_watch_codes()
            return
        if not hasattr(self.kiwoom_client, "set_holding_realtime_codes"):
            return
        rows = self.persistence.fetchall("SELECT DISTINCT code FROM positions WHERE qty > 0 ORDER BY code")
        codes = [str(row["code"] or "").strip() for row in rows if str(row["code"] or "").strip()]
        self.kiwoom_client.set_holding_realtime_codes(codes)

    def _evaluate_sell_state(self, state, strategy_manager):
        position = dict(state.get("position_row") or {})
        cycle = state.get("cycle_row")
        active_state = dict(state.get("active_sell_state") or {})
        account_no = str(state.get("account_no") or position.get("account_no") or "")
        code = str(state.get("code") or position.get("code") or "")
        sell_strategy_nos = list(state.get("applied_sell_strategy_nos") or [])
        if not sell_strategy_nos and cycle:
            extra = self._load_cycle_extra(cycle)
            active_state = dict(extra.get("active_sell_state") or active_state or {})
            sell_strategy_nos = list(active_state.get("applied_sell_strategy_nos") or [])
        if not sell_strategy_nos:
            sell_strategy_nos = strategy_manager.normalize_strategy_nos("sell", json.loads(strategy_manager.get_default_strategy_policy()["sell_strategy_nos_json"] or '[]'))
            active_state["applied_sell_strategy_nos"] = sell_strategy_nos
        evaluation = strategy_manager.evaluate_sell_strategy_list(sell_strategy_nos, position, cycle_row=cycle, active_state=active_state)
        current_active_state = dict(evaluation.get("active_state") or active_state or {})
        if evaluation.get("state_changed"):
            self._persist_cycle_active_sell_state(account_no, code, cycle, current_active_state)
        take_profit_config = self._get_take_profit_strategy_config(sell_strategy_nos)
        trigger_snapshot = self._build_sell_strategy_snapshot_from_evaluation(evaluation)
        trigger_type = str(trigger_snapshot.get("trigger_sell_strategy_type") or "")
        has_take_profit_order = self._has_take_profit_reservation(current_active_state)
        if cycle and take_profit_config and not has_take_profit_order and not self._has_pending_sell(account_no, code, cycle):
            if not (evaluation.get("passed") and trigger_type and trigger_type != "take_profit"):
                current_active_state = self._ensure_take_profit_reservation(position, cycle, current_active_state, sell_strategy_nos)
                has_take_profit_order = self._has_take_profit_reservation(current_active_state)
        if evaluation.get("passed"):
            if has_take_profit_order:
                if trigger_type and trigger_type != "take_profit":
                    current_active_state = self._arm_pending_exit_switch(position, cycle, evaluation, current_active_state)
                    self._reconcile_pending_exit_switch(account_no, code, cycle=cycle, position_row=position, active_state=current_active_state)
                return evaluation
            self._submit_sell_for_position(position, cycle, evaluation, trigger_label="sell_strategy")
        return evaluation

    def manual_sell_position(self, account_no, code):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        if not account_no or not code:
            return False

        state = None
        if self.position_state_manager is not None and hasattr(self.position_state_manager, "get_position_state"):
            try:
                state = self.position_state_manager.get_position_state(account_no, code)
            except Exception:
                state = None

        position = dict((state or {}).get("position_row") or {})
        if not position:
            row = self.persistence.fetchone(
                "SELECT * FROM positions WHERE account_no=? AND code=? AND qty > 0",
                (account_no, code),
            )
            if row:
                position = dict(row)
        if not position:
            self.log_emitted.emit("⚠️ 수동 매도 대상 보유 종목을 찾지 못했습니다: {0} / {1}".format(account_no, code))
            return False

        cycle = (state or {}).get("cycle_row") or self._find_open_cycle(account_no, code)
        if self._has_pending_sell(account_no, code, cycle):
            self.log_emitted.emit("⚠️ 이미 매도 진행 중인 종목입니다: {0} / {1}".format(account_no, code))
            return False

        evaluation = {
            "passed": True,
            "trigger_reason": "manual_sell",
            "results": [
                {
                    "strategy_type": "manual_sell",
                    "strategy_name": "수동매도",
                    "passed": True,
                }
            ],
        }
        ok = self._submit_sell_for_position(position, cycle, evaluation, trigger_label="manual_sell")
        if not ok:
            self.log_emitted.emit("⚠️ 수동 매도 요청 실패: {0} / {1}".format(account_no, code))
        return ok

    def _build_entry_market_metrics(self, code, snapshot=None, captured_at=""):
        code = str(code or "").strip()
        snapshot = dict(snapshot or {})
        if not snapshot and self.strategy_manager is not None and hasattr(self.strategy_manager, "get_realtime_market_snapshot"):
            try:
                snapshot = dict(self.strategy_manager.get_realtime_market_snapshot(code) or {})
            except Exception:
                snapshot = {}
        if not snapshot:
            snapshot = self._get_realtime_price_snapshot(code)
        current_price = self._to_float(snapshot.get("current_price"))
        acc_volume = self._to_float(snapshot.get("acc_volume"))
        acc_turnover = self._to_float(snapshot.get("acc_turnover"))
        vwap_intraday = self._to_float(snapshot.get("vwap_intraday"))
        if vwap_intraday <= 0 and acc_volume > 0 and acc_turnover > 0:
            vwap_intraday = round(acc_turnover / acc_volume, 4)
        sell_hoga_total = self._to_float(snapshot.get("sell_hoga_total"))
        buy_hoga_total = self._to_float(snapshot.get("buy_hoga_total"))
        sell_pressure_ratio = self._to_float(snapshot.get("sell_pressure_ratio"))
        if sell_pressure_ratio <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
            sell_pressure_ratio = round(sell_hoga_total / buy_hoga_total, 4)
        buy_pressure_ratio = self._to_float(snapshot.get("buy_pressure_ratio"))
        if buy_pressure_ratio <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
            buy_pressure_ratio = round(buy_hoga_total / sell_hoga_total, 4)
        return {
            "captured_at": str(captured_at or snapshot.get("updated_at") or self.persistence.now_ts()),
            "current_price": current_price,
            "vwap_intraday": vwap_intraday,
            "sell_pressure_ratio": sell_pressure_ratio,
            "buy_pressure_ratio": buy_pressure_ratio,
            "sell_hoga_total": sell_hoga_total,
            "buy_hoga_total": buy_hoga_total,
        }

    def _merge_entry_market_metrics(self, current, incoming):
        merged = dict(current or {})
        incoming = dict(incoming or {})
        for key in ["current_price", "vwap_intraday", "sell_pressure_ratio", "buy_pressure_ratio", "sell_hoga_total", "buy_hoga_total"]:
            value = self._to_float(incoming.get(key))
            if value > 0:
                merged[key] = value
        captured_at = str(incoming.get("captured_at") or "").strip()
        if captured_at:
            merged["captured_at"] = captured_at
        return merged

    def _apply_entry_market_metrics(self, target, metrics):
        if not isinstance(target, dict):
            return target
        merged = self._merge_entry_market_metrics(target.get("entry_market_metrics"), metrics)
        if not merged:
            return target
        target["entry_market_metrics"] = merged
        if str(merged.get("captured_at") or "").strip():
            target["entry_market_metrics_at"] = str(merged.get("captured_at") or "").strip()
        for key in ["vwap_intraday", "sell_pressure_ratio", "buy_pressure_ratio", "sell_hoga_total", "buy_hoga_total", "current_price"]:
            value = self._to_float(merged.get(key))
            if value > 0:
                target["entry_{0}".format(key)] = value
        return target

    def _apply_exit_market_metrics(self, target, metrics):
        if not isinstance(target, dict):
            return target
        merged = self._merge_entry_market_metrics(target.get("exit_market_metrics"), metrics)
        if not merged:
            return target
        target["exit_market_metrics"] = merged
        if str(merged.get("captured_at") or "").strip():
            target["exit_market_metrics_at"] = str(merged.get("captured_at") or "").strip()
        for key in ["vwap_intraday", "sell_pressure_ratio", "buy_pressure_ratio", "sell_hoga_total", "buy_hoga_total", "current_price"]:
            value = self._to_float(merged.get(key))
            if value > 0:
                target["exit_{0}".format(key)] = value
        return target

    def _build_sell_strategy_snapshot_from_evaluation(self, sell_evaluation):
        sell_evaluation = dict(sell_evaluation or {})
        result_items = [dict(item or {}) for item in list(sell_evaluation.get("results") or []) if isinstance(item, dict)]
        trigger_no = int(sell_evaluation.get("trigger_strategy_no") or 0)
        trigger_item = {}
        if trigger_no > 0:
            for item in result_items:
                if int(item.get("strategy_no") or 0) == trigger_no:
                    trigger_item = dict(item)
                    break
        if not trigger_item:
            for item in result_items:
                if item.get("passed"):
                    trigger_item = dict(item)
                    break
        return {
            "sell_strategy_results": result_items,
            "trigger_sell_strategy_no": int(trigger_item.get("strategy_no") or trigger_no or 0),
            "trigger_sell_strategy_name": str(trigger_item.get("strategy_name") or ""),
            "trigger_sell_strategy_type": str(trigger_item.get("strategy_type") or ""),
            "trigger_sell_reason": str(sell_evaluation.get("trigger_reason") or trigger_item.get("reason") or ""),
        }

    def _build_buy_strategy_snapshot_from_evaluation(self, evaluation, market_metrics=None):
        evaluation = dict(evaluation or {})
        trigger_item = dict(evaluation.get("trigger_buy_strategy") or {})
        passed_items = [dict(item or {}) for item in list(evaluation.get("passed_buy_strategies") or []) if isinstance(item, dict)]
        result_items = [dict(item or {}) for item in list(evaluation.get("buy_strategy_results") or []) if isinstance(item, dict)]
        snapshot = {
            "trigger_buy_strategy": trigger_item,
            "passed_buy_strategies": passed_items,
            "buy_strategy_results": result_items,
            "trigger_buy_strategy_no": int(trigger_item.get("strategy_no") or 0),
            "trigger_buy_strategy_id": str(trigger_item.get("strategy_id") or ""),
            "trigger_buy_strategy_name": str(trigger_item.get("strategy_name") or ""),
            "trigger_buy_strategy_type": str(trigger_item.get("strategy_type") or ""),
        }
        return self._apply_entry_market_metrics(snapshot, market_metrics)

    def _build_active_sell_state_from_evaluation(self, evaluation, account_no, code, detected_at, buy_filled_at="", market_metrics=None):
        buy_strategy_snapshot = self._build_buy_strategy_snapshot_from_evaluation(evaluation, market_metrics=market_metrics)
        active_state = {
            "entry_source": evaluation.get("entry_source") or evaluation.get("trigger_strategy_type") or "slot_buy",
            "entry_slot_no": evaluation.get("entry_slot_no"),
            "policy_source": evaluation.get("policy_source") or "default",
            "applied_sell_strategy_nos": list(evaluation.get("applied_sell_strategy_nos") or []),
            "buy_expression_items": list(evaluation.get("buy_expression_items") or []),
            "news_min_score": int(evaluation.get("news_min_score") or 0),
            "news_trade_min_score": int(evaluation.get("news_trade_min_score") or 0),
            "account_no": account_no,
            "code": code,
            "detected_at": detected_at or "",
            "buy_filled_at": buy_filled_at or "",
        }
        active_state.update(buy_strategy_snapshot)
        return active_state

    def _persist_cycle_active_sell_state(self, account_no, code, cycle, active_state):
        active_state = dict(active_state or {})
        if account_no and code:
            self._save_position_active_sell_state(account_no, code, active_state)
        if cycle:
            extra_obj = self._load_cycle_extra(cycle)
            extra_obj["active_sell_state"] = active_state
            self.persistence.execute(
                "UPDATE trade_cycles SET extra_json=? WHERE cycle_id=?",
                (json.dumps(extra_obj, ensure_ascii=False), cycle["cycle_id"]),
            )

    def _get_take_profit_strategy_config(self, sell_strategy_nos):
        manager = self.strategy_manager
        if manager is None:
            return {}
        try:
            strategy_nos = manager.normalize_strategy_nos("sell", sell_strategy_nos or [])
        except Exception:
            strategy_nos = list(sell_strategy_nos or [])
        for strategy_no in strategy_nos:
            row = manager.get_strategy_by_no("sell", strategy_no)
            if not row:
                continue
            row = dict(row)
            if not int(row.get("enabled") or 0):
                continue
            if str(row.get("strategy_type") or "") != "take_profit":
                continue
            params = self._safe_json_dict(row.get("params_json") or "{}")
            take_profit_pct = float(params.get("take_profit_pct") or 0.0)
            if take_profit_pct <= 0:
                continue
            return {
                "strategy_no": int(row.get("strategy_no") or 0),
                "strategy_name": str(row.get("strategy_name") or ""),
                "strategy_type": "take_profit",
                "take_profit_pct": take_profit_pct,
            }
        return {}

    def _round_price_to_tick(self, price, direction="up"):
        price = float(price or 0.0)
        if price <= 0:
            return 0
        tick = max(1, int(self._get_tick_size(price) or 1))
        if direction == "down":
            return max(tick, int(price // tick) * tick)
        return int(math.ceil(price / float(tick)) * tick)

    def _has_take_profit_reservation(self, active_state):
        active_state = dict(active_state or {})
        return bool(
            active_state.get("take_profit_order_active")
            or active_state.get("take_profit_order_pending")
            or active_state.get("pending_exit_switch")
            or active_state.get("pending_take_profit_replace")
        )

    def _clear_take_profit_reservation_state(self, active_state, clear_pending_exit=False):
        active_state = dict(active_state or {})
        for key in [
            "take_profit_order_active",
            "take_profit_order_pending",
            "take_profit_order_no",
            "take_profit_order_price",
            "take_profit_order_qty",
            "take_profit_order_requested_at",
            "take_profit_last_fill_at",
            "take_profit_filled_qty",
            "take_profit_strategy_no",
            "take_profit_strategy_name",
            "take_profit_pct",
        ]:
            active_state.pop(key, None)
        if clear_pending_exit:
            for key in [
                "pending_exit_switch",
                "pending_exit_reason",
                "pending_exit_strategy_no",
                "pending_exit_strategy_type",
                "pending_exit_strategy_name",
                "pending_exit_requested_at",
                "pending_exit_cancel_requested",
                "pending_exit_cancel_requested_at",
                "pending_exit_submitted_at",
            ]:
                active_state.pop(key, None)
        return active_state

    def _find_take_profit_open_order(self, account_no, code, active_state=None):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        active_state = dict(active_state or {})
        if not account_no or not code:
            return None
        order_no = str(active_state.get("take_profit_order_no") or "").strip()
        if order_no:
            row = self.persistence.fetchone(
                "SELECT * FROM open_orders WHERE account_no=? AND code=? AND order_no=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
                (account_no, code, order_no),
            )
            if row:
                return dict(row)
        order_price = float(active_state.get("take_profit_order_price") or 0.0)
        if order_price > 0:
            row = self.persistence.fetchone(
                "SELECT * FROM open_orders WHERE account_no=? AND code=? AND unfilled_qty>0 AND ABS(order_price - ?) < 0.5 ORDER BY updated_at DESC LIMIT 1",
                (account_no, code, order_price),
            )
            if row:
                return dict(row)
        row = self.persistence.fetchone(
            "SELECT * FROM open_orders WHERE account_no=? AND code=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
            (account_no, code),
        )
        return dict(row) if row else None

    def _find_existing_sell_open_order_for_position(self, account_no, code):
        rows = self.persistence.fetchall(
            "SELECT * FROM open_orders WHERE account_no=? AND code=? AND unfilled_qty>0 ORDER BY updated_at DESC, order_no DESC",
            (str(account_no or "").strip(), str(code or "").strip()),
        )
        for row in rows:
            open_order = dict(row)
            raw_payload = self._safe_json_dict(open_order.get("raw_json") or "{}")
            try:
                if raw_payload and self._is_buy_payload(raw_payload):
                    continue
            except Exception:
                pass
            order_gubun = str(open_order.get("order_gubun") or raw_payload.get("order_gubun") or "").strip()
            if "매수" in order_gubun:
                continue
            return open_order
        return None

    def _seconds_since_ts(self, ts_text):
        ts_text = str(ts_text or "").strip()
        if not ts_text:
            return None
        try:
            ts_dt = datetime.datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
        return max(0.0, (datetime.datetime.now() - ts_dt).total_seconds())

    def _ensure_take_profit_reservation(self, position_row, cycle, active_state, sell_strategy_nos):
        if self.execution_mode != "live":
            return dict(active_state or {})
        if not cycle or not getattr(self.kiwoom_client, "connected", False):
            return dict(active_state or {})
        active_state = dict(active_state or {})
        if self._has_take_profit_reservation(active_state):
            return active_state
        take_profit_config = self._resolve_take_profit_strategy_config(active_state, sell_strategy_nos)
        if not take_profit_config:
            return active_state
        account_no = str(position_row.get("account_no") or cycle.get("account_no") or "").strip()
        code = str(position_row.get("code") or cycle.get("code") or "").strip()
        name = str(position_row.get("name") or cycle.get("name") or code)
        qty = int(position_row.get("qty") or 0)
        avg_price = float(position_row.get("avg_price") or 0.0)
        if not account_no or not code or qty <= 0 or avg_price <= 0:
            return active_state
        target_price = self._round_price_to_tick(avg_price * (1.0 + (take_profit_config.get("take_profit_pct") or 0.0) / 100.0), direction="up")
        if target_price <= 0:
            return active_state
        ok = self.kiwoom_client.send_order(
            rq_name="SELLTP_%s" % str(cycle.get("cycle_id") or uuid.uuid4().hex)[-6:],
            screen_no="7005",
            account_no=account_no,
            order_type=2,
            code=code,
            qty=qty,
            price=target_price,
            hoga_gb="00",
            original_order_no="",
        )
        if not ok:
            return active_state
        now = self.persistence.now_ts()
        active_state.update({
            "take_profit_order_active": True,
            "take_profit_order_pending": True,
            "take_profit_order_no": "",
            "take_profit_order_price": target_price,
            "take_profit_order_qty": qty,
            "take_profit_order_requested_at": now,
            "take_profit_strategy_no": int(take_profit_config.get("strategy_no") or 0),
            "take_profit_strategy_name": str(take_profit_config.get("strategy_name") or ""),
            "take_profit_pct": float(take_profit_config.get("take_profit_pct") or 0.0),
        })
        active_state.pop("pending_take_profit_replace", None)
        active_state.pop("pending_take_profit_replace_cancel_requested", None)
        active_state.pop("pending_take_profit_replace_requested_at", None)
        self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
        self.persistence.execute(
            "UPDATE tracked_symbols SET has_open_order=1, current_state='SELL_ORDER_PENDING', updated_at=? WHERE code=?",
            (now, code),
        )
        self.log_emitted.emit("⏳ 익절 예약 주문 등록: {0} / {1} / {2}주 / {3}".format(account_no, code, qty, target_price))
        self._emit_trade_state_refresh()
        return active_state

    def _request_cancel_open_sell(self, cycle, open_order):
        account_no = str(open_order.get("account_no") or "")
        code = str(open_order.get("code") or "")
        order_no = str(open_order.get("order_no") or "")
        unfilled_qty = int(open_order.get("unfilled_qty") or 0)
        if not account_no or not code or not order_no or unfilled_qty <= 0:
            return False
        if not self._is_regular_market_hours():
            self.log_emitted.emit("⏸️ 장후에는 익절 예약 매도 취소를 차단합니다: {0} / {1}".format(account_no, code))
            return False
        ok = self.kiwoom_client.send_order(
            rq_name="SELLCANCEL_%s" % str(cycle.get("cycle_id") or uuid.uuid4().hex)[-6:],
            screen_no="7006",
            account_no=account_no,
            order_type=4,
            code=code,
            qty=unfilled_qty,
            price=0,
            hoga_gb="00",
            original_order_no=order_no,
        )
        if ok:
            self.log_emitted.emit("↩️ 익절 예약 매도 취소 요청: {0} / {1} / order={2}".format(account_no, code, order_no))
        return ok

    def _arm_pending_exit_switch(self, position_row, cycle, evaluation, active_state):
        if not cycle:
            return dict(active_state or {})
        account_no = str(position_row.get("account_no") or cycle.get("account_no") or "").strip()
        code = str(position_row.get("code") or cycle.get("code") or "").strip()
        active_state = dict(active_state or {})
        if active_state.get("pending_exit_switch"):
            return active_state
        trigger_snapshot = self._build_sell_strategy_snapshot_from_evaluation(evaluation)
        now = self.persistence.now_ts()
        active_state.update({
            "pending_exit_switch": True,
            "pending_exit_reason": str(trigger_snapshot.get("trigger_sell_reason") or evaluation.get("trigger_reason") or "sell_switch"),
            "pending_exit_strategy_no": int(trigger_snapshot.get("trigger_sell_strategy_no") or 0),
            "pending_exit_strategy_type": str(trigger_snapshot.get("trigger_sell_strategy_type") or ""),
            "pending_exit_strategy_name": str(trigger_snapshot.get("trigger_sell_strategy_name") or ""),
            "pending_exit_requested_at": now,
            "pending_exit_cancel_requested": False,
        })
        self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
        self.log_emitted.emit("⚠️ 손절/전환 매도를 위해 익절 예약 주문을 교체 대기합니다: {0} / {1}".format(account_no, code))
        return active_state

    def _arm_take_profit_replace(self, account_no, code, cycle, active_state, take_profit_pct):
        active_state = dict(active_state or {})
        active_state["take_profit_pct_override"] = float(take_profit_pct or 0.0)
        active_state["pending_take_profit_replace"] = True
        active_state["pending_take_profit_replace_cancel_requested"] = False
        active_state["pending_take_profit_replace_requested_at"] = self.persistence.now_ts()
        self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
        self.log_emitted.emit("📝 익절값 변경 대기: {0} / {1} / {2:.2f}%".format(account_no, code, float(take_profit_pct or 0.0)))
        return active_state

    def _build_pending_exit_evaluation(self, active_state):
        active_state = dict(active_state or {})
        strategy_type = str(active_state.get("pending_exit_strategy_type") or "stop_loss")
        strategy_name = str(active_state.get("pending_exit_strategy_name") or strategy_type)
        strategy_no = int(active_state.get("pending_exit_strategy_no") or 0)
        reason = str(active_state.get("pending_exit_reason") or "sell_switch")
        return {
            "passed": True,
            "trigger_reason": reason,
            "trigger_strategy_no": strategy_no,
            "results": [
                {
                    "strategy_no": strategy_no,
                    "strategy_type": strategy_type,
                    "strategy_name": strategy_name,
                    "passed": True,
                    "reason": reason,
                }
            ],
        }

    def _reconcile_pending_exit_switch(self, account_no, code, cycle=None, position_row=None, active_state=None):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        if not account_no or not code:
            return False
        cycle = cycle or self._find_open_cycle(account_no, code)
        if position_row is None:
            row = self.persistence.fetchone("SELECT * FROM positions WHERE account_no=? AND code=? AND qty>0", (account_no, code))
            position_row = dict(row) if row else {}
        active_state = dict(active_state or self._get_cycle_active_sell_state(cycle, position_row))
        if not active_state.get("pending_exit_switch"):
            return False
        open_order = self._find_take_profit_open_order(account_no, code, active_state)
        if open_order:
            order_no = str(open_order.get("order_no") or "").strip()
            if order_no and str(active_state.get("take_profit_order_no") or "").strip() != order_no:
                active_state["take_profit_order_no"] = order_no
                active_state["take_profit_order_active"] = True
                active_state["take_profit_order_pending"] = True
                self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
            if not active_state.get("pending_exit_cancel_requested"):
                ok = self._request_cancel_open_sell(cycle or {}, open_order)
                if ok:
                    active_state["pending_exit_cancel_requested"] = True
                    active_state["pending_exit_cancel_requested_at"] = self.persistence.now_ts()
                    self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
                return ok
            return False
        request_age = self._seconds_since_ts(active_state.get("take_profit_order_requested_at"))
        if active_state.get("take_profit_order_active") and not str(active_state.get("take_profit_order_no") or "").strip():
            if request_age is not None and request_age < 3.0:
                return False
        qty = int(position_row.get("qty") or 0)
        if qty <= 0:
            active_state = self._clear_take_profit_reservation_state(active_state, clear_pending_exit=True)
            self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
            return False
        evaluation = self._build_pending_exit_evaluation(active_state)
        ok = self._submit_sell_for_position(position_row, cycle, evaluation, trigger_label="pending_exit_switch", ignore_active_state=True)
        if ok:
            active_state = self._clear_take_profit_reservation_state(active_state, clear_pending_exit=True)
            active_state["pending_exit_submitted_at"] = self.persistence.now_ts()
            self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
        return ok

    def _get_cycle_active_sell_state(self, cycle=None, position_row=None):
        active_state = {}
        if position_row:
            active_state = self._safe_json_dict(position_row.get("active_sell_state_json") or "{}")
        if cycle:
            extra = self._load_cycle_extra(cycle)
            cycle_state = extra.get("active_sell_state")
            if isinstance(cycle_state, dict):
                active_state.update(dict(cycle_state or {}))
        return active_state

    def _get_default_sell_strategy_nos(self):
        if self.strategy_manager is None:
            return []
        try:
            policy = self.strategy_manager.get_default_strategy_policy() or {}
            raw = json.loads(policy.get("sell_strategy_nos_json") or "[]")
        except Exception:
            raw = []
        try:
            return list(self.strategy_manager.normalize_strategy_nos("sell", raw))
        except Exception:
            return [int(value) for value in list(raw or []) if str(value or "").strip().isdigit()]

    def _resolve_take_profit_strategy_config(self, active_state, sell_strategy_nos):
        active_state = dict(active_state or {})
        config = dict(self._get_take_profit_strategy_config(sell_strategy_nos) or {})
        override_pct = float(active_state.get("take_profit_pct_override") or 0.0)
        if override_pct > 0:
            if not config:
                config = {
                    "strategy_no": 0,
                    "strategy_name": "익절 사용자설정",
                    "strategy_type": "take_profit",
                }
            config["take_profit_pct"] = override_pct
        return config

    def _build_external_active_sell_state(self, account_no, code, position_row=None, active_state=None):
        position_row = dict(position_row or {})
        active_state = dict(active_state or {})
        now = self.persistence.now_ts()
        merged = {
            "entry_source": str(active_state.get("entry_source") or "external_position"),
            "entry_slot_no": active_state.get("entry_slot_no"),
            "policy_source": str(active_state.get("policy_source") or "external_position"),
            "applied_sell_strategy_nos": list(active_state.get("applied_sell_strategy_nos") or self._get_default_sell_strategy_nos()),
            "buy_expression_items": list(active_state.get("buy_expression_items") or []),
            "news_min_score": int(active_state.get("news_min_score") or 0),
            "news_trade_min_score": int(active_state.get("news_trade_min_score") or 0),
            "account_no": account_no,
            "code": code,
            "detected_at": str(active_state.get("detected_at") or position_row.get("updated_at") or now),
            "buy_filled_at": str(active_state.get("buy_filled_at") or position_row.get("updated_at") or now),
        }
        merged.update(active_state)
        return merged

    def _ensure_position_management_cycle(self, account_no, code, position_row=None):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        if not account_no or not code:
            return None
        cycle = self._find_open_cycle(account_no, code)
        if cycle:
            return cycle
        if position_row is None:
            row = self.persistence.fetchone(
                "SELECT * FROM positions WHERE account_no=? AND code=? AND qty>0",
                (account_no, code),
            )
            position_row = dict(row) if row else {}
        else:
            position_row = dict(position_row or {})
        if not position_row or int(position_row.get("qty") or 0) <= 0:
            return None
        active_state = self._build_external_active_sell_state(
            account_no,
            code,
            position_row=position_row,
            active_state=self._safe_json_dict(position_row.get("active_sell_state_json") or "{}"),
        )
        cycle_id = "ext_{0}_{1}_{2}".format(account_no[-4:] or "acct", code, uuid.uuid4().hex[:8])
        now = self.persistence.now_ts()
        extra_obj = {
            "entry_source": "external_position",
            "policy_source": "external_position",
            "active_sell_state": active_state,
        }
        self.persistence.execute(
            """
            INSERT INTO trade_cycles (
                cycle_id, trade_date, account_no, code, name,
                entry_detected_at, buy_order_at, buy_filled_at,
                source_conditions_json, buy_filters_json, sell_filters_json,
                news_scores_json, status, pnl_realized, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, '', ?, '[]', '[]', '[]', '{}', 'HOLDING', 0, ?)
            """,
            (
                cycle_id,
                self.persistence.today_str(),
                account_no,
                code,
                str(position_row.get("name") or code),
                now,
                str(position_row.get("updated_at") or now),
                json.dumps(extra_obj, ensure_ascii=False),
            ),
        )
        self._save_position_active_sell_state(account_no, code, active_state)
        return self._find_open_cycle(account_no, code)

    def _sync_take_profit_reservation_from_open_order(self, account_no, code, cycle, position_row, active_state, open_order):
        active_state = dict(active_state or {})
        open_order = dict(open_order or {})
        order_no = str(open_order.get("order_no") or "").strip()
        order_price = float(open_order.get("order_price") or 0.0)
        unfilled_qty = int(open_order.get("unfilled_qty") or 0)
        avg_price = float((position_row or {}).get("avg_price") or 0.0)
        active_state["take_profit_order_active"] = order_price > 0 and unfilled_qty > 0
        active_state["take_profit_order_pending"] = order_price > 0 and unfilled_qty > 0
        active_state["take_profit_order_no"] = order_no
        active_state["take_profit_order_price"] = order_price
        active_state["take_profit_order_qty"] = unfilled_qty
        if order_price > 0 and avg_price > 0 and float(active_state.get("take_profit_pct") or 0.0) <= 0:
            active_state["take_profit_pct"] = round(((order_price - avg_price) / avg_price) * 100.0, 2)
        self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
        return active_state

    def _restore_position_take_profit_management(self, account_no, code, cycle=None, position_row=None):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        if not account_no or not code:
            return False
        if position_row is None:
            row = self.persistence.fetchone(
                "SELECT * FROM positions WHERE account_no=? AND code=? AND qty>0",
                (account_no, code),
            )
            position_row = dict(row) if row else {}
        else:
            position_row = dict(position_row or {})
        if not position_row or int(position_row.get("qty") or 0) <= 0:
            return False
        cycle = cycle or self._ensure_position_management_cycle(account_no, code, position_row=position_row)
        if not cycle:
            return False
        active_state = self._build_external_active_sell_state(
            account_no,
            code,
            position_row=position_row,
            active_state=self._get_cycle_active_sell_state(cycle=cycle, position_row=position_row),
        )
        sell_strategy_nos = list(active_state.get("applied_sell_strategy_nos") or self._get_default_sell_strategy_nos())
        active_state["applied_sell_strategy_nos"] = sell_strategy_nos
        current_sell_order = self._find_existing_sell_open_order_for_position(account_no, code)
        if current_sell_order:
            if float(current_sell_order.get("order_price") or 0.0) > 0:
                active_state = self._sync_take_profit_reservation_from_open_order(
                    account_no,
                    code,
                    cycle,
                    position_row,
                    active_state,
                    current_sell_order,
                )
                if active_state.get("pending_take_profit_replace") and not active_state.get("pending_take_profit_replace_cancel_requested"):
                    ok = self._request_cancel_open_sell(cycle, current_sell_order)
                    if ok:
                        active_state["pending_take_profit_replace_cancel_requested"] = True
                        active_state["pending_take_profit_replace_requested_at"] = self.persistence.now_ts()
                        self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
                    return ok
                if active_state.get("pending_exit_switch"):
                    return self._reconcile_pending_exit_switch(account_no, code, cycle=cycle, position_row=position_row, active_state=active_state)
                return False
            return False
        if active_state.get("pending_exit_switch"):
            return self._reconcile_pending_exit_switch(account_no, code, cycle=cycle, position_row=position_row, active_state=active_state)
        if active_state.get("pending_take_profit_replace"):
            active_state = self._clear_take_profit_reservation_state(active_state, clear_pending_exit=False)
            ok = bool(self._ensure_take_profit_reservation(position_row, cycle, active_state, sell_strategy_nos))
            if ok:
                active_state = self._get_cycle_active_sell_state(cycle=cycle, position_row=position_row)
                active_state.pop("pending_take_profit_replace", None)
                active_state.pop("pending_take_profit_replace_cancel_requested", None)
                active_state.pop("pending_take_profit_replace_requested_at", None)
                self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
            return ok
        return bool(self._ensure_take_profit_reservation(position_row, cycle, active_state, sell_strategy_nos))

    def describe_take_profit_state(self, state):
        state = dict(state or {})
        active_state = dict(state.get("active_sell_state") or {})
        sell_strategy_nos = list(state.get("applied_sell_strategy_nos") or active_state.get("applied_sell_strategy_nos") or [])
        config = self._resolve_take_profit_strategy_config(active_state, sell_strategy_nos)
        take_profit_pct = float(config.get("take_profit_pct") or active_state.get("take_profit_pct") or 0.0)
        avg_price = float(state.get("avg_price") or 0.0)
        take_profit_price = self._round_price_to_tick(avg_price * (1.0 + take_profit_pct / 100.0), direction="up") if avg_price > 0 and take_profit_pct > 0 else 0
        if active_state.get("pending_exit_switch"):
            status = "손절 전환중"
        elif active_state.get("pending_take_profit_replace"):
            status = "익절 변경중"
        elif active_state.get("take_profit_order_active") or active_state.get("take_profit_order_pending"):
            status = "익절 주문중"
        elif take_profit_pct > 0:
            status = "익절 대기"
        else:
            status = "-"
        return {
            "take_profit_pct": take_profit_pct,
            "take_profit_price": take_profit_price,
            "status_text": status,
        }

    def set_position_take_profit_pct(self, account_no, code, take_profit_pct):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        try:
            take_profit_pct = float(take_profit_pct or 0.0)
        except Exception:
            return False
        if not account_no or not code or take_profit_pct <= 0:
            return False
        position_row = self.persistence.fetchone(
            "SELECT * FROM positions WHERE account_no=? AND code=? AND qty>0",
            (account_no, code),
        )
        if not position_row:
            return False
        position_row = dict(position_row)
        cycle = self._ensure_position_management_cycle(account_no, code, position_row=position_row)
        if not cycle:
            return False
        active_state = self._build_external_active_sell_state(
            account_no,
            code,
            position_row=position_row,
            active_state=self._get_cycle_active_sell_state(cycle=cycle, position_row=position_row),
        )
        active_state["take_profit_pct_override"] = take_profit_pct
        current_sell_order = self._find_existing_sell_open_order_for_position(account_no, code)
        if current_sell_order and float(current_sell_order.get("order_price") or 0.0) > 0:
            active_state = self._arm_take_profit_replace(account_no, code, cycle, active_state, take_profit_pct)
        else:
            active_state = self._clear_take_profit_reservation_state(active_state, clear_pending_exit=False)
            active_state["take_profit_pct_override"] = take_profit_pct
            self._persist_cycle_active_sell_state(account_no, code, cycle, active_state)
        return bool(self._restore_position_take_profit_management(account_no, code, cycle=cycle, position_row=position_row))

    def _update_take_profit_state_from_sell_payload(self, active_state, payload, filled_qty, unfilled_qty, remaining_qty=None):
        active_state = dict(active_state or {})
        if not self._has_take_profit_reservation(active_state):
            return active_state
        order_no = str(payload.get("order_no") or "").strip()
        stored_order_no = str(active_state.get("take_profit_order_no") or "").strip()
        if order_no and not stored_order_no:
            active_state["take_profit_order_no"] = order_no
            stored_order_no = order_no
        if stored_order_no and order_no and stored_order_no != order_no:
            return active_state
        order_state_text = str(payload.get("order_status") or payload.get("status") or "").strip()
        if filled_qty > 0:
            active_state["take_profit_last_fill_at"] = self.persistence.now_ts()
            active_state["take_profit_filled_qty"] = int(active_state.get("take_profit_filled_qty") or 0) + int(filled_qty)
            active_state["take_profit_order_pending"] = unfilled_qty > 0
            active_state["take_profit_order_active"] = unfilled_qty > 0 and (remaining_qty is None or remaining_qty > 0)
            if unfilled_qty <= 0 or (remaining_qty is not None and remaining_qty <= 0):
                active_state = self._clear_take_profit_reservation_state(active_state, clear_pending_exit=not bool(active_state.get("pending_exit_switch")))
            return active_state
        if ("취소" in order_state_text or "거부" in order_state_text) and unfilled_qty <= 0:
            active_state = self._clear_take_profit_reservation_state(active_state, clear_pending_exit=False)
            return active_state
        active_state["take_profit_order_active"] = True
        active_state["take_profit_order_pending"] = True
        return active_state

    def _load_cycle_extra(self, cycle):
        try:
            if cycle is None:
                return {}
            extra_json = "{}"
            if isinstance(cycle, dict):
                extra_json = cycle.get("extra_json") or '{}'
            else:
                extra_json = cycle["extra_json"] or '{}'
            return json.loads(extra_json)
        except Exception:
            return {}

    def _safe_json_dict(self, raw):
        if isinstance(raw, dict):
            return dict(raw)
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        return data if isinstance(data, dict) else {}

    def _safe_json_list(self, raw):
        if isinstance(raw, list):
            return list(raw)
        try:
            data = json.loads(raw or "[]")
        except Exception:
            data = []
        return data if isinstance(data, list) else []

    def _finalize_stale_daily_review_rows(self):
        today = self.persistence.today_str()
        self.persistence.execute(
            "UPDATE daily_trade_review_summary SET is_finalized=1 WHERE trade_date < ? AND COALESCE(is_finalized, 0)=0",
            (today,),
        )

    def _is_daily_review_finalized(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        if now_dt.weekday() >= 5:
            return True
        return now_dt.strftime("%H%M") >= "1530"

    def _resolve_cycle_condition_name_for_review(self, cycle, active_state=None):
        cycle = dict(cycle or {})
        active_state = dict(active_state or {})
        source_conditions = self._safe_json_list(cycle.get("source_conditions_json") or "[]")
        entry_slot_no = active_state.get("entry_slot_no")
        try:
            entry_slot_no = int(entry_slot_no or 0)
        except Exception:
            entry_slot_no = 0
        matched_name = ""
        fallback_name = ""
        latest_ts = ""
        for item in source_conditions:
            if not isinstance(item, dict):
                continue
            condition_name = str(item.get("condition_name") or "").strip()
            ts = str(item.get("ts") or "")
            try:
                slot_no = int(item.get("slot_no") or 0)
            except Exception:
                slot_no = 0
            if condition_name and slot_no > 0 and slot_no == entry_slot_no and ts >= latest_ts:
                matched_name = condition_name
                latest_ts = ts
            if condition_name and not fallback_name:
                fallback_name = condition_name
        return matched_name or fallback_name

    def _strategy_name_by_no_for_review(self, kind, strategy_no):
        if self.strategy_manager is None:
            return ""
        try:
            row = self.strategy_manager.get_strategy_by_no(kind, int(strategy_no or 0))
        except Exception:
            row = None
        if not row:
            return ""
        try:
            return str(row["strategy_name"] or row["strategy_type"] or "").strip()
        except Exception:
            return ""

    def _resolve_strategy_text_for_review(self, cycle=None, active_state=None):
        cycle = dict(cycle or {})
        active_state = dict(active_state or {})
        strategy_names = []
        for item in list(active_state.get("buy_expression_items") or []):
            if str(item.get("kind") or "") != "strategy":
                continue
            try:
                strategy_no = int(item.get("no") or 0)
            except Exception:
                strategy_no = 0
            if strategy_no <= 0:
                continue
            strategy_name = self._strategy_name_by_no_for_review("buy", strategy_no)
            if strategy_name:
                strategy_names.append(strategy_name)
        if strategy_names:
            condition_name = self._resolve_cycle_condition_name_for_review(cycle, active_state=active_state)
            joined_names = "-".join([name for name in strategy_names if name])
            return "{0}-{1}".format(condition_name, joined_names) if condition_name else joined_names
        extra = self._load_cycle_extra(cycle)
        trigger = dict(extra.get("trigger_buy_strategy") or {})
        strategy_name = str(trigger.get("strategy_name") or trigger.get("strategy_type") or "").strip()
        if strategy_name:
            condition_name = self._resolve_cycle_condition_name_for_review(cycle, active_state=active_state)
            return "{0}-{1}".format(condition_name, strategy_name) if condition_name else strategy_name
        condition_name = self._resolve_cycle_condition_name_for_review(cycle, active_state=active_state)
        if condition_name:
            return condition_name
        entry_source = str(active_state.get("entry_source") or self._load_cycle_extra(cycle).get("entry_source") or "")
        if entry_source == "news_trade":
            return "뉴스매매"
        return "-"

    def _build_daily_review_holding_item(self, account_no, position_row, snapshot_ts):
        position_row = dict(position_row or {})
        code = str(position_row.get("code") or "").strip()
        active_state = self._safe_json_dict(position_row.get("active_sell_state_json") or "{}")
        cycle = self._find_open_cycle(account_no, code) or {}
        condition_name = self._resolve_cycle_condition_name_for_review(cycle, active_state=active_state)
        strategy_text = self._resolve_strategy_text_for_review(cycle=cycle, active_state=active_state)
        qty = int(position_row.get("qty") or 0)
        ref_price = float(position_row.get("current_price") or 0.0)
        eval_profit = float(position_row.get("eval_profit") or 0.0)
        cycle_extra = self._load_cycle_extra(cycle)
        entry_market_metrics = self._merge_entry_market_metrics(
            cycle_extra.get("entry_market_metrics"),
            active_state.get("entry_market_metrics"),
        )
        extra_payload = {
            "qty": qty,
            "eval_rate": float(position_row.get("eval_rate") or 0.0),
            "buy_chain_id": str(position_row.get("buy_chain_id") or ""),
        }
        if entry_market_metrics:
            extra_payload["entry_market_metrics"] = entry_market_metrics
        return {
            "item_id": "{0}|{1}|holding|{2}".format(self.persistence.today_str(), account_no, code),
            "trade_date": self.persistence.today_str(),
            "account_no": account_no,
            "snapshot_ts": snapshot_ts,
            "row_type": "holding_eod",
            "code": code,
            "name": str(position_row.get("name") or code),
            "avg_price": float(position_row.get("avg_price") or 0.0),
            "ref_price": ref_price,
            "eval_profit": eval_profit,
            "realized_profit": 0.0,
            "contribution_profit": eval_profit,
            "strategy_text": strategy_text,
            "condition_name": condition_name,
            "cycle_id": str(cycle.get("cycle_id") or ""),
            "item_status": "보유",
            "extra_json": json.dumps(extra_payload, ensure_ascii=False),
        }

    def _build_daily_review_sold_item(self, account_no, cycle_row, snapshot_ts, trade_date):
        cycle_row = dict(cycle_row or {})
        extra = self._load_cycle_extra(cycle_row)
        active_state = dict(extra.get("active_sell_state") or {})
        realized_profit = float(cycle_row.get("pnl_realized") or 0.0)
        entry_market_metrics = self._merge_entry_market_metrics(
            extra.get("entry_market_metrics"),
            active_state.get("entry_market_metrics"),
        )
        exit_market_metrics = self._merge_entry_market_metrics(
            extra.get("exit_market_metrics"),
            active_state.get("exit_market_metrics"),
        )
        latest_chejan = dict(extra.get("latest_chejan") or {})
        ref_price = self._to_price(latest_chejan.get("fill_price"))
        if ref_price <= 0:
            ref_price = self._to_price(latest_chejan.get("current_price"))
        if ref_price <= 0:
            ref_price = float((exit_market_metrics or {}).get("current_price") or 0.0)
        extra_payload = {
            "buy_filled_at": str(cycle_row.get("buy_filled_at") or ""),
            "sell_filled_at": str(cycle_row.get("sell_filled_at") or ""),
            "status": str(cycle_row.get("status") or ""),
        }
        if entry_market_metrics:
            extra_payload["entry_market_metrics"] = entry_market_metrics
        if exit_market_metrics:
            extra_payload["exit_market_metrics"] = exit_market_metrics
        for key in ["trigger_sell_strategy_no", "trigger_sell_strategy_name", "trigger_sell_strategy_type", "trigger_sell_reason"]:
            value = extra.get(key)
            if value not in [None, "", 0, 0.0]:
                extra_payload[key] = value
        if list(extra.get("sell_strategy_results") or []):
            extra_payload["sell_strategy_results"] = list(extra.get("sell_strategy_results") or [])
        return {
            "item_id": "{0}|{1}|sold|{2}".format(trade_date, account_no, str(cycle_row.get("cycle_id") or "")),
            "trade_date": trade_date,
            "account_no": account_no,
            "snapshot_ts": snapshot_ts,
            "row_type": "sold_today",
            "code": str(cycle_row.get("code") or ""),
            "name": str(cycle_row.get("name") or cycle_row.get("code") or ""),
            "avg_price": 0.0,
            "ref_price": ref_price,
            "eval_profit": 0.0,
            "realized_profit": realized_profit,
            "contribution_profit": realized_profit,
            "strategy_text": self._resolve_strategy_text_for_review(cycle=cycle_row, active_state=active_state),
            "condition_name": self._resolve_cycle_condition_name_for_review(cycle_row, active_state=active_state),
            "cycle_id": str(cycle_row.get("cycle_id") or ""),
            "item_status": "당일매도",
            "extra_json": json.dumps(extra_payload, ensure_ascii=False),
        }

    def save_daily_review_snapshot(self):
        now_dt = datetime.datetime.now()
        trade_date = now_dt.strftime("%Y-%m-%d")
        snapshot_ts = self.persistence.now_ts()
        is_finalized = 1 if self._is_daily_review_finalized(now_dt) else 0
        accounts = self.persistence.fetchall("SELECT account_no FROM accounts ORDER BY account_no")
        for account_row in accounts:
            account_no = str(account_row["account_no"] or "").strip()
            if not account_no:
                continue
            holding_rows = [
                dict(row)
                for row in self.persistence.fetchall(
                    "SELECT * FROM positions WHERE account_no=? AND qty>0 ORDER BY code",
                    (account_no,),
                )
            ]
            sold_rows = [
                dict(row)
                for row in self.persistence.fetchall(
                    """
                    SELECT *
                    FROM trade_cycles
                    WHERE account_no=?
                      AND sell_filled_at LIKE ?
                      AND status IN ('CLOSED', 'SIMULATED_CLOSED')
                    ORDER BY sell_filled_at, code
                    """,
                    (account_no, "{0}%".format(trade_date)),
                )
            ]
            if not holding_rows and not sold_rows:
                self.persistence.execute(
                    "DELETE FROM daily_trade_review_items WHERE trade_date=? AND account_no=?",
                    (trade_date, account_no),
                )
                self.persistence.execute(
                    "DELETE FROM daily_trade_review_summary WHERE trade_date=? AND account_no=?",
                    (trade_date, account_no),
                )
                continue
            items = []
            for row in holding_rows:
                items.append(self._build_daily_review_holding_item(account_no, row, snapshot_ts))
            for row in sold_rows:
                items.append(self._build_daily_review_sold_item(account_no, row, snapshot_ts, trade_date))
            holding_eval_total = sum([float(item.get("eval_profit") or 0.0) for item in items if str(item.get("row_type") or "") == "holding_eod"])
            account_cash = self._get_account_cash_settings(account_no)
            api_total_profit = float(account_cash.get("api_total_profit", 0.0) or 0.0)
            api_realized_profit = float(account_cash.get("api_realized_profit", 0.0) or 0.0)
            sold_realized_total = sum([float(item.get("realized_profit") or 0.0) for item in items if str(item.get("row_type") or "") == "sold_today"])
            if api_realized_profit != 0:
                realized_profit_total = api_realized_profit
            elif api_total_profit != 0:
                realized_profit_total = api_total_profit - holding_eval_total
            else:
                realized_profit_total = sold_realized_total
            total_pnl = holding_eval_total + realized_profit_total
            self.persistence.execute(
                """
                INSERT INTO daily_trade_review_summary (
                    trade_date, account_no, snapshot_ts, holding_eval_total, realized_profit_total,
                    total_pnl, holding_count, sold_count, is_finalized, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, account_no) DO UPDATE SET
                    snapshot_ts=excluded.snapshot_ts,
                    holding_eval_total=excluded.holding_eval_total,
                    realized_profit_total=excluded.realized_profit_total,
                    total_pnl=excluded.total_pnl,
                    holding_count=excluded.holding_count,
                    sold_count=excluded.sold_count,
                    is_finalized=excluded.is_finalized,
                    extra_json=excluded.extra_json
                """,
                (
                    trade_date,
                    account_no,
                    snapshot_ts,
                    holding_eval_total,
                    realized_profit_total,
                    total_pnl,
                    len([item for item in items if str(item.get("row_type") or "") == "holding_eod"]),
                    len([item for item in items if str(item.get("row_type") or "") == "sold_today"]),
                    is_finalized,
                    json.dumps({"snapshot_type": "daily_review"}, ensure_ascii=False),
                ),
            )
            self.persistence.execute(
                "DELETE FROM daily_trade_review_items WHERE trade_date=? AND account_no=?",
                (trade_date, account_no),
            )
            self.persistence.executemany(
                """
                INSERT INTO daily_trade_review_items (
                    item_id, trade_date, account_no, snapshot_ts, row_type, code, name,
                    avg_price, ref_price, eval_profit, realized_profit, contribution_profit,
                    strategy_text, condition_name, cycle_id, item_status, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item["item_id"],
                        item["trade_date"],
                        item["account_no"],
                        item["snapshot_ts"],
                        item["row_type"],
                        item["code"],
                        item["name"],
                        item["avg_price"],
                        item["ref_price"],
                        item["eval_profit"],
                        item["realized_profit"],
                        item["contribution_profit"],
                        item["strategy_text"],
                        item["condition_name"],
                        item["cycle_id"],
                        item["item_status"],
                        item["extra_json"],
                    )
                    for item in items
                ],
            )
            self._reconcile_today_account_snapshots(account_no, snapshot_ts=snapshot_ts)

    def _reconcile_today_account_snapshots(self, account_no, snapshot_ts=""):
        account_no = str(account_no or "").strip()
        if not account_no:
            return
        trade_date = self.persistence.today_str()
        snapshot_ts = str(snapshot_ts or self.persistence.now_ts())
        holding_row = self.persistence.fetchone(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(eval_profit), 0) AS eval_sum FROM positions WHERE account_no=? AND qty>0",
            (account_no,),
        ) or {}
        sold_row = self.persistence.fetchone(
            "SELECT COUNT(*) AS sold_cnt, COALESCE(SUM(pnl_realized), 0) AS realized_sum FROM trade_cycles WHERE trade_date=? AND account_no=? AND status IN ('CLOSED', 'SIMULATED_CLOSED')",
            (trade_date, account_no),
        ) or {}
        holding_eval_total = float(holding_row["eval_sum"] if holding_row and "eval_sum" in holding_row.keys() else 0.0)
        holding_count = int(holding_row["cnt"] if holding_row and "cnt" in holding_row.keys() else 0)
        sold_count = int(sold_row["sold_cnt"] if sold_row and "sold_cnt" in sold_row.keys() else 0)
        cycle_realized_total = float(sold_row["realized_sum"] if sold_row and "realized_sum" in sold_row.keys() else 0.0)
        account_cash = self._get_account_cash_settings(account_no)
        api_total_profit = float(account_cash.get("api_total_profit", 0.0) or 0.0)
        api_realized_profit = float(account_cash.get("api_realized_profit", 0.0) or 0.0)
        if api_realized_profit != 0:
            realized_profit_total = api_realized_profit
        elif api_total_profit != 0:
            realized_profit_total = api_total_profit - holding_eval_total
        else:
            realized_profit_total = cycle_realized_total
        total_pnl = holding_eval_total + realized_profit_total
        self.persistence.execute(
            """
            INSERT INTO daily_account_summary (
                trade_date, account_no, eval_profit_total, realized_profit_total,
                holding_count, sold_count, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, '{}')
            ON CONFLICT(trade_date, account_no) DO UPDATE SET
                eval_profit_total=excluded.eval_profit_total,
                realized_profit_total=excluded.realized_profit_total,
                holding_count=excluded.holding_count,
                sold_count=excluded.sold_count
            """,
            (
                trade_date,
                account_no,
                holding_eval_total,
                realized_profit_total,
                holding_count,
                sold_count,
            ),
        )
        existing_review = self.persistence.fetchone(
            "SELECT is_finalized, extra_json FROM daily_trade_review_summary WHERE trade_date=? AND account_no=?",
            (trade_date, account_no),
        )
        is_finalized = int(existing_review["is_finalized"] or 0) if existing_review else (1 if self._is_daily_review_finalized() else 0)
        extra_json = existing_review["extra_json"] if existing_review and existing_review["extra_json"] else json.dumps({"snapshot_type": "daily_review"}, ensure_ascii=False)
        self.persistence.execute(
            """
            INSERT INTO daily_trade_review_summary (
                trade_date, account_no, snapshot_ts, holding_eval_total, realized_profit_total,
                total_pnl, holding_count, sold_count, is_finalized, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, account_no) DO UPDATE SET
                snapshot_ts=excluded.snapshot_ts,
                holding_eval_total=excluded.holding_eval_total,
                realized_profit_total=excluded.realized_profit_total,
                total_pnl=excluded.total_pnl,
                holding_count=excluded.holding_count,
                sold_count=excluded.sold_count,
                is_finalized=excluded.is_finalized,
                extra_json=excluded.extra_json
            """,
            (
                trade_date,
                account_no,
                snapshot_ts,
                holding_eval_total,
                realized_profit_total,
                total_pnl,
                holding_count,
                sold_count,
                is_finalized,
                extra_json,
            ),
        )

    def get_daily_review_date_status_map(self):
        rows = self.persistence.fetchall(
            """
            SELECT trade_date,
                   MAX(snapshot_ts) AS snapshot_ts,
                   SUM(holding_eval_total) AS holding_eval_total,
                   SUM(realized_profit_total) AS realized_profit_total,
                   SUM(total_pnl) AS total_pnl,
                   SUM(holding_count) AS holding_count,
                   SUM(sold_count) AS sold_count,
                   MIN(COALESCE(is_finalized, 0)) AS is_finalized
            FROM daily_trade_review_summary
            GROUP BY trade_date
            ORDER BY trade_date DESC
            """
        )
        result = {}
        for row in rows:
            result[str(row["trade_date"] or "")] = {
                "trade_date": str(row["trade_date"] or ""),
                "snapshot_ts": str(row["snapshot_ts"] or ""),
                "holding_eval_total": float(row["holding_eval_total"] or 0.0),
                "realized_profit_total": float(row["realized_profit_total"] or 0.0),
                "total_pnl": float(row["total_pnl"] or 0.0),
                "holding_count": int(row["holding_count"] or 0),
                "sold_count": int(row["sold_count"] or 0),
                "is_finalized": bool(int(row["is_finalized"] or 0)),
            }
        return result

    def get_daily_review_summary_rows(self, trade_date):
        return [
            dict(row)
            for row in self.persistence.fetchall(
                """
                SELECT trade_date, account_no, snapshot_ts, holding_eval_total, realized_profit_total,
                       total_pnl, holding_count, sold_count, is_finalized, extra_json
                FROM daily_trade_review_summary
                WHERE trade_date=?
                ORDER BY account_no
                """,
                (str(trade_date or ""),),
            )
        ]

    def get_daily_review_item_rows(self, trade_date):
        return [
            dict(row)
            for row in self.persistence.fetchall(
                """
                SELECT *
                FROM daily_trade_review_items
                WHERE trade_date=?
                ORDER BY account_no,
                         CASE WHEN row_type='holding_eod' THEN 0 ELSE 1 END,
                         contribution_profit DESC,
                         name,
                         code
                """,
                (str(trade_date or ""),),
            )
        ]

    def _save_position_active_sell_state(self, account_no, code, active_state):
        self.persistence.execute(
            "UPDATE positions SET active_sell_state_json=?, updated_at=? WHERE account_no=? AND code=?",
            (json.dumps(active_state or {}, ensure_ascii=False), self.persistence.now_ts(), account_no, code),
        )

    def _has_pending_sell(self, account_no, code, cycle=None, ignore_active_state=False):
        if cycle and str(cycle.get("status") or "") in ["SELL_PENDING", "SELL_PARTIAL"]:
            return True
        if cycle and not ignore_active_state:
            active_state = self._get_cycle_active_sell_state(cycle=cycle)
            if self._has_take_profit_reservation(active_state):
                return True
        row = self.persistence.fetchone(
            "SELECT order_no FROM open_orders WHERE account_no=? AND code=? AND unfilled_qty>0 LIMIT 1",
            (account_no, code),
        )
        return row is not None

    def _submit_sell_for_position(self, position_row, cycle, sell_evaluation, trigger_label="auto_sell", ignore_active_state=False):
        account_no = str(position_row.get("account_no") or "")
        code = str(position_row.get("code") or "")
        name = str(position_row.get("name") or code)
        qty = int(position_row.get("qty") or 0)
        if not account_no or not code or qty <= 0:
            return False
        if self._has_pending_sell(account_no, code, cycle, ignore_active_state=ignore_active_state):
            return False
        now = self.persistence.now_ts()
        reason = sell_evaluation.get("trigger_reason") or trigger_label
        sell_filters_json = json.dumps(sell_evaluation.get("results", []), ensure_ascii=False)
        exit_market_metrics = self._build_entry_market_metrics(code, captured_at=now)
        sell_strategy_snapshot = self._build_sell_strategy_snapshot_from_evaluation(sell_evaluation)
        if self.execution_mode == "simulated":
            fill_price = float(position_row.get("current_price") or position_row.get("avg_price") or 0)
            if fill_price <= 0:
                return False
            if cycle:
                extra_obj = self._load_cycle_extra(cycle)
                extra_obj.update({
                    "sell_trigger": reason,
                    "sell_signal_at": now,
                    "sell_filters": sell_evaluation.get("results", []),
                })
                extra_obj.update(sell_strategy_snapshot)
                self._apply_exit_market_metrics(extra_obj, exit_market_metrics)
                extra_json = json.dumps(extra_obj, ensure_ascii=False)
                self.persistence.execute(
                    "UPDATE trade_cycles SET sell_signal_at=?, sell_order_at=?, sell_filled_at=?, sell_filters_json=?, status='CLOSED', extra_json=? WHERE cycle_id=?",
                    (now, now, now, sell_filters_json, extra_json, cycle["cycle_id"]),
                )
            self._apply_sell_fill(account_no, code, qty, fill_price, cycle or {"cycle_id": "", "name": name, "pnl_realized": 0})
            remaining = self._current_position_qty(account_no, code)
            self.persistence.execute(
                "UPDATE tracked_symbols SET is_holding=?, has_open_order=0, current_state=?, updated_at=? WHERE code=?",
                (1 if remaining > 0 else 0, 'HOLDING' if remaining > 0 else 'CLOSED', now, code),
            )
            self.log_emitted.emit(u"🧾 자동매도 체결(모의): {0} / {1} / {2}".format(account_no, code, reason))
            self._emit_trade_state_refresh()
            return True
        if not getattr(self.kiwoom_client, "connected", False):
            self.log_emitted.emit(u"⚠️ 키움 미연결 상태라 자동매도를 실행하지 못했습니다: {0} / {1}".format(account_no, code))
            return False
        ok = self.kiwoom_client.send_order(
            rq_name="SELL_%s" % (str(cycle.get("cycle_id") if cycle else uuid.uuid4().hex)[-6:]),
            screen_no="7004",
            account_no=account_no,
            order_type=2,
            code=code,
            qty=qty,
            price=0,
            hoga_gb="03",
            original_order_no="",
        )
        if ok:
            if cycle:
                extra_obj = self._load_cycle_extra(cycle)
                extra_obj.update({
                    "sell_trigger": reason,
                    "sell_signal_at": now,
                    "sell_filters": sell_evaluation.get("results", []),
                })
                extra_obj.update(sell_strategy_snapshot)
                self._apply_exit_market_metrics(extra_obj, exit_market_metrics)
                extra_json = json.dumps(extra_obj, ensure_ascii=False)
                self.persistence.execute(
                    "UPDATE trade_cycles SET sell_signal_at=?, sell_order_at=?, sell_filters_json=?, status='SELL_PENDING', extra_json=? WHERE cycle_id=?",
                    (now, now, sell_filters_json, extra_json, cycle["cycle_id"]),
                )
            self.persistence.execute(
                "UPDATE tracked_symbols SET has_open_order=1, current_state='SELL_ORDER_PENDING', updated_at=? WHERE code=?",
                (now, code),
            )
            self.log_emitted.emit(u"📤 자동매도 주문 전송: {0} / {1} / {2}".format(account_no, code, reason))
            self.telegram_router.send_trade_message(
                "매도 신호 발생",
                [
                    "계좌: {0}".format(account_no),
                    "종목: {0} ({1})".format(name, code),
                    "사유: {0}".format(reason),
                    "수량: {0}".format(qty),
                ],
                code=code,
            )
            self._emit_trade_state_refresh()
        return ok

    def evaluate_sell_positions(self, strategy_manager):
        self.strategy_manager = strategy_manager or self.strategy_manager
        if self.position_state_manager is not None:
            position_states = self.position_state_manager.get_active_position_states()
        else:
            position_states = []
            rows = self.persistence.fetchall(
                "SELECT * FROM positions WHERE qty > 0 ORDER BY account_no, code"
            )
            for row in rows:
                position = dict(row)
                account_no = str(position.get("account_no") or "")
                code = str(position.get("code") or "")
                cycle = self._find_open_cycle(account_no, code)
                active_state = {}
                try:
                    active_state = json.loads(position.get("active_sell_state_json") or '{}')
                except Exception:
                    active_state = {}
                position_states.append({
                    "account_no": account_no,
                    "code": code,
                    "position_row": position,
                    "cycle_row": cycle,
                    "active_sell_state": active_state,
                    "applied_sell_strategy_nos": list(active_state.get("applied_sell_strategy_nos") or []),
                })
        for state in position_states:
            self._evaluate_sell_state(state, self.strategy_manager)

    def _record_policy_log(self, cycle_id, account_no, code, policy, stage, action, detail=None):
        detail = dict(detail or {})
        name = ""
        try:
            row = self.persistence.fetchone("SELECT name FROM tracked_symbols WHERE code=?", (code,))
            if row and row["name"]:
                name = row["name"]
        except Exception:
            name = ""
        payload = {
            "cycle_id": cycle_id,
            "account_no": account_no,
            "code": code,
            "name": name or code,
            "policy": policy,
            "stage": stage,
            "action": action,
            "detail": detail,
        }
        self.persistence.execute(
            """
            INSERT INTO order_policy_logs (
                ts, cycle_id, account_no, code, name, policy, stage, action, detail_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.persistence.now_ts(),
                cycle_id,
                account_no,
                code,
                name or code,
                policy,
                stage,
                action,
                json.dumps(detail, ensure_ascii=False),
            ),
        )
        self.persistence.write_event("order_policy_log", payload)

    def _build_account_sync_profiles(self, include_all=False):
        if include_all:
            rows = list(self.account_manager.get_accounts() or [])
            profiles = []
            for row in rows:
                account_no = str(row.get("account_no") or "").strip()
                if not account_no:
                    continue
                settings = dict(row.get("settings") or {})
                profiles.append({
                    "account_no": account_no,
                    "query_password_mode": str(settings.get("query_password_mode", "api_saved") or "api_saved"),
                    "query_password": str(settings.get("query_password", "") or ""),
                })
            return profiles
        return list(self.account_manager.get_active_account_profiles() or [])

    def synchronize_active_accounts(self):
        profiles = self._build_account_sync_profiles(include_all=False)
        account_numbers = [row["account_no"] for row in profiles if row.get("account_no")]
        if not account_numbers:
            self.log_emitted.emit("⚠️ 활성 계좌가 없어 동기화를 수행하지 않았습니다")
            return False
        if not getattr(self.kiwoom_client, "connected", False):
            self.log_emitted.emit("⚠️ 키움 미연결 상태라 계좌 동기화를 생략합니다")
            return False
        mode_text = []
        for row in profiles:
            mode_text.append("{0}:{1}".format(row["account_no"], row.get("query_password_mode", "api_saved")))
        self.log_emitted.emit("🔄 활성 계좌 동기화 요청: {0}".format(", ".join(mode_text)))
        return self.kiwoom_client.request_account_sync(profiles)

    def synchronize_startup_accounts(self):
        profiles = self._build_account_sync_profiles(include_all=False)
        account_numbers = [row["account_no"] for row in profiles if row.get("account_no")]
        if not account_numbers:
            self.log_emitted.emit("⚠️ 활성 계좌가 없어 시작 동기화를 수행하지 않습니다")
            return False
        if not getattr(self.kiwoom_client, "connected", False):
            self.log_emitted.emit("⚠️ 키움 미연결 상태라 시작 계좌 동기화를 생략합니다")
            return False
        startup_profiles = []
        mode_text = []
        for row in profiles:
            profile = dict(row)
            profile["include_cash"] = True
            profile["include_balance"] = True
            profile["include_realized"] = False
            profile["include_outstanding"] = False
            startup_profiles.append(profile)
            mode_text.append("{0}:startup_light".format(profile["account_no"]))
        self.log_emitted.emit("⚡ 시작 경량 계좌 동기화 요청: {0}".format(", ".join(mode_text)))
        return self.kiwoom_client.request_account_sync(startup_profiles)

    def synchronize_all_accounts(self):
        profiles = self._build_account_sync_profiles(include_all=True)
        account_numbers = [row["account_no"] for row in profiles if row.get("account_no")]
        if not account_numbers:
            self.log_emitted.emit("⚠️ 등록된 계좌가 없어 운영 동기화를 수행하지 않았습니다")
            return False
        if not getattr(self.kiwoom_client, "connected", False):
            self.log_emitted.emit("⚠️ 키움 미연결 상태라 운영 계좌 동기화를 생략합니다")
            return False
        mode_text = []
        for row in profiles:
            mode_text.append("{0}:{1}".format(row["account_no"], row.get("query_password_mode", "api_saved")))
        self.log_emitted.emit("🔄 전체 계좌 동기화 요청: {0}".format(", ".join(mode_text)))
        return self.kiwoom_client.request_account_sync(profiles)

    def submit_buy_orders(self, symbol_row, evaluation, trigger_type):
        symbol_row = self._normalize_symbol_row(symbol_row)
        code = str(symbol_row.get("code") or "").strip()
        if not code:
            self.log_emitted.emit("⚠️ 종목코드가 없어 매수 주문을 만들지 않았습니다")
            return []

        active_profiles = list(self.account_manager.get_active_account_profiles() or [])
        if not active_profiles:
            self.log_emitted.emit("⚠️ 활성 계좌가 없어 매수 주문을 만들지 않았습니다")
            return []

        if not self._is_regular_market_hours():
            detail = "after_hours_{0}".format(str(trigger_type or "buy"))
            self._mark_after_hours_buy_blocked(symbol_row, trigger_type, detail)
            self.log_emitted.emit("⏸️ 장후에는 모든 매수 주문을 차단합니다: {0} / {1}".format(code, trigger_type))
            self._emit_trade_state_refresh()
            return []

        created = []
        for profile in active_profiles:
            account_no = profile["account_no"]
            if self._has_recent_buy_rejection(account_no, code):
                continue
            if self._has_open_position_or_cycle(account_no, code):
                self.log_emitted.emit("⏭️ 중복 진입 방지: {0} / {1}".format(account_no, code))
                continue

            pricing = self._resolve_order_pricing(profile, symbol_row)
            qty, sizing_meta = self._resolve_order_quantity(profile, symbol_row, evaluation, pricing)
            if qty <= 0:
                self.log_emitted.emit("⚠️ 주문 수량 계산 실패로 매수를 건너뜁니다: {0} / {1}".format(account_no, code))
                continue
            cycle_id = "cycle_" + uuid.uuid4().hex[:16]
            now = self.persistence.now_ts()
            status = "SIMULATED_HOLDING"
            buy_filled_at = now
            if self.execution_mode == "live":
                status = "BUY_REQUESTED"
                buy_filled_at = None
            source_conditions_json = symbol_row.get("source_conditions_json") or "[]"
            buy_filters_json = json.dumps(evaluation.get("results", []), ensure_ascii=False)
            news_scores_json = json.dumps(evaluation.get("news_scores", {}), ensure_ascii=False)
            entry_market_metrics = self._build_entry_market_metrics(
                code,
                captured_at=buy_filled_at if self.execution_mode == "simulated" else now,
            )
            active_sell_state = self._build_active_sell_state_from_evaluation(
                evaluation,
                account_no,
                code,
                symbol_row.get("last_detected_at") or now,
                buy_filled_at if self.execution_mode == "simulated" else "",
                market_metrics=entry_market_metrics,
            )
            buy_strategy_snapshot = self._build_buy_strategy_snapshot_from_evaluation(
                evaluation,
                market_metrics=entry_market_metrics,
            )
            request_meta = {
                "trigger_type": trigger_type,
                "execution_mode": self.execution_mode,
                "requested_qty": qty,
                "hoga_gb": pricing.get("hoga_gb", profile.get("hoga_gb", "03")),
                "order_budget_mode": profile.get("order_budget_mode", "fixed_amount"),
                "order_budget_value": profile.get("order_budget_value", 0),
                "order_price": pricing.get("order_price", 0),
                "limit_price_option": pricing.get("limit_price_option", profile.get("limit_price_option", "current_price")),
                "unfilled_policy": profile.get("unfilled_policy", "reprice_then_market"),
                "first_wait_sec": int(profile.get("first_wait_sec", 5) or 5),
                "second_wait_sec": int(profile.get("second_wait_sec", 5) or 5),
                "price_source": pricing.get("price_source", ""),
                "sizing_meta": sizing_meta,
                "entry_source": evaluation.get("entry_source", trigger_type),
                "entry_slot_no": evaluation.get("entry_slot_no"),
                "policy_source": evaluation.get("policy_source", "default"),
                "active_sell_state": active_sell_state,
            }
            request_meta.update(buy_strategy_snapshot)

            self.persistence.execute(
                """
                INSERT INTO trade_cycles (
                    cycle_id, trade_date, account_no, code, name,
                    entry_detected_at, buy_order_at, buy_filled_at,
                    source_conditions_json, buy_filters_json, sell_filters_json,
                    news_scores_json, status, pnl_realized, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, 0, ?)
                """,
                (
                    cycle_id,
                    self.persistence.today_str(),
                    account_no,
                    symbol_row.get("code", ""),
                    symbol_row.get("name", ""),
                    symbol_row["last_detected_at"],
                    now,
                    buy_filled_at,
                    source_conditions_json,
                    buy_filters_json,
                    news_scores_json,
                    status,
                    json.dumps(request_meta, ensure_ascii=False),
                ),
            )

            if self.execution_mode == "simulated":
                simulated_fill_price = float(pricing.get("reference_price") or pricing.get("order_price") or 0)
                self._upsert_simulated_position(account_no, symbol_row, evaluation, qty, simulated_fill_price, market_metrics=entry_market_metrics)
                self.persistence.execute(
                    "UPDATE tracked_symbols SET is_holding=1, has_open_order=0, current_state='HOLDING', updated_at=? WHERE code=?",
                    (now, symbol_row.get("code", "")),
                )
                created.append(cycle_id)
            else:
                if not self.kiwoom_client.connected:
                    self.persistence.execute(
                        "UPDATE trade_cycles SET status='LIVE_SKIPPED', extra_json=? WHERE cycle_id=?",
                        (self._merge_json(request_meta, {"skip_reason": "not_connected"}), cycle_id),
                    )
                    self.log_emitted.emit("⚠️ 키움 미연결 상태라 Trade ON 주문 스킵: {0}".format(account_no))
                    continue
                ok = self.kiwoom_client.send_order(
                    rq_name="BUY_%s" % cycle_id[-6:],
                    screen_no="7001",
                    account_no=account_no,
                    order_type=1,
                    code=symbol_row.get("code", ""),
                    qty=qty,
                    price=int(pricing.get("order_price", 0) or 0),
                    hoga_gb=str(pricing.get("hoga_gb", profile.get("hoga_gb", "03"))),
                    original_order_no="",
                )
                if ok:
                    self.persistence.execute(
                        "UPDATE trade_cycles SET status='BUY_PENDING', extra_json=? WHERE cycle_id=?",
                        (self._merge_json(request_meta, {"send_order_ok": True}), cycle_id),
                    )
                    self.persistence.execute(
                        "UPDATE tracked_symbols SET has_open_order=1, current_state='BUY_ORDER_PENDING', updated_at=? WHERE code=?",
                        (now, symbol_row.get("code", "")),
                    )
                    created.append(cycle_id)
                    if str(pricing.get("hoga_gb", "03")) == "00":
                        self._schedule_unfilled_policy(cycle_id, account_no, symbol_row.get("code", ""), request_meta)
                else:
                    self.persistence.execute(
                        "UPDATE trade_cycles SET status='BUY_REJECTED', extra_json=? WHERE cycle_id=?",
                        (self._merge_json(request_meta, {"send_order_ok": False}), cycle_id),
                    )
                    self.persistence.execute(
                        "UPDATE tracked_symbols SET has_open_order=0, current_state='BUY_REJECTED', extra_json=?, updated_at=? WHERE code=?",
                        (
                            self._merge_json(
                                symbol_row.get("extra_json"),
                                {
                                    "buy_rejected_at": now,
                                    "buy_rejected_account_no": account_no,
                                    "buy_rejected_trigger": str(trigger_type or ""),
                                    "buy_rejected_reason": "send_order_failed",
                                },
                            ),
                            now,
                            symbol_row.get("code", ""),
                        ),
                    )
                    self.log_emitted.emit("❌ Trade ON 주문 요청 실패: {0} / {1}".format(account_no, symbol_row.get("code", "")))

            self.persistence.write_event(
                "buy_order_created",
                {
                    "account_no": account_no,
                    "code": symbol_row.get("code", ""),
                    "name": symbol_row.get("name", ""),
                    "trigger_type": trigger_type,
                    "execution_mode": self.execution_mode,
                    "evaluation": evaluation,
                },
            )
            self.telegram_router.send_trade_message(
                "매수 후보 생성",
                [
                    "계좌: {0}".format(account_no),
                    "종목: {0} ({1})".format(symbol_row.get("name", ""), symbol_row.get("code", "")),
                    "트리거: {0}".format(trigger_type),
                    "Trade: {0}".format(self._execution_mode_label()),
                    "주문방식: {0}".format("지정가" if pricing.get("hoga_gb") == "00" else "시장가"),
                    "지정가옵션: {0}".format(pricing.get("limit_price_option", "-")),
                    "수량: {0}".format(qty),
                    "주문가 기준: {0}".format(int(pricing.get("reference_price", 0) or 0)),
                    "뉴스점수: {0}".format(evaluation.get("news_scores", {}).get("final_score", "-")),
                ],
                code=symbol_row.get("code", ""),
            )
        self.rebuild_daily_summaries()
        self._refresh_holding_realtime_watch()
        self.positions_changed.emit()
        self.trade_cycles_changed.emit()
        self.summaries_changed.emit()
        return created

    def _schedule_unfilled_policy(self, cycle_id, account_no, code, request_meta):
        policy = str(request_meta.get("unfilled_policy", "reprice_then_market") or "reprice_then_market")
        first_wait = max(1, int(request_meta.get("first_wait_sec", 5) or 5))
        second_wait = max(1, int(request_meta.get("second_wait_sec", 5) or 5))
        self._record_policy_log(cycle_id, account_no, code, policy, "reserve", "scheduled", {"first_wait_sec": first_wait, "second_wait_sec": second_wait})
        self.log_emitted.emit("⏳ 지정가 미체결 정책 예약: {0} / {1} / {2}s".format(account_no, code, first_wait))
        self._emit_trade_state_refresh()
        QTimer.singleShot(first_wait * 1000, lambda cid=cycle_id, acct=account_no, c=code, pol=policy, second=second_wait: self._run_unfilled_policy_stage1(cid, acct, c, pol, second))

    def _run_unfilled_policy_stage1(self, cycle_id, account_no, code, policy, second_wait):
        cycle = self.persistence.fetchone("SELECT * FROM trade_cycles WHERE cycle_id=?", (cycle_id,))
        if not cycle:
            return
        cycle = dict(cycle)
        if str(cycle.get("status") or "") not in ["BUY_PENDING", "BUY_PARTIAL", "BUY_REQUESTED"]:
            self._record_policy_log(cycle_id, account_no, code, policy, "stage1", "skipped_status", {"status": str(cycle.get("status") or "")})
            self._emit_trade_state_refresh()
            return
        open_order = self._find_open_order(account_no, code, cycle)
        if not open_order or int(open_order.get("unfilled_qty") or 0) <= 0:
            self._record_policy_log(cycle_id, account_no, code, policy, "stage1", "completed_before_action", {})
            self._emit_trade_state_refresh()
            return
        self._record_policy_log(cycle_id, account_no, code, policy, "stage1", "pending_confirmed", {"order_no": open_order.get("order_no", ""), "unfilled_qty": int(open_order.get("unfilled_qty") or 0)})
        if policy == "cancel":
            self._request_cancel_open_buy(cycle, open_order)
            return
        if policy == "market":
            self._request_market_switch_open_buy(cycle, open_order)
            return
        if policy in ["reprice", "reprice_then_market"]:
            repriced = self._request_reprice_open_buy(cycle, open_order)
            if repriced:
                self._record_policy_log(cycle_id, account_no, code, policy, "stage1", "second_wait_scheduled", {"second_wait_sec": max(1, int(second_wait))})
                self.log_emitted.emit("⏳ 재호가 후 2차 대기 예약: {0} / {1} / {2}s".format(account_no, code, second_wait))
                self._emit_trade_state_refresh()
                QTimer.singleShot(max(1, int(second_wait)) * 1000, lambda cid=cycle_id, acct=account_no, c=code, pol=policy: self._run_unfilled_policy_stage2(cid, acct, c, pol))
            return

    def _run_unfilled_policy_stage2(self, cycle_id, account_no, code, policy):
        cycle = self.persistence.fetchone("SELECT * FROM trade_cycles WHERE cycle_id=?", (cycle_id,))
        if not cycle:
            return
        cycle = dict(cycle)
        if str(cycle.get("status") or "") not in ["BUY_PENDING", "BUY_PARTIAL", "BUY_REQUESTED", "BUY_REPRICE_REQUESTED"]:
            self._record_policy_log(cycle_id, account_no, code, policy, "stage2", "skipped_status", {"status": str(cycle.get("status") or "")})
            self._emit_trade_state_refresh()
            return
        open_order = self._find_open_order(account_no, code, cycle)
        if not open_order or int(open_order.get("unfilled_qty") or 0) <= 0:
            self._record_policy_log(cycle_id, account_no, code, policy, "stage2", "completed_before_action", {})
            self._emit_trade_state_refresh()
            return
        self._record_policy_log(cycle_id, account_no, code, policy, "stage2", "pending_confirmed", {"order_no": open_order.get("order_no", ""), "unfilled_qty": int(open_order.get("unfilled_qty") or 0)})
        if policy == "reprice_then_market":
            self._request_market_switch_open_buy(cycle, open_order)
        else:
            self._request_cancel_open_buy(cycle, open_order)

    def _find_cycle_for_order(self, account_no, code, order_no=""):
        rows = self.persistence.fetchall(
            """
            SELECT * FROM trade_cycles
            WHERE account_no=? AND code=?
            ORDER BY COALESCE(buy_order_at, entry_detected_at) DESC
            LIMIT 20
            """,
            (account_no, code),
        )
        matched = None
        for row in rows:
            cycle = dict(row)
            status = str(cycle.get("status") or "")
            if status not in [
                'BUY_REQUESTED','BUY_PENDING','BUY_PARTIAL','BUY_REPRICE_REQUESTED','BUY_MARKET_SWITCH_REQUESTED','BUY_CANCEL_REQUESTED'
            ]:
                continue
            try:
                extra = json.loads(cycle.get("extra_json") or '{}')
            except Exception:
                extra = {}
            if order_no and str(extra.get("order_no", "") or "").strip() == str(order_no).strip():
                return cycle
            if matched is None:
                matched = cycle
        return matched

    def manual_cancel_open_buy(self, account_no, code, order_no=""):
        open_order = None
        if order_no:
            row = self.persistence.fetchone(
                "SELECT * FROM open_orders WHERE account_no=? AND code=? AND order_no=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
                (account_no, code, str(order_no)),
            )
            if row:
                open_order = dict(row)
        if not open_order:
            open_order = self._find_open_order(account_no, code)
        cycle = self._find_cycle_for_order(account_no, code, order_no or (open_order or {}).get("order_no", ""))
        if not open_order or not cycle:
            self.log_emitted.emit("⚠️ 수동 취소 대상 미체결 매수 주문을 찾지 못했습니다: {0} / {1}".format(account_no, code))
            return False
        self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "manual", "manual_cancel_requested", {"order_no": str(open_order.get("order_no") or "")})
        return self._request_cancel_open_buy(cycle, open_order)

    def manual_reprice_open_buy(self, account_no, code, order_no=""):
        open_order = None
        if order_no:
            row = self.persistence.fetchone(
                "SELECT * FROM open_orders WHERE account_no=? AND code=? AND order_no=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
                (account_no, code, str(order_no)),
            )
            if row:
                open_order = dict(row)
        if not open_order:
            open_order = self._find_open_order(account_no, code)
        cycle = self._find_cycle_for_order(account_no, code, order_no or (open_order or {}).get("order_no", ""))
        if not open_order or not cycle:
            self.log_emitted.emit("⚠️ 수동 재호가 대상 미체결 매수 주문을 찾지 못했습니다: {0} / {1}".format(account_no, code))
            return False
        self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "manual", "manual_reprice_requested", {"order_no": str(open_order.get("order_no") or "")})
        return self._request_reprice_open_buy(cycle, open_order)

    def manual_market_switch_open_buy(self, account_no, code, order_no=""):
        open_order = None
        if order_no:
            row = self.persistence.fetchone(
                "SELECT * FROM open_orders WHERE account_no=? AND code=? AND order_no=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
                (account_no, code, str(order_no)),
            )
            if row:
                open_order = dict(row)
        if not open_order:
            open_order = self._find_open_order(account_no, code)
        cycle = self._find_cycle_for_order(account_no, code, order_no or (open_order or {}).get("order_no", ""))
        if not open_order or not cycle:
            self.log_emitted.emit("⚠️ 수동 시장가 전환 대상 미체결 매수 주문을 찾지 못했습니다: {0} / {1}".format(account_no, code))
            return False
        self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "manual", "manual_market_switch_requested", {"order_no": str(open_order.get("order_no") or "")})
        return self._request_market_switch_open_buy(cycle, open_order)

    def _find_open_order(self, account_no, code, cycle=None):
        order_no = ""
        if cycle:
            try:
                extra = json.loads(cycle.get("extra_json") or '{}')
                order_no = str(extra.get("order_no", "") or "").strip()
            except Exception:
                order_no = ""
        if order_no:
            row = self.persistence.fetchone(
                "SELECT * FROM open_orders WHERE account_no=? AND code=? AND order_no=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
                (account_no, code, order_no),
            )
            if row:
                return dict(row)
        row = self.persistence.fetchone(
            "SELECT * FROM open_orders WHERE account_no=? AND code=? AND unfilled_qty>0 ORDER BY updated_at DESC LIMIT 1",
            (account_no, code),
        )
        return dict(row) if row else None

    def _request_cancel_open_buy(self, cycle, open_order):
        account_no = str(open_order.get("account_no") or "")
        code = str(open_order.get("code") or "")
        order_no = str(open_order.get("order_no") or "")
        unfilled_qty = int(open_order.get("unfilled_qty") or 0)
        if not account_no or not code or not order_no or unfilled_qty <= 0:
            return False
        if not self._is_regular_market_hours():
            extra_json = self._merge_json(cycle.get("extra_json"), {"after_hours_buy_blocked_at": self.persistence.now_ts(), "after_hours_buy_blocked_action": "cancel"})
            self.persistence.execute("UPDATE trade_cycles SET extra_json=? WHERE cycle_id=?", (extra_json, cycle["cycle_id"]))
            self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "blocked", "cancel_after_hours", {"order_no": order_no, "unfilled_qty": unfilled_qty})
            self.log_emitted.emit("⏸️ 장후에는 미체결 매수 취소도 차단합니다: {0} / {1}".format(account_no, code))
            return False
        ok = self.kiwoom_client.send_order(
            rq_name="BUYCANCEL_%s" % cycle["cycle_id"][-6:],
            screen_no="7002",
            account_no=account_no,
            order_type=3,
            code=code,
            qty=unfilled_qty,
            price=0,
            hoga_gb="00",
            original_order_no=order_no,
        )
        if ok:
            extra_json = self._merge_json(cycle.get("extra_json"), {"unfilled_action": "cancel_requested", "cancel_target_order_no": order_no})
            self.persistence.execute("UPDATE trade_cycles SET status='BUY_CANCEL_REQUESTED', extra_json=? WHERE cycle_id=?", (extra_json, cycle["cycle_id"]))
            self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "action", "cancel_requested", {"order_no": order_no, "unfilled_qty": unfilled_qty})
            self.log_emitted.emit("🧹 미체결 매수 취소 요청: {0} / {1} / {2}".format(account_no, code, unfilled_qty))
            self._emit_trade_state_refresh()
        return ok

    def _request_market_switch_open_buy(self, cycle, open_order):
        account_no = str(open_order.get("account_no") or "")
        code = str(open_order.get("code") or "")
        order_no = str(open_order.get("order_no") or "")
        unfilled_qty = int(open_order.get("unfilled_qty") or 0)
        if not account_no or not code or not order_no or unfilled_qty <= 0:
            return False
        if not self._is_regular_market_hours():
            extra_json = self._merge_json(cycle.get("extra_json"), {"after_hours_buy_blocked_at": self.persistence.now_ts(), "after_hours_buy_blocked_action": "market_switch"})
            self.persistence.execute("UPDATE trade_cycles SET extra_json=? WHERE cycle_id=?", (extra_json, cycle["cycle_id"]))
            self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "blocked", "market_switch_after_hours", {"order_no": order_no, "unfilled_qty": unfilled_qty})
            self.log_emitted.emit("⏸️ 장후에는 미체결 매수 시장가 전환을 차단합니다: {0} / {1}".format(account_no, code))
            return False
        ok = self.kiwoom_client.send_order(
            rq_name="BUYMKT_%s" % cycle["cycle_id"][-6:],
            screen_no="7003",
            account_no=account_no,
            order_type=5,
            code=code,
            qty=unfilled_qty,
            price=0,
            hoga_gb="03",
            original_order_no=order_no,
        )
        if ok:
            extra_json = self._merge_json(cycle.get("extra_json"), {"unfilled_action": "market_switch_requested", "market_switch_target_order_no": order_no})
            self.persistence.execute("UPDATE trade_cycles SET status='BUY_MARKET_SWITCH_REQUESTED', extra_json=? WHERE cycle_id=?", (extra_json, cycle["cycle_id"]))
            self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "action", "market_switch_requested", {"order_no": order_no, "unfilled_qty": unfilled_qty})
            self.log_emitted.emit("🚀 미체결 매수 시장가 전환 요청: {0} / {1} / {2}".format(account_no, code, unfilled_qty))
            self._emit_trade_state_refresh()
        return ok

    def _request_reprice_open_buy(self, cycle, open_order):
        account_no = str(open_order.get("account_no") or "")
        code = str(open_order.get("code") or "")
        order_no = str(open_order.get("order_no") or "")
        unfilled_qty = int(open_order.get("unfilled_qty") or 0)
        if not account_no or not code or not order_no or unfilled_qty <= 0:
            return False
        if not self._is_regular_market_hours():
            extra_json = self._merge_json(cycle.get("extra_json"), {"after_hours_buy_blocked_at": self.persistence.now_ts(), "after_hours_buy_blocked_action": "reprice"})
            self.persistence.execute("UPDATE trade_cycles SET extra_json=? WHERE cycle_id=?", (extra_json, cycle["cycle_id"]))
            self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "blocked", "reprice_after_hours", {"order_no": order_no, "unfilled_qty": unfilled_qty})
            self.log_emitted.emit("⏸️ 장후에는 미체결 매수 재호가를 차단합니다: {0} / {1}".format(account_no, code))
            return False
        profile = None
        for row in self.account_manager.get_active_account_profiles() or []:
            if str(row.get("account_no") or "") == account_no:
                profile = row
                break
        if not profile:
            profile = {"hoga_gb": "00", "limit_price_option": "current_price"}
        symbol_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,)) or {"code": code, "detected_price": 0}
        pricing = self._resolve_order_pricing(profile, symbol_row)
        new_price = int(pricing.get("order_price", 0) or 0)
        if new_price <= 0:
            self.log_emitted.emit("⚠️ 재호가 계산 실패로 정정주문을 생략합니다: {0}".format(code))
            return False
        ok = self.kiwoom_client.send_order(
            rq_name="BUYMOD_%s" % cycle["cycle_id"][-6:],
            screen_no="7004",
            account_no=account_no,
            order_type=5,
            code=code,
            qty=unfilled_qty,
            price=new_price,
            hoga_gb="00",
            original_order_no=order_no,
        )
        if ok:
            extra_json = self._merge_json(cycle.get("extra_json"), {
                "unfilled_action": "reprice_requested",
                "reprice_target_order_no": order_no,
                "reprice_order_price": new_price,
                "reprice_price_source": pricing.get("price_source", ""),
            })
            self.persistence.execute("UPDATE trade_cycles SET status='BUY_REPRICE_REQUESTED', extra_json=? WHERE cycle_id=?", (extra_json, cycle["cycle_id"]))
            self._record_policy_log(cycle["cycle_id"], account_no, code, json.loads(cycle.get("extra_json") or '{}').get("unfilled_policy", ""), "action", "reprice_requested", {"order_no": order_no, "unfilled_qty": unfilled_qty, "new_price": new_price, "price_source": pricing.get("price_source", "")})
            self.log_emitted.emit("🔁 미체결 매수 재호가 요청: {0} / {1} / {2}".format(account_no, code, new_price))
            self._emit_trade_state_refresh()
        return ok

    def _get_tick_size(self, price):
        price = float(price or 0)
        if price < 2000:
            return 1
        if price < 5000:
            return 5
        if price < 20000:
            return 10
        if price < 50000:
            return 50
        if price < 200000:
            return 100
        if price < 500000:
            return 500
        return 1000

    def _resolve_order_pricing(self, profile, symbol_row):
        symbol_row = self._normalize_symbol_row(symbol_row)
        code = symbol_row.get("code", "")
        hoga_gb = str(profile.get("hoga_gb", "03") or "03")
        limit_price_option = str(profile.get("limit_price_option", "current_price") or "current_price")
        try:
            detected_price = float(symbol_row.get("detected_price", 0) or 0)
        except Exception:
            detected_price = 0.0
        reference_price = detected_price if detected_price > 0 else 0.0
        order_price = 0
        price_source = "detected_price" if reference_price > 0 else ""

        realtime_snapshot = self._get_realtime_price_snapshot(code)
        current_snapshot = float(realtime_snapshot.get("current_price", 0.0) or 0.0)
        ask1_snapshot = float(realtime_snapshot.get("ask1", 0.0) or 0.0)
        quote_snapshot = {}
        need_quote_snapshot = (limit_price_option == "ask1") or (current_snapshot <= 0)
        if need_quote_snapshot:
            try:
                quote_snapshot = dict(self.kiwoom_client.request_quote_snapshot(code) or {})
            except Exception:
                quote_snapshot = {}
            if current_snapshot <= 0:
                current_snapshot = float(quote_snapshot.get("current_price", 0.0) or 0.0)
            if ask1_snapshot <= 0:
                ask1_snapshot = float(quote_snapshot.get("ask1", 0.0) or 0.0)

        if hoga_gb == "00":
            if limit_price_option == "ask1":
                if ask1_snapshot > 0:
                    reference_price = ask1_snapshot
                    order_price = int(ask1_snapshot)
                    price_source = "pre_order_ask1_snapshot"
                elif current_snapshot > 0:
                    reference_price = current_snapshot
                    order_price = int(current_snapshot)
                    price_source = "ask1_missing_fallback_current"
                    self.log_emitted.emit("⚠️ 매도1호가를 못 받아 현재가로 지정가 기준을 대체했습니다: {0}".format(code))
            elif limit_price_option == "current_plus_1tick":
                base_price = current_snapshot if current_snapshot > 0 else reference_price
                if base_price > 0:
                    tick = self._get_tick_size(base_price)
                    reference_price = base_price + tick
                    order_price = int(reference_price)
                    price_source = "realtime_current_plus_1tick" if current_snapshot > 0 and realtime_snapshot else "pre_order_current_plus_1tick"
                elif ask1_snapshot > 0:
                    reference_price = ask1_snapshot
                    order_price = int(ask1_snapshot)
                    price_source = "current_plus_1tick_fallback_ask1"
            else:
                if current_snapshot > 0:
                    reference_price = current_snapshot
                    order_price = int(current_snapshot)
                    price_source = "realtime_current_price" if realtime_snapshot else "pre_order_current_snapshot"
                elif reference_price > 0:
                    order_price = int(reference_price)
                    price_source = "detected_price_fallback"
                    self.log_emitted.emit("⚠️ 주문직전 현재가를 못 받아 포착가로 지정가 기준을 대체했습니다: {0}".format(code))
            if order_price <= 0 and reference_price > 0:
                order_price = int(reference_price)
            if not price_source and order_price > 0:
                price_source = "limit_price_fallback"
        else:
            if current_snapshot > 0:
                reference_price = current_snapshot
                price_source = "realtime_market_snapshot" if realtime_snapshot else "pre_order_market_snapshot"
            order_price = 0

        return {
            "hoga_gb": hoga_gb,
            "limit_price_option": limit_price_option,
            "reference_price": reference_price,
            "order_price": order_price,
            "price_source": price_source,
            "current_snapshot": current_snapshot,
            "ask1_snapshot": ask1_snapshot,
            "quote_snapshot": dict(quote_snapshot or {}),
            "realtime_snapshot": dict(realtime_snapshot or {}),
        }

    def _resolve_order_quantity(self, profile, symbol_row, evaluation, pricing):
        code = symbol_row.get("code", "")
        reference_price = float(pricing.get("reference_price", 0) or 0)
        budget_mode = str(profile.get("order_budget_mode", "fixed_amount") or "fixed_amount")
        budget_value = float(profile.get("order_budget_value", 0) or 0)
        orderable_cash = float(profile.get("orderable_cash", 0) or 0)
        sizing_meta = {
            "reference_price": reference_price,
            "order_price": int(pricing.get("order_price", 0) or 0),
            "price_source": pricing.get("price_source", ""),
            "budget_mode": budget_mode,
            "budget_value": budget_value,
            "orderable_cash": orderable_cash,
        }
        if reference_price <= 0:
            self.log_emitted.emit("⚠️ 주문 기준가격을 가져오지 못했습니다: {0}".format(code))
            return 0, sizing_meta
        if budget_mode == "cash_ratio":
            if orderable_cash <= 0:
                self.log_emitted.emit("⚠️ 주문가능금액이 없어 예수금비중 주문을 계산하지 못했습니다: {0}".format(profile.get("account_no", "")))
                return 0, sizing_meta
            budget_amount = orderable_cash * (budget_value / 100.0)
        else:
            budget_amount = budget_value
        if budget_amount <= 0:
            self.log_emitted.emit("⚠️ 주문기준값이 0 이하라 주문을 계산하지 못했습니다: {0}".format(profile.get("account_no", "")))
            return 0, sizing_meta
        qty = int(budget_amount // reference_price)
        sizing_meta["budget_amount"] = budget_amount
        sizing_meta["resolved_qty"] = qty
        return max(1, qty), sizing_meta

    def _merge_json(self, current, extra):
        data = {}
        if isinstance(current, dict):
            data.update(current)
        else:
            try:
                data.update(json.loads(current or '{}'))
            except Exception:
                pass
        data.update(extra or {})
        return json.dumps(data, ensure_ascii=False)

    def _has_recent_buy_rejection(self, account_no, code, now_dt=None):
        cooldown_sec = max(0, int(getattr(self, "buy_reject_retry_cooldown_sec", 0) or 0))
        if cooldown_sec <= 0:
            return False
        row = self.persistence.fetchone(
            """
            SELECT COALESCE(buy_order_at, entry_detected_at) AS rejected_at
            FROM trade_cycles
            WHERE account_no=? AND code=? AND status='BUY_REJECTED'
            ORDER BY COALESCE(buy_order_at, entry_detected_at) DESC
            LIMIT 1
            """,
            (account_no, code),
        )
        if not row or not row["rejected_at"]:
            return False
        try:
            rejected_at = datetime.datetime.strptime(str(row["rejected_at"]), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return False
        now_dt = now_dt or datetime.datetime.now()
        return (now_dt - rejected_at).total_seconds() < cooldown_sec

    def _has_open_position_or_cycle(self, account_no, code):
        row = self.persistence.fetchone(
            "SELECT qty FROM positions WHERE account_no=? AND code=?",
            (account_no, code),
        )
        if row and int(row["qty"] or 0) > 0:
            return True
        row = self.persistence.fetchone(
            "SELECT cycle_id FROM trade_cycles WHERE account_no=? AND code=? AND status IN ('BUY_REQUESTED','BUY_PENDING', 'SIMULATED_HOLDING', 'HOLDING', 'SELL_PENDING') LIMIT 1",
            (account_no, code),
        )
        if row is not None:
            return True
        row = self.persistence.fetchone(
            "SELECT order_no FROM open_orders WHERE account_no=? AND code=? AND unfilled_qty>0 LIMIT 1",
            (account_no, code),
        )
        return row is not None

    def _upsert_simulated_position(self, account_no, symbol_row, evaluation, qty, fill_price, market_metrics=None):
        now = self.persistence.now_ts()
        active_sell_state_json = json.dumps(
            self._build_active_sell_state_from_evaluation(
                evaluation,
                account_no,
                symbol_row.get("code", ""),
                symbol_row.get("last_detected_at") or now,
                now,
                market_metrics=market_metrics,
            ),
            ensure_ascii=False,
        )
        self.persistence.execute(
            """
            INSERT INTO positions (
                account_no, code, name, qty, avg_price, current_price,
                eval_profit, eval_rate, buy_chain_id, active_sell_state_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
            ON CONFLICT(account_no, code) DO UPDATE SET
                name=excluded.name,
                qty=positions.qty + excluded.qty,
                avg_price=CASE WHEN excluded.avg_price > 0 THEN excluded.avg_price ELSE positions.avg_price END,
                current_price=CASE WHEN excluded.current_price > 0 THEN excluded.current_price ELSE positions.current_price END,
                buy_chain_id=excluded.buy_chain_id,
                active_sell_state_json=excluded.active_sell_state_json,
                updated_at=excluded.updated_at
            """,
            (
                account_no,
                symbol_row.get("code", ""),
                symbol_row.get("name", ""),
                qty,
                float(fill_price or 0),
                float(fill_price or 0),
                evaluation.get("entry_source") or evaluation.get("trigger_strategy_type", "buy_chain"),
                active_sell_state_json,
                now,
            ),
        )
        self._refresh_holding_realtime_watch()

    def _on_api_message_received(self, payload):
        self.persistence.write_event("api_message", payload)

    def _on_real_price_received(self, payload):
        if self.strategy_manager is None or self.position_state_manager is None:
            return
        code = str(payload.get("code") or "").strip()
        current_price = float(payload.get("current_price") or 0)
        if not code or current_price <= 0:
            return
        states = self.position_state_manager.update_current_price_for_code(code, current_price)
        if not states:
            return
        for state in states:
            self._evaluate_sell_state(state, self.strategy_manager)
        self.positions_changed.emit()

    def _on_account_cash_received(self, payload):
        account_no = str(payload.get("account_no", "") or "").strip()
        summary = dict(payload.get("summary") or {})
        if not account_no:
            return
        previous_cash = self._get_account_cash_settings(account_no)
        deposit_cash = float(summary.get("deposit_cash") or 0)
        orderable_cash = float(summary.get("orderable_cash") or 0)
        estimated_assets = float(summary.get("estimated_assets") or 0)
        api_total_buy = float(summary.get("api_total_buy") or 0)
        api_total_eval = float(summary.get("api_total_eval") or 0)
        api_total_profit = float(summary.get("api_total_profit") or 0)
        api_realized_profit = float(summary.get("api_realized_profit") or 0)
        if deposit_cash == 0 and previous_cash.get("deposit_cash", 0.0) != 0:
            deposit_cash = float(previous_cash.get("deposit_cash") or 0.0)
        if orderable_cash == 0 and previous_cash.get("orderable_cash", 0.0) != 0:
            orderable_cash = float(previous_cash.get("orderable_cash") or 0.0)
        if estimated_assets <= 0 and previous_cash.get("estimated_assets", 0.0) > 0:
            estimated_assets = float(previous_cash.get("estimated_assets") or 0.0)
        if api_total_buy <= 0 and previous_cash.get("api_total_buy", 0.0) > 0:
            api_total_buy = float(previous_cash.get("api_total_buy") or 0.0)
        if api_total_eval <= 0 and previous_cash.get("api_total_eval", 0.0) > 0:
            api_total_eval = float(previous_cash.get("api_total_eval") or 0.0)
        if api_total_profit == 0 and previous_cash.get("api_total_profit", 0.0) != 0:
            api_total_profit = float(previous_cash.get("api_total_profit") or 0.0)
        if api_realized_profit == 0 and previous_cash.get("api_realized_profit", 0.0) != 0:
            api_realized_profit = float(previous_cash.get("api_realized_profit") or 0.0)
        self.account_manager.set_account_live_settings(
            account_no,
            deposit_cash=deposit_cash,
            orderable_cash=orderable_cash,
            estimated_assets=estimated_assets,
            api_total_buy=api_total_buy,
            api_total_eval=api_total_eval,
            api_total_profit=api_total_profit,
            api_realized_profit=api_realized_profit,
            emit_signal=False,
        )
        self.summaries_changed.emit()
        self.log_emitted.emit(
            "💰 계좌 예수금 반영: {0} / 예수금={1} / 주문가능={2}".format(
                account_no,
                deposit_cash,
                orderable_cash,
            )
        )

    def _on_account_realized_received(self, payload):
        account_no = str(payload.get("account_no", "") or "").strip()
        summary = dict(payload.get("summary") or {})
        if not account_no:
            return
        api_realized_profit = float(summary.get("api_realized_profit") or 0.0)
        self.account_manager.set_account_live_settings(
            account_no,
            api_realized_profit=api_realized_profit,
            emit_signal=False,
        )
        self.save_daily_review_snapshot()
        self._reconcile_today_account_snapshots(account_no)
        self.summaries_changed.emit()
        self.log_emitted.emit(
            "💹 계좌 실현손익 반영: {0} / 실현손익={1} / field={2}".format(
                account_no,
                api_realized_profit,
                str(summary.get("matched_field") or "-"),
            )
        )

    def _on_account_positions_received(self, payload):
        account_no = str(payload.get("account_no", "") or "").strip()
        rows = list(payload.get("rows") or [])
        summary = dict(payload.get("summary") or {})
        if not account_no:
            return
        incoming_codes = set()
        for row in rows:
            code = str(row.get("code", "") or "").strip()
            if not code:
                continue
            incoming_codes.add(code)
            existing = self.persistence.fetchone(
                "SELECT buy_chain_id, active_sell_state_json FROM positions WHERE account_no=? AND code=?",
                (account_no, code),
            )
            buy_chain_id = existing["buy_chain_id"] if existing and existing["buy_chain_id"] else ""
            active_sell_state_json = existing["active_sell_state_json"] if existing and existing["active_sell_state_json"] else "{}"
            self.persistence.execute(
                """
                INSERT INTO positions (
                    account_no, code, name, qty, avg_price, current_price,
                    eval_profit, eval_rate, buy_chain_id, active_sell_state_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_no, code) DO UPDATE SET
                    name=excluded.name,
                    qty=excluded.qty,
                    avg_price=excluded.avg_price,
                    current_price=excluded.current_price,
                    eval_profit=excluded.eval_profit,
                    eval_rate=excluded.eval_rate,
                    updated_at=excluded.updated_at
                """,
                (
                    account_no,
                    code,
                    row.get("name") or code,
                    int(row.get("qty") or 0),
                    float(row.get("avg_price") or 0),
                    float(row.get("current_price") or 0),
                    float(row.get("eval_profit") or 0),
                    float(row.get("eval_rate") or 0),
                    buy_chain_id,
                    active_sell_state_json,
                    self.persistence.now_ts(),
                ),
            )
            persisted_row = self.persistence.fetchone(
                "SELECT * FROM positions WHERE account_no=? AND code=?",
                (account_no, code),
            )
            if persisted_row:
                self._ensure_position_management_cycle(account_no, code, position_row=dict(persisted_row))
        old_rows = self.persistence.fetchall("SELECT code FROM positions WHERE account_no=?", (account_no,))
        removed_codes = []
        for old_row in old_rows:
            code = str(old_row["code"] or "")
            if code and code not in incoming_codes:
                removed_codes.append(code)
        if removed_codes:
            placeholders = ",".join(["?"] * len(removed_codes))
            params = [account_no] + removed_codes
            self.persistence.execute(
                "DELETE FROM positions WHERE account_no=? AND code IN ({0})".format(placeholders),
                tuple(params),
            )
        previous_cash = self._get_account_cash_settings(account_no)
        deposit_cash = float(summary.get("deposit_cash") or 0)
        orderable_cash = float(summary.get("orderable_cash") or 0)
        estimated_assets = float(summary.get("estimated_assets") or 0)
        api_total_buy = float(summary.get("api_total_buy") or 0)
        api_total_eval = float(summary.get("api_total_eval") or 0)
        api_total_profit = float(summary.get("api_total_profit") or 0)
        api_realized_profit = float(summary.get("api_realized_profit") or 0)
        if deposit_cash == 0 and previous_cash.get("deposit_cash", 0.0) != 0:
            deposit_cash = float(previous_cash.get("deposit_cash") or 0.0)
        if orderable_cash == 0 and previous_cash.get("orderable_cash", 0.0) != 0:
            orderable_cash = float(previous_cash.get("orderable_cash") or 0.0)
        if estimated_assets <= 0 and previous_cash.get("estimated_assets", 0.0) > 0:
            estimated_assets = float(previous_cash.get("estimated_assets") or 0.0)
        if api_total_buy <= 0 and previous_cash.get("api_total_buy", 0.0) > 0:
            api_total_buy = float(previous_cash.get("api_total_buy") or 0.0)
        if api_total_eval <= 0 and previous_cash.get("api_total_eval", 0.0) > 0:
            api_total_eval = float(previous_cash.get("api_total_eval") or 0.0)
        if api_total_profit == 0 and previous_cash.get("api_total_profit", 0.0) != 0:
            api_total_profit = float(previous_cash.get("api_total_profit") or 0.0)
        if api_realized_profit == 0 and previous_cash.get("api_realized_profit", 0.0) != 0:
            api_realized_profit = float(previous_cash.get("api_realized_profit") or 0.0)
        self.account_manager.set_account_live_settings(
            account_no,
            deposit_cash=deposit_cash,
            orderable_cash=orderable_cash,
            estimated_assets=estimated_assets,
            api_total_buy=api_total_buy,
            api_total_eval=api_total_eval,
            api_total_profit=api_total_profit,
            api_realized_profit=api_realized_profit,
        )
        self.persistence.write_event("account_positions_sync", payload)
        self._refresh_tracked_symbol_flags(list(incoming_codes.union(set(removed_codes))))
        self.rebuild_daily_summaries()
        self._refresh_holding_realtime_watch()
        self.positions_changed.emit()
        self.summaries_changed.emit()
        self.log_emitted.emit(
            "📊 계좌 잔고 동기화 반영: {0} / {1}건 / 예수금={2} / 주문가능={3}".format(
                account_no,
                len(incoming_codes),
                deposit_cash,
                orderable_cash,
            )
        )

    def _on_outstanding_orders_received(self, payload):
        account_no = str(payload.get("account_no", "") or "").strip()
        rows = list(payload.get("rows") or [])
        summary = dict(payload.get("summary") or {})
        if not account_no:
            return
        before_rows = self.persistence.fetchall("SELECT code FROM open_orders WHERE account_no=?", (account_no,))
        before_codes = set([str(row["code"] or "") for row in before_rows if str(row["code"] or "")])
        self.persistence.execute("DELETE FROM open_orders WHERE account_no=?", (account_no,))
        incoming_codes = set()
        for row in rows:
            unfilled_qty = int(row.get("unfilled_qty") or 0)
            code = str(row.get("code", "") or "").strip()
            if not code or unfilled_qty <= 0:
                continue
            incoming_codes.add(code)
            self.persistence.execute(
                """
                INSERT INTO open_orders (
                    account_no, order_no, code, name, order_status, order_qty,
                    unfilled_qty, filled_qty, order_price, order_gubun, order_time, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_no,
                    str(row.get("order_no") or ""),
                    code,
                    row.get("name") or code,
                    row.get("order_status") or "",
                    int(row.get("order_qty") or 0),
                    unfilled_qty,
                    int(row.get("filled_qty") or 0),
                    float(row.get("order_price") or 0),
                    row.get("order_gubun") or "",
                    row.get("order_time") or "",
                    self.persistence.now_ts(),
                    json.dumps(row, ensure_ascii=False),
                ),
            )
        affected = list(incoming_codes.union(before_codes))
        holding_rows = self.persistence.fetchall(
            "SELECT * FROM positions WHERE account_no=? AND qty>0 ORDER BY code",
            (account_no,),
        )
        holding_map = {
            str(row["code"] or "").strip(): dict(row)
            for row in holding_rows
            if str(row["code"] or "").strip()
        }
        managed_codes = list(set(affected).union(set(holding_map.keys())))
        self.persistence.write_event("outstanding_orders_sync", payload)
        self._refresh_tracked_symbol_flags(affected)
        for code in managed_codes:
            position_row = holding_map.get(code)
            if position_row:
                self._restore_position_take_profit_management(account_no, code, position_row=position_row)
            else:
                self._reconcile_pending_exit_switch(account_no, code)
        self.positions_changed.emit()
        self.trade_cycles_changed.emit()
        self.summaries_changed.emit()
        self.log_emitted.emit("📋 미체결 동기화 반영: {0} / {1}건".format(account_no, len(incoming_codes)))

    def _refresh_tracked_symbol_flags(self, codes):
        codes = [str(code).strip() for code in list(codes or []) if str(code).strip()]
        if not codes:
            return
        now = self.persistence.now_ts()
        for code in codes:
            hold_row = self.persistence.fetchone(
                "SELECT COUNT(*) AS cnt FROM positions WHERE code=? AND qty>0",
                (code,),
            )
            open_row = self.persistence.fetchone(
                "SELECT COUNT(*) AS cnt FROM open_orders WHERE code=? AND unfilled_qty>0",
                (code,),
            )
            is_holding = int(hold_row["cnt"] or 0) > 0
            has_open_order = int(open_row["cnt"] or 0) > 0
            tracked = self.persistence.fetchone("SELECT current_state FROM tracked_symbols WHERE code=?", (code,))
            if not tracked:
                continue
            current_state = tracked["current_state"] or "DETECTED"
            if is_holding:
                new_state = 'HOLDING'
            elif has_open_order:
                new_state = 'BUY_ORDER_PENDING'
            elif current_state in ['HOLDING', 'BUY_ORDER_PENDING']:
                new_state = 'CLOSED'
            else:
                new_state = current_state
            self.persistence.execute(
                "UPDATE tracked_symbols SET is_holding=?, has_open_order=?, current_state=?, updated_at=? WHERE code=?",
                (1 if is_holding else 0, 1 if has_open_order else 0, new_state, now, code),
            )

    def _on_chejan_received(self, payload):
        try:
            gubun = str(payload.get("gubun", "")).strip()
            if gubun == '0':
                self._handle_order_chejan(payload)
            elif gubun == '1':
                self._handle_balance_chejan(payload)
        except Exception as exc:
            self.log_emitted.emit("❌ 체잔 처리 예외: {0}".format(exc))

    def _handle_order_chejan(self, payload):
        account_no = str(payload.get("account_no", "")).strip()
        code = str(payload.get("code", "")).strip()
        if not account_no or not code:
            return
        cycle = self._find_open_cycle(account_no, code)
        qty = self._to_int(payload.get("order_qty"))
        filled_qty = self._to_int(payload.get("fill_qty"))
        unfilled_qty = self._to_int(payload.get("unfilled_qty"))
        filled_price = self._to_price(payload.get("fill_price"))
        is_buy = self._is_buy_payload(payload)
        now = self.persistence.now_ts()

        if cycle:
            extra_json = self._merge_json(cycle["extra_json"], {
                "order_no": payload.get("order_no", ""),
                "latest_chejan": payload,
            })
            new_status = cycle["status"]
            if is_buy:
                if filled_qty > 0:
                    new_status = 'HOLDING' if unfilled_qty <= 0 else 'BUY_PARTIAL'
                    extra_obj = self._load_cycle_extra(cycle)
                    entry_market_metrics = self._build_entry_market_metrics(code, captured_at=now)
                    self._apply_entry_market_metrics(extra_obj, entry_market_metrics)
                    if isinstance(extra_obj.get("active_sell_state"), dict):
                        extra_obj["active_sell_state"]["buy_filled_at"] = now
                        self._apply_entry_market_metrics(extra_obj.get("active_sell_state"), entry_market_metrics)
                    extra_json = json.dumps(extra_obj, ensure_ascii=False)
                    self._apply_buy_fill(account_no, code, payload.get("name") or code, filled_qty, filled_price, cycle, cycle_extra=extra_obj)
                    self.persistence.execute(
                        "UPDATE tracked_symbols SET is_holding=1, has_open_order=?, current_state=?, updated_at=? WHERE code=?",
                        (1 if unfilled_qty > 0 else 0, 'HOLDING' if unfilled_qty <= 0 else 'BUY_ORDER_PENDING', now, code),
                    )
                    self.persistence.execute(
                        "UPDATE trade_cycles SET buy_filled_at=COALESCE(buy_filled_at, ?), status=?, extra_json=? WHERE cycle_id=?",
                        (now, new_status, extra_json, cycle["cycle_id"]),
                    )
                    self.telegram_router.send_formatted_event(
                        "trade_buy_filled",
                        {
                            "channel_group": "trade",
                            "account_no": account_no,
                            "code": code,
                            "name": payload.get("name") or code,
                            "filled_qty": filled_qty,
                            "filled_price": filled_price,
                            "unfilled_qty": unfilled_qty,
                        },
                    )
                    if unfilled_qty <= 0:
                        position_after_fill = self.persistence.fetchone(
                            "SELECT * FROM positions WHERE account_no=? AND code=? AND qty>0",
                            (account_no, code),
                        )
                        if position_after_fill:
                            active_state = dict((extra_obj or {}).get("active_sell_state") or {})
                            self._ensure_take_profit_reservation(
                                dict(position_after_fill),
                                cycle,
                                active_state,
                                list(active_state.get("applied_sell_strategy_nos") or []),
                            )
                else:
                    order_state_text = str(payload.get("order_status") or payload.get("status") or "").strip()
                    if unfilled_qty <= 0 and ("취소" in order_state_text or "거부" in order_state_text):
                        new_status = 'CANCELLED'
                        self.persistence.execute(
                            "UPDATE tracked_symbols SET has_open_order=0, current_state='DETECTED', updated_at=? WHERE code=?",
                            (now, code),
                        )
                    elif '정정' in order_state_text:
                        new_status = 'BUY_REPRICE_REQUESTED'
                        self.persistence.execute(
                            "UPDATE tracked_symbols SET has_open_order=1, current_state='BUY_ORDER_PENDING', updated_at=? WHERE code=?",
                            (now, code),
                        )
                    elif '취소' in order_state_text:
                        new_status = 'BUY_CANCEL_REQUESTED'
                        self.persistence.execute(
                            "UPDATE tracked_symbols SET has_open_order=?, current_state=?, updated_at=? WHERE code=?",
                            (1 if unfilled_qty > 0 else 0, 'BUY_ORDER_PENDING' if unfilled_qty > 0 else 'DETECTED', now, code),
                        )
                    else:
                        new_status = 'BUY_PENDING'
                        self.persistence.execute(
                            "UPDATE tracked_symbols SET has_open_order=1, current_state='BUY_ORDER_PENDING', updated_at=? WHERE code=?",
                            (now, code),
                        )
                    self.persistence.execute(
                        "UPDATE trade_cycles SET status=?, extra_json=? WHERE cycle_id=?",
                        (new_status, extra_json, cycle["cycle_id"]),
                    )
            else:
                extra_obj = self._load_cycle_extra(cycle)
                active_state = self._get_cycle_active_sell_state(cycle=cycle)
                if filled_qty > 0:
                    extra_obj["order_no"] = payload.get("order_no", "")
                    extra_obj["latest_chejan"] = payload
                    exit_market_metrics = self._build_entry_market_metrics(code, captured_at=now)
                    self._apply_exit_market_metrics(extra_obj, exit_market_metrics)
                    self._apply_sell_fill(account_no, code, filled_qty, filled_price, cycle)
                    remaining = self._current_position_qty(account_no, code)
                    active_state = self._update_take_profit_state_from_sell_payload(
                        active_state,
                        payload,
                        filled_qty,
                        unfilled_qty,
                        remaining_qty=remaining,
                    )
                    extra_obj["active_sell_state"] = active_state
                    extra_json = json.dumps(extra_obj, ensure_ascii=False)
                    new_status = 'CLOSED' if remaining <= 0 else 'SELL_PARTIAL'
                    self.persistence.execute(
                        "UPDATE tracked_symbols SET is_holding=?, has_open_order=?, current_state=?, updated_at=? WHERE code=?",
                        (1 if remaining > 0 else 0, 1 if unfilled_qty > 0 else 0, 'HOLDING' if remaining > 0 else 'CLOSED', now, code),
                    )
                    self.persistence.execute(
                        "UPDATE trade_cycles SET sell_filled_at=?, status=?, extra_json=? WHERE cycle_id=?",
                        (now, new_status, extra_json, cycle["cycle_id"]),
                    )
                    if remaining > 0:
                        self._save_position_active_sell_state(account_no, code, active_state)
                else:
                    active_state = self._update_take_profit_state_from_sell_payload(
                        active_state,
                        payload,
                        filled_qty,
                        unfilled_qty,
                    )
                    extra_obj["order_no"] = payload.get("order_no", "")
                    extra_obj["latest_chejan"] = payload
                    extra_obj["active_sell_state"] = active_state
                    extra_json = json.dumps(extra_obj, ensure_ascii=False)
                    self.persistence.execute(
                        "UPDATE trade_cycles SET status='SELL_PENDING', extra_json=? WHERE cycle_id=?",
                        (extra_json, cycle["cycle_id"]),
                    )
                    if self._current_position_qty(account_no, code) > 0:
                        self._save_position_active_sell_state(account_no, code, active_state)
                self._reconcile_pending_exit_switch(account_no, code, cycle=cycle, active_state=active_state)
                if self._current_position_qty(account_no, code) > 0:
                    self._restore_position_take_profit_management(account_no, code, cycle=cycle)
            self.persistence.write_event('chejan_order', payload)
            self.rebuild_daily_summaries()
            self.positions_changed.emit()
            self.trade_cycles_changed.emit()
            self.summaries_changed.emit()

    def _handle_balance_chejan(self, payload):
        account_no = str(payload.get("account_no", "")).strip()
        code = str(payload.get("code", "")).strip()
        if not account_no or not code:
            return
        name = payload.get("name") or code
        qty = self._to_int(payload.get("holding_qty"))
        avg_price = self._to_price(payload.get("avg_price"))
        current_price = self._to_price(payload.get("current_price"))
        eval_rate = self._to_float(payload.get("profit_rate"))
        eval_profit = 0.0
        if qty > 0 and avg_price > 0 and current_price > 0:
            eval_profit = (current_price - avg_price) * qty
            if eval_rate == 0:
                eval_rate = round(((current_price - avg_price) / avg_price) * 100.0, 2)
            self.persistence.execute(
                """
                INSERT INTO positions (
                    account_no, code, name, qty, avg_price, current_price,
                    eval_profit, eval_rate, buy_chain_id, active_sell_state_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '{}', ?)
                ON CONFLICT(account_no, code) DO UPDATE SET
                    name=excluded.name,
                    qty=excluded.qty,
                    avg_price=excluded.avg_price,
                    current_price=excluded.current_price,
                    eval_profit=excluded.eval_profit,
                    eval_rate=excluded.eval_rate,
                    updated_at=excluded.updated_at
                """,
                (account_no, code, name, qty, avg_price, current_price, eval_profit, eval_rate, self.persistence.now_ts()),
            )
            self.persistence.execute(
                "UPDATE tracked_symbols SET is_holding=1, current_state='HOLDING', updated_at=? WHERE code=?",
                (self.persistence.now_ts(), code),
            )
            position_row = self.persistence.fetchone(
                "SELECT * FROM positions WHERE account_no=? AND code=? AND qty>0",
                (account_no, code),
            )
            if position_row:
                cycle = self._ensure_position_management_cycle(account_no, code, position_row=dict(position_row))
                self._restore_position_take_profit_management(
                    account_no,
                    code,
                    cycle=cycle,
                    position_row=dict(position_row),
                )
        else:
            self.persistence.execute("DELETE FROM positions WHERE account_no=? AND code=?", (account_no, code))
            if not self._has_any_position(code):
                self.persistence.execute(
                    "UPDATE tracked_symbols SET is_holding=0, has_open_order=0, current_state='CLOSED', updated_at=? WHERE code=?",
                    (self.persistence.now_ts(), code),
                )
        self.persistence.write_event('chejan_balance', payload)
        self.rebuild_daily_summaries()
        self.positions_changed.emit()
        self.summaries_changed.emit()

    def _find_open_cycle(self, account_no, code):
        row = self.persistence.fetchone(
            """
            SELECT * FROM trade_cycles
            WHERE account_no=? AND code=? AND status IN ('BUY_REQUESTED','BUY_PENDING','BUY_PARTIAL','BUY_REPRICE_REQUESTED','BUY_MARKET_SWITCH_REQUESTED','BUY_CANCEL_REQUESTED','HOLDING','SELL_PENDING','SELL_PARTIAL','SIMULATED_HOLDING')
            ORDER BY COALESCE(buy_order_at, entry_detected_at) DESC
            LIMIT 1
            """,
            (account_no, code),
        )
        return dict(row) if row else None

    def _apply_buy_fill(self, account_no, code, name, filled_qty, filled_price, cycle, cycle_extra=None):
        if filled_qty <= 0:
            return
        row = self.persistence.fetchone("SELECT * FROM positions WHERE account_no=? AND code=?", (account_no, code))
        old_qty = int(row['qty']) if row else 0
        old_avg = float(row['avg_price']) if row else 0.0
        new_qty = old_qty + filled_qty
        if new_qty <= 0:
            return
        if filled_price <= 0:
            filled_price = old_avg
        new_avg = old_avg
        if filled_price > 0:
            new_avg = ((old_qty * old_avg) + (filled_qty * filled_price)) / float(new_qty)
        extra = dict(cycle_extra or self._load_cycle_extra(cycle))
        active_sell_state = dict(extra.get("active_sell_state") or {})
        if active_sell_state:
            active_sell_state["buy_filled_at"] = self.persistence.now_ts()
        active_sell_state_json = json.dumps(active_sell_state, ensure_ascii=False) if active_sell_state else (row['active_sell_state_json'] if row and row['active_sell_state_json'] else '{}')
        buy_chain_id = str(active_sell_state.get("entry_source") or (row['buy_chain_id'] if row and row['buy_chain_id'] else ''))
        self.persistence.execute(
            """
            INSERT INTO positions (
                account_no, code, name, qty, avg_price, current_price,
                eval_profit, eval_rate, buy_chain_id, active_sell_state_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
            ON CONFLICT(account_no, code) DO UPDATE SET
                name=excluded.name,
                qty=?,
                avg_price=?,
                current_price=?,
                buy_chain_id=excluded.buy_chain_id,
                active_sell_state_json=excluded.active_sell_state_json,
                updated_at=excluded.updated_at
            """,
            (account_no, code, name, new_qty, new_avg, filled_price or new_avg, buy_chain_id, active_sell_state_json, self.persistence.now_ts(), new_qty, new_avg, filled_price or new_avg),
        )

    def _apply_sell_fill(self, account_no, code, filled_qty, filled_price, cycle):
        row = self.persistence.fetchone("SELECT * FROM positions WHERE account_no=? AND code=?", (account_no, code))
        if not row:
            return
        old_qty = int(row['qty'] or 0)
        avg_price = float(row['avg_price'] or 0)
        remaining = max(0, old_qty - filled_qty)
        realized = (filled_price - avg_price) * filled_qty if filled_price > 0 else 0.0
        cycle_realized = float(cycle['pnl_realized'] or 0) + realized
        self.persistence.execute(
            "UPDATE trade_cycles SET pnl_realized=? WHERE cycle_id=?",
            (cycle_realized, cycle['cycle_id']),
        )
        if remaining <= 0:
            self.persistence.execute("DELETE FROM positions WHERE account_no=? AND code=?", (account_no, code))
        else:
            self.persistence.execute(
                "UPDATE positions SET qty=?, current_price=?, eval_profit=?, eval_rate=?, updated_at=? WHERE account_no=? AND code=?",
                (remaining, filled_price, (filled_price - avg_price) * remaining, round(((filled_price - avg_price) / avg_price) * 100.0, 2) if avg_price > 0 else 0.0, self.persistence.now_ts(), account_no, code),
            )
        self.telegram_router.send_formatted_event(
            "trade_sell_filled",
            {
                "channel_group": "trade",
                "account_no": account_no,
                "code": code,
                "name": cycle.get("name") or code,
                "filled_qty": filled_qty,
                "filled_price": filled_price,
                "cycle_realized": round(cycle_realized, 2),
            },
        )
        self._refresh_holding_realtime_watch()

    def _current_position_qty(self, account_no, code):
        row = self.persistence.fetchone("SELECT qty FROM positions WHERE account_no=? AND code=?", (account_no, code))
        return int(row['qty'] or 0) if row else 0

    def _has_any_position(self, code):
        row = self.persistence.fetchone("SELECT 1 FROM positions WHERE code=? AND qty>0 LIMIT 1", (code,))
        return row is not None

    def _is_buy_payload(self, payload):
        order_gubun = str(payload.get('order_gubun', '') or '')
        buy_sell_gubun = str(payload.get('buy_sell_gubun', '') or '')
        if '매수' in order_gubun:
            return True
        if '매도' in order_gubun:
            return False
        if buy_sell_gubun == '2':
            return True
        if buy_sell_gubun == '1':
            return False
        hold_buy_sell = str(payload.get('hold_buy_sell', '') or '')
        if hold_buy_sell == '2':
            return True
        return False

    def _to_int(self, value):
        text = str(value or '').replace(',', '').replace('+', '').strip()
        if text == '':
            return 0
        try:
            return int(float(text))
        except Exception:
            return 0

    def _to_price(self, value):
        return abs(float(self._to_int(value)))

    def _to_float(self, value):
        text = str(value or '').replace(',', '').replace('%', '').strip()
        if text == '':
            return 0.0
        try:
            return float(text)
        except Exception:
            return 0.0

    def rebuild_daily_summaries(self):
        trade_date = self.persistence.today_str()
        accounts = self.persistence.fetchall("SELECT account_no FROM accounts ORDER BY account_no")
        for account_row in accounts:
            account_no = account_row["account_no"]
            hold_row = self.persistence.fetchone(
                "SELECT COUNT(*) AS cnt, COALESCE(SUM(eval_profit), 0) AS eval_sum FROM positions WHERE account_no=? AND qty>0",
                (account_no,),
            )
            realized_row = self.persistence.fetchone(
                "SELECT COUNT(*) AS sold_cnt, COALESCE(SUM(pnl_realized), 0) AS realized_sum FROM trade_cycles WHERE trade_date=? AND account_no=? AND status IN ('CLOSED', 'SIMULATED_CLOSED')",
                (trade_date, account_no),
            )
            self.persistence.execute(
                """
                INSERT INTO daily_account_summary (
                    trade_date, account_no, eval_profit_total, realized_profit_total,
                    holding_count, sold_count, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, '{}')
                ON CONFLICT(trade_date, account_no) DO UPDATE SET
                    eval_profit_total=excluded.eval_profit_total,
                    realized_profit_total=excluded.realized_profit_total,
                    holding_count=excluded.holding_count,
                    sold_count=excluded.sold_count
                """,
                (
                    trade_date,
                    account_no,
                    float(hold_row["eval_sum"] or 0),
                    float(realized_row["realized_sum"] or 0),
                    int(hold_row["cnt"] or 0),
                    int(realized_row["sold_cnt"] or 0),
                ),
            )
            self._reconcile_today_account_snapshots(account_no)
        self.save_daily_review_snapshot()
