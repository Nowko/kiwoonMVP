# -*- coding: utf-8 -*-
import json


class TradeControlActionService(object):
    def __init__(self, persistence, account_manager, order_manager, condition_manager, strategy_manager, main_window=None):
        self.persistence = persistence
        self.account_manager = account_manager
        self.order_manager = order_manager
        self.condition_manager = condition_manager
        self.strategy_manager = strategy_manager
        self.main_window = main_window

    def set_main_window(self, main_window):
        self.main_window = main_window

    def get_default_account_no(self):
        rows = list(self.account_manager.get_accounts() or [])
        for row in rows:
            if int(row.get("is_enabled") or 0):
                return str(row.get("account_no") or "")
        return str(rows[0].get("account_no") or "") if rows else ""

    def get_trade_enabled(self):
        return str(self.order_manager.execution_mode or "live") == "live"

    def set_trade_enabled(self, enabled):
        mode = "live" if enabled else "simulated"
        self.order_manager.set_execution_mode(mode)
        if self.main_window is not None and hasattr(self.main_window, "set_trade_enabled"):
            self.main_window.set_trade_enabled(bool(enabled))
        return {"ok": True, "message": "자동매매를 {0} 했습니다.".format("ON" if enabled else "OFF")}

    def panic_stop(self):
        result = self.set_trade_enabled(False)
        if self.main_window is not None and hasattr(self.main_window, "stop_all_condition_realtime"):
            self.main_window.stop_all_condition_realtime()
        result["message"] = "전체 매매를 정지했습니다."
        return result

    def resume_from_panic(self):
        result = self.set_trade_enabled(True)
        if self.main_window is not None and hasattr(self.main_window, "resume_enabled_condition_realtime"):
            self.main_window.resume_enabled_condition_realtime()
        result["message"] = "정지를 해제했습니다."
        return result

    def get_account_summaries(self):
        rows = []
        for row in list(self.account_manager.get_accounts() or []):
            settings = dict(row.get("settings") or {})
            rows.append({
                "account_no": str(row.get("account_no") or ""),
                "estimated_assets_text": self._fmt_num(settings.get("estimated_assets", 0.0)),
                "deposit_cash_text": self._fmt_num(settings.get("deposit_cash", 0.0), signed=True),
                "orderable_cash_text": self._fmt_num(settings.get("orderable_cash", 0.0), signed=True),
                "total_profit_text": self._fmt_num(settings.get("api_total_profit", 0.0), signed=True),
                "realized_profit_text": self._fmt_num(settings.get("api_realized_profit", 0.0), signed=True),
            })
        return rows

    def get_account_detail(self, account_no):
        account_no = str(account_no or "").strip()
        open_order_count = len(self.get_open_orders(account_no))
        holding_count = len(list(self.persistence.fetchall("SELECT code FROM positions WHERE account_no=? AND qty > 0", (account_no,)) or []))
        for row in list(self.account_manager.get_accounts() or []):
            if str(row.get("account_no") or "").strip() != account_no:
                continue
            settings = dict(row.get("settings") or {})
            return {
                "account_no": account_no,
                "estimated_assets_text": self._fmt_num(settings.get("estimated_assets", 0.0)),
                "deposit_cash_text": self._fmt_num(settings.get("deposit_cash", 0.0), signed=True),
                "orderable_cash_text": self._fmt_num(settings.get("orderable_cash", 0.0), signed=True),
                "total_profit_text": self._fmt_num(settings.get("api_total_profit", 0.0), signed=True),
                "realized_profit_text": self._fmt_num(settings.get("api_realized_profit", 0.0), signed=True),
                "holding_count": holding_count,
                "open_order_count": open_order_count,
            }
        return {
            "account_no": account_no,
            "estimated_assets_text": "-",
            "deposit_cash_text": "-",
            "orderable_cash_text": "-",
            "total_profit_text": "-",
            "realized_profit_text": "-",
            "holding_count": holding_count,
            "open_order_count": open_order_count,
        }

    def get_overall_status(self, selected_account_no=None):
        account_no = str(selected_account_no or "").strip() or self.get_default_account_no()
        row = self.get_account_detail(account_no)
        return {
            "selected_account_no": account_no or "-",
            "trade_enabled": "ON" if self.get_trade_enabled() else "OFF",
            "estimated_assets": row.get("estimated_assets_text", "-"),
            "deposit_cash": row.get("deposit_cash_text", "-"),
            "orderable_cash": row.get("orderable_cash_text", "-"),
            "total_profit": row.get("total_profit_text", "-"),
            "realized_profit": row.get("realized_profit_text", "-"),
            "holding_count": row.get("holding_count", 0),
            "open_order_count": row.get("open_order_count", 0),
        }

    def get_holdings(self, account_no):
        account_no = str(account_no or "").strip()
        rows = list(
            self.persistence.fetchall(
                """
                SELECT * FROM positions
                WHERE account_no=? AND qty > 0
                ORDER BY eval_profit DESC, code
                """,
                (account_no,),
            )
            or []
        )
        result = []
        for row in rows:
            item = dict(row)
            result.append({
                "account_no": account_no,
                "code": str(item.get("code") or ""),
                "name": str(item.get("name") or item.get("code") or ""),
                "qty": int(item.get("qty") or 0),
                "avg_price_text": self._fmt_num(item.get("avg_price") or 0.0),
                "current_price_text": self._fmt_num(item.get("current_price") or 0.0),
                "eval_profit_text": self._fmt_num(item.get("eval_profit") or 0.0, signed=True),
                "eval_rate_text": self._fmt_rate(item.get("eval_rate") or 0.0),
            })
        return result

    def get_holding_detail(self, account_no, code):
        account_no = str(account_no or "").strip()
        code = str(code or "").strip()
        row = self.persistence.fetchone(
            "SELECT * FROM positions WHERE account_no=? AND code=? AND qty > 0",
            (account_no, code),
        )
        if not row:
            return {
                "account_no": account_no,
                "code": code,
                "name": "-",
                "qty": 0,
                "avg_price_text": "-",
                "current_price_text": "-",
                "eval_profit_text": "-",
                "eval_rate_text": "-",
                "buy_strategy_text": "-",
                "sell_strategy_text": "-",
            }
        item = dict(row)
        cycle = self.persistence.fetchone(
            """
            SELECT * FROM trade_cycles
            WHERE account_no=? AND code=?
              AND status NOT IN ('CLOSED', 'SIMULATED_CLOSED')
            ORDER BY COALESCE(buy_filled_at, buy_order_at, entry_detected_at) DESC
            LIMIT 1
            """,
            (account_no, code),
        )
        buy_strategy_text = "-"
        sell_strategy_text = "-"
        if cycle:
            extra = self._safe_json(cycle["extra_json"])
            trigger_buy = dict(extra.get("trigger_buy_strategy") or {})
            if trigger_buy:
                buy_strategy_text = self._strategy_name("buy", trigger_buy.get("strategy_no"), trigger_buy.get("strategy_name"))
            applied_sell = list(extra.get("applied_sell_strategy_nos") or [])
            if applied_sell:
                sell_strategy_text = " OR ".join([self._strategy_name("sell", no) for no in applied_sell])
        if sell_strategy_text == "-":
            state_json = self._safe_json(item.get("active_sell_state_json"))
            applied_sell = list(state_json.get("applied_sell_strategy_nos") or [])
            if applied_sell:
                sell_strategy_text = " OR ".join([self._strategy_name("sell", no) for no in applied_sell])
        return {
            "account_no": account_no,
            "code": code,
            "name": str(item.get("name") or code),
            "qty": int(item.get("qty") or 0),
            "avg_price_text": self._fmt_num(item.get("avg_price") or 0.0),
            "current_price_text": self._fmt_num(item.get("current_price") or 0.0),
            "eval_profit_text": self._fmt_num(item.get("eval_profit") or 0.0, signed=True),
            "eval_rate_text": self._fmt_rate(item.get("eval_rate") or 0.0),
            "buy_strategy_text": buy_strategy_text,
            "sell_strategy_text": sell_strategy_text,
        }

    def get_condition_slots(self):
        rows = []
        for row in list(self.condition_manager.get_slots() or []):
            item = dict(row)
            slot_no = int(item.get("slot_no") or 0)
            policy = self.strategy_manager.resolve_slot_strategy_policy(slot_no)
            rows.append({
                "slot_no": slot_no,
                "condition_name": str(item.get("condition_name") or "미지정"),
                "is_enabled": bool(int(item.get("is_enabled") or 0)),
                "is_realtime": bool(int(item.get("is_realtime") or 0)),
                "current_count": int(item.get("current_count") or 0),
                "buy_strategy_text": self._format_buy_expression(policy.get("buy_expression_json")),
                "sell_strategy_text": self._format_sell_strategy_list(policy.get("sell_strategy_nos_json")),
            })
        return rows

    def get_condition_slot_detail(self, slot_no):
        slot_no = int(slot_no or 0)
        for row in self.get_condition_slots():
            if int(row.get("slot_no") or 0) == slot_no:
                row["enabled_text"] = "Y" if row.get("is_enabled") else "N"
                row["realtime_text"] = "Y" if row.get("is_realtime") else "N"
                return row
        return {
            "slot_no": slot_no,
            "condition_name": "미지정",
            "enabled_text": "N",
            "realtime_text": "N",
            "current_count": 0,
            "buy_strategy_text": "-",
            "sell_strategy_text": "-",
        }

    def get_open_orders(self, account_no):
        account_no = str(account_no or "").strip()
        rows = list(
            self.persistence.fetchall(
                "SELECT * FROM open_orders WHERE account_no=? AND unfilled_qty > 0 ORDER BY updated_at DESC, order_no DESC",
                (account_no,),
            )
            or []
        )
        result = []
        for row in rows:
            item = dict(row)
            result.append({
                "account_no": str(item.get("account_no") or ""),
                "order_no": str(item.get("order_no") or ""),
                "name": str(item.get("name") or item.get("code") or ""),
                "code": str(item.get("code") or ""),
                "order_type": str(item.get("order_gubun") or item.get("order_status") or ""),
                "order_qty": int(item.get("order_qty") or 0),
                "unfilled_qty": int(item.get("unfilled_qty") or 0),
                "order_price": self._fmt_num(item.get("order_price") or 0.0),
                "order_status": str(item.get("order_status") or ""),
            })
        return result

    def get_open_order_detail(self, account_no, order_no):
        for row in self.get_open_orders(account_no):
            if str(row.get("order_no") or "") == str(order_no or ""):
                return row
        return {
            "account_no": str(account_no or ""),
            "order_no": str(order_no or ""),
            "name": "-",
            "code": "-",
            "order_type": "-",
            "order_qty": 0,
            "unfilled_qty": 0,
            "order_price": "-",
            "order_status": "-",
        }

    def cancel_open_order(self, account_no, order_no):
        detail = self.get_open_order_detail(account_no, order_no)
        ok = self.order_manager.manual_cancel_open_buy(account_no, detail.get("code"), order_no)
        return {"ok": bool(ok), "message": "미체결 취소 요청을 전송했습니다." if ok else "미체결 취소 요청에 실패했습니다."}

    def reprice_open_order(self, account_no, order_no):
        detail = self.get_open_order_detail(account_no, order_no)
        ok = self.order_manager.manual_reprice_open_buy(account_no, detail.get("code"), order_no)
        return {"ok": bool(ok), "message": "미체결 정정 요청을 전송했습니다." if ok else "미체결 정정 요청에 실패했습니다."}

    def market_switch_open_order(self, account_no, order_no):
        detail = self.get_open_order_detail(account_no, order_no)
        ok = self.order_manager.manual_market_switch_open_buy(account_no, detail.get("code"), order_no)
        return {"ok": bool(ok), "message": "시장가 전환 요청을 전송했습니다." if ok else "시장가 전환 요청에 실패했습니다."}

    def sell_all_position(self, account_no, code):
        ok = self.order_manager.manual_sell_position(account_no, code)
        return {"ok": bool(ok), "message": "전량 매도 요청을 전송했습니다." if ok else "전량 매도 요청에 실패했습니다."}

    def toggle_condition_slot(self, slot_no):
        slot_no = int(slot_no or 0)
        detail = self.get_condition_slot_detail(slot_no)
        enabled = not bool(detail.get("enabled_text") == "Y")
        realtime = enabled and detail.get("condition_name") != "미지정"
        self.condition_manager.set_slot_enabled(slot_no, enabled, realtime)
        if not enabled:
            try:
                self.condition_manager.stop_realtime_slot(slot_no)
            except Exception:
                pass
        return {
            "ok": True,
            "message": "슬롯 {0}을 {1}했습니다.".format(slot_no, "활성" if enabled else "비활성"),
        }

    def restart_condition_slot(self, slot_no):
        slot_no = int(slot_no or 0)
        detail = self.get_condition_slot_detail(slot_no)
        if detail.get("condition_name") == "미지정":
            return {"ok": False, "message": "조건식이 지정되지 않은 슬롯입니다."}
        try:
            self.condition_manager.stop_realtime_slot(slot_no)
        except Exception:
            pass
        ok = self.condition_manager.start_realtime_slot(slot_no)
        return {
            "ok": bool(ok),
            "message": "슬롯 {0} 실시간 재등록을 요청했습니다.".format(slot_no) if ok else "실시간 재등록 요청에 실패했습니다.",
        }

    def execute_confirmed(self, action, parts):
        if action == "trade_on":
            return self.set_trade_enabled(True)
        if action == "trade_off":
            return self.set_trade_enabled(False)
        if action == "panic_stop":
            return self.panic_stop()
        if action == "panic_resume":
            return self.resume_from_panic()
        if action == "open_cancel" and len(parts) >= 2:
            return self.cancel_open_order(parts[0], parts[1])
        if action == "open_reprice" and len(parts) >= 2:
            return self.reprice_open_order(parts[0], parts[1])
        if action == "open_market" and len(parts) >= 2:
            return self.market_switch_open_order(parts[0], parts[1])
        if action == "hold_sellall" and len(parts) >= 2:
            return self.sell_all_position(parts[0], parts[1])
        if action == "cond_toggle" and len(parts) >= 1:
            return self.toggle_condition_slot(parts[0])
        if action == "cond_restart" and len(parts) >= 1:
            return self.restart_condition_slot(parts[0])
        return {"ok": False, "message": "지원하지 않는 요청입니다."}

    def _fmt_num(self, value, signed=False):
        try:
            number = float(value or 0.0)
        except Exception:
            return "-"
        return "{0:+,.0f}".format(number) if signed else "{0:,.0f}".format(number)

    def _fmt_rate(self, value):
        try:
            number = float(value or 0.0)
        except Exception:
            return "-"
        return "{0:+.2f}%".format(number)

    def _safe_json(self, value):
        try:
            return json.loads(value or "{}")
        except Exception:
            return {}

    def _strategy_name(self, kind, strategy_no, fallback_name=""):
        try:
            no = int(strategy_no or 0)
        except Exception:
            no = 0
        if no > 0:
            row = self.strategy_manager.get_strategy_by_no(kind, no)
            if row:
                return "[{0}] {1}".format(no, row["strategy_name"] or row["strategy_type"] or no)
        fallback_name = str(fallback_name or "").strip()
        return fallback_name or "-"

    def _format_buy_expression(self, expression_json):
        try:
            items = json.loads(expression_json or "[]")
        except Exception:
            items = []
        rendered = []
        for item in list(items or []):
            if str(item.get("kind") or "").lower() == "strategy":
                rendered.append(self._strategy_name("buy", item.get("no")))
            elif str(item.get("kind") or "").lower() == "op":
                rendered.append(str(item.get("value") or "").upper())
        return " ".join([part for part in rendered if part]) or "-"

    def _format_sell_strategy_list(self, sell_json):
        try:
            strategy_nos = json.loads(sell_json or "[]")
        except Exception:
            strategy_nos = []
        names = [self._strategy_name("sell", no) for no in list(strategy_nos or [])]
        names = [name for name in names if name and name != "-"]
        return " OR ".join(names) if names else "-"
