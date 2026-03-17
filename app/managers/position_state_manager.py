# -*- coding: utf-8 -*-
import datetime
import json

from PyQt5.QtCore import QObject, pyqtSignal


class PositionStateManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, realtime_market_state_manager=None, parent=None):
        super(PositionStateManager, self).__init__(parent)
        self.persistence = persistence
        self.realtime_market_state_manager = realtime_market_state_manager

    def _safe_json_loads(self, value, default):
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value or "")
        except Exception:
            return default

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

    def _load_cycle_extra(self, cycle_row):
        if not cycle_row:
            return {}
        return self._safe_json_loads(cycle_row.get("extra_json"), {})

    def _resolve_active_sell_state(self, position_row, cycle_row):
        position_state = self._safe_json_loads(position_row.get("active_sell_state_json"), {})
        cycle_extra = self._load_cycle_extra(cycle_row)
        cycle_state = self._safe_json_loads(cycle_extra.get("active_sell_state"), {})
        if cycle_state and not position_state:
            return dict(cycle_state)
        if position_state and cycle_state:
            merged = dict(cycle_state)
            merged.update(position_state)
            return merged
        return dict(position_state or cycle_state or {})

    def _parse_dt(self, text):
        raw = str(text or "").strip()
        if not raw:
            return None
        try:
            return datetime.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _compute_hold_minutes(self, active_state, cycle_row):
        ts = self._parse_dt(active_state.get("buy_filled_at"))
        if ts is None and cycle_row:
            ts = self._parse_dt(cycle_row.get("buy_filled_at"))
        if ts is None and cycle_row:
            ts = self._parse_dt(cycle_row.get("buy_order_at"))
        if ts is None and cycle_row:
            ts = self._parse_dt(cycle_row.get("entry_detected_at"))
        if ts is None:
            return 0
        delta = datetime.datetime.now() - ts
        return max(0, int(delta.total_seconds() // 60))

    def build_position_state(self, position_row, cycle_row=None):
        position = dict(position_row or {})
        cycle = dict(cycle_row or {}) if cycle_row else None
        active_state = self._resolve_active_sell_state(position, cycle)
        market_state = self.realtime_market_state_manager.get_snapshot(position.get("code")) if self.realtime_market_state_manager is not None else {}
        qty = int(position.get("qty") or 0)
        avg_price = float(position.get("avg_price") or 0.0)
        current_price = float(position.get("current_price") or 0.0)
        if float(market_state.get("current_price") or 0) > 0:
            current_price = float(market_state.get("current_price") or 0.0)
        eval_profit = float(position.get("eval_profit") or 0.0)
        eval_rate = float(position.get("eval_rate") or 0.0)
        if qty > 0 and avg_price > 0 and current_price > 0:
            eval_profit = (current_price - avg_price) * qty
            eval_rate = round(((current_price - avg_price) / avg_price) * 100.0, 2)
        sell_strategy_nos = []
        for value in list(active_state.get("applied_sell_strategy_nos") or []):
            try:
                number = int(value or 0)
            except Exception:
                continue
            if number > 0:
                sell_strategy_nos.append(number)
        return {
            "account_no": str(position.get("account_no") or ""),
            "code": str(position.get("code") or ""),
            "name": str(position.get("name") or position.get("code") or ""),
            "qty": qty,
            "avg_price": avg_price,
            "current_price": current_price,
            "eval_profit": eval_profit,
            "eval_rate": eval_rate,
            "buy_chain_id": str(position.get("buy_chain_id") or ""),
            "updated_at": str(position.get("updated_at") or ""),
            "entry_source": str(active_state.get("entry_source") or ""),
            "entry_slot_no": active_state.get("entry_slot_no"),
            "policy_source": str(active_state.get("policy_source") or ""),
            "trigger_buy_strategy": dict(active_state.get("trigger_buy_strategy") or {}),
            "passed_buy_strategies": list(active_state.get("passed_buy_strategies") or []),
            "buy_strategy_results": list(active_state.get("buy_strategy_results") or []),
            "trigger_buy_strategy_no": int(active_state.get("trigger_buy_strategy_no") or 0),
            "trigger_buy_strategy_id": str(active_state.get("trigger_buy_strategy_id") or ""),
            "trigger_buy_strategy_name": str(active_state.get("trigger_buy_strategy_name") or ""),
            "trigger_buy_strategy_type": str(active_state.get("trigger_buy_strategy_type") or ""),
            "applied_sell_strategy_nos": sell_strategy_nos,
            "buy_expression_items": list(active_state.get("buy_expression_items") or []),
            "news_min_score": int(active_state.get("news_min_score") or 0),
            "news_trade_min_score": int(active_state.get("news_trade_min_score") or 0),
            "detected_at": str(active_state.get("detected_at") or ""),
            "buy_filled_at": str(active_state.get("buy_filled_at") or (cycle.get("buy_filled_at") if cycle else "") or ""),
            "hold_minutes": self._compute_hold_minutes(active_state, cycle),
            "cycle_row": cycle,
            "cycle_id": str((cycle or {}).get("cycle_id") or ""),
            "cycle_status": str((cycle or {}).get("status") or ""),
            "active_sell_state": active_state,
            "position_row": position,
            "market_state": dict(market_state or {}),
        }

    def get_position_state(self, account_no, code):
        row = self.persistence.fetchone(
            "SELECT * FROM positions WHERE account_no=? AND code=?",
            (account_no, code),
        )
        if not row:
            return None
        position = dict(row)
        cycle = self._find_open_cycle(account_no, code)
        return self.build_position_state(position, cycle)

    def get_position_states_for_code(self, code):
        code = str(code or "").strip()
        if not code:
            return []
        rows = self.persistence.fetchall(
            "SELECT * FROM positions WHERE code=? AND qty > 0 ORDER BY account_no, code",
            (code,),
        )
        states = []
        for row in rows:
            position = dict(row)
            cycle = self._find_open_cycle(str(position.get("account_no") or ""), code)
            states.append(self.build_position_state(position, cycle))
        return states

    def update_current_price_for_code(self, code, current_price):
        code = str(code or "").strip()
        try:
            current_price = float(current_price or 0)
        except Exception:
            current_price = 0.0
        if not code or current_price <= 0:
            return []
        rows = self.persistence.fetchall(
            "SELECT account_no, qty, avg_price FROM positions WHERE code=? AND qty > 0",
            (code,),
        )
        if not rows:
            return []
        now = self.persistence.now_ts()
        for row in rows:
            qty = int(row["qty"] or 0)
            avg_price = float(row["avg_price"] or 0)
            eval_profit = (current_price - avg_price) * qty if qty > 0 and avg_price > 0 else 0.0
            eval_rate = round(((current_price - avg_price) / avg_price) * 100.0, 2) if avg_price > 0 else 0.0
            self.persistence.execute(
                "UPDATE positions SET current_price=?, eval_profit=?, eval_rate=?, updated_at=? WHERE account_no=? AND code=?",
                (current_price, eval_profit, eval_rate, now, str(row["account_no"] or ""), code),
            )
        return self.get_position_states_for_code(code)

    def get_active_position_states(self):
        rows = self.persistence.fetchall(
            "SELECT * FROM positions WHERE qty > 0 ORDER BY account_no, code"
        )
        states = []
        for row in rows:
            position = dict(row)
            cycle = self._find_open_cycle(str(position.get("account_no") or ""), str(position.get("code") or ""))
            states.append(self.build_position_state(position, cycle))
        return states
