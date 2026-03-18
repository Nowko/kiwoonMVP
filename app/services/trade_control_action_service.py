# -*- coding: utf-8 -*-


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

    def get_open_orders(self, account_no):
        account_no = str(account_no or "").strip()
        rows = list(self.persistence.fetchall("SELECT * FROM open_orders WHERE account_no=? AND unfilled_qty > 0 ORDER BY updated_at DESC, order_no DESC", (account_no,)) or [])
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
        return {"ok": False, "message": "지원하지 않는 요청입니다."}

    def _fmt_num(self, value, signed=False):
        try:
            number = float(value or 0.0)
        except Exception:
            return "-"
        return "{0:+,.0f}".format(number) if signed else "{0:,.0f}".format(number)
