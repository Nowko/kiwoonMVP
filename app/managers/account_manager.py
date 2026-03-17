# -*- coding: utf-8 -*-
import base64
import json

from PyQt5.QtCore import QObject, pyqtSignal


class AccountActivationManager(QObject):
    accounts_changed = pyqtSignal()
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, parent=None):
        super(AccountActivationManager, self).__init__(parent)
        self.persistence = persistence
        self.activation_mode = "multi_active"
        self.active_accounts = []

    def sync_accounts(self, account_numbers):
        now = self.persistence.now_ts()
        for idx, account_no in enumerate(account_numbers):
            existing = self.persistence.fetchone("SELECT settings_json FROM accounts WHERE account_no=?", (account_no,))
            settings_json = existing["settings_json"] if existing and existing["settings_json"] else json.dumps(self._default_settings(), ensure_ascii=False)
            self.persistence.execute(
                """
                INSERT INTO accounts (account_no, account_name, is_enabled, is_primary, settings_json, created_at, updated_at)
                VALUES (?, ?, 0, ?, ?, ?, ?)
                ON CONFLICT(account_no) DO UPDATE SET
                    account_name=excluded.account_name,
                    settings_json=CASE WHEN accounts.settings_json IS NULL OR accounts.settings_json='' THEN excluded.settings_json ELSE accounts.settings_json END,
                    updated_at=excluded.updated_at
                """,
                (account_no, account_no, 1 if idx == 0 else 0, settings_json, now, now),
            )
        self.accounts_changed.emit()

    def _default_settings(self):
        return {
            "order_budget_mode": "fixed_amount",
            "order_budget_value": 300000.0,
            "hoga_gb": "03",
            "limit_price_option": "current_price",
            "unfilled_policy": "reprice_then_market",
            "first_wait_sec": 5,
            "second_wait_sec": 5,
            "query_password_mode": "api_saved",
            "query_password_enc": "",
            "deposit_cash": 0.0,
            "orderable_cash": 0.0,
            "estimated_assets": 0.0,
        }

    def _encrypt_value(self, value):
        if not value:
            return ""
        try:
            return base64.b64encode(str(value).encode("utf-8")).decode("ascii")
        except Exception:
            return ""

    def _decrypt_value(self, value):
        if not value:
            return ""
        try:
            return base64.b64decode(str(value).encode("ascii")).decode("utf-8")
        except Exception:
            return ""

    def _load_settings(self, raw):
        try:
            data = json.loads(raw or '{}')
        except Exception:
            data = {}
        base = self._default_settings()
        base.update(data)
        return base

    def set_activation_mode(self, mode):
        if mode not in ["single_active", "multi_active"]:
            raise ValueError("invalid mode")
        self.activation_mode = mode
        self.log_emitted.emit("🔄 계좌 활성화 모드: {0}".format(mode))
        self.accounts_changed.emit()

    def set_active_accounts(self, account_numbers):
        account_numbers = list(account_numbers)
        self.active_accounts = account_numbers
        rows = self.persistence.fetchall("SELECT account_no FROM accounts ORDER BY account_no")
        active_set = set(account_numbers)
        for row in rows:
            self.persistence.execute(
                "UPDATE accounts SET is_enabled=?, updated_at=? WHERE account_no=?",
                (1 if row["account_no"] in active_set else 0, self.persistence.now_ts(), row["account_no"]),
            )
        self.log_emitted.emit("✅ 활성 계좌: {0}".format(", ".join(self.active_accounts) if self.active_accounts else "없음"))
        self.accounts_changed.emit()

    def set_account_live_settings(self, account_no, order_budget_mode=None, order_budget_value=None, hoga_gb=None, limit_price_option=None, unfilled_policy=None, first_wait_sec=None, second_wait_sec=None, query_password_mode=None, query_password=None, deposit_cash=None, orderable_cash=None, estimated_assets=None, emit_signal=True):
        row = self.persistence.fetchone("SELECT settings_json FROM accounts WHERE account_no=?", (account_no,))
        if not row:
            return
        settings = self._load_settings(row["settings_json"])
        if order_budget_mode is not None:
            mode = str(order_budget_mode)
            if mode not in ["fixed_amount", "cash_ratio"]:
                mode = "fixed_amount"
            settings["order_budget_mode"] = mode
        if order_budget_value is not None:
            try:
                settings["order_budget_value"] = float(order_budget_value)
            except Exception:
                pass
        if hoga_gb is not None:
            settings["hoga_gb"] = str(hoga_gb)
        if limit_price_option is not None:
            option = str(limit_price_option)
            if option not in ["current_price", "ask1", "current_plus_1tick"]:
                option = "current_price"
            settings["limit_price_option"] = option
        if unfilled_policy is not None:
            policy = str(unfilled_policy)
            if policy not in ["cancel", "reprice", "market", "reprice_then_market"]:
                policy = "reprice_then_market"
            settings["unfilled_policy"] = policy
        if first_wait_sec is not None:
            try:
                settings["first_wait_sec"] = max(1, int(first_wait_sec))
            except Exception:
                pass
        if second_wait_sec is not None:
            try:
                settings["second_wait_sec"] = max(1, int(second_wait_sec))
            except Exception:
                pass
        if query_password_mode is not None:
            mode = str(query_password_mode)
            if mode not in ["api_saved", "program_input"]:
                mode = "api_saved"
            settings["query_password_mode"] = mode
        if query_password is not None:
            settings["query_password_enc"] = self._encrypt_value(str(query_password).strip())
        if deposit_cash is not None:
            try:
                settings["deposit_cash"] = float(deposit_cash)
            except Exception:
                pass
        if orderable_cash is not None:
            try:
                settings["orderable_cash"] = float(orderable_cash)
            except Exception:
                pass
        if estimated_assets is not None:
            try:
                settings["estimated_assets"] = float(estimated_assets)
            except Exception:
                pass
        self.persistence.execute(
            "UPDATE accounts SET settings_json=?, updated_at=? WHERE account_no=?",
            (json.dumps(settings, ensure_ascii=False), self.persistence.now_ts(), account_no),
        )
        if emit_signal:
            self.accounts_changed.emit()

    def get_accounts(self):
        rows = self.persistence.fetchall("SELECT * FROM accounts ORDER BY account_no")
        data = []
        for row in rows:
            item = dict(row)
            settings = self._load_settings(row["settings_json"])
            settings["query_password"] = self._decrypt_value(settings.get("query_password_enc", ""))
            item["settings"] = settings
            data.append(item)
        return data

    def export_account_profile(self):
        rows = self.get_accounts()
        data = []
        for row in rows:
            settings = dict(row.get("settings", {}))
            settings.pop("query_password", None)
            data.append({
                "account_no": row["account_no"],
                "is_enabled": 1 if int(row.get("is_enabled") or 0) else 0,
                "settings": settings,
            })
        return data

    def apply_account_profile(self, rows, emit_signal=True):
        rows = rows or []
        row_map = dict((str(item.get("account_no", "")), item) for item in rows if item.get("account_no"))
        db_rows = self.persistence.fetchall("SELECT account_no FROM accounts ORDER BY account_no")
        active_accounts = []
        now = self.persistence.now_ts()
        for db_row in db_rows:
            account_no = str(db_row["account_no"])
            saved = row_map.get(account_no)
            current = self.persistence.fetchone("SELECT settings_json FROM accounts WHERE account_no=?", (account_no,))
            settings = self._load_settings(current["settings_json"] if current else '{}')
            is_enabled = 0
            if saved:
                saved_settings = dict(saved.get("settings", {}))
                settings.update(saved_settings)
                is_enabled = 1 if int(saved.get("is_enabled", 0) or 0) else 0
            self.persistence.execute(
                "UPDATE accounts SET is_enabled=?, settings_json=?, updated_at=? WHERE account_no=?",
                (is_enabled, json.dumps(settings, ensure_ascii=False), now, account_no),
            )
            if is_enabled:
                active_accounts.append(account_no)
        self.active_accounts = active_accounts
        if emit_signal:
            self.accounts_changed.emit()

    def get_active_account_profiles(self):
        rows = self.persistence.fetchall("SELECT * FROM accounts WHERE is_enabled=1 ORDER BY account_no")
        data = []
        for row in rows:
            settings = self._load_settings(row["settings_json"])
            data.append({
                "account_no": row["account_no"],
                "account_name": row["account_name"] or row["account_no"],
                "order_budget_mode": str(settings.get("order_budget_mode", "fixed_amount") or "fixed_amount"),
                "order_budget_value": float(settings.get("order_budget_value", 300000.0) or 300000.0),
                "hoga_gb": str(settings.get("hoga_gb", "03") or "03"),
                "limit_price_option": str(settings.get("limit_price_option", "current_price") or "current_price"),
                "unfilled_policy": str(settings.get("unfilled_policy", "reprice_then_market") or "reprice_then_market"),
                "first_wait_sec": int(settings.get("first_wait_sec", 5) or 5),
                "second_wait_sec": int(settings.get("second_wait_sec", 5) or 5),
                "query_password_mode": str(settings.get("query_password_mode", "api_saved") or "api_saved"),
                "query_password": self._decrypt_value(settings.get("query_password_enc", "")),
                "deposit_cash": float(settings.get("deposit_cash", 0.0) or 0.0),
                "orderable_cash": float(settings.get("orderable_cash", 0.0) or 0.0),
                "estimated_assets": float(settings.get("estimated_assets", 0.0) or 0.0),
            })
        return data
