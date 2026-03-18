# -*- coding: utf-8 -*-
import json


class TradeControlSessionStore(object):
    def __init__(self, persistence):
        self.persistence = persistence

    def get_session(self, user_id, chat_id):
        row = self.persistence.fetchone(
            "SELECT * FROM telegram_trade_sessions WHERE user_id=? AND chat_id=?",
            (str(user_id or ""), str(chat_id or "")),
        )
        if not row:
            return {}
        data = dict(row)
        try:
            data["pending_action_json"] = json.loads(data.get("pending_action_json") or "{}")
        except Exception:
            data["pending_action_json"] = {}
        return data

    def save_session(self, user_id, chat_id, **fields):
        current = self.get_session(user_id, chat_id)
        current.update(fields)
        self.persistence.execute(
            """
            INSERT OR REPLACE INTO telegram_trade_sessions
            (user_id, chat_id, selected_account_no, current_menu, pending_action_json, last_message_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(user_id or ""),
                str(chat_id or ""),
                str(current.get("selected_account_no") or ""),
                str(current.get("current_menu") or ""),
                json.dumps(current.get("pending_action_json") or {}, ensure_ascii=False),
                str(current.get("last_message_id") or ""),
                self.persistence.now_ts(),
            ),
        )

    def get_selected_account(self, user_id, chat_id):
        return str(self.get_session(user_id, chat_id).get("selected_account_no") or "")

    def set_selected_account(self, user_id, chat_id, account_no):
        self.save_session(user_id, chat_id, selected_account_no=str(account_no or ""))

    def set_current_menu(self, user_id, chat_id, current_menu):
        self.save_session(user_id, chat_id, current_menu=str(current_menu or ""))

    def set_pending_action(self, user_id, chat_id, action_type, payload=None):
        self.save_session(
            user_id,
            chat_id,
            pending_action_json={
                "action_type": str(action_type or ""),
                "payload": dict(payload or {}),
            },
        )

    def clear_pending_action(self, user_id, chat_id):
        self.save_session(user_id, chat_id, pending_action_json={})

    def set_last_message_id(self, user_id, chat_id, message_id):
        self.save_session(user_id, chat_id, last_message_id=str(message_id or ""))
