# -*- coding: utf-8 -*-
import json

from PyQt5.QtCore import QObject, pyqtSignal

from app.services.telegram_service import TelegramService
from app.services.telegram_formatter import TelegramFormatter


class TelegramManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, credential_manager, persistence, service=None, formatter=None, parent=None):
        super(TelegramManager, self).__init__(parent)
        self.credential_manager = credential_manager
        self.persistence = persistence
        self.service = service or TelegramService()
        self.formatter = formatter or TelegramFormatter()

    def test_bot_identity(self, bot_token):
        result = self.service.get_me(bot_token)
        if not result.get("ok"):
            self.log_emitted.emit("❌ 텔레그램 봇 확인 실패: {0}".format(result.get("message", "")))
        return result

    def test_chat_delivery(self, bot_token, chat_id, channel_group=""):
        result = self.service.get_chat(bot_token, chat_id)
        if not result.get("ok"):
            self.log_emitted.emit("❌ 텔레그램 채팅방 확인 실패: {0}".format(result.get("message", "")))
        return result

    def send_news_articles(self, code, name, trigger_type, articles):
        payload = {
            "channel_group": "news",
            "code": code,
            "name": name,
            "trigger_type": trigger_type,
            "articles": list(articles or []),
        }
        return self.send_formatted_event("news_articles", payload)

    def send_trade_message(self, title, lines, code=""):
        payload = {
            "channel_group": "trade",
            "title": title,
            "lines": list(lines or []),
            "code": code,
        }
        return self.send_formatted_event("trade_message", payload)

    def send_formatted_event(self, event_type, payload):
        payload = dict(payload or {})
        channel_group = payload.get("channel_group", "trade")
        channels = self.credential_manager.get_telegram_channels(channel_group, include_token=True)
        channels = [row for row in channels if row.get("enabled") and row.get("chat_id") and row.get("bot_token")]
        if not channels:
            self.log_emitted.emit("⚠️ 텔레그램 채널 미설정: {0}".format(channel_group))
            return False
        message = self.formatter.format_event(event_type, payload)
        return self._broadcast(channel_group, channels, event_type, payload.get("code", ""), message)

    def _broadcast(self, channel_group, channels, message_kind, code, message):
        all_ok = True
        for row in channels:
            result = self.service.send_message(row.get("bot_token", ""), row.get("chat_id", ""), message)
            ok = bool(result.get("ok"))
            error_message = result.get("message", "") if not ok else ""
            self.persistence.execute(
                """
                INSERT INTO telegram_send_logs (
                    ts, channel_group, slot_no, target_chat_id, message_kind, related_code,
                    send_status, error_message, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}')
                """,
                (
                    self.persistence.now_ts(),
                    channel_group,
                    int(row.get("slot_no", 0)),
                    row.get("chat_id", ""),
                    message_kind,
                    code,
                    "success" if ok else "error",
                    error_message,
                ),
            )
            if not ok:
                self.log_emitted.emit("❌ 텔레그램 전송 실패: {0}".format(error_message))
                all_ok = False
        return all_ok
