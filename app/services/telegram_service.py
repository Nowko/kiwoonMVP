# -*- coding: utf-8 -*-
import json

import requests


class TelegramService(object):
    def __init__(self, timeout=8):
        self.timeout = int(timeout or 8)

    def _post(self, url, payload):
        response = requests.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_me(self, bot_token):
        bot_token = str(bot_token or '').strip()
        if not bot_token:
            return {"ok": False, "message": "Bot Token 미입력", "bot_name": "", "username": ""}
        url = "https://api.telegram.org/bot{0}/getMe".format(bot_token)
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False), "bot_name": "", "username": ""}
            result = data.get("result", {})
            return {
                "ok": True,
                "message": "봇 확인 성공",
                "bot_name": result.get("first_name", "") or "",
                "username": result.get("username", "") or "",
                "raw": result,
            }
        except Exception as exc:
            return {"ok": False, "message": str(exc), "bot_name": "", "username": ""}

    def get_chat(self, bot_token, chat_id):
        bot_token = str(bot_token or '').strip()
        chat_id = str(chat_id or '').strip()
        if not bot_token:
            return {"ok": False, "message": "Bot Token 미입력", "chat_title": ""}
        if not chat_id:
            return {"ok": False, "message": "Chat ID 미입력", "chat_title": ""}
        url = "https://api.telegram.org/bot{0}/getChat".format(bot_token)
        params = {"chat_id": chat_id}
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False), "chat_title": ""}
            result = data.get("result", {})
            title = result.get("title") or result.get("username") or result.get("first_name") or result.get("id") or ""
            return {"ok": True, "message": "채팅방 연결 확인", "chat_title": str(title or ""), "raw": result}
        except Exception as exc:
            return {"ok": False, "message": str(exc), "chat_title": ""}

    def send_message(self, bot_token, chat_id, text, parse_mode="HTML", reply_markup=None):
        bot_token = str(bot_token or '').strip()
        chat_id = str(chat_id or '').strip()
        if not bot_token or not chat_id:
            return {"ok": False, "message": "missing token/chat_id"}
        url = "https://api.telegram.org/bot{0}/sendMessage".format(bot_token)
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            data = self._post(url, payload)
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False)}
            return {"ok": True, "message": "", "message_id": dict(data.get("result") or {}).get("message_id")}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def edit_message(self, bot_token, chat_id, message_id, text, parse_mode="HTML", reply_markup=None):
        bot_token = str(bot_token or '').strip()
        chat_id = str(chat_id or '').strip()
        if not bot_token or not chat_id or not message_id:
            return {"ok": False, "message": "missing token/chat_id/message_id"}
        url = "https://api.telegram.org/bot{0}/editMessageText".format(bot_token)
        payload = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        try:
            data = self._post(url, payload)
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False)}
            return {"ok": True, "message": ""}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def answer_callback_query(self, bot_token, callback_query_id, text=""):
        bot_token = str(bot_token or '').strip()
        callback_query_id = str(callback_query_id or '').strip()
        if not bot_token or not callback_query_id:
            return {"ok": False, "message": "missing token/callback_query_id"}
        url = "https://api.telegram.org/bot{0}/answerCallbackQuery".format(bot_token)
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = str(text)
        try:
            data = self._post(url, payload)
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False)}
            return {"ok": True, "message": ""}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def get_updates(self, bot_token, offset=None, allowed_updates=None):
        bot_token = str(bot_token or '').strip()
        if not bot_token:
            return {"ok": False, "message": "missing token", "updates": []}
        url = "https://api.telegram.org/bot{0}/getUpdates".format(bot_token)
        payload = {"timeout": 0}
        if offset is not None:
            payload["offset"] = int(offset)
        if allowed_updates:
            payload["allowed_updates"] = list(allowed_updates)
        try:
            data = self._post(url, payload)
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False), "updates": []}
            return {"ok": True, "message": "", "updates": list(data.get("result") or [])}
        except Exception as exc:
            return {"ok": False, "message": str(exc), "updates": []}
