# -*- coding: utf-8 -*-
import json

import requests


class TelegramService(object):
    def __init__(self, timeout=8):
        self.timeout = int(timeout or 8)

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

    def send_message(self, bot_token, chat_id, text, parse_mode="HTML"):
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
        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                return {"ok": False, "message": json.dumps(data, ensure_ascii=False)}
            return {"ok": True, "message": ""}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
