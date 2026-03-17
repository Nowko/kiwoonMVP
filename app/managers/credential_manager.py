# -*- coding: utf-8 -*-
import base64
import json
import os

from PyQt5.QtCore import QObject, pyqtSignal


class CredentialManager(QObject):
    credentials_changed = pyqtSignal()
    log_emitted = pyqtSignal(str)

    def __init__(self, paths, persistence, parent=None):
        super(CredentialManager, self).__init__(parent)
        self.paths = paths
        self.persistence = persistence
        self._data = self._load_file()

    def _default_data(self):
        return {
            "naver_keys": [],
            "telegram": {
                "news": [],
                "trade": [],
            },
            "telegram_settings": {
                "news_send_min_score": 60,
            },
            "ai_apis": [],
            "app_settings": {
                "auto_login_on_startup": False,
            },
        }

    def _normalize_data(self, data):
        normalized = dict(data or {})
        normalized.setdefault("naver_keys", [])
        normalized.setdefault("telegram", {"news": [], "trade": []})
        normalized.setdefault("telegram_settings", {"news_send_min_score": 60})
        normalized.setdefault("ai_apis", [])
        app_settings = normalized.setdefault("app_settings", {})
        if not isinstance(app_settings, dict):
            app_settings = {}
            normalized["app_settings"] = app_settings
        app_settings["auto_login_on_startup"] = bool(app_settings.get("auto_login_on_startup", False))
        return normalized

    def _load_file(self):
        if not os.path.exists(self.paths.credential_path):
            return self._default_data()
        try:
            with open(self.paths.credential_path, "r", encoding="utf-8") as fp:
                return self._normalize_data(json.load(fp))
        except Exception as exc:
            self.log_emitted.emit("⚠️ 자격증명 파일 로드 실패: {0}".format(exc))
            return self._default_data()

    def save(self):
        with open(self.paths.credential_path, "w", encoding="utf-8") as fp:
            json.dump(self._data, fp, ensure_ascii=False, indent=2)
        self.credentials_changed.emit()

    def simple_encrypt(self, value):
        if not value:
            return ""
        return base64.b64encode(value.encode("utf-8")).decode("ascii")

    def simple_decrypt(self, value):
        if not value:
            return ""
        try:
            return base64.b64decode(value.encode("ascii")).decode("utf-8")
        except Exception:
            return ""

    def mask(self, value, keep_head=4, keep_tail=3):
        if not value:
            return ""
        if len(value) <= keep_head + keep_tail:
            return "*" * len(value)
        return value[:keep_head] + "*" * (len(value) - keep_head - keep_tail) + value[-keep_tail:]

    def set_naver_key(self, key_set_id, client_id, client_secret, enabled):
        client_secret_enc = self.simple_encrypt(client_secret)
        data = {
            "key_set_id": int(key_set_id),
            "client_id": client_id,
            "client_secret": client_secret_enc,
            "enabled": bool(enabled),
        }
        naver_keys = [row for row in self._data.get("naver_keys", []) if int(row.get("key_set_id", 0)) != int(key_set_id)]
        naver_keys.append(data)
        naver_keys = sorted(naver_keys, key=lambda x: int(x.get("key_set_id", 0)))
        self._data["naver_keys"] = naver_keys
        self.save()

    def get_naver_keys(self, include_secret=False):
        rows = []
        for row in self._data.get("naver_keys", []):
            item = dict(row)
            if include_secret:
                item["client_secret"] = self.simple_decrypt(item.get("client_secret", ""))
            else:
                item["client_secret"] = self.mask(self.simple_decrypt(item.get("client_secret", "")))
            rows.append(item)
        return rows


    def set_ai_api(self, slot_no, provider, api_key, base_url, model_name, analysis_label, enabled):
        slot_no = int(slot_no)
        ai_rows = [row for row in self._data.get("ai_apis", []) if int(row.get("slot_no", 0)) != slot_no]
        ai_rows.append({
            "slot_no": slot_no,
            "provider": str(provider or "openai").strip() or "openai",
            "api_key": self.simple_encrypt(str(api_key or "")),
            "base_url": str(base_url or "").strip(),
            "model_name": str(model_name or "").strip(),
            "analysis_label": str(analysis_label or "").strip(),
            "enabled": bool(enabled),
        })
        self._data["ai_apis"] = sorted(ai_rows, key=lambda x: int(x.get("slot_no", 0)))
        self.save()

    def get_ai_apis(self, include_key=False):
        rows = []
        for row in self._data.get("ai_apis", []):
            item = dict(row)
            api_key = self.simple_decrypt(item.get("api_key", ""))
            item["api_key"] = api_key if include_key else self.mask(api_key)
            rows.append(item)
        return rows

    def get_active_ai_apis(self, include_key=False):
        rows = []
        for row in self.get_ai_apis(include_key=include_key):
            if bool(row.get("enabled")):
                rows.append(row)
        return rows

    def set_news_send_min_score(self, value):
        try:
            score = int(value)
        except Exception:
            score = 60
        score = max(0, min(100, score))
        settings = self._data.setdefault("telegram_settings", {})
        settings["news_send_min_score"] = score
        self.save()

    def get_news_send_min_score(self):
        settings = self._data.setdefault("telegram_settings", {})
        try:
            score = int(settings.get("news_send_min_score", 60) or 60)
        except Exception:
            score = 60
        return max(0, min(100, score))

    def set_auto_login_on_startup(self, enabled):
        settings = self._data.setdefault("app_settings", {})
        settings["auto_login_on_startup"] = bool(enabled)
        self.save()

    def get_auto_login_on_startup(self):
        settings = self._data.setdefault("app_settings", {})
        return bool(settings.get("auto_login_on_startup", False))

    def set_telegram_channel(self, channel_group, slot_no, bot_token, chat_id, enabled):
        channel_group = str(channel_group)
        slot_no = int(slot_no)
        root = self._data.setdefault("telegram", {})
        rows = root.setdefault(channel_group, [])
        new_rows = [row for row in rows if int(row.get("slot_no", 0)) != slot_no]
        new_rows.append(
            {
                "slot_no": slot_no,
                "bot_token": self.simple_encrypt(bot_token),
                "chat_id": chat_id,
                "enabled": bool(enabled),
            }
        )
        root[channel_group] = sorted(new_rows, key=lambda x: int(x.get("slot_no", 0)))
        self.save()

    def get_telegram_channels(self, channel_group, include_token=False):
        root = self._data.setdefault("telegram", {})
        rows = []
        for row in root.get(channel_group, []):
            item = dict(row)
            token = self.simple_decrypt(item.get("bot_token", ""))
            item["bot_token"] = token if include_token else self.mask(token)
            rows.append(item)
        return rows
