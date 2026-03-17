# -*- coding: utf-8 -*-
from app.services.telegram_manager import TelegramManager


class TelegramRouter(TelegramManager):
    """Backward-compatible wrapper. New code should use app.services.telegram_manager.TelegramManager."""
    pass
