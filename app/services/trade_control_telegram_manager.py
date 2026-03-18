# -*- coding: utf-8 -*-
from PyQt5.QtCore import QObject, QTimer, pyqtSignal


class TradeControlTelegramManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, credential_manager, telegram_service, formatter, session_store, action_service, persistence, parent=None):
        super(TradeControlTelegramManager, self).__init__(parent)
        self.credential_manager = credential_manager
        self.telegram_service = telegram_service
        self.formatter = formatter
        self.session_store = session_store
        self.action_service = action_service
        self.persistence = persistence
        self._offset_by_token = {}
        self._polling = False
        self._poll_timer = QTimer(self)
        self._poll_timer.setInterval(3500)
        self._poll_timer.timeout.connect(self._poll_updates)

    def start(self):
        if not self._poll_timer.isActive():
            self._poll_timer.start()

    def stop(self):
        if self._poll_timer.isActive():
            self._poll_timer.stop()

    def _enabled_trade_channels(self):
        rows = []
        for row in list(self.credential_manager.get_telegram_channels("trade", include_token=True) or []):
            token = str(row.get("bot_token") or "").strip()
            chat_id = str(row.get("chat_id") or "").strip()
            if not token or not chat_id or not bool(row.get("enabled")):
                continue
            rows.append({"bot_token": token, "chat_id": chat_id, "slot_no": int(row.get("slot_no", 0) or 0)})
        return rows

    def _poll_updates(self):
        if self._polling:
            return
        self._polling = True
        try:
            token_chat_map = {}
            for row in self._enabled_trade_channels():
                token_chat_map.setdefault(row["bot_token"], set()).add(str(row["chat_id"]))
            for bot_token, allowed_chat_ids in token_chat_map.items():
                result = self.telegram_service.get_updates(
                    bot_token,
                    offset=self._offset_by_token.get(bot_token),
                    allowed_updates=["message", "callback_query"],
                )
                if not result.get("ok"):
                    continue
                for update in list(result.get("updates") or []):
                    update_id = int(update.get("update_id") or 0)
                    self._offset_by_token[bot_token] = update_id + 1
                    self._handle_update(bot_token, update, allowed_chat_ids)
        except Exception as exc:
            self.log_emitted.emit("텔레그램 매매관리 업데이트 오류: {0}".format(exc))
        finally:
            self._polling = False

    def _handle_update(self, bot_token, update, allowed_chat_ids):
        callback = dict(update.get("callback_query") or {})
        if callback:
            message = dict(callback.get("message") or {})
            chat = dict(message.get("chat") or {})
            chat_id = str(chat.get("id") or "")
            if chat_id not in allowed_chat_ids:
                return
            user_id = str(dict(callback.get("from") or {}).get("id") or "")
            callback_data = str(callback.get("data") or "")
            callback_query_id = str(callback.get("id") or "")
            self.handle_callback(
                bot_token,
                callback_data,
                user_id,
                chat_id,
                message_id=message.get("message_id"),
                callback_query_id=callback_query_id,
            )
            return

        message = dict(update.get("message") or {})
        if not message:
            return
        chat_id = str(dict(message.get("chat") or {}).get("id") or "")
        if chat_id not in allowed_chat_ids:
            return
        user_id = str(dict(message.get("from") or {}).get("id") or "")
        text = str(message.get("text") or "").strip()
        if not text:
            return
        self.handle_command(bot_token, text, user_id, chat_id)

    def handle_command(self, bot_token, text, user_id, chat_id):
        command = str(text or "").strip().split()[0].lower()
        if command in ["/start", "/menu"]:
            return self._show_home(bot_token, user_id, chat_id)
        if command == "/status":
            return self._show_status(bot_token, user_id, chat_id)
        if command == "/accounts":
            return self._show_accounts(bot_token, user_id, chat_id)
        if command == "/hold":
            return self._show_holdings(bot_token, user_id, chat_id, self._resolve_selected_account(user_id, chat_id))
        if command == "/open":
            return self._show_open_orders(bot_token, user_id, chat_id, self._resolve_selected_account(user_id, chat_id))
        if command == "/conditions":
            return self._show_conditions(bot_token, user_id, chat_id)
        if command == "/trade":
            return self._show_trade_control(bot_token, user_id, chat_id)
        if command == "/panic":
            return self._show_panic_menu(bot_token, user_id, chat_id)
        return self._show_home(bot_token, user_id, chat_id)

    def handle_callback(self, bot_token, callback_data, user_id, chat_id, message_id=None, callback_query_id=""):
        parts = str(callback_data or "").split("|")
        if len(parts) < 3 or parts[0] != "tc":
            return False
        if callback_query_id:
            self.telegram_service.answer_callback_query(bot_token, callback_query_id)
        return self._dispatch(bot_token, parts[1:], user_id, chat_id, message_id)

    def _dispatch(self, bot_token, parts, user_id, chat_id, message_id=None):
        area = str(parts[0] or "")
        action = str(parts[1] or "") if len(parts) > 1 else ""
        if area == "menu" and action == "home":
            return self._show_home(bot_token, user_id, chat_id, message_id)
        if area == "menu" and action == "status":
            return self._show_status(bot_token, user_id, chat_id, message_id)
        if area == "acct" and action == "list":
            return self._show_accounts(bot_token, user_id, chat_id, message_id)
        if area == "acct" and action == "detail" and len(parts) >= 3:
            return self._show_account_detail(bot_token, user_id, chat_id, parts[2], message_id)
        if area == "acct" and action == "select" and len(parts) >= 3:
            return self._select_account(bot_token, user_id, chat_id, parts[2], message_id)
        if area == "hold" and action == "list":
            account_no = parts[2] if len(parts) >= 3 and str(parts[2] or "").strip() else self._resolve_selected_account(user_id, chat_id)
            return self._show_holdings(bot_token, user_id, chat_id, account_no, message_id)
        if area == "hold" and action == "detail" and len(parts) >= 4:
            return self._show_holding_detail(bot_token, user_id, chat_id, parts[2], parts[3], message_id)
        if area == "open" and action == "list":
            account_no = parts[2] if len(parts) >= 3 and str(parts[2] or "").strip() else self._resolve_selected_account(user_id, chat_id)
            return self._show_open_orders(bot_token, user_id, chat_id, account_no, message_id)
        if area == "open" and action == "detail" and len(parts) >= 4:
            return self._show_open_order_detail(bot_token, user_id, chat_id, parts[2], parts[3], message_id)
        if area == "cond" and action == "list":
            return self._show_conditions(bot_token, user_id, chat_id, message_id)
        if area == "cond" and action == "detail" and len(parts) >= 3:
            return self._show_condition_detail(bot_token, user_id, chat_id, parts[2], message_id)
        if area == "trade" and action == "status":
            return self._show_trade_control(bot_token, user_id, chat_id, message_id)
        if area == "panic" and action == "menu":
            return self._show_panic_menu(bot_token, user_id, chat_id, message_id)
        if area == "confirm" and action:
            return self._confirm_action(bot_token, user_id, chat_id, action, parts[2:], message_id)
        if area == "exec" and action:
            return self._execute_action(bot_token, user_id, chat_id, action, parts[2:], message_id)
        return False

    def _resolve_selected_account(self, user_id, chat_id):
        selected = self.session_store.get_selected_account(user_id, chat_id)
        if selected:
            return selected
        selected = self.action_service.get_default_account_no()
        if selected:
            self.session_store.set_selected_account(user_id, chat_id, selected)
        return selected

    def _show_home(self, bot_token, user_id, chat_id, message_id=None):
        self.session_store.set_current_menu(user_id, chat_id, "home")
        selected = self._resolve_selected_account(user_id, chat_id)
        text, buttons = self.formatter.build_home(selected, self.action_service.get_trade_enabled())
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_status(self, bot_token, user_id, chat_id, message_id=None):
        self.session_store.set_current_menu(user_id, chat_id, "status")
        selected = self._resolve_selected_account(user_id, chat_id)
        text, buttons = self.formatter.build_status(self.action_service.get_overall_status(selected))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_accounts(self, bot_token, user_id, chat_id, message_id=None):
        self.session_store.set_current_menu(user_id, chat_id, "accounts")
        selected = self._resolve_selected_account(user_id, chat_id)
        text, buttons = self.formatter.build_accounts(self.action_service.get_account_summaries(), selected)
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_account_detail(self, bot_token, user_id, chat_id, account_no, message_id=None):
        selected = self._resolve_selected_account(user_id, chat_id)
        row = self.action_service.get_account_detail(account_no)
        text, buttons = self.formatter.build_account_detail(row, is_selected=(str(selected or "") == str(account_no or "")))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _select_account(self, bot_token, user_id, chat_id, account_no, message_id=None):
        self.session_store.set_selected_account(user_id, chat_id, account_no)
        self._log_action(user_id, chat_id, account_no, "select_account", "account", account_no, "success", "선택 계좌를 변경했습니다.")
        return self._show_account_detail(bot_token, user_id, chat_id, account_no, message_id)

    def _show_holdings(self, bot_token, user_id, chat_id, account_no, message_id=None):
        self.session_store.set_current_menu(user_id, chat_id, "holdings")
        text, buttons = self.formatter.build_holdings(account_no, self.action_service.get_holdings(account_no))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_holding_detail(self, bot_token, user_id, chat_id, account_no, code, message_id=None):
        text, buttons = self.formatter.build_holding_detail(self.action_service.get_holding_detail(account_no, code))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_open_orders(self, bot_token, user_id, chat_id, account_no, message_id=None):
        self.session_store.set_current_menu(user_id, chat_id, "open_orders")
        text, buttons = self.formatter.build_open_orders(account_no, self.action_service.get_open_orders(account_no))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_open_order_detail(self, bot_token, user_id, chat_id, account_no, order_no, message_id=None):
        text, buttons = self.formatter.build_open_order_detail(self.action_service.get_open_order_detail(account_no, order_no))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_conditions(self, bot_token, user_id, chat_id, message_id=None):
        self.session_store.set_current_menu(user_id, chat_id, "conditions")
        text, buttons = self.formatter.build_conditions(self.action_service.get_condition_slots())
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_condition_detail(self, bot_token, user_id, chat_id, slot_no, message_id=None):
        text, buttons = self.formatter.build_condition_detail(self.action_service.get_condition_slot_detail(slot_no))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_trade_control(self, bot_token, user_id, chat_id, message_id=None):
        text, buttons = self.formatter.build_trade_control(self.action_service.get_trade_enabled())
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _show_panic_menu(self, bot_token, user_id, chat_id, message_id=None):
        text, buttons = self.formatter.build_panic_menu()
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _confirm_action(self, bot_token, user_id, chat_id, action, parts, message_id=None):
        if action == "trade_on":
            text, buttons = self.formatter.build_confirm("자동매매 ON", "자동매매를 ON 하시겠습니까?", "tc|exec|trade_on")
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action == "trade_off":
            text, buttons = self.formatter.build_confirm("자동매매 OFF", "자동매매를 OFF 하시겠습니까?", "tc|exec|trade_off")
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action == "panic_stop":
            text, buttons = self.formatter.build_confirm("전체 매매 정지", "전체 매매를 정지하시겠습니까?", "tc|exec|panic_stop")
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action == "panic_resume":
            text, buttons = self.formatter.build_confirm("정지 해제", "정지를 해제하시겠습니까?", "tc|exec|panic_resume")
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action in ["open_cancel", "open_reprice", "open_market"] and len(parts) >= 2:
            account_no, order_no = parts[0], parts[1]
            title_map = {
                "open_cancel": "미체결 취소",
                "open_reprice": "미체결 정정",
                "open_market": "시장가 전환",
            }
            text, buttons = self.formatter.build_confirm(
                title_map.get(action, "확인"),
                "계좌 {0}\n주문번호 {1}\n실행하시겠습니까?".format(account_no, order_no),
                "tc|exec|{0}|{1}|{2}".format(action, account_no, order_no),
                "tc|open|detail|{0}|{1}".format(account_no, order_no),
            )
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action == "hold_sellall" and len(parts) >= 2:
            account_no, code = parts[0], parts[1]
            text, buttons = self.formatter.build_confirm(
                "전량 매도",
                "계좌 {0}\n종목 {1}\n전량 매도하시겠습니까?".format(account_no, code),
                "tc|exec|hold_sellall|{0}|{1}".format(account_no, code),
                "tc|hold|detail|{0}|{1}".format(account_no, code),
            )
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action == "cond_toggle" and len(parts) >= 1:
            slot_no = str(parts[0] or "")
            text, buttons = self.formatter.build_confirm(
                "조건식 활성 전환",
                "슬롯 {0}의 활성 상태를 전환하시겠습니까?".format(slot_no),
                "tc|exec|cond_toggle|{0}".format(slot_no),
                "tc|cond|detail|{0}".format(slot_no),
            )
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        if action == "cond_restart" and len(parts) >= 1:
            slot_no = str(parts[0] or "")
            text, buttons = self.formatter.build_confirm(
                "실시간 재등록",
                "슬롯 {0}의 실시간 등록을 다시 요청하시겠습니까?".format(slot_no),
                "tc|exec|cond_restart|{0}".format(slot_no),
                "tc|cond|detail|{0}".format(slot_no),
            )
            return self._render(bot_token, user_id, chat_id, text, buttons, message_id)
        return False

    def _execute_action(self, bot_token, user_id, chat_id, action, parts, message_id=None):
        result = self.action_service.execute_confirmed(action, parts)
        account_no = parts[0] if parts else self._resolve_selected_account(user_id, chat_id)
        self._log_action(
            user_id,
            chat_id,
            account_no,
            action,
            "callback",
            "|".join([str(part or "") for part in parts]),
            "success" if result.get("ok") else "error",
            result.get("message", ""),
        )
        text, buttons = self.formatter.build_result(result.get("message", ""))
        return self._render(bot_token, user_id, chat_id, text, buttons, message_id)

    def _render(self, bot_token, user_id, chat_id, text, buttons, message_id=None):
        if message_id:
            result = self.telegram_service.edit_message(bot_token, chat_id, message_id, text, reply_markup=buttons)
            if result.get("ok"):
                self.session_store.set_last_message_id(user_id, chat_id, message_id)
                return True
        result = self.telegram_service.send_message(bot_token, chat_id, text, reply_markup=buttons)
        if result.get("ok"):
            if result.get("message_id"):
                self.session_store.set_last_message_id(user_id, chat_id, result.get("message_id"))
            return True
        self.log_emitted.emit("텔레그램 매매관리 메시지 전송 실패: {0}".format(result.get("message", "")))
        return False

    def _log_action(self, user_id, chat_id, account_no, action_type, target_type, target_value, result, message):
        self.persistence.execute(
            """
            INSERT INTO telegram_trade_action_logs (
                ts, user_id, chat_id, account_no, action_type, target_type, target_value, result, message, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
            """,
            (
                self.persistence.now_ts(),
                str(user_id or ""),
                str(chat_id or ""),
                str(account_no or ""),
                str(action_type or ""),
                str(target_type or ""),
                str(target_value or ""),
                str(result or ""),
                str(message or ""),
            ),
        )
