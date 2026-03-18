# -*- coding: utf-8 -*-


class TradeControlTelegramFormatter(object):
    def _btn(self, text, callback_data):
        return {"text": str(text or ""), "callback_data": str(callback_data or "")}

    def _markup(self, rows):
        return {"inline_keyboard": rows}

    def build_home(self, selected_account_no, trade_enabled):
        text = (
            "[매매 관리자]\n\n"
            "현재 선택 계좌: {0}\n"
            "자동매매: {1}\n\n"
            "원하는 메뉴를 선택하세요."
        ).format(selected_account_no or "-", "ON" if trade_enabled else "OFF")
        return text, self._markup([
            [self._btn("운영 현황", "tc|menu|status"), self._btn("계좌 선택", "tc|acct|list")],
            [self._btn("보유 종목", "tc|hold|list|{0}".format(selected_account_no or "")), self._btn("조건식 관리", "tc|cond|list")],
            [self._btn("미체결 관리", "tc|open|list|{0}".format(selected_account_no or ""))],
            [self._btn("자동매매 제어", "tc|trade|status"), self._btn("긴급 정지", "tc|panic|menu")],
        ])

    def build_status(self, summary):
        text = (
            "[운영 현황]\n\n"
            "현재 선택 계좌: {selected_account_no}\n"
            "자동매매: {trade_enabled}\n\n"
            "추정자산: {estimated_assets}\n"
            "예수금: {deposit_cash}\n"
            "주문가능금액: {orderable_cash}\n"
            "총손익: {total_profit}\n"
            "실현손익: {realized_profit}\n\n"
            "보유종목: {holding_count}\n"
            "미체결: {open_order_count}"
        ).format(**summary)
        return text, self._markup([
            [self._btn("전체 계좌 보기", "tc|acct|list"), self._btn("보유 종목", "tc|hold|list|{0}".format(summary.get("selected_account_no") or ""))],
            [self._btn("미체결 보기", "tc|open|list|{0}".format(summary.get("selected_account_no") or "")), self._btn("조건식 관리", "tc|cond|list")],
            [self._btn("자동매매 제어", "tc|trade|status"), self._btn("메인 메뉴", "tc|menu|home")],
        ])

    def build_accounts(self, rows, selected_account_no):
        lines = ["[전체 계좌 요약]", ""]
        buttons = []
        for row in rows:
            account_no = str(row.get("account_no") or "-")
            selected = " | 선택중" if account_no == str(selected_account_no or "") else ""
            lines.append(
                "{0}{1}\n추정자산 {2} / 예수금 {3}\n주문가능 {4} / 총손익 {5} / 실현 {6}\n".format(
                    account_no,
                    selected,
                    row.get("estimated_assets_text", "-"),
                    row.get("deposit_cash_text", "-"),
                    row.get("orderable_cash_text", "-"),
                    row.get("total_profit_text", "-"),
                    row.get("realized_profit_text", "-"),
                )
            )
            buttons.append([self._btn("{0}{1}".format(account_no, selected), "tc|acct|detail|{0}".format(account_no))])
        buttons.append([self._btn("메인 메뉴", "tc|menu|home")])
        return "\n".join(lines), self._markup(buttons)

    def build_account_detail(self, row, is_selected=False):
        text = (
            "[계좌 상세]\n\n"
            "계좌: {account_no}\n"
            "추정자산: {estimated_assets_text}\n"
            "예수금: {deposit_cash_text}\n"
            "주문가능금액: {orderable_cash_text}\n"
            "총손익: {total_profit_text}\n"
            "실현손익: {realized_profit_text}\n"
            "보유종목: {holding_count}\n"
            "미체결: {open_order_count}\n"
            "현재 선택 계좌: {selected_flag}"
        ).format(selected_flag="예" if is_selected else "아니오", **row)
        return text, self._markup([
            [self._btn("이 계좌 선택", "tc|acct|select|{0}".format(row.get("account_no") or ""))],
            [self._btn("보유 종목 보기", "tc|hold|list|{0}".format(row.get("account_no") or "")), self._btn("미체결 보기", "tc|open|list|{0}".format(row.get("account_no") or ""))],
            [self._btn("계좌 목록", "tc|acct|list"), self._btn("메인 메뉴", "tc|menu|home")],
        ])

    def build_holdings(self, account_no, rows):
        lines = ["[보유 종목]", "", "계좌: {0}".format(account_no or "-"), ""]
        buttons = []
        if not rows:
            lines.append("보유 종목이 없습니다.")
        for row in rows:
            label = "{0} | {1}".format(
                row.get("name") or row.get("code") or "-",
                row.get("eval_profit_text") or "-",
            )
            buttons.append([self._btn(label, "tc|hold|detail|{0}|{1}".format(account_no, row.get("code") or ""))])
        buttons.append([self._btn("메인 메뉴", "tc|menu|home")])
        return "\n".join(lines), self._markup(buttons)

    def build_holding_detail(self, row):
        text = (
            "[보유 종목 상세]\n\n"
            "종목: {name} ({code})\n"
            "수량: {qty}\n"
            "평균가: {avg_price_text}\n"
            "현재가: {current_price_text}\n"
            "평가손익: {eval_profit_text}\n"
            "수익률: {eval_rate_text}\n"
            "매수전략: {buy_strategy_text}\n"
            "매도전략: {sell_strategy_text}"
        ).format(**row)
        account_no = row.get("account_no") or ""
        code = row.get("code") or ""
        return text, self._markup([
            [self._btn("전량 매도", "tc|confirm|hold_sellall|{0}|{1}".format(account_no, code))],
            [self._btn("뒤로", "tc|hold|list|{0}".format(account_no))],
        ])

    def build_open_orders(self, account_no, rows):
        lines = ["[미체결 관리]", "", "계좌: {0}".format(account_no or "-"), ""]
        buttons = []
        if not rows:
            lines.append("미체결 주문이 없습니다.")
        for row in rows:
            label = "{0} | {1} | {2}주".format(
                row.get("name") or row.get("code") or "-",
                row.get("order_type") or "-",
                row.get("unfilled_qty") or 0,
            )
            buttons.append([self._btn(label, "tc|open|detail|{0}|{1}".format(account_no, row.get("order_no") or ""))])
        buttons.append([self._btn("메인 메뉴", "tc|menu|home")])
        return "\n".join(lines), self._markup(buttons)

    def build_open_order_detail(self, row):
        text = (
            "[미체결 상세]\n\n"
            "종목: {name} ({code})\n"
            "주문번호: {order_no}\n"
            "주문구분: {order_type}\n"
            "주문수량: {order_qty}\n"
            "미체결수량: {unfilled_qty}\n"
            "주문가격: {order_price}\n"
            "상태: {order_status}"
        ).format(**row)
        account_no = row.get("account_no") or ""
        order_no = row.get("order_no") or ""
        return text, self._markup([
            [self._btn("취소", "tc|confirm|open_cancel|{0}|{1}".format(account_no, order_no))],
            [self._btn("정정", "tc|confirm|open_reprice|{0}|{1}".format(account_no, order_no))],
            [self._btn("시장가 전환", "tc|confirm|open_market|{0}|{1}".format(account_no, order_no))],
            [self._btn("뒤로", "tc|open|list|{0}".format(account_no))],
        ])

    def build_trade_control(self, trade_enabled):
        text = "[자동매매 제어]\n\n현재 자동매매 상태: {0}".format("ON" if trade_enabled else "OFF")
        return text, self._markup([
            [self._btn("자동매매 ON", "tc|confirm|trade_on"), self._btn("자동매매 OFF", "tc|confirm|trade_off")],
            [self._btn("메인 메뉴", "tc|menu|home")],
        ])

    def build_panic_menu(self):
        return "[긴급 정지]\n\n즉시 실행할 제어를 선택하세요.", self._markup([
            [self._btn("전체 매매 정지", "tc|confirm|panic_stop"), self._btn("정지 해제", "tc|confirm|panic_resume")],
            [self._btn("메인 메뉴", "tc|menu|home")],
        ])

    def build_conditions(self, rows):
        lines = ["[조건식 관리]", "", "슬롯을 선택하세요.", ""]
        buttons = []
        if not rows:
            lines.append("등록된 조건식이 없습니다.")
        for row in rows:
            label = "{0}번 | {1} | 활성{2} 실시간{3}".format(
                row.get("slot_no"),
                row.get("condition_name") or "미지정",
                "Y" if row.get("is_enabled") else "N",
                "Y" if row.get("is_realtime") else "N",
            )
            buttons.append([self._btn(label, "tc|cond|detail|{0}".format(row.get("slot_no") or 0))])
        buttons.append([self._btn("메인 메뉴", "tc|menu|home")])
        return "\n".join(lines), self._markup(buttons)

    def build_condition_detail(self, row):
        text = (
            "[조건식 상세]\n\n"
            "슬롯: {slot_no}\n"
            "조건식: {condition_name}\n"
            "활성: {enabled_text}\n"
            "실시간 등록: {realtime_text}\n"
            "편입 종목 수: {current_count}\n\n"
            "매수전략: {buy_strategy_text}\n"
            "매도전략: {sell_strategy_text}"
        ).format(**row)
        slot_no = row.get("slot_no") or 0
        buttons = [
            [self._btn("활성 전환", "tc|confirm|cond_toggle|{0}".format(slot_no)), self._btn("실시간 재등록", "tc|confirm|cond_restart|{0}".format(slot_no))],
            [self._btn("매수전략 변경", "tc|cond|buy_menu|{0}".format(slot_no)), self._btn("매도전략 변경", "tc|cond|sell_menu|{0}".format(slot_no))],
            [self._btn("뒤로", "tc|cond|list"), self._btn("메인 메뉴", "tc|menu|home")],
        ]
        return text, self._markup(buttons)

    def build_condition_buy_menu(self, slot_row, strategy_rows):
        lines = [
            "[매수전략 변경]",
            "",
            "슬롯: {0}".format(slot_row.get("slot_no") or 0),
            "조건식: {0}".format(slot_row.get("condition_name") or "-"),
            "현재 매수전략: {0}".format(slot_row.get("buy_strategy_text") or "-"),
            "",
        ]
        buttons = []
        for row in strategy_rows:
            label = "[{0}] {1}".format(row.get("strategy_no") or 0, row.get("strategy_name") or "-")
            buttons.append([self._btn(label, "tc|confirm|cond_buy|{0}|{1}".format(slot_row.get("slot_no") or 0, row.get("strategy_no") or 0))])
        buttons.append([self._btn("뒤로", "tc|cond|detail|{0}".format(slot_row.get("slot_no") or 0))])
        return "\n".join(lines), self._markup(buttons)

    def build_condition_sell_menu(self, slot_row, strategy_rows):
        lines = [
            "[매도전략 변경]",
            "",
            "슬롯: {0}".format(slot_row.get("slot_no") or 0),
            "조건식: {0}".format(slot_row.get("condition_name") or "-"),
            "현재 매도전략: {0}".format(slot_row.get("sell_strategy_text") or "-"),
            "",
            "버튼을 누르면 추가/제거가 전환됩니다.",
        ]
        buttons = []
        selected_text = str(slot_row.get("sell_strategy_text") or "")
        for row in strategy_rows:
            strategy_no = row.get("strategy_no") or 0
            strategy_name = row.get("strategy_name") or "-"
            selected = "[ON] " if "[{0}]".format(strategy_no) in selected_text else ""
            label = "{0}[{1}] {2}".format(selected, strategy_no, strategy_name)
            buttons.append([self._btn(label, "tc|confirm|cond_sell_toggle|{0}|{1}".format(slot_row.get("slot_no") or 0, strategy_no))])
        buttons.append([self._btn("뒤로", "tc|cond|detail|{0}".format(slot_row.get("slot_no") or 0))])
        return "\n".join(lines), self._markup(buttons)

    def build_confirm(self, title, body, confirm_callback, cancel_callback="tc|menu|home"):
        return "[확인]\n\n{0}\n\n{1}".format(title, body), self._markup([
            [self._btn("확인", confirm_callback), self._btn("취소", cancel_callback)],
        ])

    def build_result(self, message):
        return "[처리 결과]\n\n{0}".format(message or "-"), self._markup([
            [self._btn("메인 메뉴", "tc|menu|home")],
        ])
