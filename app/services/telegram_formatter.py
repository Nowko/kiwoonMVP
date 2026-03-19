# -*- coding: utf-8 -*-
import html
import json
import re


class TelegramFormatter(object):
    def _clean_text(self, value, default="-"):
        value = "" if value is None else str(value)
        value = re.sub(r"<[^>]+>", "", value)
        value = html.unescape(value).strip()
        return value if value else default

    def _escape(self, value, default="-"):
        return html.escape(self._clean_text(value, default=default))

    def _text(self, value, default="-"):
        return self._clean_text(value, default=default)

    def _fmt_int(self, value, default="-"):
        try:
            return "{0:,}".format(int(float(value)))
        except Exception:
            return default

    def _fmt_float(self, value, default="-"):
        try:
            return "{0:.2f}".format(float(value))
        except Exception:
            return default

    def _fmt_float1(self, value, default="-"):
        try:
            return "{0:.1f}".format(float(value))
        except Exception:
            return default

    def _fmt_ratio(self, value, default="-"):
        try:
            value = float(value)
            if value <= 0:
                return default
            return "{0:.1f}배".format(value)
        except Exception:
            return default

    def _pick_first(self, *values):
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            return value
        return None

    def _pick_first_positive_number(self, *values):
        for value in values:
            try:
                number = float(value)
                if number > 0:
                    return number
            except Exception:
                continue
        return None

    def _label_hoga(self, hoga_gb):
        return "지정가" if str(hoga_gb or "") == "00" else "시장가"

    def _label_limit_option(self, option):
        mapping = {
            "current_price": "현재가",
            "ask1": "매도1호가",
            "current_plus_1tick": "현재가+1틱",
        }
        return mapping.get(str(option or ""), self._text(option))

    def _label_direction(self, direction):
        mapping = {
            "bullish": "상승 관점",
            "bearish": "하락 경계",
            "neutral": "중립",
        }
        return mapping.get(str(direction or "").strip(), self._text(direction, default="-"))

    def _label_event_type(self, event_type):
        mapping = {
            "contract": "공급계약",
            "earnings": "실적",
            "buyback": "자사주",
            "mna": "M&A",
            "approval": "승인/허가",
            "clinical": "임상",
            "investment": "투자",
            "litigation": "소송",
            "general": "일반",
        }
        return mapping.get(str(event_type or "").strip(), self._text(event_type, default="일반"))

    def _label_trade_action(self, trade_action):
        mapping = {
            "buy_now": "즉시 매수 검토",
            "watch_breakout": "돌파 시 매수 검토",
            "watch_pullback": "눌림 시 재확인",
            "watch": "추가 관찰",
            "ignore": "무시",
            "risk_only": "리스크 경고",
        }
        return mapping.get(str(trade_action or "").strip(), self._text(trade_action, default="-"))

    def _label_time_horizon(self, time_horizon):
        mapping = {
            "intraday": "당일 대응",
            "overnight": "하루 이상 관찰",
            "multi_day": "며칠 추적 관찰",
        }
        return mapping.get(str(time_horizon or "").strip(), self._text(time_horizon, default="-"))

    def _label_trigger_type(self, trigger_type, final_score=0, is_holding=False):
        trigger_type = str(trigger_type or "").strip()
        if trigger_type in ["holding", "important_news"] or is_holding:
            return "[보유]"
        if trigger_type in ["tracked", "manual_recheck"]:
            return "[추적]"
        if float(final_score or 0) >= 80:
            return "[고점수]"
        return "[감시]"

    def _article_extra(self, article):
        try:
            return json.loads(article.get("extra_json") or "{}")
        except Exception:
            return {}

    def _short_compare_basis(self, compare_label, default="동시간"):
        label = self._text(compare_label, default="")
        if ("일간" in label) or ("최근 5일" in label and "동시간" not in label):
            return "일간"
        if "동시간" in label:
            return "동시간"
        return default

    def _resolve_message_metric(self, symbol_meta, metric_kind):
        symbol_meta = dict(symbol_meta or {})
        metric_mode = str(symbol_meta.get("message_metric_mode") or symbol_meta.get("metric_mode") or "").strip().lower()

        if metric_kind == "turnover":
            explicit_ratio = self._pick_first_positive_number(symbol_meta.get("message_turnover_ratio"))
            explicit_label = self._text(symbol_meta.get("message_turnover_compare_label"), default="")
            same_time_ratio = self._pick_first_positive_number(symbol_meta.get("turnover_ratio_5d_same_time"))
            full_day_ratio = self._pick_first_positive_number(symbol_meta.get("turnover_ratio_5d"))
            same_time_label = self._text(
                self._pick_first(
                    symbol_meta.get("same_time_turnover_compare_label"),
                    symbol_meta.get("turnover_compare_label"),
                    "최근 5일 동시간 평균 거래대금",
                ),
                default="최근 5일 동시간 평균 거래대금",
            )
            full_day_label = self._text(
                self._pick_first(
                    symbol_meta.get("full_day_turnover_compare_label"),
                    symbol_meta.get("turnover_compare_label"),
                    "최근 5일 일간 평균 거래대금",
                ),
                default="최근 5일 일간 평균 거래대금",
            )
        else:
            explicit_ratio = self._pick_first_positive_number(symbol_meta.get("message_volume_ratio"))
            explicit_label = self._text(symbol_meta.get("message_volume_compare_label"), default="")
            same_time_ratio = self._pick_first_positive_number(symbol_meta.get("volume_ratio_5d_same_time"))
            full_day_ratio = self._pick_first_positive_number(symbol_meta.get("volume_ratio_5d"))
            same_time_label = self._text(
                self._pick_first(
                    symbol_meta.get("same_time_volume_compare_label"),
                    symbol_meta.get("volume_compare_label"),
                    "최근 5일 동시간 평균 거래량",
                ),
                default="최근 5일 동시간 평균 거래량",
            )
            full_day_label = self._text(
                self._pick_first(
                    symbol_meta.get("full_day_volume_compare_label"),
                    symbol_meta.get("volume_compare_label"),
                    "최근 5일 일간 평균 거래량",
                ),
                default="최근 5일 일간 평균 거래량",
            )

        if explicit_ratio is not None:
            compare_label = explicit_label or (full_day_label if metric_mode == "full_day" else same_time_label)
            return explicit_ratio, compare_label

        if metric_mode == "full_day":
            if full_day_ratio is not None:
                return full_day_ratio, full_day_label
            return same_time_ratio, same_time_label

        if same_time_ratio is not None:
            return same_time_ratio, same_time_label
        return full_day_ratio, full_day_label

    def _multiline_compact_text(self, value):
        text = self._text(value, default="-")
        if text == "-":
            return text
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        for sep in [". ", "; ", " / ", " | "]:
            text = text.replace(sep, "\n")
        lines = [line.strip(" -") for line in text.split("\n") if line.strip(" -")]
        return "\n".join(self._escape(line) for line in lines) if lines else "-"

    def _build_reason_text(self, event_type, direction, final_score, trade_action):
        event_label = self._label_event_type(event_type)
        direction = str(direction or "").strip().lower()
        try:
            score = float(final_score or 0)
        except Exception:
            score = 0.0
        if direction == "bullish":
            if score >= 80:
                return "{0} 뉴스가 강하고, 단기 매매 판단에 활용 가능한 편입니다.".format(event_label)
            if score >= 70:
                return "{0} 뉴스가 긍정적이지만, 진입 전 추가 확인이 필요합니다.".format(event_label)
            return "{0} 뉴스는 있으나 강도는 높지 않습니다.".format(event_label)
        if direction == "bearish":
            if score >= 70:
                return "{0} 악재 성격이 강해 보수적으로 볼 필요가 있습니다.".format(event_label)
            return "{0} 부정 뉴스 가능성이 있어 확인이 필요합니다.".format(event_label)
        if str(trade_action or "").strip() in ["watch_breakout", "watch_pullback", "watch"]:
            return "{0} 뉴스는 중립적이지만 주가 반응은 관찰할 가치가 있습니다.".format(event_label)
        return "{0} 뉴스는 확인됐지만 바로 매매로 연결하기엔 애매합니다.".format(event_label)

    def _build_risk_text(self, certainty, novelty, direction):
        try:
            certainty = float(certainty or 0)
        except Exception:
            certainty = 0.0
        try:
            novelty = float(novelty or 0)
        except Exception:
            novelty = 0.0
        direction = str(direction or "").strip().lower()
        notes = []
        if certainty < 60:
            notes.append("기사 표현이 모호할 수 있어 원문 확인이 필요합니다.")
        if novelty < 60:
            notes.append("이미 알려진 재료일 수 있어 추격 매수는 주의가 필요합니다.")
        if direction == "bearish":
            notes.append("악재 해석 가능성이 있어 변동성이 커질 수 있습니다.")
        return "\n".join(notes)

    def _normalize_risk_note(self, risk_note, certainty, novelty, direction):
        text = self._text(risk_note, default="")
        fallback = self._build_risk_text(certainty, novelty, direction)
        generic_phrases = [
            "선반영 가능성 있음",
            "선반영 가능성",
            "주의 필요",
            "확인 필요",
        ]

        candidates = []
        for raw in [text, fallback]:
            raw = self._text(raw, default="")
            if not raw:
                continue
            raw = raw.replace("\r\n", "\n").replace("\r", "\n")
            for part in raw.split("\n"):
                line = part.strip(" -")
                if not line:
                    continue
                if any(phrase == line for phrase in generic_phrases):
                    continue
                if line in candidates:
                    continue
                candidates.append(line)

        if not candidates:
            return "-"

        priority_keywords = [
            ("악재", 0),
            ("부정", 0),
            ("변동성", 1),
            ("모호", 2),
            ("원문", 2),
            ("중복", 3),
            ("재료", 3),
            ("추격", 3),
        ]

        def _priority(line):
            for keyword, order in priority_keywords:
                if keyword in line:
                    return order
            return 9

        candidates.sort(key=lambda line: (_priority(line), len(line)))
        return self._multiline_compact_text(candidates[0])

    def format_lines(self, title, lines):
        lines = list(lines or [])
        safe_lines = [html.escape(str(x)) for x in lines]
        return "<b>{0}</b>\n".format(html.escape(str(title))) + "\n".join(safe_lines)

    def format_trade_message(self, title, lines):
        return self.format_lines(title, lines)

    def format_system_message(self, title, lines):
        return self.format_lines(title, lines)

    def format_news_articles(self, code, name, trigger_type, articles, symbol_meta=None):
        articles = list(articles or [])
        symbol_meta = dict(symbol_meta or {})
        top = articles[0] if articles else {}
        extra = self._article_extra(top)
        final_score = float(top.get("final_score") or 0)
        title = self._label_trigger_type(trigger_type, final_score, bool(top.get("is_holding", False)))
        analysis_source = str(extra.get("analysis_source", "") or "").strip().lower()
        analysis_label = "GPT 분석" if analysis_source == "gpt" else "프로그램 분석"
        event_type = self._label_event_type(extra.get("event_type"))
        direction = self._label_direction(extra.get("direction"))
        importance = self._fmt_float1(top.get("importance_score"), default="-")
        certainty = self._fmt_float1(
            self._pick_first(extra.get("certainty_score"), extra.get("confidence_score")),
            default="-",
        )
        actionability = self._fmt_float1(extra.get("actionability_score"), default="-")
        novelty = self._fmt_float1(extra.get("novelty_score"), default="-")
        trade_action = self._label_trade_action(extra.get("trade_action"))
        time_horizon = self._label_time_horizon(extra.get("time_horizon"))
        theme_summary = self._text(extra.get("theme_summary"), default="-")
        event_theme_summary = self._text(extra.get("event_theme_summary"), default="-")
        theme_items = [self._text(item, default="") for item in list(extra.get("themes") or []) if self._text(item, default="")]
        event_theme_items = [self._text(item, default="") for item in list(extra.get("event_themes") or []) if self._text(item, default="")]
        reason = self._multiline_compact_text(
            self._build_reason_text(extra.get("event_type"), extra.get("direction"), final_score, extra.get("trade_action"))
        )
        risk_note = self._normalize_risk_note(
            self._pick_first(extra.get("risk_note"), top.get("risk_note")),
            self._pick_first(extra.get("certainty_score"), extra.get("confidence_score")),
            extra.get("novelty_score"),
            extra.get("direction"),
        )
        dart_signal = dict(extra.get("dart_signal") or {})
        reference_price_value = self._pick_first_positive_number(
            symbol_meta.get("reference_price"),
            symbol_meta.get("detected_price"),
        )
        reference_price = self._fmt_int(reference_price_value, default="-")

        volume_ratio_value, volume_compare_label = self._resolve_message_metric(symbol_meta, "volume")
        turnover_ratio_value, turnover_compare_label = self._resolve_message_metric(symbol_meta, "turnover")
        volume_ratio = self._fmt_ratio(volume_ratio_value)
        turnover_ratio = self._fmt_ratio(turnover_ratio_value)
        volume_basis = self._short_compare_basis(volume_compare_label, default="동시간")
        turnover_basis = self._short_compare_basis(turnover_compare_label, default="동시간")

        rows = [
            "📌 [{0} {1}] : <b>{2}</b>".format(
                self._escape(name),
                self._escape(code),
                "{0}원".format(reference_price) if reference_price != "-" else "-",
            ),
            "최근 5일 대비 거래량 <b>{0}</b> ({1})".format(self._escape(volume_ratio), self._escape(volume_basis))
            if volume_ratio != "-" else "최근 5일 대비 거래량 <b>-</b> ({0})".format(self._escape(volume_basis)),
            "최근 5일 대비 거래대금 <b>{0}</b> ({1})".format(self._escape(turnover_ratio), self._escape(turnover_basis))
            if turnover_ratio != "-" else "최근 5일 대비 거래대금 <b>-</b> ({0})".format(self._escape(turnover_basis)),
            "{0}".format(self._escape(title)),
            "",
        ]
        rows.append("🕒 시기테마: {0}".format(self._escape(", ".join(event_theme_items) if event_theme_items else event_theme_summary)))
        for idx, article in enumerate(articles[:2], 1):
            article_title = self._text(
                article.get("article_title")
                or article.get("title")
                or article.get("headline"),
                default="기사 보기",
            )
            article_url = str(
                article.get("article_url")
                or article.get("original_link")
                or article.get("link")
                or article.get("url")
                or ""
            ).strip()
            if article_url.startswith("http"):
                linked_title = '<a href="{0}">{1}</a>'.format(
                    html.escape(article_url, quote=True),
                    self._escape(article_title),
                )
            else:
                linked_title = self._escape(article_title)
            rows.append("{0}. {1}".format(idx, linked_title))

        rows.extend([
            "",
            "🏷️ 고정테마: {0}".format(self._escape(", ".join(theme_items) if theme_items else theme_summary)),
            "🧠 <b>{0}</b> / {1} / {2}".format(
                self._escape(analysis_label),
                self._escape(direction),
                self._escape(event_type),
            ),
            "<b>{0}</b> : 중요 {1} / 신뢰 {2} / 실용 {3} / 시의 {4}".format(
                self._fmt_float1(final_score, default="-"),
                importance,
                certainty,
                actionability,
                novelty,
            ),
            "<b>GPT-5.0 제안</b>",
            "{0}\n{1}".format(
                self._escape(trade_action),
                self._escape(time_horizon),
            ),
            "<b>판단 근거</b>",
            "{0}".format(reason),
        ])
        if risk_note != "-":
            rows.extend([
                "<b>주의 사항</b>",
                "{0}".format(risk_note),
            ])
        dart_block = self._format_dart_signal_block(dart_signal)
        if dart_block:
            rows.extend([
                "",
                "<b>전자공시 확인</b>",
                dart_block,
            ])
        return "\n".join(rows)

    def _format_dart_signal_block(self, dart_signal):
        dart_signal = dict(dart_signal or {})
        if not dart_signal:
            return ""
        warning_level = self._text(
            self._pick_first(dart_signal.get("gpt_risk_level"), dart_signal.get("warning_level")),
            default="",
        )
        if not warning_level:
            return ""
        summary = self._text(
            self._pick_first(dart_signal.get("gpt_summary"), dart_signal.get("warning_summary")),
            default="-",
        )
        evidence = list(self._pick_first(dart_signal.get("gpt_evidence"), dart_signal.get("evidence")) or [])
        evidence = [self._text(item, default="") for item in evidence if self._text(item, default="")]
        lines = [
            "판정: {0}".format(self._escape(warning_level)),
            "요약: {0}".format(self._escape(summary)),
        ]
        if evidence:
            lines.append("근거: {0}".format(self._escape(" / ".join(evidence[:3]))))
        return "\n".join(lines)

    def _format_dart_signal_block(self, dart_signal):
        dart_signal = dict(dart_signal or {})
        if not dart_signal:
            return ""
        warning_level = self._normalize_dart_risk_level(
            self._text(
                self._pick_first(dart_signal.get("gpt_risk_level"), dart_signal.get("warning_level")),
                default="",
            )
        )
        if not warning_level:
            return ""
        summary = self._text(
            self._pick_first(dart_signal.get("gpt_summary"), dart_signal.get("warning_summary")),
            default="-",
        )
        summary = self._normalize_dart_summary(summary, warning_level)
        evidence = list(self._pick_first(dart_signal.get("gpt_evidence"), dart_signal.get("evidence")) or [])
        evidence = [self._text(item, default="") for item in evidence if self._text(item, default="")]
        lines = [self._escape(summary or warning_level)]
        if evidence:
            lines.append("근거: {0}".format(self._escape(" / ".join(evidence[:3]))))
        return "\n".join(lines)

    def _normalize_dart_risk_level(self, value):
        value = self._text(value, default="")
        mapping = {
            "주의 없음": "작전 없음",
            "관찰": "작전 의심",
            "주의": "작전 주의",
            "강한 주의": "강한 작전",
            "작전 없음": "작전 없음",
            "작전 의심": "작전 의심",
            "작전 주의": "작전 주의",
            "강한 작전": "강한 작전",
        }
        return mapping.get(value, value)

    def _normalize_dart_summary(self, summary, warning_level):
        summary = self._text(summary, default="")
        warning_level = self._text(warning_level, default="")
        if not summary or summary == "-":
            return warning_level
        for raw_level in [
            "주의 없음",
            "관찰",
            "주의",
            "강한 주의",
            "작전 없음",
            "작전 의심",
            "작전 주의",
            "강한 작전",
        ]:
            normalized = self._normalize_dart_risk_level(raw_level)
            if summary == raw_level:
                return normalized
            prefix = raw_level + ":"
            if summary.startswith(prefix):
                return normalized + summary[len(raw_level):]
        if warning_level:
            return "{0}: {1}".format(warning_level, summary)
        return summary

    def format_trade_buy_candidate(self, payload):
        pricing = dict(payload.get("pricing") or {})
        evaluation = dict(payload.get("evaluation") or {})
        news_scores = dict(evaluation.get("news_scores") or {})
        lines = [
            "계좌: {0}".format(self._text(payload.get("account_no"))),
            "종목: {0} ({1})".format(self._text(payload.get("name")), self._text(payload.get("code"))),
            "트리거: {0}".format(self._text(payload.get("trigger_type"))),
            "실행 모드: {0}".format(self._text(payload.get("execution_mode"))),
            "주문 방식: {0}".format(self._label_hoga(pricing.get("hoga_gb"))),
            "지정가 옵션: {0}".format(self._label_limit_option(pricing.get("limit_price_option"))),
            "수량: {0}".format(self._fmt_int(payload.get("qty"))),
            "주문 기준가: {0}".format(self._fmt_int(pricing.get("reference_price"))),
            "예상 주문가: {0}".format(self._fmt_int(pricing.get("order_price"))),
            "뉴스 점수: {0}".format(self._fmt_float(news_scores.get("final_score"), default="-")),
        ]
        return self.format_lines("매수 후보 생성", lines)

    def format_trade_buy_filled(self, payload):
        lines = [
            "계좌: {0}".format(self._text(payload.get("account_no"))),
            "종목: {0} ({1})".format(self._text(payload.get("name")), self._text(payload.get("code"))),
            "체결 수량: {0}".format(self._fmt_int(payload.get("filled_qty"))),
            "체결가: {0}".format(self._fmt_int(payload.get("filled_price"))),
            "미체결: {0}".format(self._fmt_int(payload.get("unfilled_qty"))),
        ]
        return self.format_lines("매수 체결", lines)

    def format_trade_sell_filled(self, payload):
        lines = [
            "계좌: {0}".format(self._text(payload.get("account_no"))),
            "종목: {0} ({1})".format(self._text(payload.get("name")), self._text(payload.get("code"))),
            "체결 수량: {0}".format(self._fmt_int(payload.get("filled_qty"))),
            "체결가: {0}".format(self._fmt_int(payload.get("filled_price"))),
            "실현 손익: {0}".format(self._fmt_float(payload.get("cycle_realized"))),
        ]
        return self.format_lines("매도 체결", lines)

    def format_trade_sell_filled(self, payload):
        lines = [
            "계좌: {0}".format(self._text(payload.get("account_no"))),
            "종목: {0} ({1})".format(self._text(payload.get("name")), self._text(payload.get("code"))),
            "체결 수량: {0}".format(self._fmt_int(payload.get("filled_qty"))),
            "체결가: {0}".format(self._fmt_int(payload.get("filled_price"))),
            "실현 손익: {0}".format(self._fmt_int(payload.get("cycle_realized"))),
        ]
        return self.format_lines("매도 체결", lines)

    def format_unfilled_policy_step(self, payload):
        lines = [
            "계좌: {0}".format(self._text(payload.get("account_no"))),
            "종목: {0} ({1})".format(self._text(payload.get("name")), self._text(payload.get("code"))),
            "정책: {0}".format(self._text(payload.get("policy"))),
            "단계: {0}".format(self._text(payload.get("stage"))),
            "조치: {0}".format(self._text(payload.get("action"))),
            "상세: {0}".format(self._text(payload.get("detail"))),
        ]
        return self.format_lines("미체결 정책 진행", lines)

    def format_event(self, event_type, payload):
        event_type = str(event_type or "").strip()
        payload = dict(payload or {})
        if event_type == "news_articles":
            return self.format_news_articles(
                payload.get("code", ""),
                payload.get("name", ""),
                payload.get("trigger_type", ""),
                payload.get("articles", []),
                payload.get("symbol_meta", {}),
            )
        if event_type == "trade_buy_candidate":
            return self.format_trade_buy_candidate(payload)
        if event_type == "trade_buy_filled":
            return self.format_trade_buy_filled(payload)
        if event_type == "trade_sell_filled":
            return self.format_trade_sell_filled(payload)
        if event_type == "unfilled_policy_step":
            return self.format_unfilled_policy_step(payload)
        title = payload.get("title", event_type)
        lines = payload.get("lines", [])
        return self.format_lines(title, lines)
