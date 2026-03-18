# -*- coding: utf-8 -*-
import json
import os
import re

import requests


def _bounded_score(value, default=0):
    try:
        return max(0, min(100, int(round(float(value)))))
    except Exception:
        try:
            return max(0, min(100, int(round(float(default)))))
        except Exception:
            return 0


def _weighted_news_score(importance_score, certainty_score, actionability_score, novelty_score):
    base_score = (
        float(importance_score or 0) * 0.40 +
        float(actionability_score or 0) * 0.35 +
        float(certainty_score or 0) * 0.15 +
        float(novelty_score or 0) * 0.10
    )
    return round(base_score, 2)


def _gated_news_score(base_score, certainty_score, actionability_score):
    score = float(base_score or 0.0)
    certainty_score = float(certainty_score or 0.0)
    actionability_score = float(actionability_score or 0.0)
    if actionability_score < 45:
        score = min(score, 49.0)
    elif actionability_score < 60:
        score = min(score, 64.0)
    if certainty_score < 50:
        score = min(score, 59.0)
    elif certainty_score < 60:
        score = min(score, 74.0)
    return round(max(0.0, min(100.0, score)), 2)


def _derive_trade_action(direction, actionability_score, certainty_score):
    direction = str(direction or "neutral").strip().lower()
    actionability_score = float(actionability_score or 0.0)
    certainty_score = float(certainty_score or 0.0)
    if direction == "bearish":
        return "risk_only" if certainty_score >= 55 else "ignore"
    if direction != "bullish":
        if actionability_score >= 65 and certainty_score >= 60:
            return "watch"
        return "ignore"
    if actionability_score >= 80 and certainty_score >= 70:
        return "buy_now"
    if actionability_score >= 65 and certainty_score >= 60:
        return "watch_breakout"
    if actionability_score >= 50:
        return "watch_pullback"
    return "watch"


def _derive_time_horizon(event_type, actionability_score):
    event_type = str(event_type or "general").strip().lower()
    actionability_score = float(actionability_score or 0.0)
    if event_type in ["mna", "approval", "earnings", "contract"]:
        return "multi_day" if actionability_score >= 60 else "overnight"
    if event_type in ["investment", "buyback", "clinical"]:
        return "overnight" if actionability_score >= 55 else "intraday"
    return "intraday" if actionability_score >= 60 else "overnight"


def _derive_novelty_type(novelty_score):
    novelty_score = float(novelty_score or 0.0)
    if novelty_score >= 80:
        return "new_fact"
    if novelty_score >= 60:
        return "update"
    return "recap"


class RuleNewsAnalyzer(object):
    def __init__(self):
        self.positive_keywords = [u"공급", u"계약", u"수주", u"실적", u"흑자", u"투자", u"인수", u"합병", u"임상", u"승인", u"자사주"]
        self.negative_keywords = [u"유상증자", u"전환사채", u"소송", u"거래정지", u"관리종목", u"적자", u"감자"]
        self.vague_keywords = [u"전망", u"가능성", u"기대", u"추정"]
        self.event_mapping = [
            (u"공급", "contract"),
            (u"계약", "contract"),
            (u"수주", "contract"),
            (u"실적", "earnings"),
            (u"자사주", "buyback"),
            (u"인수", "mna"),
            (u"합병", "mna"),
            (u"승인", "approval"),
            (u"임상", "clinical"),
            (u"투자", "investment"),
            (u"소송", "litigation"),
        ]

    def analyze(self, context):
        title = context.get("title", "") or ""
        description = context.get("description", "") or ""
        original_link = context.get("original_link", "") or ""
        duplicate_count = int(context.get("duplicate_count", 1) or 1)
        text = u"{0} {1}".format(title, description)

        importance_score = self._calc_importance_score(text)
        certainty_score = self._calc_certainty_score(text, title, description, original_link)
        direction = self._infer_direction(text)
        event_type = self._infer_event_type(text)
        novelty_score = self._calc_novelty_score(text, duplicate_count)
        actionability_score = self._calc_actionability_score(text, event_type, direction, certainty_score)
        frequency_score = min(100, max(1, duplicate_count) * 20)
        final_score = _gated_news_score(
            _weighted_news_score(importance_score, certainty_score, actionability_score, novelty_score),
            certainty_score,
            actionability_score,
        )
        trade_action = _derive_trade_action(direction, actionability_score, certainty_score)
        time_horizon = _derive_time_horizon(event_type, actionability_score)
        novelty_type = _derive_novelty_type(novelty_score)
        return {
            "analysis_source": "rule",
            "analysis_model": "rule-engine-v1",
            "analysis_label": "규칙점수 분석",
            "event_type": event_type,
            "direction": direction,
            "importance_score": importance_score,
            "confidence_score": certainty_score,
            "certainty_score": certainty_score,
            "actionability_score": actionability_score,
            "novelty_score": novelty_score,
            "frequency_score": frequency_score,
            "final_news_score": final_score,
            "trade_action": trade_action,
            "time_horizon": time_horizon,
            "novelty_type": novelty_type,
            "recheck_needed": final_score >= 80.0 and direction == "bullish" and actionability_score >= 60 and certainty_score >= 60,
            "brief_reason": self._build_reason(event_type, direction, final_score),
            "risk_note": self._build_risk_note(certainty_score, novelty_score, direction),
        }

    def _calc_importance_score(self, text):
        score = 40
        for keyword in self.positive_keywords:
            if keyword in text:
                score += 12
        for keyword in self.negative_keywords:
            if keyword in text:
                score -= 10
        return max(0, min(100, score))

    def _calc_certainty_score(self, text, title, description, original_link):
        score = 40
        if title:
            score += 5
        if description:
            score += 10
        if original_link and original_link.startswith("http"):
            score += 10
        if len((description or "").strip()) >= 20:
            score += 10
        for keyword in self.vague_keywords:
            if keyword in text:
                score -= 12
        if u"공시" in text or u"확정" in text or u"체결" in text or u"결정" in text:
            score += 12
        if u"검토" in text or u"추진" in text or u"가능성" in text or u"예정" in text:
            score -= 10
        return max(0, min(100, score))

    def _calc_actionability_score(self, text, event_type, direction, certainty_score):
        score = 35
        for keyword in [u"공급", u"계약", u"수주", u"실적", u"인수", u"매각", u"승인", u"허가"]:
            if keyword in text:
                score += 12
        for keyword in [u"검토", u"추진", u"가능성", u"예정", u"설", u"관계자"]:
            if keyword in text:
                score -= 10
        if str(direction or "").strip().lower() == "bullish":
            score += 5
        if str(event_type or "").strip().lower() in ["mna", "contract", "earnings", "approval"]:
            score += 8
        if float(certainty_score or 0) < 55:
            score -= 10
        return max(0, min(100, score))

    def _calc_novelty_score(self, text, duplicate_count):
        score = 80
        score -= max(0, duplicate_count - 1) * 10
        if u"재탕" in text or u"다시" in text:
            score -= 5
        return max(0, min(100, score))

    def _infer_direction(self, text):
        pos = sum(1 for k in self.positive_keywords if k in text)
        neg = sum(1 for k in self.negative_keywords if k in text)
        if pos > neg:
            return "bullish"
        if neg > pos:
            return "bearish"
        return "neutral"

    def _infer_event_type(self, text):
        for keyword, event_type in self.event_mapping:
            if keyword in text:
                return event_type
        return "general"

    def _event_label(self, event_type):
        mapping = {
            "contract": u"수주/계약",
            "earnings": u"실적",
            "buyback": u"자사주",
            "mna": u"M&A",
            "approval": u"인허가",
            "clinical": u"임상",
            "investment": u"투자",
            "litigation": u"소송",
            "general": u"일반",
        }
        return mapping.get(str(event_type or "").strip(), u"일반")

    def _build_reason(self, event_type, direction, final_score):
        score = float(final_score or 0)
        event_label = self._event_label(event_type)
        direction = str(direction or "neutral").strip().lower()

        if direction == "bullish":
            if score >= 85:
                return u"{0} 기대 큼\n단기 모멘텀 가능".format(event_label)
            if score >= 70:
                return u"{0} 기대 있음\n수급 유입 가능".format(event_label)
            if score >= 60:
                return u"{0} 확인 필요\n추가 뉴스 확인".format(event_label)
            return u"{0} 약함\n추가 확인 필요".format(event_label)

        if direction == "bearish":
            if score >= 80:
                return u"{0} 우려 큼\n변동성 확대 가능".format(event_label)
            if score >= 65:
                return u"{0} 우려 있음\n보수적 접근 필요".format(event_label)
            return u"{0} 영향 약함\n추가 확인 필요".format(event_label)

        if score >= 80:
            return u"{0} 영향 있음\n재평가 가능".format(event_label)
        if score >= 65:
            return u"{0} 중립적\n확인 필요".format(event_label)
        return u"{0} 약함\n관찰 유지".format(event_label)

    def _build_risk_note(self, certainty_score, novelty_score, direction):
        if certainty_score < 60:
            return u"확정성 낮음\n원문 확인 필요"
        if novelty_score < 60:
            return u"중복 가능성 있음\n재료 지속성 약할 수 있음"
        if direction == "bearish":
            return u"악재 해석 가능\n변동성 큼"
        return u"선반영 가능성 있음"



class GPTNewsAnalyzer(object):
    def __init__(self, api_key="", model="gpt-5-nano", timeout=20, base_url="https://api.openai.com/v1/chat/completions", model_label=""):
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "gpt-5-nano").strip()
        self.timeout = int(timeout or 20)
        self.base_url = str(base_url or "https://api.openai.com/v1/chat/completions").strip()
        self.model_label = str(model_label or "").strip() or self._derive_label(self.model)

    @property
    def enabled(self):
        return bool(self.api_key)

    def analyze(self, context, fallback):
        if not self.enabled:
            return dict(fallback or {})
        payload = self._build_payload(context)
        headers = {
            "Authorization": "Bearer {0}".format(self.api_key),
            "Content-Type": "application/json",
        }
        try:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
            response = requests.post(self.base_url, headers=headers, data=body, timeout=self.timeout)
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = self._extract_http_error_detail(exc)
            if detail:
                raise RuntimeError(u"OpenAI API 오류: {0}".format(detail))
            raise
        data = response.json()
        content = self._extract_content(data)
        parsed = self._parse_json_block(content)
        result = dict(fallback or {})
        event_type = self._text(parsed.get("event_type"), result.get("event_type", "general"))
        direction = self._text(parsed.get("direction"), result.get("direction", "neutral"))
        importance_score = self._clamp_score(parsed.get("importance_score"), result.get("importance_score", 0))
        certainty_score = self._clamp_score(
            parsed.get("certainty_score", parsed.get("confidence_score")),
            result.get("certainty_score", result.get("confidence_score", 0)),
        )
        actionability_score = self._clamp_score(
            parsed.get("actionability_score"),
            result.get("actionability_score", 0),
        )
        novelty_score = self._clamp_score(
            parsed.get("novelty_score"),
            result.get("novelty_score", 0),
        )
        final_news_score = _gated_news_score(
            _weighted_news_score(importance_score, certainty_score, actionability_score, novelty_score),
            certainty_score,
            actionability_score,
        )
        trade_action = self._text(
            parsed.get("trade_action"),
            result.get("trade_action", _derive_trade_action(direction, actionability_score, certainty_score)),
        )
        time_horizon = self._text(
            parsed.get("time_horizon"),
            result.get("time_horizon", _derive_time_horizon(event_type, actionability_score)),
        )
        novelty_type = self._text(
            parsed.get("novelty_type"),
            result.get("novelty_type", _derive_novelty_type(novelty_score)),
        )
        brief_reason = self._text(parsed.get("brief_reason"), "")
        if brief_reason in [u"", u"뉴스 필터 검토 대상", u"뉴스 매매 기준 통과", u"중요뉴스 발생", u"뉴스 필터 미통과"]:
            brief_reason = self._build_reason(event_type, direction, final_news_score)
        result.update({
            "analysis_source": "gpt",
            "analysis_model": self.model,
            "analysis_label": self.model_label,
            "event_type": event_type,
            "direction": direction,
            "importance_score": importance_score,
            "confidence_score": certainty_score,
            "certainty_score": certainty_score,
            "actionability_score": actionability_score,
            "novelty_score": novelty_score,
            "final_news_score": final_news_score,
            "trade_action": trade_action,
            "time_horizon": time_horizon,
            "novelty_type": novelty_type,
            "recheck_needed": bool(result.get("recheck_needed", False)) or (direction == "bullish" and final_news_score >= 80 and actionability_score >= 60 and certainty_score >= 60),
            "brief_reason": brief_reason,
            "risk_note": self._text(parsed.get("risk_note"), result.get("risk_note", "")),
        })
        return result

    def _build_payload(self, context):
        title = self._sanitize_prompt_text(context.get("title", "") or "", max_len=300)
        description = self._sanitize_prompt_text(context.get("description", "") or "", max_len=1200)
        original_link = self._sanitize_prompt_text(context.get("original_link", "") or "", max_len=500)
        duplicate_count = int(context.get("duplicate_count", 1) or 1)
        system_prompt = (
            "You are a Korean stock-news trading assistant. "
            "Return only one JSON object. "
            "Required keys are event_type, direction, importance_score, certainty_score, actionability_score, novelty_score, trade_action, time_horizon, novelty_type, brief_reason, risk_note. "
            "event_type must be one of contract, earnings, buyback, mna, approval, clinical, investment, litigation, general. "
            "direction must be bullish, bearish, or neutral. "
            "trade_action must be one of buy_now, watch_breakout, watch_pullback, watch, ignore, risk_only. "
            "time_horizon must be one of intraday, overnight, multi_day. "
            "novelty_type must be one of new_fact, update, recap. "
            "importance_score means price impact. "
            "certainty_score means how confirmed and concrete the news is. "
            "actionability_score means how usable the news is for an immediate trading decision. "
            "novelty_score means how new the fact is rather than a recap. "
            "All scores must be integers from 0 to 100. "
            "brief_reason and risk_note must be concise Korean phrases."
        )
        user_prompt = (
            "종목 뉴스 분석\n"
            "제목: {0}\n"
            "요약: {1}\n"
            "링크: {2}\n"
            "유사 기사 수: {3}\n"
            "확정성은 공시/공식발표/구체조건 여부를 반영하고, 행동성은 지금 매매판단에 바로 쓸 수 있는지를 반영하세요.\n"
            "참신성은 새 사실인지, 업데이트인지, 재탕인지 구분해 점수화하세요."
        ).format(title, description, original_link, duplicate_count)
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if self._supports_temperature():
            payload["temperature"] = 0
        return payload

    def _sanitize_prompt_text(self, value, max_len=1000):
        text = "" if value is None else str(value)
        text = re.sub(r"<[^>]+>", " ", text)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text)
        text = text.encode("utf-8", "replace").decode("utf-8", "replace")
        text = re.sub(r"\s+", " ", text).strip()
        if int(max_len or 0) > 0 and len(text) > int(max_len):
            text = text[: int(max_len)].rstrip()
        return text

    def _extract_content(self, data):
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            return "{}"

    def _parse_json_block(self, content):
        text = str(content or "").strip()
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return {}
        try:
            return json.loads(match.group(0))
        except Exception:
            return {}

    def _supports_temperature(self):
        model = str(self.model or "").strip().lower()
        if not model:
            return True
        if model.startswith("gpt-5.1"):
            return True
        if model in ["gpt-5", "gpt-5-mini", "gpt-5-nano"]:
            return False
        if model.startswith("gpt-5-"):
            return False
        return True

    def _extract_http_error_detail(self, exc):
        response = getattr(exc, "response", None)
        if response is None:
            return str(exc)
        try:
            data = response.json()
        except Exception:
            data = None
        if isinstance(data, dict):
            error = data.get("error")
            if isinstance(error, dict):
                message = self._text(error.get("message"), "")
                code = self._text(error.get("code"), "")
                param = self._text(error.get("param"), "")
                pieces = [piece for piece in [message, u"code={0}".format(code) if code else "", u"param={0}".format(param) if param else ""] if piece]
                if pieces:
                    return u" / ".join(pieces)
        text = self._text(getattr(response, "text", ""), "")
        if text:
            return text[:500]
        return str(exc)

    def _clamp_score(self, value, default=0):
        try:
            return max(0, min(100, int(round(float(value)))))
        except Exception:
            try:
                return max(0, min(100, int(round(float(default)))))
            except Exception:
                return 0

    def _text(self, value, default=""):
        value = "" if value is None else str(value).strip()
        return value or default

    def _derive_label(self, model):
        model = str(model or "").strip()
        if not model:
            return "GPT 분석"
        pretty = model.replace("gpt-", "GPT-").replace("mini", "mini")
        return u"{0} 분석".format(pretty)
