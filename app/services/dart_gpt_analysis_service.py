# -*- coding: utf-8 -*-
import json
import os
import re

import requests


class DartGPTAnalysisService(object):
    def __init__(self, credential_manager=None, api_key="", model="gpt-5-mini", timeout=20, base_url="https://api.openai.com/v1/chat/completions"):
        self.credential_manager = credential_manager
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "gpt-5-mini").strip()
        self.timeout = int(timeout or 20)
        self.base_url = str(base_url or "https://api.openai.com/v1/chat/completions").strip()
        self.refresh_from_credentials()

    @property
    def enabled(self):
        return bool(self.api_key)

    def refresh_from_credentials(self):
        if self.credential_manager is not None:
            for row in self.credential_manager.get_active_ai_apis(include_key=True):
                provider = str(row.get("provider", "") or "").strip().lower()
                api_key = str(row.get("api_key", "") or "").strip()
                if provider == "openai" and api_key:
                    self.api_key = api_key
                    self.model = str(row.get("model_name", "gpt-5-mini") or "gpt-5-mini").strip()
                    self.base_url = str(row.get("base_url", self.base_url) or self.base_url).strip()
                    return
        self.api_key = str(os.environ.get("OPENAI_API_KEY", self.api_key) or "").strip()
        self.model = str(os.environ.get("OPENAI_DART_MODEL", self.model) or self.model).strip()
        self.base_url = str(os.environ.get("OPENAI_CHAT_COMPLETIONS_URL", self.base_url) or self.base_url).strip()

    def build_prompt_payload(self, name, code, disclosures):
        return self._build_user_prompt(name, code, disclosures)

    def analyze_disclosures_with_gpt(self, name, code, disclosures, fallback=None):
        fallback = dict(fallback or {})
        if (not self.enabled) or (not disclosures):
            return dict(fallback)
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self._build_system_prompt()},
                {"role": "user", "content": self._build_user_prompt(name, code, disclosures)},
            ],
        }
        if self._supports_temperature():
            payload["temperature"] = 0
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        response = requests.post(
            self.base_url,
            headers={
                "Authorization": "Bearer {0}".format(self.api_key),
                "Content-Type": "application/json",
            },
            data=body,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        content = self._extract_content(data)
        parsed = self._parse_json_block(content)
        result = dict(fallback)
        result.update({
            "summary": self._text(parsed.get("summary"), fallback.get("warning_summary", "")),
            "mezzanine_signal": self._text(parsed.get("mezzanine_signal"), ""),
            "dilution_signal": self._text(parsed.get("dilution_signal"), ""),
            "overhang_signal": self._text(parsed.get("overhang_signal"), ""),
            "association_signal": self._text(parsed.get("association_signal"), ""),
            "control_change_signal": self._text(parsed.get("control_change_signal"), ""),
            "risk_level": self._text(parsed.get("risk_level"), fallback.get("warning_level", "")),
            "evidence": list(parsed.get("evidence") or fallback.get("evidence") or []),
            "notes": self._text(parsed.get("notes"), ""),
            "analysis_source": "gpt_dart",
            "analysis_model": self.model,
        })
        return result

    def _build_system_prompt(self):
        return (
            "당신은 한국 상장사 공시 분석 전문가다. "
            "특정 종목의 최근 공시가 주가조작에 자주 악용되는 채권/증자 구조와 유사한 징후를 가지는지 신중하게 판별한다. "
            "범죄 여부를 단정하지 말고 구조적 위험 징후만 평가하라. "
            "반드시 메자닌 징후, 희석 징후, 출회 징후, 조합 징후, 지배구조 징후, 종합 판단, 핵심 근거를 다뤄라. "
            "출력은 JSON 객체 하나만 반환하라. "
            "risk_level은 작전 없음, 작전 의심, 작전 주의, 강한 작전 중 하나여야 한다."
        )

    def _build_user_prompt(self, name, code, disclosures):
        lines = [
            "[종목]",
            "- 종목명: {0}".format(self._sanitize(name, 120)),
            "- 종목코드: {0}".format(self._sanitize(code, 20)),
            "",
            "[최근 공시 목록]",
        ]
        for idx, row in enumerate(disclosures or [], start=1):
            lines.extend([
                "{0}.".format(idx),
                "- 공시일: {0}".format(self._sanitize(row.get("disclosure_date", ""), 40)),
                "- 공시명: {0}".format(self._sanitize(row.get("report_name", ""), 200)),
                "- 유형: {0}".format(self._sanitize(row.get("event_type", ""), 60)),
                "- 상대방: {0}".format(self._sanitize(row.get("counterparty", ""), 120)),
                "- 자금목적: {0}".format(self._sanitize(row.get("fund_purpose", ""), 200)),
                "- 금액: {0}".format(self._sanitize(row.get("amount", ""), 60)),
                "- 전환가액: {0}".format(self._sanitize(row.get("conversion_price", ""), 60)),
                "- 상장예정일: {0}".format(self._sanitize(row.get("listing_due_date", ""), 40)),
                "- 주요문구: {0}".format(self._sanitize(row.get("detail_excerpt", ""), 320)),
                "",
            ])
        lines.extend([
            "아래 JSON 형식으로만 답하라.",
            "{",
            '  "summary": "한 줄 요약",',
            '  "mezzanine_signal": "없음|약함|있음|강함",',
            '  "dilution_signal": "없음|약함|있음|강함",',
            '  "overhang_signal": "없음|약함|있음|강함",',
            '  "association_signal": "없음|약함|있음|강함",',
            '  "control_change_signal": "없음|약함|있음|강함",',
            '  "risk_level": "작전 없음|작전 의심|작전 주의|강한 작전",',
            '  "evidence": ["근거 1", "근거 2", "근거 3"],',
            '  "notes": "실무 메모"',
            "}",
        ])
        return "\n".join(lines)

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

    def _sanitize(self, value, max_len):
        text = "" if value is None else str(value)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > int(max_len):
            text = text[: int(max_len)].rstrip()
        return text

    def _text(self, value, default=""):
        value = "" if value is None else str(value).strip()
        return value or default

    def _supports_temperature(self):
        model = str(self.model or "").strip().lower()
        if model in ["gpt-5", "gpt-5-mini", "gpt-5-nano"]:
            return False
        if model.startswith("gpt-5-"):
            return False
        return True
