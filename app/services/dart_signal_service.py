# -*- coding: utf-8 -*-
import datetime
import json
import re


class DartSignalService(object):
    EVENT_RULES = [
        ("cb_issue", [u"전환사채", "CB"]),
        ("bw_issue", [u"신주인수권부사채", "BW"]),
        ("eb_issue", [u"교환사채", "EB"]),
        ("third_party_allocation", [u"제3자배정", u"유상증자결정"]),
        ("conversion_exercise", [u"전환청구권 행사", u"전환청구권행사"]),
        ("bw_exercise", [u"신주인수권 행사", u"신주인수권행사"]),
        ("conversion_price_adjustment", [u"전환가액의 조정", u"전환가액 조정", u"리픽싱"]),
        ("new_share_listing", [u"신주상장", u"상장예정"]),
        ("major_shareholder_change", [u"주식등의 대량보유상황보고"]),
        ("largest_shareholder_change", [u"최대주주 변경", u"최대주주변경"]),
    ]

    ASSOCIATION_KEYWORDS = [u"투자조합", u"조합", u"사모", u"파트너스", "PE", u"신기술조합"]

    def __init__(self, persistence=None):
        self.persistence = persistence

    def filter_risky_financing_disclosures(self, disclosures):
        filtered = []
        for row in disclosures or []:
            normalized = self.normalize_disclosure(row)
            if str(normalized.get("event_type", "") or "").strip():
                filtered.append(normalized)
        return filtered

    def normalize_disclosure(self, row):
        data = dict(row or {})
        report_name = str(data.get("report_name", "") or "").strip()
        detail_text = str(data.get("detail_text", "") or "")
        event_type = self._classify_event_type(report_name, detail_text=detail_text)
        detail_fields = dict(data.get("detail_fields") or {})
        return {
            "event_id": str(data.get("event_id", "") or ""),
            "code": str(data.get("code", "") or ""),
            "corp_name": str(data.get("corp_name", "") or ""),
            "disclosure_date": str(data.get("disclosure_date", "") or ""),
            "report_name": report_name,
            "event_type": event_type,
            "sub_type": self._derive_sub_type(event_type, report_name),
            "counterparty": self._extract_counterparty(data, detail_fields=detail_fields),
            "fund_purpose": self._extract_fund_purpose(data, detail_fields=detail_fields),
            "amount": self._extract_amount(data, detail_fields=detail_fields),
            "shares": self._extract_shares(data, detail_fields=detail_fields),
            "conversion_price": self._extract_conversion_price(data, detail_fields=detail_fields),
            "refixing_flag": 1 if (event_type == "conversion_price_adjustment" or int(detail_fields.get("refixing_flag", 0) or 0)) else 0,
            "listing_due_date": self._extract_listing_due_date(data, detail_fields=detail_fields),
            "detail_excerpt": str(data.get("detail_excerpt", "") or detail_fields.get("excerpt", "") or ""),
            "source_url": str(data.get("source_url", "") or ""),
            "raw_json": dict(data.get("raw_json") or data or {}),
        }

    def score_signals(self, code, corp_name, events):
        events = list(events or [])
        score = 0.0
        tags = {
            "mezzanine_flag": 0,
            "dilution_flag": 0,
            "overhang_flag": 0,
            "association_flag": 0,
            "control_change_flag": 0,
        }
        evidence = []
        event_counts = {}
        for event in events:
            event_type = str(event.get("event_type", "") or "")
            event_counts[event_type] = int(event_counts.get(event_type, 0)) + 1
            counterparty = str(event.get("counterparty", "") or "")
            report_name = str(event.get("report_name", "") or "")
            date_label = str(event.get("disclosure_date", "") or "")
            excerpt = str(event.get("detail_excerpt", "") or "")

            if event_type == "cb_issue":
                score += 20
                tags["mezzanine_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"CB 발행 공시", excerpt))
            elif event_type == "bw_issue":
                score += 22
                tags["mezzanine_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"BW 발행 공시", excerpt))
            elif event_type == "eb_issue":
                score += 18
                tags["mezzanine_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"EB 발행 공시", excerpt))
            elif event_type == "third_party_allocation":
                score += 20
                tags["dilution_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"제3자배정 유상증자 공시", excerpt))
            elif event_type in ["conversion_exercise", "bw_exercise"]:
                score += 18
                tags["dilution_flag"] = 1
                tags["overhang_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"전환/행사 공시", excerpt))
            elif event_type == "conversion_price_adjustment":
                score += 20
                tags["mezzanine_flag"] = 1
                tags["dilution_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"전환가액 조정 공시", excerpt))
            elif event_type == "new_share_listing":
                score += 15
                tags["overhang_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"신주 상장 관련 공시", excerpt))
            elif event_type in ["major_shareholder_change", "largest_shareholder_change"]:
                score += 12
                tags["control_change_flag"] = 1
                evidence.append(self._evidence_line(date_label, u"지배구조 변경 관련 공시", excerpt))

            if self._has_association(counterparty):
                score += 15
                tags["association_flag"] = 1
                if counterparty:
                    evidence.append(u"상대방에 {0} 성격 명칭 포함".format(counterparty))

        if event_counts.get("cb_issue", 0) >= 1 and event_counts.get("conversion_price_adjustment", 0) >= 1:
            score += 15
            tags["mezzanine_flag"] = 1
            tags["dilution_flag"] = 1
            evidence.append(u"CB 발행과 전환가액 조정이 함께 확인됩니다.")
        if event_counts.get("cb_issue", 0) >= 1 and event_counts.get("conversion_exercise", 0) >= 1:
            score += 15
            tags["overhang_flag"] = 1
            evidence.append(u"CB 발행 이후 전환청구권 행사 공시가 이어집니다.")
        if event_counts.get("third_party_allocation", 0) >= 1 and tags["association_flag"]:
            score += 18
            evidence.append(u"제3자배정과 조합/사모 성격 상대방이 함께 확인됩니다.")
        if (event_counts.get("conversion_exercise", 0) + event_counts.get("bw_exercise", 0)) >= 1 and event_counts.get("new_share_listing", 0) >= 1:
            score += 18
            tags["overhang_flag"] = 1
            evidence.append(u"행사 공시 이후 신주 상장 관련 공시가 연결됩니다.")
        if tags["control_change_flag"] and tags["mezzanine_flag"]:
            score += 20
            evidence.append(u"지배구조 변경과 메자닌 이벤트가 함께 확인됩니다.")
        if sum(event_counts.values()) >= 3:
            score += 15
            evidence.append(u"최근 6개월 내 관련 자금조달 공시가 반복됩니다.")

        warning_level = self._warning_level(score)
        summary = self._build_summary(warning_level, tags, evidence)
        return {
            "code": str(code or ""),
            "trade_date": datetime.date.today().isoformat(),
            "corp_name": str(corp_name or ""),
            "mezzanine_flag": int(tags["mezzanine_flag"]),
            "dilution_flag": int(tags["dilution_flag"]),
            "overhang_flag": int(tags["overhang_flag"]),
            "association_flag": int(tags["association_flag"]),
            "control_change_flag": int(tags["control_change_flag"]),
            "warning_level": warning_level,
            "warning_score": round(float(score), 2),
            "warning_summary": summary,
            "evidence": evidence[:8],
            "events": events,
        }

    def save_event_cache(self, events):
        if self.persistence is None:
            return
        rows = []
        now_ts = self.persistence.now_ts()
        for event in events or []:
            rows.append(
                (
                    str(event.get("event_id", "") or ""),
                    str(event.get("code", "") or ""),
                    str(event.get("corp_name", "") or ""),
                    str(event.get("disclosure_date", "") or ""),
                    str(event.get("report_name", "") or ""),
                    str(event.get("event_type", "") or ""),
                    str(event.get("sub_type", "") or ""),
                    str(event.get("counterparty", "") or ""),
                    str(event.get("fund_purpose", "") or ""),
                    float(event.get("amount", 0) or 0),
                    float(event.get("shares", 0) or 0),
                    float(event.get("conversion_price", 0) or 0),
                    int(event.get("refixing_flag", 0) or 0),
                    str(event.get("listing_due_date", "") or ""),
                    str(event.get("source_url", "") or ""),
                    json.dumps(event.get("raw_json") or {}, ensure_ascii=False),
                    now_ts,
                )
            )
        if not rows:
            return
        self.persistence.executemany(
            """
            INSERT OR REPLACE INTO dart_event_cache
            (event_id, code, corp_name, disclosure_date, report_name, event_type, sub_type,
             counterparty, fund_purpose, amount, shares, conversion_price, refixing_flag,
             listing_due_date, source_url, raw_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def save_signal_summary(self, signal_result):
        if self.persistence is None:
            return
        result = dict(signal_result or {})
        self.persistence.execute(
            """
            INSERT OR REPLACE INTO stock_risk_signals
            (code, trade_date, corp_name, mezzanine_flag, dilution_flag, overhang_flag,
             association_flag, control_change_flag, warning_level, warning_score,
             warning_summary, evidence_json, extra_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(result.get("code", "") or ""),
                str(result.get("trade_date", "") or datetime.date.today().isoformat()),
                str(result.get("corp_name", "") or ""),
                int(result.get("mezzanine_flag", 0) or 0),
                int(result.get("dilution_flag", 0) or 0),
                int(result.get("overhang_flag", 0) or 0),
                int(result.get("association_flag", 0) or 0),
                int(result.get("control_change_flag", 0) or 0),
                str(result.get("warning_level", "") or ""),
                float(result.get("warning_score", 0) or 0),
                str(result.get("warning_summary", "") or ""),
                json.dumps(result.get("evidence") or [], ensure_ascii=False),
                json.dumps({"event_count": len(result.get("events") or [])}, ensure_ascii=False),
                self.persistence.now_ts(),
            ),
        )

    def _classify_event_type(self, report_name, detail_text=""):
        report_name = str(report_name or "").strip()
        detail_text = str(detail_text or "").strip()
        if not report_name and not detail_text:
            return ""
        lowered = u"{0}\n{1}".format(report_name, detail_text).lower()
        for event_type, keywords in self.EVENT_RULES:
            for keyword in keywords:
                if keyword and (keyword.lower() in lowered):
                    return event_type
        return ""

    def _derive_sub_type(self, event_type, report_name):
        if event_type == "third_party_allocation" and u"제3자배정" in str(report_name or ""):
            return "third_party"
        if event_type in ["cb_issue", "bw_issue", "eb_issue"] and u"사모" in str(report_name or ""):
            return "private"
        return ""

    def _extract_counterparty(self, row, detail_fields=None):
        detail_fields = dict(detail_fields or {})
        detail_counterparty = str(detail_fields.get("counterparty", "") or "").strip()
        if detail_counterparty:
            return detail_counterparty
        raw = dict(row.get("raw_json") or {})
        return str(raw.get("flr_nm", "") or row.get("flr_name", "") or "").strip()

    def _extract_fund_purpose(self, row, detail_fields=None):
        detail_fields = dict(detail_fields or {})
        detail_value = str(detail_fields.get("fund_purpose", "") or "").strip()
        if detail_value:
            return detail_value
        raw = dict(row.get("raw_json") or {})
        return str(raw.get("rm", "") or row.get("rm", "") or "").strip()

    def _extract_amount(self, row, detail_fields=None):
        detail_fields = dict(detail_fields or {})
        detail_amount = self._to_float(detail_fields.get("amount"))
        if detail_amount > 0:
            return detail_amount
        raw = dict(row.get("raw_json") or {})
        return self._extract_number(raw, ["amount", "issu", "total_amount"])

    def _extract_shares(self, row, detail_fields=None):
        detail_fields = dict(detail_fields or {})
        detail_value = self._to_float(detail_fields.get("shares"))
        if detail_value > 0:
            return detail_value
        raw = dict(row.get("raw_json") or {})
        return self._extract_number(raw, ["shares", "stkcnt", "stock_cnt"])

    def _extract_conversion_price(self, row, detail_fields=None):
        detail_fields = dict(detail_fields or {})
        detail_value = self._to_float(detail_fields.get("conversion_price"))
        if detail_value > 0:
            return detail_value
        raw = dict(row.get("raw_json") or {})
        return self._extract_number(raw, ["conversion_price", "conv_prc", "stock_knd"])

    def _extract_listing_due_date(self, row, detail_fields=None):
        detail_fields = dict(detail_fields or {})
        detail_value = str(detail_fields.get("listing_due_date", "") or "").strip()
        if detail_value:
            return detail_value
        raw = dict(row.get("raw_json") or {})
        for key in ["listing_due_date", "list_dt", "stk_lstg_dt", "rcept_dt"]:
            value = str(raw.get(key, "") or "").strip()
            if value:
                return value
        return ""

    def _evidence_line(self, date_label, event_label, excerpt):
        prefix = u"{0} {1}".format(date_label or "", event_label or "").strip()
        excerpt = str(excerpt or "").strip()
        if excerpt:
            return u"{0}: {1}".format(prefix, excerpt[:180])
        return prefix

    def _extract_number(self, raw, keys):
        for key in keys:
            value = raw.get(key)
            number = self._to_float(value)
            if number > 0:
                return number
        return 0.0

    def _to_float(self, value):
        text = str(value or "").strip()
        if not text:
            return 0.0
        text = re.sub(r"[^0-9.\-]", "", text)
        try:
            return float(text)
        except Exception:
            return 0.0

    def _has_association(self, text):
        text = str(text or "").strip()
        if not text:
            return False
        for keyword in self.ASSOCIATION_KEYWORDS:
            if keyword.lower() in text.lower():
                return True
        return False

    def _warning_level(self, score):
        score = float(score or 0.0)
        if score >= 70:
            return u"강한 주의"
        if score >= 50:
            return u"주의"
        if score >= 25:
            return u"관찰"
        return u"주의 없음"

    def _build_summary(self, warning_level, tags, evidence):
        pieces = []
        if tags.get("mezzanine_flag"):
            pieces.append(u"메자닌")
        if tags.get("dilution_flag"):
            pieces.append(u"희석")
        if tags.get("overhang_flag"):
            pieces.append(u"출회")
        if tags.get("association_flag"):
            pieces.append(u"조합")
        if tags.get("control_change_flag"):
            pieces.append(u"지배구조")
        if not pieces:
            return u"관련 공시가 제한적입니다."
        evidence_line = u", ".join((evidence or [])[:2])
        return u"{0}: {1} 징후가 확인됩니다. {2}".format(warning_level, u"/".join(pieces), evidence_line).strip()
