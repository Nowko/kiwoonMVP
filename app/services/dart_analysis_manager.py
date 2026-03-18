# -*- coding: utf-8 -*-
import datetime
import json

from PyQt5.QtCore import QObject, pyqtSignal

from app.services.dart_api_service import DartApiService
from app.services.dart_signal_service import DartSignalService
from app.services.dart_gpt_analysis_service import DartGPTAnalysisService


class DartAnalysisManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, paths, persistence=None, credential_manager=None, parent=None):
        super(DartAnalysisManager, self).__init__(parent)
        self.paths = paths
        self.persistence = persistence
        self.credential_manager = credential_manager
        self.api_service = DartApiService(paths, credential_manager=credential_manager)
        self.signal_service = DartSignalService(persistence=persistence)
        self.gpt_service = DartGPTAnalysisService(credential_manager=credential_manager)
        if self.credential_manager is not None and hasattr(self.credential_manager, "credentials_changed"):
            self.credential_manager.credentials_changed.connect(self.refresh_from_credentials)

    def refresh_from_credentials(self):
        self.gpt_service.refresh_from_credentials()

    def get_cached_signal(self, code, max_age_minutes=30):
        if self.persistence is None:
            return {}
        code = str(code or "").strip()
        if not code:
            return {}
        row = self.persistence.fetchone(
            "SELECT * FROM stock_risk_signals WHERE code=?",
            (code,),
        )
        if row is None:
            return {}
        updated_at = self._parse_ts(row["updated_at"])
        if updated_at is None:
            return {}
        if (datetime.datetime.now() - updated_at).total_seconds() > max(1, int(max_age_minutes or 30)) * 60:
            return {}
        return self._row_to_result(row)

    def analyze_stock(self, name, code, days=180, allow_ai=True, use_cache=True, max_age_minutes=30):
        code = str(code or "").strip()
        name = str(name or "").strip()
        if not code:
            return {}
        if use_cache:
            cached = self.get_cached_signal(code, max_age_minutes=max_age_minutes)
            if cached:
                return cached
        disclosures = self.api_service.fetch_recent_disclosures(code, days=days)
        disclosures = self.api_service.enrich_disclosures(disclosures, force=False, max_age_hours=168)
        risk_disclosures = self.signal_service.filter_risky_financing_disclosures(disclosures)
        signal_result = self.signal_service.score_signals(code, name, risk_disclosures)
        self.signal_service.save_event_cache(risk_disclosures)
        self.signal_service.save_signal_summary(signal_result)
        if allow_ai and risk_disclosures:
            try:
                gpt_result = self.gpt_service.analyze_disclosures_with_gpt(
                    name=name,
                    code=code,
                    disclosures=risk_disclosures,
                    fallback=signal_result,
                )
                signal_result["gpt_analysis"] = dict(gpt_result or {})
                self._save_gpt_payload(code, gpt_result)
                self.log_emitted.emit(u"🤖 DART 공시 GPT 분석 적용: {0} / {1}".format(code, self.gpt_service.model))
            except Exception as exc:
                self.log_emitted.emit(u"⚠️ DART 공시 GPT 분석 실패: {0} / {1}".format(code, exc))
        return signal_result

    def _save_gpt_payload(self, code, gpt_result):
        if self.persistence is None:
            return
        code = str(code or "").strip()
        if not code:
            return
        row = self.persistence.fetchone("SELECT extra_json FROM stock_risk_signals WHERE code=?", (code,))
        extra = {}
        if row is not None:
            try:
                extra = json.loads(str(row["extra_json"] or "{}"))
            except Exception:
                extra = {}
        extra["gpt_analysis"] = dict(gpt_result or {})
        self.persistence.execute(
            "UPDATE stock_risk_signals SET extra_json=?, updated_at=? WHERE code=?",
            (
                json.dumps(extra, ensure_ascii=False),
                self.persistence.now_ts(),
                code,
            ),
        )

    def _row_to_result(self, row):
        extra = {}
        evidence = []
        try:
            extra = json.loads(str(row["extra_json"] or "{}"))
        except Exception:
            extra = {}
        try:
            evidence = list(json.loads(str(row["evidence_json"] or "[]")))
        except Exception:
            evidence = []
        result = {
            "code": str(row["code"] or ""),
            "trade_date": str(row["trade_date"] or ""),
            "corp_name": str(row["corp_name"] or ""),
            "mezzanine_flag": int(row["mezzanine_flag"] or 0),
            "dilution_flag": int(row["dilution_flag"] or 0),
            "overhang_flag": int(row["overhang_flag"] or 0),
            "association_flag": int(row["association_flag"] or 0),
            "control_change_flag": int(row["control_change_flag"] or 0),
            "warning_level": str(row["warning_level"] or ""),
            "warning_score": float(row["warning_score"] or 0),
            "warning_summary": str(row["warning_summary"] or ""),
            "evidence": evidence,
        }
        if isinstance(extra, dict) and extra.get("gpt_analysis"):
            result["gpt_analysis"] = dict(extra.get("gpt_analysis") or {})
        return result

    def _parse_ts(self, value):
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
