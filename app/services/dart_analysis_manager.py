# -*- coding: utf-8 -*-
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

    def analyze_stock(self, name, code, days=180, allow_ai=True):
        code = str(code or "").strip()
        name = str(name or "").strip()
        if not code:
            return {}
        disclosures = self.api_service.fetch_recent_disclosures(code, days=days)
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
