# -*- coding: utf-8 -*-
import datetime
import json
import queue
import threading

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
        self._analysis_job_queue = queue.Queue()
        self._pending_codes = set()
        self._pending_lock = threading.RLock()
        self._error_log_ts = {}
        self._analysis_thread = threading.Thread(
            target=self._analysis_worker_loop,
            name="dart-analysis-worker",
            daemon=True,
        )
        self._analysis_thread.start()
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

    def get_signal_for_news(self, name, code, days=180, fresh_max_age_minutes=30, stale_max_age_minutes=720):
        code = str(code or "").strip()
        name = str(name or "").strip()
        if not code:
            return {}

        fresh = self.get_cached_signal(code, max_age_minutes=fresh_max_age_minutes)
        if fresh:
            return fresh

        stale = self.get_cached_signal(code, max_age_minutes=stale_max_age_minutes)
        if stale:
            self.request_analysis_refresh(name, code, days=days, allow_ai=True)
            return stale

        quick = self.analyze_stock(
            name=name,
            code=code,
            days=days,
            allow_ai=False,
            use_cache=False,
            include_details=False,
            emit_logs=False,
            persist=True,
        )
        self.request_analysis_refresh(name, code, days=days, allow_ai=True)
        return quick

    def request_analysis_refresh(self, name, code, days=180, allow_ai=True):
        code = str(code or "").strip()
        if not code:
            return False
        with self._pending_lock:
            if code in self._pending_codes:
                return False
            self._pending_codes.add(code)
        self._analysis_job_queue.put(
            {
                "name": str(name or "").strip(),
                "code": code,
                "days": max(1, int(days or 180)),
                "allow_ai": bool(allow_ai),
            }
        )
        return True

    def analyze_stock(self, name, code, days=180, allow_ai=True, use_cache=True, max_age_minutes=30, include_details=True, emit_logs=True, persist=True):
        code = str(code or "").strip()
        name = str(name or "").strip()
        if not code:
            return {}
        if use_cache:
            cached = self.get_cached_signal(code, max_age_minutes=max_age_minutes)
            if cached:
                return cached
        disclosures = self.api_service.fetch_recent_disclosures(code, days=days)
        risk_disclosures = self.signal_service.filter_risky_financing_disclosures(disclosures)
        if include_details and risk_disclosures:
            risk_disclosures = self.api_service.enrich_disclosures(risk_disclosures, force=False, max_age_hours=168)
            risk_disclosures = self.signal_service.filter_risky_financing_disclosures(risk_disclosures)
        signal_result = self.signal_service.score_signals(code, name, risk_disclosures)
        if persist:
            self.signal_service.save_event_cache(risk_disclosures)
            self.signal_service.save_signal_summary(signal_result)
        if allow_ai and include_details and risk_disclosures and self._should_run_ai(signal_result):
            try:
                gpt_result = self.gpt_service.analyze_disclosures_with_gpt(
                    name=name,
                    code=code,
                    disclosures=risk_disclosures,
                    fallback=signal_result,
                )
                signal_result["gpt_analysis"] = dict(gpt_result or {})
                if persist:
                    self._save_gpt_payload(code, gpt_result)
                if emit_logs:
                    self.log_emitted.emit(u"🤖 DART 공시 GPT 분석 적용: {0} / {1}".format(code, self.gpt_service.model))
            except Exception as exc:
                if emit_logs:
                    self.log_emitted.emit(u"⚠️ DART 공시 GPT 분석 실패: {0} / {1}".format(code, exc))
        return signal_result

    def _should_run_ai(self, signal_result):
        if not self.gpt_service.enabled:
            return False
        result = dict(signal_result or {})
        warning_score = float(result.get("warning_score", 0) or 0)
        active_flags = sum(
            1 for key in [
                "mezzanine_flag",
                "dilution_flag",
                "overhang_flag",
                "association_flag",
                "control_change_flag",
            ]
            if int(result.get(key, 0) or 0)
        )
        return warning_score >= 50 or active_flags >= 2

    def _analysis_worker_loop(self):
        while True:
            job = self._analysis_job_queue.get()
            if job is None:
                self._analysis_job_queue.task_done()
                break
            code = str(job.get("code", "") or "").strip()
            try:
                self.analyze_stock(
                    name=job.get("name", ""),
                    code=code,
                    days=job.get("days", 180),
                    allow_ai=bool(job.get("allow_ai", True)),
                    use_cache=False,
                    include_details=True,
                    emit_logs=False,
                    persist=True,
                )
            except Exception as exc:
                self._emit_background_error(code, exc)
            finally:
                with self._pending_lock:
                    self._pending_codes.discard(code)
                self._analysis_job_queue.task_done()

    def _emit_background_error(self, code, exc, min_interval_sec=300):
        now_dt = datetime.datetime.now()
        key = str(code or "").strip() or "_global"
        last_dt = self._error_log_ts.get(key)
        if last_dt is not None:
            try:
                if (now_dt - last_dt).total_seconds() < float(min_interval_sec):
                    return
            except Exception:
                pass
        self._error_log_ts[key] = now_dt
        self.log_emitted.emit(u"⚠️ DART 백그라운드 분석 실패: {0} / {1}".format(code, exc))

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
