# -*- coding: utf-8 -*-
import datetime
import json
import time

from PyQt5.QtCore import QObject, QTimer, pyqtSignal


class SignalPipelineManager(QObject):
    pipeline_changed = pyqtSignal()
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, condition_manager, strategy_manager, news_manager, order_manager, account_manager, parent=None):
        super(SignalPipelineManager, self).__init__(parent)
        self.persistence = persistence
        self.condition_manager = condition_manager
        self.strategy_manager = strategy_manager
        self.news_manager = news_manager
        self.order_manager = order_manager
        self.account_manager = account_manager
        self._recent_detected = {}
        self._detected_cooldown_sec = 12.0
        self._pending_detection_jobs = []
        self._pending_detection_job_keys = set()
        self._detection_job_spacing_ms = 150
        self._detection_job_timer = QTimer(self)
        self._detection_job_timer.setSingleShot(True)
        self._detection_job_timer.timeout.connect(self._process_next_detection_job)

        self.condition_manager.symbol_detected.connect(self._on_symbol_detected)
        self.news_manager.news_found.connect(self._on_news_found)

    def _is_buy_rejected_cooldown(self, symbol_row, now_dt=None):
        if str(symbol_row.get("current_state") or "") != "BUY_REJECTED":
            return False
        cooldown_sec = max(0, int(getattr(self.order_manager, "buy_reject_retry_cooldown_sec", 0) or 0))
        if cooldown_sec <= 0:
            return False
        extra = {}
        try:
            extra = json.loads(symbol_row.get("extra_json") or "{}")
        except Exception:
            extra = {}
        rejected_at = str(extra.get("buy_rejected_at") or "").strip()
        if not rejected_at:
            return False
        try:
            rejected_dt = datetime.datetime.strptime(rejected_at, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return False
        now_dt = now_dt or datetime.datetime.now()
        return (now_dt - rejected_dt).total_seconds() < cooldown_sec

    def _enqueue_detection_job(self, payload):
        code = str(payload.get("code") or "")
        slot_no = int(payload.get("slot_no") or 0)
        event_type = str(payload.get("event_type") or "")
        if not code:
            return
        job_key = "{0}:{1}:{2}".format(code, slot_no, event_type)
        if job_key in self._pending_detection_job_keys:
            return
        self._pending_detection_job_keys.add(job_key)
        self._pending_detection_jobs.append(dict(payload))
        if not self._detection_job_timer.isActive():
            self._detection_job_timer.start(max(50, int(self._detection_job_spacing_ms)))

    def get_pending_detection_job_count(self):
        return int(len(self._pending_detection_jobs or []))

    def _process_next_detection_job(self):
        if not self._pending_detection_jobs:
            return
        payload = dict(self._pending_detection_jobs.pop(0) or {})
        job_key = "{0}:{1}:{2}".format(
            str(payload.get("code") or ""),
            int(payload.get("slot_no") or 0),
            str(payload.get("event_type") or ""),
        )
        self._pending_detection_job_keys.discard(job_key)
        try:
            self._handle_symbol_detected(payload)
        finally:
            if self._pending_detection_jobs:
                self._detection_job_timer.start(int(self._detection_job_spacing_ms))

    def _handle_symbol_detected(self, payload):
        code = str(payload.get("code") or "")
        if not code:
            return
        symbol_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        if not symbol_row:
            return
        symbol_row = dict(symbol_row)
        if int(symbol_row.get("is_spam") or 0):
            self.log_emitted.emit("SPAM BLOCKED: {0}".format(symbol_row.get("code", "")))
            return
        if self._is_buy_rejected_cooldown(symbol_row):
            return
        self.log_emitted.emit("PIPELINE START: {0} ({1})".format(symbol_row.get("name", ""), symbol_row.get("code", "")))
        self.news_manager.search_news_for_symbol_async(
            symbol_row.get("code", ""),
            symbol_row.get("name", ""),
            trigger_type="detected",
            min_score=None,
        )
        news_scores = self.news_manager.get_latest_news_scores(symbol_row.get("code", ""))
        evaluation = self.strategy_manager.evaluate_slot_buy_policy(symbol_row, news_scores, slot_no=payload.get("slot_no", 0))
        if evaluation.get("passed"):
            self.order_manager.submit_buy_orders(symbol_row, evaluation, "condition_detected")
        else:
            now = self.persistence.now_ts()
            reason = evaluation.get("terminal_reason", "unknown")
            extra = {}
            try:
                extra = json.loads(symbol_row.get("extra_json") or "{}")
            except Exception:
                extra = {}
            extra["buy_block_reason"] = reason
            extra["buy_blocked_at"] = now
            self.persistence.execute(
                "UPDATE tracked_symbols SET current_state='BUY_BLOCKED', extra_json=?, updated_at=? WHERE code=?",
                (json.dumps(extra, ensure_ascii=False), now, code),
            )
            self.log_emitted.emit("BUY BLOCKED: {0} / {1}".format(symbol_row.get("code", ""), reason))
        self.pipeline_changed.emit()

    def _on_symbol_detected(self, payload):
        if payload.get("event_type") not in ["condition_enter", "condition_snapshot"]:
            return
        code = str(payload.get("code") or "")
        if not code:
            return
        slot_no = int(payload.get("slot_no") or 0)
        event_type = str(payload.get("event_type") or "")
        dedupe_key = "{0}:{1}:{2}".format(code, slot_no, event_type)
        now_mono = time.monotonic()
        expire_before = now_mono - max(5.0, self._detected_cooldown_sec * 3.0)
        stale_keys = [key for key, ts in self._recent_detected.items() if ts < expire_before]
        for key in stale_keys:
            self._recent_detected.pop(key, None)
        prev_ts = self._recent_detected.get(dedupe_key)
        if prev_ts is not None and (now_mono - prev_ts) < self._detected_cooldown_sec:
            return
        self._recent_detected[dedupe_key] = now_mono
        self._enqueue_detection_job(payload)

    def _on_news_found(self, payload):
        articles = payload.get("articles") or []
        if not articles:
            return
        code = payload.get("code")
        symbol_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        if not symbol_row:
            return
        symbol_row = dict(symbol_row)
        if int(symbol_row.get("is_spam") or 0):
            return
        if int(symbol_row.get("is_holding") or 0):
            return
        news_scores = self.news_manager.get_latest_news_scores(code)
        evaluation = self.strategy_manager.evaluate_news_trade_candidate(symbol_row, news_scores)
        if evaluation.get("passed"):
            self.news_manager.queue_recheck(code, "important_news", priority=95)
            self.order_manager.submit_buy_orders(symbol_row, evaluation, "important_news")
            self.log_emitted.emit("NEWS BUY PASSED: {0}".format(code))
        else:
            self.log_emitted.emit("NEWS RECHECK HELD: {0}".format(code))
        self.pipeline_changed.emit()

    def run_periodic_maintenance(self):
        self._prune_expired_symbols()
        self.order_manager.evaluate_sell_positions(self.strategy_manager)
        self.order_manager.rebuild_daily_summaries()
        self.pipeline_changed.emit()

    def _prune_expired_symbols(self):
        rows = self.persistence.fetchall(
            "SELECT code, name FROM tracked_symbols WHERE expire_at IS NOT NULL AND expire_at < ? AND is_holding=0 AND has_open_order=0 AND is_spam=0",
            (self.persistence.now_ts(),),
        )
        for row in rows:
            self.persistence.execute("DELETE FROM tracked_symbols WHERE code=?", (row["code"],))
            self.persistence.write_event("tracked_symbol_pruned", {"code": row["code"], "name": row["name"]})
            self.log_emitted.emit("TRACKED EXPIRED: {0} ({1})".format(row["name"], row["code"]))
