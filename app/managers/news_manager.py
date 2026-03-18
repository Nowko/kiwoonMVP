# -*- coding: utf-8 -*-
import datetime
import hashlib
import json
import os
import queue
import threading

import requests
from requests import HTTPError
from PyQt5.QtCore import QObject, pyqtSignal

from app.services.theme_resolver import ThemeResolver


class NaverNewsManager(QObject):
    news_found = pyqtSignal(dict)
    news_search_completed = pyqtSignal(dict)
    log_emitted = pyqtSignal(str)

    def __init__(self, credential_manager, persistence, telegram_router, kiwoom_client=None, analysis_manager=None, dart_analysis_manager=None, daily_watch_snapshot_manager=None, parent=None):
        super(NaverNewsManager, self).__init__(parent)
        self.credential_manager = credential_manager
        self.persistence = persistence
        self.telegram_router = telegram_router
        self.kiwoom_client = kiwoom_client
        self.analysis_manager = analysis_manager
        self.dart_analysis_manager = dart_analysis_manager
        self.daily_watch_snapshot_manager = daily_watch_snapshot_manager
        self.theme_resolver = ThemeResolver(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "theme_catalog.json")
        )
        if self.analysis_manager and hasattr(self.analysis_manager, "log_emitted"):
            self.analysis_manager.log_emitted.connect(self.log_emitted.emit)
        if self.dart_analysis_manager and hasattr(self.dart_analysis_manager, "log_emitted"):
            self.dart_analysis_manager.log_emitted.connect(self.log_emitted.emit)
        self.timeout = 8
        self._symbol_meta_cache = {}
        self._symbol_meta_cache_live_ttl_sec = 180.0
        self._symbol_meta_cache_after_hours_ttl_sec = 600.0
        self._startup_heavy_meta_grace_sec = 75.0
        self._startup_heavy_meta_grace_until = datetime.datetime.now() + datetime.timedelta(seconds=self._startup_heavy_meta_grace_sec)
        self._news_job_queue = queue.Queue()
        self._pending_news_job_keys = set()
        self._pending_news_job_lock = threading.RLock()
        self.news_search_completed.connect(self._apply_search_result)
        self._news_worker_thread = threading.Thread(
            target=self._news_worker_loop,
            name="news-search-worker",
            daemon=True,
        )
        self._news_worker_thread.start()

    def _positive_float(self, value):
        try:
            value = float(value)
            return value if value > 0 else 0.0
        except Exception:
            return 0.0

    def _is_regular_market_hours(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        hhmm = now_dt.strftime("%H%M")
        return now_dt.weekday() < 5 and "0900" <= hhmm <= "1530"

    def _is_startup_heavy_meta_window(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        grace_until = getattr(self, "_startup_heavy_meta_grace_until", None)
        if grace_until is None:
            return False
        try:
            return now_dt < grace_until
        except Exception:
            return False

    def _get_fast_quote_seed(self, code):
        code = str(code or "").strip()
        if not code:
            return {}
        snapshot = {}
        if self.kiwoom_client is not None and hasattr(self.kiwoom_client, "get_realtime_snapshot"):
            try:
                snapshot = dict(self.kiwoom_client.get_realtime_snapshot(code) or {})
            except Exception:
                snapshot = {}
        if self._positive_float(snapshot.get("current_volume")) <= 0 and self._positive_float(snapshot.get("acc_volume")) > 0:
            snapshot["current_volume"] = self._positive_float(snapshot.get("acc_volume"))
        if self._positive_float(snapshot.get("current_turnover")) <= 0 and self._positive_float(snapshot.get("acc_turnover")) > 0:
            snapshot["current_turnover"] = self._positive_float(snapshot.get("acc_turnover"))
        if self._positive_float(snapshot.get("current_price")) <= 0 and self.kiwoom_client is not None and hasattr(self.kiwoom_client, "get_master_last_price"):
            try:
                master_last_price = self._positive_float(self.kiwoom_client.get_master_last_price(code))
            except Exception:
                master_last_price = 0.0
            if master_last_price > 0:
                snapshot["current_price"] = master_last_price
        return snapshot

    def _get_cached_symbol_meta(self, code, now_dt=None):
        code = str(code or "").strip()
        if not code:
            return None
        now_dt = now_dt or datetime.datetime.now()
        ttl_sec = self._symbol_meta_cache_live_ttl_sec if self._is_regular_market_hours(now_dt) else self._symbol_meta_cache_after_hours_ttl_sec
        item = self._symbol_meta_cache.get(code)
        if not item:
            return None
        try:
            cached_at = item.get("cached_at")
            if cached_at is None:
                return None
            elapsed = (now_dt - cached_at).total_seconds()
            if elapsed > float(ttl_sec):
                return None
            return dict(item.get("value") or {})
        except Exception:
            return None

    def _set_cached_symbol_meta(self, code, symbol_meta, now_dt=None):
        code = str(code or "").strip()
        if not code:
            return
        try:
            self._symbol_meta_cache[code] = {
                "cached_at": now_dt or datetime.datetime.now(),
                "value": dict(symbol_meta or {}),
            }
        except Exception:
            pass

    def _make_job_key(self, code, trigger_type):
        return "{0}:{1}".format(str(code or "").strip(), str(trigger_type or "").strip())

    def _news_worker_loop(self):
        while True:
            job = self._news_job_queue.get()
            if job is None:
                self._news_job_queue.task_done()
                break
            try:
                result = self._execute_search_job(dict(job or {}))
            except Exception as exc:
                result = {
                    "job_key": str(job.get("job_key", "") if isinstance(job, dict) else ""),
                    "code": str(job.get("code", "") if isinstance(job, dict) else ""),
                    "name": str(job.get("name", "") if isinstance(job, dict) else ""),
                    "trigger_type": str(job.get("trigger_type", "") if isinstance(job, dict) else ""),
                    "status": "error",
                    "last_error": str(exc),
                    "fresh_articles": [],
                    "top_articles": [],
                    "scored_count": 0,
                    "duplicate_count": 0,
                    "score_fail_count": 0,
                }
            self.news_search_completed.emit(result)
            self._news_job_queue.task_done()

    def _queue_search_job(self, payload):
        job_key = self._make_job_key(payload.get("code", ""), payload.get("trigger_type", ""))
        with self._pending_news_job_lock:
            if job_key in self._pending_news_job_keys:
                return False
            self._pending_news_job_keys.add(job_key)
        queued = dict(payload or {})
        queued["job_key"] = job_key
        self._news_job_queue.put(queued)
        return True

    def _has_usable_live_reference_metrics(self, symbol_meta, market_open=False):
        if not market_open:
            return False
        symbol_meta = dict(symbol_meta or {})
        reference_price = self._positive_float(symbol_meta.get("reference_price"))
        detected_volume = self._positive_float(symbol_meta.get("detected_volume"))
        detected_turnover = self._positive_float(symbol_meta.get("detected_turnover"))
        avg_volume = max(
            self._positive_float(symbol_meta.get("avg_volume_same_time_5d")),
            self._positive_float(symbol_meta.get("avg_volume_5d")),
        )
        avg_turnover = max(
            self._positive_float(symbol_meta.get("avg_turnover_same_time_5d")),
            self._positive_float(symbol_meta.get("avg_turnover_5d")),
        )
        volume_ratio = max(
            self._positive_float(symbol_meta.get("volume_ratio_5d_same_time")),
            self._positive_float(symbol_meta.get("volume_ratio_5d")),
        )
        turnover_ratio = max(
            self._positive_float(symbol_meta.get("turnover_ratio_5d_same_time")),
            self._positive_float(symbol_meta.get("turnover_ratio_5d")),
        )
        metric_mode = str(symbol_meta.get("metric_mode") or "")
        if metric_mode == "pending_same_time_refresh":
            return False
        has_reference = reference_price > 0
        has_compare = any([
            detected_volume > 0,
            detected_turnover > 0,
            avg_volume > 0,
            avg_turnover > 0,
            volume_ratio > 0,
            turnover_ratio > 0,
        ])
        return has_reference and has_compare

    def _has_usable_reference_metrics(self, symbol_meta):
        symbol_meta = dict(symbol_meta or {})
        metric_mode = str(symbol_meta.get("metric_mode") or "")
        if metric_mode == "pending_same_time_refresh":
            return False
        reference_price = self._positive_float(symbol_meta.get("reference_price"))
        detected_volume = self._positive_float(symbol_meta.get("detected_volume"))
        detected_turnover = self._positive_float(symbol_meta.get("detected_turnover"))
        avg_volume = max(
            self._positive_float(symbol_meta.get("avg_volume_5d")),
            self._positive_float(symbol_meta.get("avg_volume_same_time_5d")),
        )
        avg_turnover = max(
            self._positive_float(symbol_meta.get("avg_turnover_5d")),
            self._positive_float(symbol_meta.get("avg_turnover_same_time_5d")),
        )
        volume_ratio = max(
            self._positive_float(symbol_meta.get("volume_ratio_5d")),
            self._positive_float(symbol_meta.get("volume_ratio_5d_same_time")),
        )
        turnover_ratio = max(
            self._positive_float(symbol_meta.get("turnover_ratio_5d")),
            self._positive_float(symbol_meta.get("turnover_ratio_5d_same_time")),
        )
        has_reference = reference_price > 0
        has_compare = any([
            detected_volume > 0,
            detected_turnover > 0,
            avg_volume > 0,
            avg_turnover > 0,
            volume_ratio > 0,
            turnover_ratio > 0,
        ])
        return has_reference and has_compare

    def _apply_metric_stats(self, symbol_meta, stats, detected_price=0.0, prefer_detected_price=False):
        stats = dict(stats or {})
        metric_mode = str(stats.get("metric_mode") or "")
        latest_day = str(stats.get("latest_day") or "")
        target_hhmm = str(stats.get("target_hhmm") or "")

        reference_price = self._positive_float(stats.get("reference_price"))
        if prefer_detected_price and detected_price > 0:
            reference_price = detected_price
        elif reference_price <= 0 and detected_price > 0:
            reference_price = detected_price
        if reference_price > 0:
            symbol_meta["reference_price"] = reference_price

        current_volume = self._positive_float(stats.get("current_volume"))
        current_turnover = self._positive_float(stats.get("current_turnover"))
        if current_volume > 0:
            symbol_meta["detected_volume"] = current_volume
        if current_turnover > 0:
            symbol_meta["detected_turnover"] = current_turnover

        avg_volume = self._positive_float(stats.get("avg_volume"))
        avg_turnover = self._positive_float(stats.get("avg_turnover"))
        volume_ratio = self._positive_float(stats.get("volume_ratio"))
        turnover_ratio = self._positive_float(stats.get("turnover_ratio"))

        if metric_mode == "full_day":
            if avg_volume > 0:
                symbol_meta["avg_volume_5d"] = avg_volume
            if avg_turnover > 0:
                symbol_meta["avg_turnover_5d"] = avg_turnover
            if volume_ratio > 0:
                symbol_meta["volume_ratio_5d"] = volume_ratio
            if turnover_ratio > 0:
                symbol_meta["turnover_ratio_5d"] = turnover_ratio
            symbol_meta["full_day_volume_compare_label"] = stats.get("volume_compare_label") or "최근 5일 일간 평균 거래량"
            symbol_meta["full_day_turnover_compare_label"] = stats.get("turnover_compare_label") or "최근 5일 일간 평균 거래대금"
            symbol_meta["volume_compare_label"] = symbol_meta.get("full_day_volume_compare_label")
            symbol_meta["turnover_compare_label"] = symbol_meta.get("full_day_turnover_compare_label")
        else:
            if avg_volume > 0:
                symbol_meta["avg_volume_same_time_5d"] = avg_volume
            if avg_turnover > 0:
                symbol_meta["avg_turnover_same_time_5d"] = avg_turnover
            if volume_ratio > 0:
                symbol_meta["volume_ratio_5d_same_time"] = volume_ratio
            if turnover_ratio > 0:
                symbol_meta["turnover_ratio_5d_same_time"] = turnover_ratio
            if (volume_ratio > 0) or (turnover_ratio > 0):
                symbol_meta["same_time_volume_compare_label"] = stats.get("volume_compare_label") or "최근 5일 동시간 평균 거래량"
                symbol_meta["same_time_turnover_compare_label"] = stats.get("turnover_compare_label") or "최근 5일 동시간 평균 거래대금"
                symbol_meta["volume_compare_label"] = symbol_meta.get("same_time_volume_compare_label")
                symbol_meta["turnover_compare_label"] = symbol_meta.get("same_time_turnover_compare_label")

        if latest_day:
            symbol_meta["metric_base_day"] = latest_day
        if target_hhmm:
            symbol_meta["target_hhmm"] = target_hhmm
        if metric_mode:
            symbol_meta["metric_mode"] = metric_mode

        return {
            "metric_mode": metric_mode,
            "latest_day": latest_day,
            "reference_price": reference_price,
            "current_volume": current_volume,
            "current_turnover": current_turnover,
            "avg_volume": avg_volume,
            "avg_turnover": avg_turnover,
            "volume_ratio": volume_ratio,
            "turnover_ratio": turnover_ratio,
        }

    def _apply_message_metric_fields(self, symbol_meta, market_open=False):
        symbol_meta = dict(symbol_meta or {})
        same_time_volume_ratio = self._positive_float(symbol_meta.get("volume_ratio_5d_same_time"))
        same_time_turnover_ratio = self._positive_float(symbol_meta.get("turnover_ratio_5d_same_time"))
        full_day_volume_ratio = self._positive_float(symbol_meta.get("volume_ratio_5d"))
        full_day_turnover_ratio = self._positive_float(symbol_meta.get("turnover_ratio_5d"))

        if market_open:
            message_metric_mode = "same_time" if (same_time_volume_ratio > 0 or same_time_turnover_ratio > 0) else "full_day"
            volume_ratio = same_time_volume_ratio or full_day_volume_ratio
            turnover_ratio = same_time_turnover_ratio or full_day_turnover_ratio
            volume_label = (
                symbol_meta.get("same_time_volume_compare_label")
                or symbol_meta.get("volume_compare_label")
                or "최근 5일 동시간 평균 거래량"
            )
            turnover_label = (
                symbol_meta.get("same_time_turnover_compare_label")
                or symbol_meta.get("turnover_compare_label")
                or "최근 5일 동시간 평균 거래대금"
            )
            if message_metric_mode == "full_day":
                volume_label = (
                    symbol_meta.get("full_day_volume_compare_label")
                    or symbol_meta.get("volume_compare_label")
                    or "최근 5일 일간 평균 거래량"
                )
                turnover_label = (
                    symbol_meta.get("full_day_turnover_compare_label")
                    or symbol_meta.get("turnover_compare_label")
                    or "최근 5일 일간 평균 거래대금"
                )
        else:
            message_metric_mode = "full_day" if (full_day_volume_ratio > 0 or full_day_turnover_ratio > 0) else "same_time"
            volume_ratio = full_day_volume_ratio or same_time_volume_ratio
            turnover_ratio = full_day_turnover_ratio or same_time_turnover_ratio
            volume_label = (
                symbol_meta.get("full_day_volume_compare_label")
                or symbol_meta.get("volume_compare_label")
                or "최근 5일 일간 평균 거래량"
            )
            turnover_label = (
                symbol_meta.get("full_day_turnover_compare_label")
                or symbol_meta.get("turnover_compare_label")
                or "최근 5일 일간 평균 거래대금"
            )
            if message_metric_mode == "same_time":
                volume_label = (
                    symbol_meta.get("same_time_volume_compare_label")
                    or symbol_meta.get("volume_compare_label")
                    or "최근 5일 동시간 평균 거래량"
                )
                turnover_label = (
                    symbol_meta.get("same_time_turnover_compare_label")
                    or symbol_meta.get("turnover_compare_label")
                    or "최근 5일 동시간 평균 거래대금"
                )

        symbol_meta["message_metric_mode"] = message_metric_mode
        symbol_meta["message_volume_ratio"] = volume_ratio
        symbol_meta["message_turnover_ratio"] = turnover_ratio
        symbol_meta["message_volume_compare_label"] = volume_label
        symbol_meta["message_turnover_compare_label"] = turnover_label
        return symbol_meta

    def search_news_for_symbol(self, code, name, trigger_type="detected", min_score=None):
        if self._is_spam(code):
            self.log_emitted.emit("🚫 스팸 종목 뉴스검색 차단: {0}".format(code))
            return []
        if min_score is None:
            min_score = self.credential_manager.get_news_send_min_score()
        self.log_emitted.emit("🔎 뉴스 검색 시작: {0} ({1}) / 경로={2} / 발송기준={3}".format(name, code, trigger_type, int(min_score)))
        allowed, skip_reason = self._should_search_now(code, trigger_type, return_reason=True)
        if not allowed:
            self.log_emitted.emit("ℹ️ 뉴스 검색 생략: {0} ({1}) / {2}".format(name, code, skip_reason or "쿨다운"))
            return []
        keys = [row for row in self.credential_manager.get_naver_keys(include_secret=True) if row.get("enabled")]
        if not keys:
            self.log_emitted.emit("⚠️ 활성 네이버 API 키가 없습니다")
            return []
        query = name
        last_error = None
        for row in keys:
            try:
                items = self._call_news_api(row, query)
                self.log_emitted.emit("📰 뉴스 응답: {0} ({1}) / {2}건".format(name, code, len(items)))
                self._mark_key_usage(row.get("key_set_id"), success=True)
                self._record_search_event(code, name, trigger_type, row.get("key_set_id"), query, len(items), "success")
                scored = self._score_articles(code, name, items)
                duplicate_count = 0
                score_fail_count = 0
                fresh = []
                for article in scored:
                    exists = self._article_exists(article["article_hash"])
                    if exists:
                        duplicate_count += 1
                        continue
                    if float(article["final_score"]) < float(min_score):
                        score_fail_count += 1
                        continue
                    fresh.append(article)
                for article in fresh:
                    self._save_article(article)
                if fresh:
                    top = fresh[:3]
                    symbol_meta = self._build_symbol_meta(code)
                    sent_ok = self.telegram_router.send_formatted_event(
                        "news_articles",
                        {
                            "channel_group": "news",
                            "code": code,
                            "name": name,
                            "trigger_type": trigger_type,
                            "articles": top,
                            "symbol_meta": symbol_meta,
                        },
                    )
                    if sent_ok:
                        self.log_emitted.emit("✅ 뉴스 텔레그램 발송: {0} ({1}) / {2}건".format(name, code, len(top)))
                    else:
                        self.log_emitted.emit("⚠️ 뉴스 텔레그램 발송 실패 또는 채널 미설정: {0} ({1})".format(name, code))
                    self.persistence.execute(
                        "UPDATE tracked_symbols SET last_news_sent_at=?, last_important_news_at=?, updated_at=? WHERE code=?",
                        (self.persistence.now_ts(), self.persistence.now_ts(), self.persistence.now_ts(), code),
                    )
                    self.news_found.emit({"code": code, "name": name, "articles": top, "trigger_type": trigger_type})
                else:
                    if not scored:
                        self.log_emitted.emit("ℹ️ 뉴스 검색 결과 없음: {0} ({1})".format(name, code))
                    elif duplicate_count == len(scored):
                        self.log_emitted.emit("ℹ️ 중복 기사만 존재: {0} ({1}) / {2}건".format(name, code, duplicate_count))
                    elif score_fail_count and (duplicate_count + score_fail_count == len(scored)):
                        fresh_candidates = [x for x in scored if not self._article_exists(x["article_hash"])]
                        best_score = max([float(x.get("final_score", 0) or 0) for x in fresh_candidates] or [0.0])
                        self.log_emitted.emit("ℹ️ 새 기사는 있으나 발송 기준 미달: {0} ({1}) / 기준={2} / 최고점수={3:.2f}".format(name, code, int(min_score), best_score))
                    else:
                        self.log_emitted.emit("ℹ️ 발송 대상 기사 없음: {0} ({1}) / 전체={2} / 중복={3} / 점수미달={4}".format(name, code, len(scored), duplicate_count, score_fail_count))
                self.persistence.execute(
                    "UPDATE tracked_symbols SET last_news_checked_at=?, updated_at=? WHERE code=?",
                    (self.persistence.now_ts(), self.persistence.now_ts(), code),
                )
                return fresh
            except Exception as exc:
                last_error = exc
                self._mark_key_usage(row.get("key_set_id"), success=False, error_message=str(exc))
                self._record_search_event(code, name, trigger_type, row.get("key_set_id"), query, 0, "error", {"error": str(exc)})
                self.log_emitted.emit("⚠️ 뉴스 검색 키 실패: {0} ({1}) / key={2} / {3}".format(name, code, row.get("key_set_no", row.get("key_set_id", "?")), exc))
                continue
        self.log_emitted.emit("❌ 뉴스 검색 실패: {0} / {1}".format(code, last_error))
        return []


    def search_news_for_symbol_async(self, code, name, trigger_type="detected", min_score=None):
        if self._is_spam(code):
            self.log_emitted.emit("NEWS SEARCH BLOCKED (SPAM): {0}".format(code))
            return []
        if min_score is None:
            min_score = self.credential_manager.get_news_send_min_score()
        self.log_emitted.emit("NEWS SEARCH QUEUED: {0} ({1}) / trigger={2} / min_score={3}".format(name, code, trigger_type, int(min_score)))
        allowed, skip_reason = self._should_search_now(code, trigger_type, return_reason=True)
        if not allowed:
            self.log_emitted.emit("NEWS SEARCH SKIPPED: {0} ({1}) / {2}".format(name, code, skip_reason or "cooldown"))
            return []
        keys = [row for row in self.credential_manager.get_naver_keys(include_secret=True) if row.get("enabled")]
        if not keys:
            self.log_emitted.emit("NEWS SEARCH FAILED: no enabled Naver API keys")
            return []
        queued = self._queue_search_job(
            {
                "code": str(code or ""),
                "name": str(name or ""),
                "trigger_type": str(trigger_type or ""),
                "min_score": float(min_score or 0),
                "keys": list(keys),
                "query": str(name or ""),
            }
        )
        if not queued:
            self.log_emitted.emit("NEWS SEARCH ALREADY QUEUED: {0} ({1}) / trigger={2}".format(name, code, trigger_type))
        return []

    def _execute_search_job(self, job):
        code = str(job.get("code", "") or "")
        name = str(job.get("name", "") or "")
        trigger_type = str(job.get("trigger_type", "") or "")
        min_score = float(job.get("min_score", 0) or 0)
        keys = list(job.get("keys") or [])
        query = str(job.get("query", name) or name)
        last_error = None

        for row in keys:
            try:
                items = self._call_news_api(row, query)
                self.log_emitted.emit("NEWS RESPONSE: {0} ({1}) / {2} items".format(name, code, len(items)))
                self._mark_key_usage(row.get("key_set_id"), success=True)
                self._record_search_event(code, name, trigger_type, row.get("key_set_id"), query, len(items), "success")
                scored = self._score_articles(code, name, items)
                duplicate_count = 0
                score_fail_count = 0
                fresh = []
                best_fresh_score = 0.0
                for article in scored:
                    exists = self._article_exists(article["article_hash"])
                    if exists:
                        duplicate_count += 1
                        continue
                    best_fresh_score = max(best_fresh_score, float(article.get("final_score", 0) or 0))
                    if float(article["final_score"]) < min_score:
                        score_fail_count += 1
                        continue
                    fresh.append(article)
                for article in fresh:
                    self._save_article(article)
                return {
                    "job_key": str(job.get("job_key", "") or ""),
                    "code": code,
                    "name": name,
                    "trigger_type": trigger_type,
                    "min_score": min_score,
                    "status": "success",
                    "fresh_articles": fresh,
                    "top_articles": fresh[:3],
                    "scored_count": len(scored),
                    "duplicate_count": duplicate_count,
                    "score_fail_count": score_fail_count,
                    "best_fresh_score": best_fresh_score,
                    "last_error": "",
                }
            except Exception as exc:
                last_error = exc
                self._mark_key_usage(row.get("key_set_id"), success=False, error_message=str(exc))
                self._record_search_event(code, name, trigger_type, row.get("key_set_id"), query, 0, "error", {"error": str(exc)})
                self.log_emitted.emit(
                    "NEWS SEARCH FAILED: {0} ({1}) / key={2} / {3}".format(
                        name,
                        code,
                        row.get("key_set_no", row.get("key_set_id", "?")),
                        exc,
                    )
                )
                continue

        return {
            "job_key": str(job.get("job_key", "") or ""),
            "code": code,
            "name": name,
            "trigger_type": trigger_type,
            "min_score": min_score,
            "status": "error",
            "fresh_articles": [],
            "top_articles": [],
            "scored_count": 0,
            "duplicate_count": 0,
            "score_fail_count": 0,
            "best_fresh_score": 0.0,
            "last_error": str(last_error or ""),
        }

    def _apply_search_result(self, result):
        result = dict(result or {})
        job_key = str(result.get("job_key", "") or "")
        if job_key:
            with self._pending_news_job_lock:
                self._pending_news_job_keys.discard(job_key)

        code = str(result.get("code", "") or "")
        name = str(result.get("name", "") or "")
        trigger_type = str(result.get("trigger_type", "") or "")
        status = str(result.get("status", "") or "")
        fresh = list(result.get("fresh_articles") or [])
        top_articles = list(result.get("top_articles") or [])
        scored_count = int(result.get("scored_count", 0) or 0)
        duplicate_count = int(result.get("duplicate_count", 0) or 0)
        score_fail_count = int(result.get("score_fail_count", 0) or 0)
        best_fresh_score = float(result.get("best_fresh_score", 0) or 0)
        now_ts = self.persistence.now_ts()

        if status != "success":
            self.log_emitted.emit("NEWS SEARCH FAILED: {0} / {1}".format(code, result.get("last_error", "")))
            return

        if fresh:
            symbol_meta = self._build_symbol_meta(code)
            sent_ok = self.telegram_router.send_formatted_event(
                "news_articles",
                {
                    "channel_group": "news",
                    "code": code,
                    "name": name,
                    "trigger_type": trigger_type,
                    "articles": top_articles,
                    "symbol_meta": symbol_meta,
                },
            )
            if sent_ok:
                self.log_emitted.emit("NEWS SENT: {0} ({1}) / {2} items".format(name, code, len(top_articles)))
            else:
                self.log_emitted.emit("NEWS SEND FAILED: {0} ({1})".format(name, code))
            self.persistence.execute(
                "UPDATE tracked_symbols SET last_news_checked_at=?, last_news_sent_at=?, last_important_news_at=?, updated_at=? WHERE code=?",
                (now_ts, now_ts, now_ts, now_ts, code),
            )
            self.news_found.emit({"code": code, "name": name, "articles": top_articles, "trigger_type": trigger_type})
            return

        if not scored_count:
            self.log_emitted.emit("NEWS EMPTY: {0} ({1})".format(name, code))
        elif duplicate_count == scored_count:
            self.log_emitted.emit("NEWS DUPLICATE ONLY: {0} ({1}) / {2} items".format(name, code, duplicate_count))
        elif score_fail_count and (duplicate_count + score_fail_count == scored_count):
            self.log_emitted.emit(
                "NEWS BELOW SCORE: {0} ({1}) / min_score={2} / best={3:.2f}".format(
                    name,
                    code,
                    int(result.get("min_score", 0) or 0),
                    best_fresh_score,
                )
            )
        else:
            self.log_emitted.emit(
                "NEWS NO SEND: {0} ({1}) / total={2} / duplicate={3} / below_score={4}".format(
                    name,
                    code,
                    scored_count,
                    duplicate_count,
                    score_fail_count,
                )
            )
        self.persistence.execute(
            "UPDATE tracked_symbols SET last_news_checked_at=?, updated_at=? WHERE code=?",
            (now_ts, now_ts, code),
        )

    def _build_symbol_meta(self, code):
        now_dt = datetime.datetime.now()
        cached_meta = self._get_cached_symbol_meta(code, now_dt)
        if cached_meta is not None:
            return cached_meta

        tracked_row = self.persistence.fetchone(
            "SELECT code, name, detected_price, extra_json, first_detected_at, last_detected_at FROM tracked_symbols WHERE code=?",
            (code,),
        )
        tracked = dict(tracked_row) if tracked_row else {"code": code}
        symbol_meta = {}
        try:
            symbol_meta.update(json.loads(tracked.get("extra_json") or "{}"))
        except Exception:
            symbol_meta = {}

        detected_price = self._positive_float(tracked.get("detected_price") or symbol_meta.get("detected_price") or symbol_meta.get("reference_price"))
        if detected_price > 0:
            symbol_meta["detected_price"] = detected_price
            if self._positive_float(symbol_meta.get("reference_price")) <= 0:
                symbol_meta["reference_price"] = detected_price

        market_open = self._is_regular_market_hours(now_dt)
        startup_grace = self._is_startup_heavy_meta_window(now_dt)
        live_quote_seed = self._get_fast_quote_seed(code)
        if detected_price <= 0 and self._positive_float(live_quote_seed.get("current_price")) > 0:
            detected_price = self._positive_float(live_quote_seed.get("current_price"))
            symbol_meta["detected_price"] = detected_price
        if self._positive_float(symbol_meta.get("reference_price")) <= 0 and detected_price > 0:
            symbol_meta["reference_price"] = detected_price
        if self._positive_float(symbol_meta.get("detected_volume")) <= 0 and self._positive_float(live_quote_seed.get("current_volume")) > 0:
            symbol_meta["detected_volume"] = self._positive_float(live_quote_seed.get("current_volume"))
        if self._positive_float(symbol_meta.get("detected_turnover")) <= 0 and self._positive_float(live_quote_seed.get("current_turnover")) > 0:
            symbol_meta["detected_turnover"] = self._positive_float(live_quote_seed.get("current_turnover"))
        if (not market_open) and self.daily_watch_snapshot_manager is not None:
            try:
                after_hours_snapshot = {}
                if self.kiwoom_client is not None and hasattr(self.kiwoom_client, "get_realtime_snapshot"):
                    after_hours_snapshot = dict(self.kiwoom_client.get_realtime_snapshot(code) or {})
                daily_entry = self.daily_watch_snapshot_manager.capture_symbol(
                    tracked_row=tracked,
                    live_snapshot=after_hours_snapshot,
                    source="after_hours_meta",
                    target_dt=now_dt,
                )
                symbol_meta.update(dict(daily_entry.get("symbol_meta") or {}))
                if self._has_usable_reference_metrics(symbol_meta):
                    symbol_meta["reference_price_basis"] = symbol_meta.get("reference_price_basis") or "daily_watch_snapshot"
            except Exception as exc:
                self.log_emitted.emit("⚠️ 장후 일일 뉴스감시 스냅샷 반영 실패: {0} / {1}".format(code, exc))

        if self.kiwoom_client and getattr(self.kiwoom_client, "connected", False):
            try:
                intraday_info = {}
                daily_info = {}
                selected_info = {}
                quote_snapshot = {}

                if startup_grace:
                    symbol_meta["reference_price_basis"] = symbol_meta.get("reference_price_basis") or (
                        "startup_cached_intraday" if market_open else "startup_cached_after_hours"
                    )
                    symbol_meta = self._apply_message_metric_fields(symbol_meta, market_open=market_open)
                    return symbol_meta

                if market_open and self._has_usable_live_reference_metrics(symbol_meta, market_open=True):
                    intraday_info = {
                        "reference_price": self._positive_float(symbol_meta.get("reference_price")),
                        "volume_ratio": self._positive_float(symbol_meta.get("volume_ratio_5d_same_time"))
                        or self._positive_float(symbol_meta.get("volume_ratio_5d")),
                        "turnover_ratio": self._positive_float(symbol_meta.get("turnover_ratio_5d_same_time"))
                        or self._positive_float(symbol_meta.get("turnover_ratio_5d")),
                        "latest_day": str(symbol_meta.get("metric_base_day") or now_dt.strftime("%Y%m%d")),
                        "metric_mode": "same_time" if (
                            self._positive_float(symbol_meta.get("volume_ratio_5d_same_time")) > 0
                            or self._positive_float(symbol_meta.get("turnover_ratio_5d_same_time")) > 0
                        ) else str(symbol_meta.get("metric_mode") or "same_time"),
                    }
                    symbol_meta["reference_price_basis"] = symbol_meta.get("reference_price_basis") or "tracked_intraday_cached"
                    selected_info = intraday_info
                elif market_open:
                    intraday_stats = dict(
                        self.kiwoom_client.request_intraday_reference_stats(
                            code,
                            target_dt=now_dt,
                            lookback_days=5,
                            timeout_ms=3200,
                            max_pages=4,
                            allow_quote_fallback=False,
                            seed_snapshot=live_quote_seed,
                        ) or {}
                    )
                    intraday_info = self._apply_metric_stats(
                        symbol_meta,
                        intraday_stats,
                        detected_price=detected_price,
                        prefer_detected_price=(str(intraday_stats.get("metric_mode") or "") == "same_time" and market_open),
                    )

                    need_daily_fallback = (
                        intraday_info.get("reference_price", 0.0) <= 0
                        or (
                            intraday_info.get("volume_ratio", 0.0) <= 0
                            and intraday_info.get("turnover_ratio", 0.0) <= 0
                        )
                        or (str(intraday_info.get("latest_day") or "") and str(intraday_info.get("latest_day") or "") != now_dt.strftime("%Y%m%d"))
                    )

                    if need_daily_fallback:
                        daily_stats = dict(self.kiwoom_client.request_daily_reference_stats(code, target_dt=now_dt, lookback_days=5) or {})
                        daily_info = self._apply_metric_stats(
                            symbol_meta,
                            daily_stats,
                            detected_price=detected_price,
                            prefer_detected_price=False,
                        )
                    selected_info = daily_info or intraday_info
                else:
                    if self._has_usable_reference_metrics(symbol_meta):
                        cached_daily_volume_ratio = self._positive_float(symbol_meta.get("volume_ratio_5d"))
                        cached_daily_turnover_ratio = self._positive_float(symbol_meta.get("turnover_ratio_5d"))
                        if cached_daily_volume_ratio <= 0 and cached_daily_turnover_ratio <= 0:
                            daily_stats = dict(
                                self.kiwoom_client.request_daily_reference_stats(
                                    code,
                                    target_dt=now_dt,
                                    lookback_days=5,
                                ) or {}
                            )
                            daily_info = self._apply_metric_stats(
                                symbol_meta,
                                daily_stats,
                                detected_price=detected_price,
                                prefer_detected_price=False,
                            )
                        else:
                            daily_info = {
                                "reference_price": self._positive_float(symbol_meta.get("reference_price")),
                                "volume_ratio": cached_daily_volume_ratio,
                                "turnover_ratio": cached_daily_turnover_ratio,
                                "latest_day": str(symbol_meta.get("metric_base_day") or now_dt.strftime("%Y%m%d")),
                                "metric_mode": "full_day",
                            }
                            symbol_meta["reference_price_basis"] = symbol_meta.get("reference_price_basis") or "tracked_reference_cached"
                        selected_info = daily_info
                    else:
                        daily_stats = dict(self.kiwoom_client.request_daily_reference_stats(code, target_dt=now_dt, lookback_days=5) or {})
                        try:
                            quote_snapshot = dict(getattr(self.kiwoom_client, "get_realtime_snapshot", lambda _code: {})(code) or {})
                        except Exception:
                            quote_snapshot = {}
                        if self._positive_float(quote_snapshot.get("current_price")) <= 0:
                            try:
                                master_last_price = self._positive_float(getattr(self.kiwoom_client, "get_master_last_price", lambda _code: 0)(code))
                            except Exception:
                                master_last_price = 0.0
                            if master_last_price > 0:
                                quote_snapshot["current_price"] = master_last_price
                        if self._positive_float(daily_stats.get("reference_price")) <= 0 and self._positive_float(quote_snapshot.get("current_price")) > 0:
                            daily_stats["reference_price"] = self._positive_float(quote_snapshot.get("current_price"))
                        if self._positive_float(daily_stats.get("current_volume")) <= 0 and self._positive_float(quote_snapshot.get("current_volume")) > 0:
                            daily_stats["current_volume"] = self._positive_float(quote_snapshot.get("current_volume"))
                        if self._positive_float(daily_stats.get("current_turnover")) <= 0 and self._positive_float(quote_snapshot.get("current_turnover")) > 0:
                            daily_stats["current_turnover"] = self._positive_float(quote_snapshot.get("current_turnover"))
                        daily_info = self._apply_metric_stats(
                            symbol_meta,
                            daily_stats,
                            detected_price=detected_price,
                            prefer_detected_price=False,
                        )
                        if self._positive_float(symbol_meta.get("reference_price")) <= 0 and self._positive_float(quote_snapshot.get("current_price")) > 0:
                            symbol_meta["reference_price"] = self._positive_float(quote_snapshot.get("current_price"))
                        symbol_meta["reference_price_basis"] = "after_hours_close_or_cached_quote"
                        selected_info = daily_info

                latest_day = str(selected_info.get("latest_day") or intraday_info.get("latest_day") or "")
                metric_mode = str(selected_info.get("metric_mode") or intraday_info.get("metric_mode") or "")
                today = now_dt.strftime("%Y%m%d")

                if latest_day and latest_day != today:
                    symbol_meta["reference_price_basis"] = "recent_business_day_close"
                elif metric_mode == "full_day" and self._positive_float(symbol_meta.get("reference_price")) > 0:
                    symbol_meta["reference_price_basis"] = symbol_meta.get("reference_price_basis") or "today_close"
                elif detected_price > 0:
                    symbol_meta["reference_price_basis"] = symbol_meta.get("reference_price_basis") or "detected_price"

                symbol_meta = self._apply_message_metric_fields(symbol_meta, market_open=market_open)

                selected_ref = self._positive_float(symbol_meta.get("reference_price"))
                selected_vol_ratio = self._positive_float(symbol_meta.get("message_volume_ratio"))
                selected_turn_ratio = self._positive_float(symbol_meta.get("message_turnover_ratio"))
                if not (selected_ref > 0 or selected_vol_ratio > 0 or selected_turn_ratio > 0):
                    self.log_emitted.emit(
                        "ℹ️ 종목 메타 fallback 사용: {0} / ref={1} / vol_ratio={2} / turn_ratio={3} / latest_day={4} / metric={5}".format(
                            code,
                            selected_ref,
                            selected_vol_ratio,
                            selected_turn_ratio,
                            latest_day or "-",
                            str(symbol_meta.get("message_metric_mode") or metric_mode or "-"),
                        )
                    )
            except Exception as exc:
                self.log_emitted.emit("⚠️ 종목 메타 계산 실패: {0} / {1}".format(code, exc))
        symbol_meta = self._apply_message_metric_fields(symbol_meta, market_open=market_open)
        self._set_cached_symbol_meta(code, symbol_meta, now_dt)
        return symbol_meta


    def test_api_key(self, client_id, client_secret):
        client_id = str(client_id or '').strip()
        client_secret = str(client_secret or '').strip()
        if not client_id or not client_secret:
            return {"ok": False, "message": "Client ID/Secret 미입력", "key_detected": False, "status_code": 0}
        try:
            items = self._call_news_api({"client_id": client_id, "client_secret": client_secret}, "삼성전자", display=1)
            return {"ok": True, "message": "API 인증 성공", "key_detected": True, "result_count": len(items or []), "status_code": 200}
        except HTTPError as exc:
            status_code = 0
            try:
                status_code = int(exc.response.status_code)
            except Exception:
                pass
            return {"ok": False, "message": str(exc), "key_detected": True, "status_code": status_code}
        except Exception as exc:
            return {"ok": False, "message": str(exc), "key_detected": True, "status_code": 0}

    def _mark_key_usage(self, key_set_id, success=True, error_message=""):
        if not key_set_id:
            return
        row = self.persistence.fetchone("SELECT daily_used, daily_error_count FROM naver_api_keys WHERE key_set_id=?", (int(key_set_id),))
        if not row:
            return
        self.persistence.execute(
            "UPDATE naver_api_keys SET daily_used=?, daily_error_count=?, last_used_at=?, last_error=?, updated_at=? WHERE key_set_id=?",
            (
                int(row["daily_used"] or 0) + 1,
                int(row["daily_error_count"] or 0) + (0 if success else 1),
                self.persistence.now_ts(),
                "" if success else error_message,
                self.persistence.now_ts(),
                int(key_set_id),
            ),
        )

    def _call_news_api(self, key_row, query, display=5):
        url = "https://openapi.naver.com/v1/search/news.json"
        headers = {
            "X-Naver-Client-Id": key_row.get("client_id", ""),
            "X-Naver-Client-Secret": key_row.get("client_secret", ""),
        }
        params = {
            "query": query,
            "display": int(display),
            "sort": "date",
        }
        response = requests.get(url, headers=headers, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])

    def _score_articles(self, code, name, items):
        scored = []
        same_minute_counter = {}
        ai_candidate_count = 0
        theme_hint_meta = self._load_symbol_theme_meta(code)
        dart_signal = self._load_dart_signal(code, name)
        for item in items:
            title = item.get("title", "")
            description = item.get("description", "")
            original_link = item.get("originallink") or item.get("link") or ""
            pub_date = item.get("pubDate", "")
            minute_key = pub_date[:16] if pub_date else ""
            same_minute_counter[minute_key] = same_minute_counter.get(minute_key, 0) + 1
            duplicate_count = same_minute_counter[minute_key]
            article_hash = self._hash(original_link or title)
            analysis_context = {
                "code": code,
                "name": name,
                "title": title,
                "description": description,
                "original_link": original_link,
                "published_at": pub_date,
                "duplicate_count": duplicate_count,
            }
            base_analysis = self._fallback_rule_analysis(analysis_context)
            if self.analysis_manager:
                allow_ai = (not self._article_exists(article_hash)) and (ai_candidate_count < 3)
                analysis = self.analysis_manager.analyze_article(analysis_context, base_result=base_analysis, allow_ai=allow_ai)
                if allow_ai and str(analysis.get("analysis_source", "rule") or "rule") == "gpt":
                    ai_candidate_count += 1
            else:
                analysis = base_analysis
            importance_score = int(analysis.get("importance_score", 0) or 0)
            confidence_score = int(analysis.get("certainty_score", analysis.get("confidence_score", 0)) or 0)
            actionability_score = int(analysis.get("actionability_score", 0) or 0)
            novelty_score = int(analysis.get("novelty_score", 0) or 0)
            frequency_score = int(analysis.get("frequency_score", min(100, duplicate_count * 20)) or min(100, duplicate_count * 20))
            final_score = float(analysis.get("final_news_score", 0) or 0)
            direction = str(analysis.get("direction", "neutral") or "neutral")
            event_type = str(analysis.get("event_type", "general") or "general")
            theme_info = self.theme_resolver.resolve(
                code=code,
                name=name,
                title=title,
                description=description,
                symbol_meta=theme_hint_meta,
            )
            scored.append(
                {
                    "article_hash": article_hash,
                    "trade_date": self.persistence.today_str(),
                    "code": code,
                    "name": name,
                    "article_title": title,
                    "article_url": original_link,
                    "press_name": self._extract_press_name(original_link),
                    "published_at": pub_date,
                    "summary_text": description,
                    "importance_score": importance_score,
                    "frequency_score": frequency_score,
                    "final_score": final_score,
                    "sent_channels_json": json.dumps([], ensure_ascii=False),
                    "extra_json": json.dumps({
                        "analysis_source": analysis.get("analysis_source", "rule"),
                        "analysis_model": analysis.get("analysis_model", "rule-engine-v1"),
                        "analysis_label": analysis.get("analysis_label", "규칙점수 분석"),
                        "event_type": event_type,
                        "direction": direction,
                        "confidence_score": confidence_score,
                        "certainty_score": confidence_score,
                        "actionability_score": actionability_score,
                        "novelty_score": novelty_score,
                        "trade_action": analysis.get("trade_action", ""),
                        "time_horizon": analysis.get("time_horizon", ""),
                        "novelty_type": analysis.get("novelty_type", ""),
                        "themes": theme_info.get("themes", []),
                        "primary_theme": theme_info.get("primary_theme", ""),
                        "theme_summary": theme_info.get("theme_summary", ""),
                        "event_themes": theme_info.get("event_themes", []),
                        "event_theme": theme_info.get("event_theme", ""),
                        "event_theme_summary": theme_info.get("event_theme_summary", ""),
                        "theme_source": theme_info.get("theme_source", ""),
                        "theme_hits": theme_info.get("dynamic_hits", []),
                        "event_theme_hits": theme_info.get("event_hits", []),
                        "brief_reason": analysis.get("brief_reason", self._build_reason(event_type, direction, final_score)),
                        "risk_note": analysis.get("risk_note", ""),
                        "dart_signal": dart_signal,
                    }, ensure_ascii=False),
                }
            )
        return sorted(scored, key=lambda x: x["final_score"], reverse=True)

    def _load_dart_signal(self, code, name):
        if self.dart_analysis_manager is None:
            return {}
        try:
            cfg = self.credential_manager.get_dart_api(include_key=True)
        except Exception:
            cfg = {"enabled": False, "api_key": ""}
        if (not bool(cfg.get("enabled"))) or (not str(cfg.get("api_key", "") or "").strip()):
            return {}
        try:
            result = self.dart_analysis_manager.analyze_stock(
                name=name,
                code=code,
                days=180,
                allow_ai=True,
                use_cache=True,
                max_age_minutes=30,
            )
            if not result:
                return {}
            payload = {
                "warning_level": str(result.get("warning_level", "") or ""),
                "warning_score": float(result.get("warning_score", 0) or 0),
                "warning_summary": str(result.get("warning_summary", "") or ""),
                "evidence": list(result.get("evidence") or []),
                "mezzanine_flag": int(result.get("mezzanine_flag", 0) or 0),
                "dilution_flag": int(result.get("dilution_flag", 0) or 0),
                "overhang_flag": int(result.get("overhang_flag", 0) or 0),
                "association_flag": int(result.get("association_flag", 0) or 0),
                "control_change_flag": int(result.get("control_change_flag", 0) or 0),
            }
            gpt_result = dict(result.get("gpt_analysis") or {})
            if gpt_result:
                payload["gpt_summary"] = str(gpt_result.get("summary", "") or "")
                payload["gpt_risk_level"] = str(gpt_result.get("risk_level", "") or "")
                payload["gpt_evidence"] = list(gpt_result.get("evidence") or [])
            return payload
        except Exception as exc:
            self.log_emitted.emit(u"⚠️ DART 징후 분석 실패: {0} / {1}".format(code, exc))
            return {}

    def _load_symbol_theme_meta(self, code):
        row = self.persistence.fetchone(
            "SELECT extra_json FROM tracked_symbols WHERE code=?",
            (str(code or "").strip(),),
        )
        if not row:
            return {}
        try:
            extra = json.loads(row["extra_json"] or "{}")
        except Exception:
            extra = {}
        if not isinstance(extra, dict):
            return {}
        return {
            "themes": list(extra.get("themes") or []),
            "theme_tags": list(extra.get("theme_tags") or []),
        }

    def _fallback_rule_analysis(self, context):
        title = context.get("title", "")
        description = context.get("description", "")
        original_link = context.get("original_link", "")
        duplicate_count = int(context.get("duplicate_count", 1) or 1)
        importance_score = self._calc_importance_score(title, description)
        confidence_score = self._calc_confidence_score(title, description, original_link)
        novelty_score = max(0, min(100, 80 - max(0, duplicate_count - 1) * 10))
        frequency_score = min(100, duplicate_count * 20)
        direction = self._infer_direction(title, description)
        event_type = self._infer_event_type(title, description)
        actionability_score = self._calc_actionability_score(title, description, event_type, direction, confidence_score)
        base_score = (
            float(importance_score or 0) * 0.40 +
            float(actionability_score or 0) * 0.35 +
            float(confidence_score or 0) * 0.15 +
            float(novelty_score or 0) * 0.10
        )
        final_score = round(base_score, 2)
        if actionability_score < 45:
            final_score = min(final_score, 49.0)
        elif actionability_score < 60:
            final_score = min(final_score, 64.0)
        if confidence_score < 50:
            final_score = min(final_score, 59.0)
        elif confidence_score < 60:
            final_score = min(final_score, 74.0)
        if direction == "bearish":
            trade_action = "risk_only" if confidence_score >= 55 else "ignore"
        elif direction != "bullish":
            trade_action = "watch" if actionability_score >= 65 and confidence_score >= 60 else "ignore"
        elif actionability_score >= 80 and confidence_score >= 70:
            trade_action = "buy_now"
        elif actionability_score >= 65 and confidence_score >= 60:
            trade_action = "watch_breakout"
        elif actionability_score >= 50:
            trade_action = "watch_pullback"
        else:
            trade_action = "watch"
        if event_type in ["mna", "approval", "earnings", "contract"]:
            time_horizon = "multi_day" if actionability_score >= 60 else "overnight"
        elif event_type in ["investment", "buyback", "clinical"]:
            time_horizon = "overnight" if actionability_score >= 55 else "intraday"
        else:
            time_horizon = "intraday" if actionability_score >= 60 else "overnight"
        if novelty_score >= 80:
            novelty_type = "new_fact"
        elif novelty_score >= 60:
            novelty_type = "update"
        else:
            novelty_type = "recap"
        return {
            "analysis_source": "rule",
            "analysis_model": "rule-engine-v1",
            "analysis_label": "규칙점수 분석",
            "event_type": event_type,
            "direction": direction,
            "importance_score": importance_score,
            "confidence_score": confidence_score,
            "certainty_score": confidence_score,
            "actionability_score": actionability_score,
            "novelty_score": novelty_score,
            "frequency_score": frequency_score,
            "final_news_score": final_score,
            "trade_action": trade_action,
            "time_horizon": time_horizon,
            "novelty_type": novelty_type,
            "recheck_needed": final_score >= 80.0 and direction == "bullish" and actionability_score >= 60 and confidence_score >= 60,
            "brief_reason": self._build_reason(event_type, direction, final_score),
            "risk_note": u"확정성 낮음\n원문 확인 필요" if confidence_score < 60 else (u"중복 가능성 있음\n재료 지속성 약할 수 있음" if novelty_score < 60 else (u"악재 해석 가능\n변동성 큼" if direction == "bearish" else u"선반영 가능성 있음")),
        }

    def _calc_importance_score(self, title, description):
        text = u"{0} {1}".format(title, description)
        positive_keywords = [u"공급", u"계약", u"수주", u"실적", u"흑자", u"투자", u"인수", u"합병", u"임상", u"승인", u"자사주"]
        negative_keywords = [u"유상증자", u"전환사채", u"소송", u"거래정지", u"관리종목", u"적자", u"감자"]
        score = 40
        for keyword in positive_keywords:
            if keyword in text:
                score += 12
        for keyword in negative_keywords:
            if keyword in text:
                score -= 10
        return max(0, min(100, score))

    def _calc_confidence_score(self, title, description, original_link):
        score = 40
        if title:
            score += 5
        if description:
            score += 10
        if original_link and original_link.startswith("http"):
            score += 10
        if len((description or "").strip()) >= 20:
            score += 10
        vague_keywords = [u"전망", u"가능성", u"기대", u"추정"]
        text = u"{0} {1}".format(title, description)
        for keyword in vague_keywords:
            if keyword in text:
                score -= 12
        if u"공시" in text or u"확정" in text or u"체결" in text or u"결정" in text:
            score += 12
        if u"검토" in text or u"추진" in text or u"가능성" in text or u"예정" in text:
            score -= 10
        return max(0, min(100, score))

    def _calc_actionability_score(self, title, description, event_type, direction, confidence_score):
        text = u"{0} {1}".format(title, description)
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
        if float(confidence_score or 0) < 55:
            score -= 10
        return max(0, min(100, score))

    def _infer_direction(self, title, description):
        text = u"{0} {1}".format(title, description)
        positive_keywords = [u"공급", u"계약", u"수주", u"흑자", u"실적 개선", u"투자", u"승인", u"자사주"]
        negative_keywords = [u"유상증자", u"전환사채", u"소송", u"거래정지", u"관리종목", u"적자", u"감자"]
        pos = sum(1 for k in positive_keywords if k in text)
        neg = sum(1 for k in negative_keywords if k in text)
        if pos > neg:
            return "bullish"
        if neg > pos:
            return "bearish"
        return "neutral"

    def _infer_event_type(self, title, description):
        text = u"{0} {1}".format(title, description)
        mapping = [
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
        for keyword, event_type in mapping:
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
        try:
            score = float(final_score or 0)
        except Exception:
            score = 0.0
        event_label = self._event_label(event_type)
        direction = str(direction or "neutral").strip().lower()

        if direction == "bullish":
            if score >= 90:
                return u"{0} 강한 매수 후보".format(event_label)
            if score >= 80:
                return u"{0} 매수 후보".format(event_label)
            if score >= 70:
                return u"{0} 매수 우호 검토".format(event_label)
            if score >= 60:
                return u"{0} 관찰 대상".format(event_label)
            return u"{0} 필터 미통과".format(event_label)

        if direction == "bearish":
            if score >= 85:
                return u"{0} 악재성 중요뉴스".format(event_label)
            if score >= 70:
                return u"{0} 악재 경계".format(event_label)
            if score >= 60:
                return u"{0} 악재 관찰".format(event_label)
            return u"{0} 약한 악재".format(event_label)

        if score >= 85:
            return u"{0} 중요뉴스 재평가".format(event_label)
        if score >= 70:
            return u"{0} 중립 검토".format(event_label)
        if score >= 60:
            return u"{0} 관찰 대상".format(event_label)
        return u"{0} 필터 미통과".format(event_label)

    def _hash(self, value):
        return hashlib.sha256((value or "").encode("utf-8")).hexdigest()

    def _extract_press_name(self, url):
        if not url:
            return ""
        try:
            return url.split("/")[2]
        except Exception:
            return ""

    def _article_exists(self, article_hash):
        row = self.persistence.fetchone("SELECT article_hash FROM news_articles WHERE article_hash=?", (article_hash,))
        return row is not None

    def _save_article(self, article):
        self.persistence.execute(
            """
            INSERT INTO news_articles (
                article_hash, trade_date, code, name, article_title, article_url,
                press_name, published_at, summary_text, importance_score, frequency_score,
                final_score, is_sent, sent_channels_json, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                article["article_hash"], article["trade_date"], article["code"], article["name"],
                article["article_title"], article["article_url"], article["press_name"], article["published_at"],
                article["summary_text"], article["importance_score"], article["frequency_score"], article["final_score"],
                article["sent_channels_json"], article["extra_json"],
            ),
        )

    def _record_search_event(self, code, name, trigger_type, key_set_id, query_text, result_count, status, extra=None):
        extra = extra or {}
        self.persistence.execute(
            """
            INSERT INTO news_search_events (
                trade_date, ts, code, name, trigger_type, trigger_condition_slot,
                query_text, key_set_id, result_count, status, extra_json
            ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
            """,
            (
                self.persistence.today_str(),
                self.persistence.now_ts(),
                code,
                name,
                trigger_type,
                query_text,
                key_set_id,
                result_count,
                status,
                json.dumps(extra, ensure_ascii=False),
            ),
        )
        self.persistence.write_event("news_search_" + status, {"code": code, "trigger_type": trigger_type, "query": query_text})

    def _is_spam(self, code):
        row = self.persistence.fetchone("SELECT code FROM spam_symbols WHERE code=?", (code,))
        return row is not None

    def _should_search_now(self, code, trigger_type, return_reason=False):
        row = self.persistence.fetchone("SELECT is_holding, last_news_checked_at, last_detected_at FROM tracked_symbols WHERE code=?", (code,))
        if not row:
            return (True, "") if return_reason else True
        last_checked = self._parse_dt(row["last_news_checked_at"])
        if last_checked is None:
            return (True, "") if return_reason else True
        now = datetime.datetime.now()
        cooldown = datetime.timedelta(minutes=5)
        if trigger_type in ["holding", "important_news"] or int(row["is_holding"] or 0):
            cooldown = datetime.timedelta(minutes=2)
        elif trigger_type in ["tracked", "manual_recheck"]:
            cooldown = datetime.timedelta(minutes=5)
        elapsed = now - last_checked
        allowed = elapsed >= cooldown
        if return_reason:
            remain = cooldown - elapsed
            if allowed:
                return True, ""
            remain_sec = max(1, int(remain.total_seconds()))
            return False, "쿨다운 중 ({0}s 남음)".format(remain_sec)
        return allowed

    def _parse_dt(self, value):
        if not value:
            return None
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def get_latest_news_scores(self, code):
        row = self.persistence.fetchone(
            "SELECT importance_score, frequency_score, final_score, published_at, article_title FROM news_articles WHERE code=? ORDER BY final_score DESC, published_at DESC LIMIT 1",
            (code,),
        )
        if not row:
            return {"importance_score": 0, "frequency_score": 0, "final_score": 0}
        return {
            "importance_score": float(row["importance_score"] or 0),
            "frequency_score": float(row["frequency_score"] or 0),
            "final_score": float(row["final_score"] or 0),
            "published_at": row["published_at"] or "",
            "article_title": row["article_title"] or "",
        }

    def queue_recheck(self, code, reason, priority=50):
        tracked = self.persistence.fetchone(
            "SELECT is_holding FROM tracked_symbols WHERE code=? LIMIT 1",
            (str(code or ""),),
        )
        if tracked and int(tracked["is_holding"] or 0):
            return
        existing = self.persistence.fetchone(
            "SELECT queue_id FROM news_recheck_queue WHERE code=? AND reason=? AND status='pending' LIMIT 1",
            (code, reason),
        )
        if existing:
            return
        self.persistence.execute(
            "INSERT INTO news_recheck_queue (code, reason, scheduled_at, priority, status, extra_json) VALUES (?, ?, ?, ?, 'pending', '{}')",
            (code, reason, self.persistence.now_ts(), int(priority)),
        )

    def process_recheck_queue(self, limit=20):
        rows = self.persistence.fetchall(
            "SELECT * FROM news_recheck_queue WHERE status='pending' ORDER BY priority DESC, scheduled_at ASC LIMIT ?",
            (int(limit),),
        )
        for row in rows:
            symbol = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (row["code"],))
            if not symbol:
                self.persistence.execute("UPDATE news_recheck_queue SET status='missing' WHERE queue_id=?", (row["queue_id"],))
                continue
            if int(symbol["is_holding"] or 0):
                self.persistence.execute("UPDATE news_recheck_queue SET status='skipped' WHERE queue_id=?", (row["queue_id"],))
                continue
            self.search_news_for_symbol_async(symbol["code"], symbol["name"], trigger_type=row["reason"], min_score=None)
            self.persistence.execute("UPDATE news_recheck_queue SET status='done' WHERE queue_id=?", (row["queue_id"],))

    def schedule_periodic_checks(self):
        tracked = self.persistence.fetchall(
            "SELECT code, name, is_holding, last_detected_at, last_news_checked_at, is_spam FROM tracked_symbols"
        )
        now = datetime.datetime.now()
        for row in tracked:
            if int(row["is_spam"] or 0):
                continue
            if int(row["is_holding"] or 0):
                continue
            priority = 50
            reason = "tracked"
            last_detected = self._parse_dt(row["last_detected_at"])
            if last_detected is not None and (now - last_detected) > datetime.timedelta(days=1):
                priority = 20
            self.queue_recheck(row["code"], reason, priority)
