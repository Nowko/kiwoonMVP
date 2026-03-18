# -*- coding: utf-8 -*-
import datetime
import json
import time

from PyQt5.QtCore import QObject, pyqtSignal, QTimer


class ConditionCatalogManager(QObject):
    catalog_changed = pyqtSignal()
    slots_changed = pyqtSignal()
    tracked_symbol_changed = pyqtSignal()
    symbol_detected = pyqtSignal(dict)
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, kiwoom_client, daily_watch_snapshot_manager=None, parent=None):
        super(ConditionCatalogManager, self).__init__(parent)
        self.persistence = persistence
        self.kiwoom_client = kiwoom_client
        self.daily_watch_snapshot_manager = daily_watch_snapshot_manager
        self._pending_snapshot_jobs = []
        self._pending_snapshot_job_keys = set()
        self._snapshot_refresh_last_ts = {}
        self._snapshot_refresh_cooldown_sec = 90.0
        self._initial_tr_condition_priority_count = 3
        self._startup_background_mode_until = 0.0
        self._realtime_start_queue = []
        self._realtime_start_attempts = {}
        self._realtime_start_timer = QTimer(self)
        self._realtime_start_timer.setSingleShot(True)
        self._realtime_start_timer.timeout.connect(self._process_next_realtime_slot_start)
        self._snapshot_job_timer = QTimer(self)
        self._snapshot_job_timer.setSingleShot(True)
        self._snapshot_job_timer.timeout.connect(self._process_next_snapshot_job)
        self.kiwoom_client.conditions_loaded.connect(self._on_conditions_loaded)
        self.kiwoom_client.condition_event_received.connect(self._on_condition_event)

    def request_load_conditions(self):
        return self.kiwoom_client.load_conditions()

    def get_pending_snapshot_job_count(self):
        return int(len(self._pending_snapshot_jobs or []))

    def set_startup_background_mode(self, enabled=True, duration_sec=120):
        if enabled:
            self._startup_background_mode_until = time.monotonic() + max(10.0, float(duration_sec or 120))
        else:
            self._startup_background_mode_until = 0.0

    def _is_startup_background_mode(self):
        return time.monotonic() < float(self._startup_background_mode_until or 0.0)

    def _on_conditions_loaded(self, rows):
        now = self.persistence.now_ts()
        for row in rows:
            self.persistence.execute(
                """
                INSERT INTO condition_catalog (condition_id, condition_index, condition_name, is_available, extra_json, created_at, updated_at)
                VALUES (?, ?, ?, 1, '{}', ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    condition_index=excluded.condition_index,
                    condition_name=excluded.condition_name,
                    updated_at=excluded.updated_at
                """,
                (row["condition_id"], row["condition_index"], row["condition_name"], now, now),
            )
        self.catalog_changed.emit()

    def get_catalog(self):
        return self.persistence.fetchall("SELECT * FROM condition_catalog ORDER BY condition_index")

    def get_slots(self):
        sql = """
        SELECT s.slot_no, s.condition_id, s.is_enabled, s.is_realtime, s.current_count, s.last_event_at,
               c.condition_index, c.condition_name
        FROM active_condition_slots s
        LEFT JOIN condition_catalog c ON s.condition_id = c.condition_id
        ORDER BY s.slot_no
        """
        return self.persistence.fetchall(sql)

    def find_slot_by_condition_id(self, condition_id, exclude_slot_no=None):
        sql = """
        SELECT s.slot_no, s.condition_id, c.condition_index, c.condition_name
        FROM active_condition_slots s
        LEFT JOIN condition_catalog c ON s.condition_id = c.condition_id
        WHERE s.condition_id=?
        """
        params = [condition_id]
        if exclude_slot_no is not None:
            sql += " AND s.slot_no<>?"
            params.append(int(exclude_slot_no))
        sql += " ORDER BY s.slot_no LIMIT 1"
        return self.persistence.fetchone(sql, tuple(params))

    def assign_condition_to_slot(self, slot_no, condition_id):
        slot_no = int(slot_no)
        current = self.persistence.fetchone(
            "SELECT slot_no, condition_id FROM active_condition_slots WHERE slot_no=?",
            (slot_no,),
        )
        if current and str(current["condition_id"] or "") == str(condition_id or ""):
            self.log_emitted.emit("ℹ️ 슬롯 {0}에는 이미 동일한 조건식이 배치되어 있습니다".format(slot_no))
            return False

        duplicate = self.find_slot_by_condition_id(condition_id, exclude_slot_no=slot_no)
        if duplicate:
            self.log_emitted.emit(
                "⚠️ 조건식 중복 배치 차단: 슬롯 {0} / 이미 슬롯 {1} 사용 중".format(
                    slot_no,
                    int(duplicate["slot_no"] or 0),
                )
            )
            return False

        now = self.persistence.now_ts()
        self.persistence.execute(
            "UPDATE active_condition_slots SET condition_id=?, updated_at=? WHERE slot_no=?",
            (condition_id, now, slot_no),
        )
        self.log_emitted.emit("✅ 슬롯 {0}에 조건식 배치: {1}".format(slot_no, condition_id))
        self.slots_changed.emit()
        return True

    def clear_slot(self, slot_no):
        slot_no = int(slot_no)
        self._realtime_start_queue = [item for item in list(self._realtime_start_queue or []) if int(item.get("slot_no") or 0) != slot_no]
        self._realtime_start_attempts.pop(slot_no, None)
        now = self.persistence.now_ts()
        self.persistence.execute(
            "UPDATE active_condition_slots SET condition_id=NULL, is_enabled=0, is_realtime=0, current_count=0, last_event_at=NULL, updated_at=? WHERE slot_no=?",
            (now, slot_no),
        )
        self.log_emitted.emit("🧹 슬롯 {0} 비움".format(slot_no))
        self.slots_changed.emit()

    def set_slot_enabled(self, slot_no, enabled, realtime):
        now = self.persistence.now_ts()
        self.persistence.execute(
            "UPDATE active_condition_slots SET is_enabled=?, is_realtime=?, updated_at=? WHERE slot_no=?",
            (1 if enabled else 0, 1 if realtime else 0, now, int(slot_no)),
        )
        self.slots_changed.emit()

    def set_slot_realtime_status(self, slot_no, realtime):
        now = self.persistence.now_ts()
        self.persistence.execute(
            "UPDATE active_condition_slots SET is_realtime=?, updated_at=? WHERE slot_no=?",
            (1 if realtime else 0, now, int(slot_no)),
        )
        self.slots_changed.emit()

    def _queue_realtime_slot_start(self, slot_no, attempt=1, delay_ms=250):
        slot_no = int(slot_no or 0)
        if slot_no <= 0:
            return
        for item in list(self._realtime_start_queue or []):
            if int(item.get("slot_no") or 0) == slot_no:
                if int(item.get("attempt") or 0) >= int(attempt or 0):
                    return
                item["attempt"] = int(attempt or 1)
                break
        else:
            self._realtime_start_queue.append({
                "slot_no": slot_no,
                "attempt": int(attempt or 1),
            })
        if not self._realtime_start_timer.isActive():
            self._realtime_start_timer.start(max(0, int(delay_ms or 0)))

    def _process_next_realtime_slot_start(self):
        if not self._realtime_start_queue:
            return
        item = self._realtime_start_queue.pop(0)
        slot_no = int(item.get("slot_no") or 0)
        attempt = int(item.get("attempt") or 1)
        self._start_realtime_slot_now(slot_no, attempt=attempt)
        if self._realtime_start_queue:
            self._realtime_start_timer.start(350)

    def start_realtime_slot(self, slot_no):
        self._queue_realtime_slot_start(slot_no, attempt=1, delay_ms=0)
        return True

    def _start_realtime_slot_now(self, slot_no, attempt=1):
        row = self.persistence.fetchone(
            """
            SELECT s.slot_no, s.condition_id, c.condition_index, c.condition_name
            FROM active_condition_slots s
            LEFT JOIN condition_catalog c ON s.condition_id = c.condition_id
            WHERE s.slot_no=?
            """,
            (int(slot_no),),
        )
        if not row or not row["condition_id"]:
            self.log_emitted.emit("⚠️ 슬롯 {0}에 조건식이 없습니다".format(slot_no))
            return False
        screen_no = "51{0:02d}".format(int(slot_no))
        ok = self.kiwoom_client.send_condition(screen_no, row["condition_name"], row["condition_index"], 1)
        if ok:
            self._realtime_start_attempts.pop(int(slot_no), None)
            self.set_slot_enabled(slot_no, True, True)
        else:
            self._realtime_start_attempts[int(slot_no)] = int(attempt or 1)
            self.set_slot_realtime_status(slot_no, False)
            if int(attempt or 1) < 3:
                self.log_emitted.emit("🔁 조건검색 재시도 예약: 슬롯 {0} / {1}회차".format(slot_no, int(attempt or 1) + 1))
                self._queue_realtime_slot_start(slot_no, attempt=int(attempt or 1) + 1, delay_ms=1200)
        return ok

    def stop_realtime_slot(self, slot_no):
        slot_no = int(slot_no)
        self._realtime_start_queue = [item for item in list(self._realtime_start_queue or []) if int(item.get("slot_no") or 0) != slot_no]
        self._realtime_start_attempts.pop(slot_no, None)
        row = self.persistence.fetchone(
            """
            SELECT s.slot_no, s.condition_id, c.condition_index, c.condition_name
            FROM active_condition_slots s
            LEFT JOIN condition_catalog c ON s.condition_id = c.condition_id
            WHERE s.slot_no=?
            """,
            (slot_no,),
        )
        if not row or not row["condition_id"]:
            return False
        screen_no = "51{0:02d}".format(slot_no)
        ok = self.kiwoom_client.stop_condition(screen_no, row["condition_name"], row["condition_index"])
        if ok:
            self.set_slot_realtime_status(slot_no, False)
        return ok

    def export_slot_profile(self):
        rows = self.get_slots()
        data = []
        for row in rows:
            data.append({
                "slot_no": int(row["slot_no"] or 0),
                "condition_name": row["condition_name"] or "",
                "condition_index": int(row["condition_index"] or 0),
                "is_enabled": 1 if int(row["is_enabled"] or 0) else 0,
                "is_realtime": 1 if int(row["is_realtime"] or 0) else 0,
            })
        return data

    def reset_slots(self):
        now = self.persistence.now_ts()
        self._realtime_start_queue = []
        self._realtime_start_attempts = {}
        self._realtime_start_timer.stop()
        for slot_no in range(1, 11):
            self.persistence.execute(
                "UPDATE active_condition_slots SET condition_id=NULL, is_enabled=0, is_realtime=0, current_count=0, last_event_at=NULL, updated_at=? WHERE slot_no=?",
                (now, slot_no),
            )
            self.persistence.execute(
                "DELETE FROM slot_strategy_policy WHERE slot_no=?",
                (slot_no,),
            )
        self.slots_changed.emit()

    def import_slot_profile(self, items):
        items = items or []
        now = self.persistence.now_ts()
        self._realtime_start_queue = []
        self._realtime_start_attempts = {}
        self._realtime_start_timer.stop()
        catalog = self.get_catalog()
        by_name = dict((str(row["condition_name"] or ""), row) for row in catalog)
        by_index = dict((int(row["condition_index"] or 0), row) for row in catalog)
        current_slots = self.get_slots()
        for current in current_slots:
            if int(current["is_realtime"] or 0):
                try:
                    self.stop_realtime_slot(int(current["slot_no"] or 0))
                except Exception:
                    pass
        # clear current assignments
        for slot_no in range(1, 11):
            self.persistence.execute(
                "UPDATE active_condition_slots SET condition_id=NULL, is_enabled=0, is_realtime=0, current_count=0, last_event_at=NULL, updated_at=? WHERE slot_no=?",
                (now, slot_no),
            )
            self.persistence.execute(
                "DELETE FROM slot_strategy_policy WHERE slot_no=?",
                (slot_no,),
            )
        restore_realtime = []
        for item in items:
            slot_no = int(item.get("slot_no", 0) or 0)
            if slot_no < 1 or slot_no > 10:
                continue
            catalog_row = None
            name = str(item.get("condition_name", "") or "")
            index = int(item.get("condition_index", 0) or 0)
            if name and name in by_name:
                catalog_row = by_name.get(name)
            elif index and index in by_index:
                catalog_row = by_index.get(index)
            if not catalog_row:
                self.log_emitted.emit("⚠️ 저장된 조건식을 현재 로그인 ID에서 찾지 못했습니다: 슬롯 {0} / {1}".format(slot_no, name or index))
                continue
            self.persistence.execute(
                "UPDATE active_condition_slots SET condition_id=?, is_enabled=?, is_realtime=?, current_count=0, last_event_at=NULL, updated_at=? WHERE slot_no=?",
                (catalog_row["condition_id"], 1 if int(item.get("is_enabled", 0) or 0) else 0, 0, now, slot_no),
            )
            if int(item.get("is_realtime", 0) or 0):
                restore_realtime.append(slot_no)
        self.slots_changed.emit()
        for slot_no in restore_realtime:
            self._queue_realtime_slot_start(slot_no, attempt=1, delay_ms=250)

    def _find_slot_by_condition_index(self, condition_index):
        return self.persistence.fetchone(
            """
            SELECT s.slot_no, s.condition_id, c.condition_name
            FROM active_condition_slots s
            LEFT JOIN condition_catalog c ON s.condition_id = c.condition_id
            WHERE c.condition_index=?
            """,
            (int(condition_index),),
        )

    def _on_condition_event(self, payload):
        if payload.get("source") == "tr_condition":
            self._handle_tr_condition(payload)
        elif payload.get("source") == "real_condition":
            self._handle_real_condition(payload)

    def _handle_tr_condition(self, payload):
        slot = self._find_slot_by_condition_index(payload.get("condition_index"))
        slot_no = int(slot["slot_no"]) if slot else 0
        codes = payload.get("codes", [])
        priority_count = max(0, int(self._initial_tr_condition_priority_count or 0))
        self.persistence.execute(
            "UPDATE active_condition_slots SET current_count=?, last_event_at=?, updated_at=? WHERE slot_no=?",
            (len(codes), self.persistence.now_ts(), self.persistence.now_ts(), slot_no),
        )
        if len(codes) > priority_count > 0:
            self.log_emitted.emit(
                "⏳ 초기 편입 후속처리 분산: 슬롯 {0} / 즉시 {1}건, 지연 {2}건".format(
                    slot_no,
                    min(len(codes), priority_count),
                    max(0, len(codes) - priority_count),
                )
            )
        for index, code in enumerate(codes):
            symbol_row = self._upsert_tracked_symbol(code, slot_no, payload.get("condition_name", ""), "DETECTED", refresh_reference=False)
            delay_ms = 0
            if priority_count > 0 and index >= priority_count:
                delay_ms = 2500 + ((index - priority_count) * 700)
            self._enqueue_snapshot_detection_job(
                code,
                slot_no,
                payload.get("condition_name", ""),
                delay_ms=delay_ms,
            )
            if not symbol_row:
                continue
        self.slots_changed.emit()
        self.tracked_symbol_changed.emit()

    def _handle_real_condition(self, payload):
        slot = self._find_slot_by_condition_index(payload.get("condition_index"))
        slot_no = int(slot["slot_no"]) if slot else 0
        event_type = payload.get("event_type", "")
        code = payload.get("code", "")
        condition_name = payload.get("condition_name", "")
        state = "DETECTED" if event_type == "I" else "ARCHIVE_READY"
        symbol_row = self._upsert_tracked_symbol(code, slot_no, condition_name, state, refresh_reference=False)
        event_name = "condition_enter" if event_type == "I" else "condition_leave"
        self._record_symbol_event(code, symbol_row["name"], event_name, slot_no, condition_name, payload)
        self.persistence.execute(
            "UPDATE active_condition_slots SET last_event_at=?, updated_at=? WHERE slot_no=?",
            (self.persistence.now_ts(), self.persistence.now_ts(), slot_no),
        )
        if event_type == "I":
            self._enqueue_snapshot_detection_job(code, slot_no, condition_name, emit_detected=False, delay_ms=1500)
            self.symbol_detected.emit({
                "code": code,
                "name": symbol_row["name"],
                "slot_no": slot_no,
                "condition_name": condition_name,
                "event_type": event_name,
            })
        self.slots_changed.emit()
        self.tracked_symbol_changed.emit()

    def _resolve_name(self, code):
        row = self.persistence.fetchone("SELECT name FROM tracked_symbols WHERE code=?", (code,))
        if row and row["name"] and row["name"] != code:
            return row["name"]
        return self.kiwoom_client.get_master_code_name(code)

    def _is_regular_market_hours(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        if now_dt.weekday() >= 5:
            return False
        hhmm = int(now_dt.strftime("%H%M"))
        return 900 <= hhmm < 1530

    def _get_snapshot_job_min_interval_ms(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        if self._is_startup_background_mode():
            if self._is_regular_market_hours(now_dt):
                return 2200
            return 3200
        if self._is_regular_market_hours(now_dt):
            return 900
        return 2500

    def _is_account_sync_busy(self):
        try:
            if hasattr(self.kiwoom_client, "is_account_sync_busy"):
                return bool(self.kiwoom_client.is_account_sync_busy())
        except Exception:
            pass
        try:
            if getattr(self.kiwoom_client, "_current_sync_context", None) is not None:
                return True
            if list(getattr(self.kiwoom_client, "_account_sync_queue", []) or []):
                return True
        except Exception:
            return False
        return False

    def _get_fast_quote_snapshot(self, code):
        snapshot = {}
        if hasattr(self.kiwoom_client, "get_realtime_snapshot"):
            try:
                snapshot = dict(self.kiwoom_client.get_realtime_snapshot(code) or {})
            except Exception:
                snapshot = {}
        try:
            if float(snapshot.get("current_volume") or 0.0) <= 0 and float(snapshot.get("acc_volume") or 0.0) > 0:
                snapshot["current_volume"] = float(snapshot.get("acc_volume") or 0.0)
        except Exception:
            pass
        try:
            if float(snapshot.get("current_turnover") or 0.0) <= 0 and float(snapshot.get("acc_turnover") or 0.0) > 0:
                snapshot["current_turnover"] = float(snapshot.get("acc_turnover") or 0.0)
        except Exception:
            pass
        try:
            sell_hoga_total = float(snapshot.get("sell_hoga_total") or 0.0)
            buy_hoga_total = float(snapshot.get("buy_hoga_total") or 0.0)
            if float(snapshot.get("sell_pressure_ratio") or 0.0) <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
                snapshot["sell_pressure_ratio"] = round(float(sell_hoga_total / buy_hoga_total), 4)
        except Exception:
            pass
        if float(snapshot.get("current_price") or 0.0) <= 0 and hasattr(self.kiwoom_client, "get_master_last_price"):
            try:
                master_last_price = float(self.kiwoom_client.get_master_last_price(code) or 0.0)
            except Exception:
                master_last_price = 0.0
            if master_last_price > 0:
                snapshot["current_price"] = master_last_price
        return snapshot

    def _build_reference_metrics(self, code, now_dt, quote, use_intraday=True):
        quote = dict(quote or {})
        market_open = self._is_regular_market_hours(now_dt)
        if market_open and use_intraday:
            reference = self.kiwoom_client.request_intraday_reference_stats(
                code,
                target_dt=now_dt,
                lookback_days=5,
                timeout_ms=3200,
                max_pages=4,
                allow_quote_fallback=False,
                seed_snapshot=quote,
            ) or {}
        else:
            reference = self.kiwoom_client.request_daily_reference_stats(code, target_dt=now_dt, lookback_days=5) or {}
            if float(reference.get("reference_price") or 0.0) <= 0 and float(quote.get("current_price") or 0.0) > 0:
                reference["reference_price"] = float(quote.get("current_price") or 0.0)
            if float(reference.get("current_volume") or 0.0) <= 0 and float(quote.get("current_volume") or 0.0) > 0:
                reference["current_volume"] = float(quote.get("current_volume") or 0.0)
            if float(reference.get("current_turnover") or 0.0) <= 0 and float(quote.get("current_turnover") or 0.0) > 0:
                reference["current_turnover"] = float(quote.get("current_turnover") or 0.0)
        detected_price = float(quote.get("current_price") or reference.get("reference_price") or 0.0)
        detected_volume = float(reference.get("current_volume") or quote.get("current_volume") or 0.0)
        detected_turnover = float(reference.get("current_turnover") or quote.get("current_turnover") or 0.0)
        vwap_intraday = float(reference.get("vwap_intraday") or quote.get("vwap_intraday") or 0.0)
        if (
            vwap_intraday <= 0
            and float(reference.get("current_volume") or 0.0) > 0
            and float(reference.get("current_turnover") or 0.0) > 0
        ):
            vwap_intraday = float(detected_turnover / detected_volume)
        avg_volume = float(reference.get("avg_volume") or 0.0)
        avg_turnover = float(reference.get("avg_turnover") or 0.0)
        volume_ratio = float(reference.get("volume_ratio") or 0.0)
        turnover_ratio = float(reference.get("turnover_ratio") or 0.0)
        sell_hoga_total = float(quote.get("sell_hoga_total") or 0.0)
        buy_hoga_total = float(quote.get("buy_hoga_total") or 0.0)
        sell_pressure_ratio = float(quote.get("sell_pressure_ratio") or 0.0)
        if sell_pressure_ratio <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
            sell_pressure_ratio = round(float(sell_hoga_total / buy_hoga_total), 4)
        metrics = {
            "reference_price": detected_price,
            "detected_volume": detected_volume,
            "detected_turnover": detected_turnover,
            "vwap_intraday": vwap_intraday,
            "sell_hoga_total": sell_hoga_total,
            "buy_hoga_total": buy_hoga_total,
            "sell_pressure_ratio": sell_pressure_ratio,
            "avg_volume_same_time_5d": avg_volume,
            "avg_turnover_same_time_5d": avg_turnover,
            "volume_ratio_5d_same_time": volume_ratio,
            "turnover_ratio_5d_same_time": turnover_ratio,
            "reference_days_count": int(reference.get("days_count") or 0),
            "target_hhmm": reference.get("target_hhmm", now_dt.strftime("%H%M")),
            "metric_mode": reference.get("metric_mode", "same_time"),
            "metric_base_day": reference.get("latest_day", now_dt.strftime("%Y%m%d")),
            "volume_compare_label": reference.get("volume_compare_label", "최근 5일 동시간 평균"),
            "turnover_compare_label": reference.get("turnover_compare_label", "최근 5일 동시간 평균"),
            "avg_volume_5d": avg_volume,
            "avg_turnover_5d": avg_turnover,
            "volume_ratio_5d": volume_ratio,
            "turnover_ratio_5d": turnover_ratio,
        }
        return {
            "reference": dict(reference or {}),
            "metrics": metrics,
            "detected_price": detected_price,
        }

    def _enqueue_snapshot_detection_job(self, code, slot_no, condition_name, emit_detected=True, delay_ms=0):
        code = str(code or "").strip()
        if not code:
            return
        key = "{0}|{1}|{2}".format(code, int(slot_no or 0), str(condition_name or ""))
        refresh_key = "{0}|{1}".format(code, int(slot_no or 0))
        now_dt = datetime.datetime.now()
        prev_dt = self._snapshot_refresh_last_ts.get(refresh_key)
        if prev_dt is not None:
            elapsed = (now_dt - prev_dt).total_seconds()
            if elapsed < self._snapshot_refresh_cooldown_sec:
                return
        if key in self._pending_snapshot_job_keys:
            return
        self._snapshot_refresh_last_ts[refresh_key] = now_dt
        self._pending_snapshot_job_keys.add(key)
        self._pending_snapshot_jobs.append({
            "key": key,
            "code": code,
            "slot_no": int(slot_no or 0),
            "condition_name": str(condition_name or ""),
            "emit_detected": bool(emit_detected),
            "delay_ms": max(0, int(delay_ms or 0)),
        })
        if not self._snapshot_job_timer.isActive():
            start_delay = max(0, int(self._pending_snapshot_jobs[0].get("delay_ms") or 0))
            if self._is_account_sync_busy():
                start_delay = max(start_delay, 1800)
            start_delay = max(start_delay, self._get_snapshot_job_min_interval_ms(now_dt))
            self._snapshot_job_timer.start(start_delay)

    def _process_next_snapshot_job(self):
        if not self._pending_snapshot_jobs:
            return
        if self._is_account_sync_busy():
            self._snapshot_job_timer.start(1800)
            return
        job = self._pending_snapshot_jobs.pop(0)
        self._pending_snapshot_job_keys.discard(job.get("key"))
        code = str(job.get("code") or "")
        slot_no = int(job.get("slot_no") or 0)
        condition_name = str(job.get("condition_name") or "")
        emit_detected = bool(job.get("emit_detected", True))
        try:
            self._refresh_tracked_symbol_reference_metrics(code)
            symbol_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
            if symbol_row:
                self._record_symbol_event(code, symbol_row["name"], "condition_snapshot", slot_no, condition_name, {
                    "code": code,
                    "slot_no": slot_no,
                    "condition_name": condition_name,
                    "event_type": "condition_snapshot_delayed",
                })
                if emit_detected:
                    self.symbol_detected.emit({
                        "code": code,
                        "name": symbol_row["name"],
                        "slot_no": slot_no,
                        "condition_name": condition_name,
                        "event_type": "condition_snapshot",
                    })
                self.tracked_symbol_changed.emit()
        except Exception as exc:
            self.log_emitted.emit("⚠️ 조건식 스냅샷 후속 기준치 갱신 실패: {0} / {1}".format(code, exc))
        if self._pending_snapshot_jobs:
            next_delay = max(self._get_snapshot_job_min_interval_ms(), int(self._pending_snapshot_jobs[0].get("delay_ms") or 0))
            if self._is_account_sync_busy():
                next_delay = max(next_delay, 1800)
            self._snapshot_job_timer.start(next_delay)

    def _refresh_tracked_symbol_reference_metrics(self, code):
        code = str(code or "").strip()
        if not code:
            return None
        row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        if not row:
            return None
        now = self.persistence.now_ts()
        now_dt = datetime.datetime.now()
        quote = dict(self._get_fast_quote_snapshot(code) or {})
        built = self._build_reference_metrics(code, now_dt, quote, use_intraday=True)
        detected_price = float(built.get("detected_price") or 0.0)
        if detected_price <= 0:
            detected_price = float(row["detected_price"] or 0.0)
        old_extra = json.loads(row["extra_json"] or "{}")
        old_extra.update(dict(built.get("metrics") or {}))
        self.persistence.execute(
            "UPDATE tracked_symbols SET detected_price=?, extra_json=?, updated_at=? WHERE code=?",
            (detected_price, json.dumps(old_extra, ensure_ascii=False), now, code),
        )
        updated_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        self._capture_daily_watch_snapshot(updated_row, live_snapshot=quote, source="reference_refresh", target_dt=now_dt)
        return updated_row

    def _upsert_tracked_symbol(self, code, slot_no, condition_name, state, refresh_reference=True):
        now = self.persistence.now_ts()
        now_dt = datetime.datetime.now()
        expire_at = (now_dt + datetime.timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        name = self._resolve_name(code)
        quote = dict(self._get_fast_quote_snapshot(code) or {})

        metrics = {}
        detected_price = float(quote.get("current_price") or 0.0)
        if refresh_reference:
            built = self._build_reference_metrics(code, now_dt, quote, use_intraday=True)
            metrics = dict(built.get("metrics") or {})
            detected_price = float(built.get("detected_price") or detected_price or 0.0)
        else:
            detected_volume = float(quote.get("current_volume") or 0.0)
            detected_turnover = float(quote.get("current_turnover") or 0.0)
            vwap_intraday = float(quote.get("vwap_intraday") or 0.0)
            sell_hoga_total = float(quote.get("sell_hoga_total") or 0.0)
            buy_hoga_total = float(quote.get("buy_hoga_total") or 0.0)
            sell_pressure_ratio = float(quote.get("sell_pressure_ratio") or 0.0)
            if sell_pressure_ratio <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
                sell_pressure_ratio = round(float(sell_hoga_total / buy_hoga_total), 4)
            metrics = {
                "reference_price": detected_price,
                "detected_volume": detected_volume,
                "detected_turnover": detected_turnover,
                "vwap_intraday": vwap_intraday,
                "sell_hoga_total": sell_hoga_total,
                "buy_hoga_total": buy_hoga_total,
                "sell_pressure_ratio": sell_pressure_ratio,
                "avg_volume_same_time_5d": 0.0,
                "avg_turnover_same_time_5d": 0.0,
                "volume_ratio_5d_same_time": 0.0,
                "turnover_ratio_5d_same_time": 0.0,
                "reference_days_count": 0,
                "target_hhmm": now_dt.strftime("%H%M"),
                "metric_mode": "pending_same_time_refresh" if self._is_regular_market_hours(now_dt) else "full_day",
                "metric_base_day": now_dt.strftime("%Y%m%d"),
                "volume_compare_label": "후속 분봉 기준치 갱신 예정" if self._is_regular_market_hours(now_dt) else "최근 5일 일간 평균 거래량",
                "turnover_compare_label": "후속 분봉 기준치 갱신 예정" if self._is_regular_market_hours(now_dt) else "최근 5일 일간 평균 거래대금",
                "avg_volume_5d": 0.0,
                "avg_turnover_5d": 0.0,
                "volume_ratio_5d": 0.0,
                "turnover_ratio_5d": 0.0,
            }
        row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        if row:
            if detected_price <= 0:
                detected_price = float(row["detected_price"] or 0.0)
            source_conditions = json.loads(row["source_conditions_json"] or "[]")
            item = {"slot_no": slot_no, "condition_name": condition_name, "ts": now}
            if item not in source_conditions:
                source_conditions.append(item)
            old_extra = json.loads(row["extra_json"] or "{}")
            old_extra.update(metrics)
            self.persistence.execute(
                """
                UPDATE tracked_symbols
                SET name=?, last_detected_at=?, expire_at=?, detected_price=?, current_state=?, source_conditions_json=?, extra_json=?, updated_at=?
                WHERE code=?
                """,
                (name, now, expire_at, detected_price, state, json.dumps(source_conditions, ensure_ascii=False), json.dumps(old_extra, ensure_ascii=False), now, code),
            )
        else:
            self.persistence.execute(
                """
                INSERT INTO tracked_symbols (
                    code, name, first_detected_at, last_detected_at, expire_at, detected_price, current_state,
                    is_holding, has_open_order, news_watch_priority, source_conditions_json, buy_attempt_count,
                    is_spam, extra_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 50, ?, 0, 0, ?, ?, ?)
                """,
                (
                    code,
                    name,
                    now,
                    now,
                    expire_at,
                    detected_price,
                    state,
                    json.dumps([{"slot_no": slot_no, "condition_name": condition_name, "ts": now}], ensure_ascii=False),
                    json.dumps(metrics, ensure_ascii=False),
                    now,
                    now,
                ),
            )
        updated_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        self._capture_daily_watch_snapshot(updated_row, live_snapshot=quote, source="condition_detected", target_dt=now_dt)
        return updated_row

    def _capture_daily_watch_snapshot(self, tracked_row, live_snapshot=None, source="tracked_symbol", target_dt=None):
        if self.daily_watch_snapshot_manager is None or not tracked_row:
            return
        try:
            self.daily_watch_snapshot_manager.capture_symbol(
                tracked_row=tracked_row,
                live_snapshot=live_snapshot,
                source=source,
                target_dt=target_dt,
            )
        except Exception as exc:
            code = str((tracked_row or {}).get("code") or "")
            self.log_emitted.emit("⚠️ 일일 뉴스감시 스냅샷 기록 실패: {0} / {1}".format(code, exc))

    def _record_symbol_event(self, code, name, event_type, slot_no, condition_name, payload):
        self.persistence.execute(
            """
            INSERT INTO symbol_events (
                trade_date, ts, code, name, event_type, source_condition_slot, source_condition_name,
                account_scope, payload_json, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
            """,
            (
                self.persistence.today_str(),
                self.persistence.now_ts(),
                code,
                name,
                event_type,
                slot_no,
                condition_name,
                "global",
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        self.persistence.write_event(event_type, payload)

    def get_tracked_symbols(self):
        return self.persistence.fetchall("SELECT * FROM tracked_symbols ORDER BY last_detected_at DESC")
