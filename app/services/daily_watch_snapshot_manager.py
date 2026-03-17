# -*- coding: utf-8 -*-
import datetime
import json
import os

from PyQt5.QtCore import QObject, pyqtSignal


class DailyWatchSnapshotManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, paths, parent=None):
        super(DailyWatchSnapshotManager, self).__init__(parent)
        self.paths = paths
        self._day_cache = {}
        self.ensure_dir()

    def ensure_dir(self):
        path = str(getattr(self.paths, "daily_watch_snapshot_dir", "") or "")
        if not path:
            return
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except Exception as exc:
            self.log_emitted.emit("DAILY WATCH SNAPSHOT DIR CREATE FAILED: {0}".format(exc))

    def _today_key(self, target_dt=None):
        target_dt = target_dt or datetime.datetime.now()
        return target_dt.strftime("%Y-%m-%d")

    def _path_for_date(self, date_key):
        return os.path.join(
            self.paths.daily_watch_snapshot_dir,
            "{0}.json".format(str(date_key or "").strip()),
        )

    def _to_float(self, value):
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    def _safe_json_dict(self, raw):
        if isinstance(raw, dict):
            return dict(raw)
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        return data if isinstance(data, dict) else {}

    def _is_regular_market_hours(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        hhmm = now_dt.strftime("%H%M")
        return now_dt.weekday() < 5 and "0900" <= hhmm <= "1530"

    def _load_day(self, date_key):
        date_key = str(date_key or "").strip() or self._today_key()
        cached = self._day_cache.get(date_key)
        if cached is not None:
            return cached
        path = self._path_for_date(date_key)
        payload = {"date": date_key, "symbols": {}}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    loaded = json.load(fp)
                if isinstance(loaded, dict):
                    payload["date"] = str(loaded.get("date") or date_key)
                    symbols = loaded.get("symbols") or {}
                    payload["symbols"] = dict(symbols) if isinstance(symbols, dict) else {}
            except Exception as exc:
                self.log_emitted.emit(
                    "DAILY WATCH SNAPSHOT LOAD FAILED: {0} / {1}".format(date_key, exc)
                )
        self._day_cache[date_key] = payload
        return payload

    def _save_day(self, date_key):
        payload = self._load_day(date_key)
        path = self._path_for_date(date_key)
        tmp_path = path + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fp:
                json.dump(payload, fp, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
        except Exception as exc:
            self.log_emitted.emit(
                "DAILY WATCH SNAPSHOT SAVE FAILED: {0} / {1}".format(date_key, exc)
            )
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def _build_live_snapshot(self, live_snapshot, symbol_meta=None):
        live_snapshot = dict(live_snapshot or {})
        symbol_meta = dict(symbol_meta or {})
        current_price = self._to_float(live_snapshot.get("current_price"))
        acc_volume = self._to_float(live_snapshot.get("acc_volume"))
        acc_turnover = self._to_float(live_snapshot.get("acc_turnover"))
        if current_price <= 0 and self._to_float(symbol_meta.get("reference_price")) > 0:
            current_price = self._to_float(symbol_meta.get("reference_price"))
        if acc_volume <= 0 and self._to_float(symbol_meta.get("detected_volume")) > 0:
            acc_volume = self._to_float(symbol_meta.get("detected_volume"))
        if acc_turnover <= 0 and self._to_float(symbol_meta.get("detected_turnover")) > 0:
            acc_turnover = self._to_float(symbol_meta.get("detected_turnover"))
        sell_hoga_total = self._to_float(live_snapshot.get("sell_hoga_total"))
        buy_hoga_total = self._to_float(live_snapshot.get("buy_hoga_total"))
        vwap_intraday = self._to_float(live_snapshot.get("vwap_intraday") or symbol_meta.get("vwap_intraday"))
        if vwap_intraday <= 0 and acc_volume > 0 and acc_turnover > 0:
            vwap_intraday = float(acc_turnover / acc_volume)
        sell_pressure_ratio = self._to_float(live_snapshot.get("sell_pressure_ratio"))
        if sell_pressure_ratio <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
            sell_pressure_ratio = round(float(sell_hoga_total / buy_hoga_total), 4)
        return {
            "current_price": current_price,
            "vwap_intraday": vwap_intraday,
            "sell_pressure_ratio": sell_pressure_ratio,
            "acc_volume": acc_volume,
            "acc_turnover": acc_turnover,
            "sell_hoga_total": sell_hoga_total,
            "buy_hoga_total": buy_hoga_total,
        }

    def _build_symbol_meta(self, tracked_row):
        tracked_row = dict(tracked_row or {})
        symbol_meta = self._safe_json_dict(tracked_row.get("extra_json") or "{}")
        detected_price = self._to_float(
            tracked_row.get("detected_price")
            or symbol_meta.get("detected_price")
            or symbol_meta.get("reference_price")
        )
        if detected_price > 0:
            symbol_meta["detected_price"] = detected_price
            if self._to_float(symbol_meta.get("reference_price")) <= 0:
                symbol_meta["reference_price"] = detected_price
        return symbol_meta

    def _is_missing_value(self, value):
        if value in [None, "", [], {}]:
            return True
        if isinstance(value, str) and str(value).strip() in ["-", "--"]:
            return True
        if isinstance(value, (int, float)):
            return float(value) <= 0
        return False

    def _merge_fill_missing(self, base, incoming):
        merged = dict(base or {})
        for key, value in dict(incoming or {}).items():
            if self._is_missing_value(merged.get(key)) and not self._is_missing_value(value):
                merged[key] = value
        return merged

    def _merge_keep_latest_non_missing(self, base, incoming):
        merged = dict(base or {})
        for key, value in dict(incoming or {}).items():
            if self._is_missing_value(value):
                continue
            merged[key] = value
        return merged

    def capture_symbol(self, tracked_row, live_snapshot=None, source="detected", target_dt=None):
        tracked_row = dict(tracked_row or {})
        code = str(tracked_row.get("code") or "").strip()
        if not code:
            return {}
        target_dt = target_dt or datetime.datetime.now()
        date_key = self._today_key(target_dt)
        day_payload = self._load_day(date_key)
        symbols = day_payload.setdefault("symbols", {})
        existing = dict(symbols.get(code) or {})
        built_meta = self._build_symbol_meta(tracked_row)
        built_live = self._build_live_snapshot(live_snapshot, symbol_meta=built_meta)
        new_entry = {
            "code": code,
            "name": str(tracked_row.get("name") or existing.get("name") or code),
            "captured_at": str(
                existing.get("captured_at")
                or tracked_row.get("first_detected_at")
                or tracked_row.get("last_detected_at")
                or target_dt.strftime("%Y-%m-%d %H:%M:%S")
            ),
            "last_recorded_at": target_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "source": str(existing.get("source") or source or "detected"),
            "first_detected_at": str(tracked_row.get("first_detected_at") or existing.get("first_detected_at") or ""),
            "last_detected_at": str(tracked_row.get("last_detected_at") or existing.get("last_detected_at") or ""),
            "live_snapshot": self._merge_keep_latest_non_missing(existing.get("live_snapshot") or {}, built_live),
            "symbol_meta": self._merge_keep_latest_non_missing(existing.get("symbol_meta") or {}, built_meta),
        }
        symbols[code] = new_entry
        self._save_day(date_key)
        return dict(new_entry)

    def capture_realtime_reference(self, code, name="", live_snapshot=None, source="realtime_reference", target_dt=None):
        code = str(code or "").strip()
        if not code:
            return {}
        target_dt = target_dt or datetime.datetime.now()
        date_key = self._today_key(target_dt)
        day_payload = self._load_day(date_key)
        symbols = day_payload.setdefault("symbols", {})
        existing = dict(symbols.get(code) or {})
        merged_live = self._merge_keep_latest_non_missing(
            existing.get("live_snapshot") or {},
            dict(live_snapshot or {}),
        )
        existing_meta = dict(existing.get("symbol_meta") or {})
        if self._is_missing_value(existing_meta.get("reference_price")) and not self._is_missing_value(merged_live.get("current_price")):
            existing_meta["reference_price"] = self._to_float(merged_live.get("current_price"))
        if self._is_missing_value(existing_meta.get("vwap_intraday")) and not self._is_missing_value(merged_live.get("vwap_intraday")):
            existing_meta["vwap_intraday"] = self._to_float(merged_live.get("vwap_intraday"))
        if self._is_missing_value(existing_meta.get("sell_pressure_ratio")) and not self._is_missing_value(merged_live.get("sell_pressure_ratio")):
            existing_meta["sell_pressure_ratio"] = self._to_float(merged_live.get("sell_pressure_ratio"))
        if self._is_missing_value(existing_meta.get("sell_hoga_total")) and not self._is_missing_value(merged_live.get("sell_hoga_total")):
            existing_meta["sell_hoga_total"] = self._to_float(merged_live.get("sell_hoga_total"))
        if self._is_missing_value(existing_meta.get("buy_hoga_total")) and not self._is_missing_value(merged_live.get("buy_hoga_total")):
            existing_meta["buy_hoga_total"] = self._to_float(merged_live.get("buy_hoga_total"))
        entry = {
            "code": code,
            "name": str(name or existing.get("name") or code),
            "captured_at": str(existing.get("captured_at") or target_dt.strftime("%Y-%m-%d %H:%M:%S")),
            "last_recorded_at": target_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "source": str(existing.get("source") or source or "realtime_reference"),
            "first_detected_at": str(existing.get("first_detected_at") or ""),
            "last_detected_at": str(existing.get("last_detected_at") or ""),
            "live_snapshot": merged_live,
            "symbol_meta": existing_meta,
        }
        symbols[code] = entry
        self._save_day(date_key)
        return dict(entry)

    def get_entry(self, code, target_dt=None):
        code = str(code or "").strip()
        if not code:
            return {}
        date_key = self._today_key(target_dt)
        day_payload = self._load_day(date_key)
        return dict((day_payload.get("symbols") or {}).get(code) or {})

    def get_live_snapshot(self, code, target_dt=None):
        entry = self.get_entry(code, target_dt=target_dt)
        return dict(entry.get("live_snapshot") or {})

    def get_symbol_meta(self, code, target_dt=None):
        entry = self.get_entry(code, target_dt=target_dt)
        return dict(entry.get("symbol_meta") or {})

    def capture_missing_rows(
        self,
        tracked_rows,
        snapshot_provider=None,
        target_dt=None,
        source="after_hours_backfill",
    ):
        target_dt = target_dt or datetime.datetime.now()
        if self._is_regular_market_hours(target_dt):
            return 0
        updated_count = 0
        for row in list(tracked_rows or []):
            row = dict(row or {})
            code = str(row.get("code") or "").strip()
            if not code:
                continue
            entry = self.get_entry(code, target_dt=target_dt)
            live_snapshot = {}
            if callable(snapshot_provider):
                try:
                    live_snapshot = dict(snapshot_provider(code) or {})
                except Exception:
                    live_snapshot = {}
            live_values = entry.get("live_snapshot") or {}
            meta_values = entry.get("symbol_meta") or {}
            if (not live_values) or (not meta_values):
                self.capture_symbol(
                    row,
                    live_snapshot=live_snapshot,
                    source=source,
                    target_dt=target_dt,
                )
                updated_count += 1
                continue
            needs_fill = False
            for value in list(live_values.values()) + list(meta_values.values()):
                if self._is_missing_value(value):
                    needs_fill = True
                    break
            if needs_fill:
                self.capture_symbol(
                    row,
                    live_snapshot=live_snapshot,
                    source=source,
                    target_dt=target_dt,
                )
                updated_count += 1
        return updated_count
