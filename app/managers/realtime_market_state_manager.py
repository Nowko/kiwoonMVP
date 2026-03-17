# -*- coding: utf-8 -*-
import json

from PyQt5.QtCore import QObject, pyqtSignal


class RealtimeMarketStateManager(QObject):
    market_state_changed = pyqtSignal(dict)
    watch_codes_changed = pyqtSignal(list)
    log_emitted = pyqtSignal(str)

    def __init__(self, persistence, kiwoom_client, parent=None):
        super(RealtimeMarketStateManager, self).__init__(parent)
        self.persistence = persistence
        self.kiwoom_client = kiwoom_client
        self._state_map = {}
        self._watch_codes = []
        if hasattr(self.kiwoom_client, "real_market_data_received"):
            self.kiwoom_client.real_market_data_received.connect(self._on_real_market_data_received)

    def _normalize_code(self, code):
        return str(code or "").strip()

    def _to_float(self, value):
        try:
            return float(value or 0)
        except Exception:
            return 0.0

    def _safe_json_loads(self, value, default):
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value or "")
        except Exception:
            return default

    def _compute_vwap(self, acc_volume, acc_turnover):
        acc_volume = self._to_float(acc_volume)
        acc_turnover = self._to_float(acc_turnover)
        if acc_volume <= 0 or acc_turnover <= 0:
            return 0.0
        return float(acc_turnover / acc_volume)

    def _collect_watch_codes(self):
        codes = []
        seen = set()
        rows = self.persistence.fetchall(
            "SELECT DISTINCT code FROM positions WHERE qty > 0 ORDER BY code"
        )
        for row in rows:
            code = self._normalize_code(row["code"])
            if not code or code in seen:
                continue
            seen.add(code)
            codes.append(code)
        now_ts = self.persistence.now_ts()
        rows = self.persistence.fetchall(
            """
            SELECT DISTINCT code
            FROM tracked_symbols
            WHERE is_spam=0
              AND (
                    is_holding=1
                 OR has_open_order=1
                 OR expire_at IS NULL
                 OR expire_at=''
                 OR expire_at>=?
              )
            ORDER BY code
            """,
            (now_ts,),
        )
        for row in rows:
            code = self._normalize_code(row["code"])
            if not code or code in seen:
                continue
            seen.add(code)
            codes.append(code)
        return codes

    def refresh_watch_codes(self):
        codes = self._collect_watch_codes()
        if codes == self._watch_codes:
            return self._watch_codes
        self._watch_codes = list(codes)
        if hasattr(self.kiwoom_client, 'set_market_realtime_codes'):
            self.kiwoom_client.set_market_realtime_codes(self._watch_codes)
        self.watch_codes_changed.emit(list(self._watch_codes))
        self.log_emitted.emit("📡 실시간 감시 종목 갱신: {0}건".format(len(self._watch_codes)))
        return list(self._watch_codes)

    def _on_real_market_data_received(self, payload):
        self.update_from_payload(payload)

    def update_from_payload(self, payload):
        code = self._normalize_code((payload or {}).get('code'))
        if not code:
            return None
        current_price = self._to_float((payload or {}).get('current_price'))
        acc_volume = self._to_float((payload or {}).get('acc_volume'))
        acc_turnover = self._to_float((payload or {}).get('acc_turnover'))
        sell_hoga_total = self._to_float((payload or {}).get('sell_hoga_total'))
        buy_hoga_total = self._to_float((payload or {}).get('buy_hoga_total'))
        state = dict(self._state_map.get(code) or {})
        state.update({
            'code': code,
            'real_type': str((payload or {}).get('real_type') or state.get('real_type') or ''),
            'current_price': current_price if current_price > 0 else self._to_float(state.get('current_price')),
            'acc_volume': acc_volume if acc_volume > 0 else self._to_float(state.get('acc_volume')),
            'acc_turnover': acc_turnover if acc_turnover > 0 else self._to_float(state.get('acc_turnover')),
            'sell_hoga_total': sell_hoga_total if sell_hoga_total > 0 else self._to_float(state.get('sell_hoga_total')),
            'buy_hoga_total': buy_hoga_total if buy_hoga_total > 0 else self._to_float(state.get('buy_hoga_total')),
            'updated_at': str((payload or {}).get('received_at') or self.persistence.now_ts()),
        })
        state['vwap_intraday'] = self._compute_vwap(state.get('acc_volume'), state.get('acc_turnover'))
        if self._to_float(state.get('buy_hoga_total')) > 0:
            state['sell_pressure_ratio'] = round(self._to_float(state.get('sell_hoga_total')) / self._to_float(state.get('buy_hoga_total')), 4)
        else:
            state['sell_pressure_ratio'] = 0.0
        self._state_map[code] = state
        self.market_state_changed.emit(dict(state))
        return dict(state)

    def _load_tracked_symbol_meta(self, code):
        row = self.persistence.fetchone(
            "SELECT extra_json FROM tracked_symbols WHERE code=?",
            (self._normalize_code(code),),
        )
        if not row:
            return {}
        return self._safe_json_loads(row["extra_json"], {})

    def get_snapshot(self, code):
        code = self._normalize_code(code)
        if not code:
            return {}
        snapshot = dict(self._state_map.get(code) or {})
        if self.kiwoom_client is not None and hasattr(self.kiwoom_client, "get_enriched_realtime_snapshot"):
            try:
                snapshot = dict(
                    self.kiwoom_client.get_enriched_realtime_snapshot(
                        code,
                        seed_snapshot=snapshot,
                        allow_tr=False,
                    ) or snapshot
                )
            except Exception:
                snapshot = dict(snapshot or {})
        meta = self._load_tracked_symbol_meta(code)
        if self._to_float(snapshot.get("current_price")) <= 0 and self._to_float(meta.get("reference_price")) > 0:
            snapshot["current_price"] = self._to_float(meta.get("reference_price"))
        if self._to_float(snapshot.get("acc_volume")) <= 0 and self._to_float(meta.get("detected_volume")) > 0:
            snapshot["acc_volume"] = self._to_float(meta.get("detected_volume"))
        if self._to_float(snapshot.get("current_volume")) <= 0 and self._to_float(meta.get("detected_volume")) > 0:
            snapshot["current_volume"] = self._to_float(meta.get("detected_volume"))
        if self._to_float(snapshot.get("acc_turnover")) <= 0 and self._to_float(meta.get("detected_turnover")) > 0:
            snapshot["acc_turnover"] = self._to_float(meta.get("detected_turnover"))
        if self._to_float(snapshot.get("current_turnover")) <= 0 and self._to_float(meta.get("detected_turnover")) > 0:
            snapshot["current_turnover"] = self._to_float(meta.get("detected_turnover"))
        if self._to_float(snapshot.get("sell_hoga_total")) <= 0 and self._to_float(meta.get("sell_hoga_total")) > 0:
            snapshot["sell_hoga_total"] = self._to_float(meta.get("sell_hoga_total"))
        if self._to_float(snapshot.get("buy_hoga_total")) <= 0 and self._to_float(meta.get("buy_hoga_total")) > 0:
            snapshot["buy_hoga_total"] = self._to_float(meta.get("buy_hoga_total"))
        if self._to_float(snapshot.get("vwap_intraday")) <= 0:
            meta_vwap = self._to_float(meta.get("vwap_intraday"))
            if meta_vwap <= 0:
                meta_vwap = self._compute_vwap(meta.get("detected_volume"), meta.get("detected_turnover"))
            if meta_vwap > 0:
                snapshot["vwap_intraday"] = meta_vwap
        if self._to_float(snapshot.get("vwap_intraday")) <= 0:
            snapshot["vwap_intraday"] = self._compute_vwap(
                snapshot.get("acc_volume"),
                snapshot.get("acc_turnover"),
            )
        if self._to_float(snapshot.get("sell_pressure_ratio")) <= 0 and self._to_float(meta.get("sell_pressure_ratio")) > 0:
            snapshot["sell_pressure_ratio"] = self._to_float(meta.get("sell_pressure_ratio"))
        if self._to_float(snapshot.get("sell_pressure_ratio")) <= 0 and self._to_float(snapshot.get("buy_hoga_total")) > 0:
            snapshot["sell_pressure_ratio"] = round(
                self._to_float(snapshot.get("sell_hoga_total")) / self._to_float(snapshot.get("buy_hoga_total")),
                4,
            )
        return snapshot

    def get_snapshots(self, codes=None):
        if not codes:
            return [dict(self._state_map[code]) for code in sorted(self._state_map.keys())]
        result = []
        for code in list(codes or []):
            snapshot = self.get_snapshot(code)
            if snapshot:
                result.append(snapshot)
        return result
