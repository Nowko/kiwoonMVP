# -*- coding: utf-8 -*-
import datetime
import traceback

from PyQt5.QtCore import QObject, QEventLoop, QTimer, pyqtSignal

try:
    from PyQt5.QAxContainer import QAxWidget
except Exception:
    QAxWidget = None


class KiwoomApiClient(QObject):
    connection_changed = pyqtSignal(bool, str)
    log_emitted = pyqtSignal(str)
    accounts_loaded = pyqtSignal(list)
    conditions_loaded = pyqtSignal(list)
    condition_event_received = pyqtSignal(dict)
    chejan_received = pyqtSignal(dict)
    api_message_received = pyqtSignal(dict)
    account_cash_received = pyqtSignal(dict)
    account_positions_received = pyqtSignal(dict)
    account_realized_received = pyqtSignal(dict)
    outstanding_orders_received = pyqtSignal(dict)
    account_sync_finished = pyqtSignal(dict)
    real_price_received = pyqtSignal(dict)
    real_market_data_received = pyqtSignal(dict)

    def __init__(self, persistence, parent=None):
        super(KiwoomApiClient, self).__init__(parent)
        self.persistence = persistence
        self.widget = None
        self.connected = False
        self.user_id = ""
        self.user_name = ""
        self.server_gubun = ""
        self.account_list = []
        self._condition_cache = []
        self._rq_context_map = {}
        self._account_sync_queue = []
        self._current_sync_context = None
        self._snapshot_loop = None
        self._snapshot_wait_code = ""
        self._snapshot_result = 0.0
        self._quote_snapshot_loop = None
        self._quote_snapshot_wait_code = ""
        self._quote_snapshot_result = {"current_price": 0.0, "ask1": 0.0, "current_volume": 0.0, "current_turnover": 0.0}
        self._minute_snapshot_loop = None
        self._minute_snapshot_wait_code = ""
        self._minute_snapshot_context = {}
        self._daily_snapshot_loop = None
        self._daily_snapshot_wait_code = ""
        self._daily_snapshot_result = {}
        self._holding_real_screen_no = "6210"
        self._holding_real_codes = []
        self._market_real_screen_no = "6211"
        self._market_real_codes = []
        self._sync_screen_prefix = "63"
        self._real_snapshot_cache = {}
        self._quote_snapshot_cache = {}
        self._daily_reference_cache = {}
        self._intraday_reference_cache = {}
        self._quote_snapshot_cache_ttl_sec = 2.0
        self._daily_reference_cache_live_ttl_sec = 180.0
        self._daily_reference_cache_after_hours_ttl_sec = 900.0
        self._intraday_reference_cache_ttl_sec = 20.0
        self._init_control()

    def _init_control(self):
        if QAxWidget is None:
            self.log_emitted.emit("⚠️ QAxContainer를 사용할 수 없는 환경입니다. Windows 32bit + PyQt5 QAxContainer 필요")
            return
        try:
            self.widget = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
            self.widget.OnEventConnect.connect(self._on_event_connect)
            self.widget.OnReceiveConditionVer.connect(self._on_receive_condition_ver)
            self.widget.OnReceiveTrCondition.connect(self._on_receive_tr_condition)
            self.widget.OnReceiveRealCondition.connect(self._on_receive_real_condition)
            if hasattr(self.widget, "OnReceiveTrData"):
                self.widget.OnReceiveTrData.connect(self._on_receive_tr_data)
            if hasattr(self.widget, "OnReceiveChejanData"):
                self.widget.OnReceiveChejanData.connect(self._on_receive_chejan_data)
            if hasattr(self.widget, "OnReceiveMsg"):
                self.widget.OnReceiveMsg.connect(self._on_receive_msg)
            if hasattr(self.widget, "OnReceiveRealData"):
                self.widget.OnReceiveRealData.connect(self._on_receive_real_data)
        except Exception as exc:
            self.widget = None
            self.log_emitted.emit("❌ 키움 컨트롤 초기화 실패: {0}".format(exc))
            self.log_emitted.emit(traceback.format_exc())

    def is_available(self):
        return self.widget is not None

    def get_connect_state(self):
        if not self.widget:
            return 0
        try:
            return int(self.widget.dynamicCall("GetConnectState()") or 0)
        except Exception:
            return 0

    def notify_connection_lost(self, message="연결 끊김 감지"):
        was_connected = bool(self.connected)
        self.connected = False
        if was_connected:
            self.connection_changed.emit(False, message)
        self.log_emitted.emit("❌ 키움 연결 끊김: {0}".format(message))

    def _reset_connection_runtime_state(self):
        self.connected = False
        self.user_id = ""
        self.user_name = ""
        self.server_gubun = ""
        self.account_list = []
        self._condition_cache = []
        self._rq_context_map = {}
        self._account_sync_queue = []
        self._current_sync_context = None
        self._snapshot_wait_code = ""
        self._snapshot_result = 0.0
        self._quote_snapshot_wait_code = ""
        self._quote_snapshot_result = {"current_price": 0.0, "ask1": 0.0, "current_volume": 0.0, "current_turnover": 0.0}
        self._minute_snapshot_wait_code = ""
        self._minute_snapshot_context = {}
        self._daily_snapshot_wait_code = ""
        self._daily_snapshot_result = {}
        self._holding_real_codes = []
        self._market_real_codes = []
        self._real_snapshot_cache = {}
        self._quote_snapshot_cache = {}
        self._daily_reference_cache = {}
        self._intraday_reference_cache = {}
        for loop_name in ["_snapshot_loop", "_quote_snapshot_loop", "_minute_snapshot_loop", "_daily_snapshot_loop"]:
            loop = getattr(self, loop_name, None)
            if loop is not None:
                try:
                    loop.exit()
                except Exception:
                    pass
            setattr(self, loop_name, None)

    def _rebuild_control(self):
        old_widget = self.widget
        self.widget = None
        if old_widget is not None:
            try:
                old_widget.clear()
            except Exception:
                pass
            try:
                old_widget.deleteLater()
            except Exception:
                pass
        self._init_control()

    def connect_server(self):
        if not self.widget:
            self.log_emitted.emit("❌ 키움 API 컨트롤이 준비되지 않았습니다")
            return False
        try:
            self.widget.dynamicCall("CommConnect()")
            self.log_emitted.emit("🔐 키움 로그인 창 호출")
            return True
        except Exception as exc:
            self.log_emitted.emit("❌ 로그인 호출 실패: {0}".format(exc))
            return False

    def disconnect_server(self, reason="사용자 요청"):
        reason = str(reason or "사용자 요청").strip() or "사용자 요청"
        if not self.widget:
            self._reset_connection_runtime_state()
            self.connection_changed.emit(False, "{0}으로 연결 종료".format(reason))
            self.log_emitted.emit("🔌 키움 API 연결 종료: {0}".format(reason))
            return True
        try:
            self.widget.dynamicCall("DisconnectRealData(QString)", self._market_real_screen_no)
        except Exception:
            pass
        try:
            self.widget.dynamicCall("DisconnectRealData(QString)", self._holding_real_screen_no)
        except Exception:
            pass
        try:
            self.widget.dynamicCall("SetRealRemove(QString, QString)", "ALL", "ALL")
        except Exception:
            pass
        terminate_ok = True
        try:
            self.widget.dynamicCall("CommTerminate()")
        except Exception as exc:
            terminate_ok = False
            self.log_emitted.emit("⚠️ 키움 연결 종료 호출 경고: {0}".format(exc))
        self._reset_connection_runtime_state()
        self.connection_changed.emit(False, "{0}으로 연결 종료".format(reason))
        self.log_emitted.emit("🔌 키움 API 연결 종료: {0}".format(reason))
        if not terminate_ok:
            self.log_emitted.emit("🔄 키움 API 컨트롤 재초기화")
            self._rebuild_control()
        return True

    def _on_event_connect(self, err_code):
        ok = int(err_code) == 0
        self.connected = ok
        if ok:
            self.user_id = self.get_login_info("USER_ID")
            self.user_name = self.get_login_info("USER_NAME")
            self.server_gubun = self.get_login_info("GetServerGubun")
            accno = self.get_login_info("ACCNO")
            self.account_list = [x for x in str(accno).split(";") if x.strip()]
            self.connection_changed.emit(True, "연결 성공")
            self.accounts_loaded.emit(self.account_list)
            self.log_emitted.emit("✅ 키움 로그인 성공: {0} / 계좌 {1}개".format(self.user_name, len(self.account_list)))
        else:
            self.connection_changed.emit(False, "연결 실패: {0}".format(err_code))
            self.log_emitted.emit("❌ 키움 로그인 실패: {0}".format(err_code))

    def get_login_info(self, tag):
        if not self.widget:
            return ""
        try:
            return str(self.widget.dynamicCall("GetLoginInfo(QString)", tag))
        except Exception:
            return ""

    def get_master_code_name(self, code):
        if not self.widget or not code:
            return code or ""
        try:
            name = str(self.widget.dynamicCall("GetMasterCodeName(QString)", str(code)))
            return name or code
        except Exception:
            return code

    def get_master_last_price(self, code):
        if not self.widget or not code:
            return 0
        try:
            value = str(self.widget.dynamicCall("GetMasterLastPrice(QString)", str(code))).strip()
            return abs(int(value.replace(",", "") or "0"))
        except Exception:
            return 0

    def set_market_realtime_codes(self, codes, fids="10;13;14;121;125"):
        if not self.widget:
            return False
        normalized = []
        seen = set()
        for code in list(codes or []):
            code = str(code or "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            normalized.append(code)
        try:
            self.widget.dynamicCall("DisconnectRealData(QString)", self._market_real_screen_no)
        except Exception:
            pass
        self._market_real_codes = normalized
        self._holding_real_codes = list(normalized)
        if not normalized:
            self.log_emitted.emit("📴 실시간 시장데이터 구독 해제")
            return True
        try:
            code_list = ";".join(normalized)
            result = int(
                self.widget.dynamicCall(
                    "SetRealReg(QString, QString, QString, QString)",
                    self._market_real_screen_no,
                    code_list,
                    str(fids or "10;13;14;121;125"),
                    "0",
                ) or 0
            )
            self.log_emitted.emit("📡 실시간 시장데이터 등록: {0}건 / result={1}".format(len(normalized), result))
            return result == 0
        except Exception as exc:
            self.log_emitted.emit("❌ 실시간 시장데이터 등록 실패: {0}".format(exc))
            return False

    def set_holding_realtime_codes(self, codes, fids="10"):
        return self.set_market_realtime_codes(codes, fids="10;13;14;121;125")


    def get_realtime_snapshot(self, code):
        code = str(code or "").strip()
        if not code:
            return {}
        return dict(self._real_snapshot_cache.get(code) or {})

    def _compute_intraday_vwap(self, volume, turnover):
        volume = self._to_abs_float(volume)
        turnover = self._to_abs_float(turnover)
        if volume <= 0 or turnover <= 0:
            return 0.0
        return float(turnover / volume)

    def _normalize_realtime_turnover(self, turnover, price=0.0, volume=0.0):
        turnover = self._to_abs_float(turnover)
        price = self._to_abs_float(price)
        volume = self._to_abs_float(volume)
        if turnover <= 0:
            return 0.0
        if price <= 0 or volume <= 0:
            return turnover
        implied_vwap = float(turnover / volume)
        # Kiwoom real-time FID 14 can arrive in condensed units. If the implied
        # price is implausibly tiny versus the current price, treat it as million KRW.
        if implied_vwap > 0 and implied_vwap < max(10.0, price * 0.1):
            scaled_turnover = float(turnover * 1000000.0)
            scaled_vwap = float(scaled_turnover / volume) if volume > 0 else 0.0
            if scaled_vwap > 0 and scaled_vwap <= max(price * 5.0, price + 500000.0):
                return scaled_turnover
        return turnover

    def _get_latest_intraday_reference_snapshot(self, code, target_dt=None):
        code = str(code or "").strip()
        if not code:
            return {}
        target_dt = target_dt or datetime.datetime.now()
        prefix = "{0}|{1}".format(code, target_dt.strftime("%Y%m%d"))
        best_cached_at = None
        best_value = {}
        for cache_key, item in list((self._intraday_reference_cache or {}).items()):
            if not str(cache_key or "").startswith(prefix):
                continue
            cached_at = item.get("cached_at")
            if not isinstance(cached_at, datetime.datetime):
                continue
            if (target_dt - cached_at).total_seconds() > float(self._intraday_reference_cache_ttl_sec):
                continue
            if best_cached_at is None or cached_at > best_cached_at:
                best_cached_at = cached_at
                best_value = dict(item.get("value") or {})
        return best_value

    def get_enriched_realtime_snapshot(self, code, seed_snapshot=None, target_dt=None, allow_tr=False, timeout_ms=1500):
        code = str(code or "").strip()
        if not code:
            return {}
        target_dt = target_dt or datetime.datetime.now()
        snapshot = dict(seed_snapshot or {})
        live_snapshot = dict(self.get_realtime_snapshot(code) or {})
        for key in [
            "current_price",
            "ask1",
            "current_volume",
            "current_turnover",
            "acc_volume",
            "acc_turnover",
            "sell_hoga_total",
            "buy_hoga_total",
            "vwap_intraday",
            "sell_pressure_ratio",
        ]:
            if self._to_abs_float(snapshot.get(key)) <= 0 and self._to_abs_float(live_snapshot.get(key)) > 0:
                snapshot[key] = self._to_abs_float(live_snapshot.get(key))

        if self._to_abs_float(snapshot.get("current_volume")) <= 0 and self._to_abs_float(snapshot.get("acc_volume")) > 0:
            snapshot["current_volume"] = self._to_abs_float(snapshot.get("acc_volume"))
        if self._to_abs_float(snapshot.get("current_turnover")) <= 0 and self._to_abs_float(snapshot.get("acc_turnover")) > 0:
            snapshot["current_turnover"] = self._to_abs_float(snapshot.get("acc_turnover"))

        if self._to_abs_float(snapshot.get("vwap_intraday")) <= 0:
            computed_vwap = self._compute_intraday_vwap(
                snapshot.get("acc_volume"),
                snapshot.get("acc_turnover"),
            )
            if computed_vwap > 0:
                snapshot["vwap_intraday"] = computed_vwap

        if self._to_abs_float(snapshot.get("vwap_intraday")) <= 0:
            reference_snapshot = self._get_latest_intraday_reference_snapshot(code, target_dt=target_dt)
            reference_volume = self._to_abs_float(reference_snapshot.get("current_volume"))
            reference_turnover = self._to_abs_float(reference_snapshot.get("current_turnover"))
            reference_vwap = self._to_abs_float(reference_snapshot.get("vwap_intraday"))
            if reference_vwap <= 0:
                reference_vwap = self._compute_intraday_vwap(reference_volume, reference_turnover)
            if self._to_abs_float(snapshot.get("acc_volume")) <= 0 and reference_volume > 0:
                snapshot["acc_volume"] = reference_volume
            if self._to_abs_float(snapshot.get("current_volume")) <= 0 and reference_volume > 0:
                snapshot["current_volume"] = reference_volume
            if self._to_abs_float(snapshot.get("acc_turnover")) <= 0 and reference_turnover > 0:
                snapshot["acc_turnover"] = reference_turnover
            if self._to_abs_float(snapshot.get("current_turnover")) <= 0 and reference_turnover > 0:
                snapshot["current_turnover"] = reference_turnover
            if self._to_abs_float(snapshot.get("vwap_intraday")) <= 0 and reference_vwap > 0:
                snapshot["vwap_intraday"] = reference_vwap

        if self._to_abs_float(snapshot.get("vwap_intraday")) <= 0 and allow_tr and self.connected and self.widget:
            try:
                reference_snapshot = dict(
                    self.request_intraday_reference_stats(
                        code,
                        target_dt=target_dt,
                        lookback_days=5,
                        timeout_ms=min(int(timeout_ms or 1500), 1800),
                        max_pages=2,
                        allow_quote_fallback=False,
                        seed_snapshot=snapshot,
                    ) or {}
                )
            except Exception:
                reference_snapshot = {}
            reference_volume = self._to_abs_float(reference_snapshot.get("current_volume"))
            reference_turnover = self._to_abs_float(reference_snapshot.get("current_turnover"))
            reference_vwap = self._to_abs_float(reference_snapshot.get("vwap_intraday"))
            if reference_vwap <= 0:
                reference_vwap = self._compute_intraday_vwap(reference_volume, reference_turnover)
            if self._to_abs_float(snapshot.get("acc_volume")) <= 0 and reference_volume > 0:
                snapshot["acc_volume"] = reference_volume
            if self._to_abs_float(snapshot.get("current_volume")) <= 0 and reference_volume > 0:
                snapshot["current_volume"] = reference_volume
            if self._to_abs_float(snapshot.get("acc_turnover")) <= 0 and reference_turnover > 0:
                snapshot["acc_turnover"] = reference_turnover
            if self._to_abs_float(snapshot.get("current_turnover")) <= 0 and reference_turnover > 0:
                snapshot["current_turnover"] = reference_turnover
            if self._to_abs_float(snapshot.get("vwap_intraday")) <= 0 and reference_vwap > 0:
                snapshot["vwap_intraday"] = reference_vwap

        if self._to_abs_float(snapshot.get("sell_pressure_ratio")) <= 0:
            buy_total = self._to_abs_float(snapshot.get("buy_hoga_total"))
            sell_total = self._to_abs_float(snapshot.get("sell_hoga_total"))
            if buy_total > 0 and sell_total > 0:
                snapshot["sell_pressure_ratio"] = round(float(sell_total / buy_total), 4)
        return snapshot

    def _parse_cache_dt(self, value):
        if isinstance(value, datetime.datetime):
            return value
        value = str(value or "").strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                return datetime.datetime.strptime(value, fmt)
            except Exception:
                continue
        return None

    def _get_cached_snapshot(self, cache_map, cache_key, ttl_sec, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        item = dict(cache_map.get(cache_key) or {})
        if not item:
            return None
        cached_at = item.get("cached_at")
        if not isinstance(cached_at, datetime.datetime):
            return None
        if (now_dt - cached_at).total_seconds() > float(ttl_sec):
            return None
        return dict(item.get("value") or {})

    def _set_cached_snapshot(self, cache_map, cache_key, value, now_dt=None):
        if not cache_key:
            return
        cache_map[cache_key] = {
            "cached_at": now_dt or datetime.datetime.now(),
            "value": dict(value or {}),
        }

    def _daily_reference_cache_ttl(self, target_dt=None):
        target_dt = target_dt or datetime.datetime.now()
        hhmm = target_dt.strftime("%H%M")
        market_open = target_dt.weekday() < 5 and "0900" <= hhmm <= "1530"
        if market_open:
            return float(self._daily_reference_cache_live_ttl_sec)
        return float(self._daily_reference_cache_after_hours_ttl_sec)

    def _get_cached_quote_snapshot(self, code, now_dt=None):
        code = str(code or "").strip()
        if not code:
            return None
        now_dt = now_dt or datetime.datetime.now()
        live_snapshot = dict(self._real_snapshot_cache.get(code) or {})
        live_updated_at = self._parse_cache_dt(live_snapshot.get("updated_at"))
        if live_updated_at is not None and (now_dt - live_updated_at).total_seconds() <= float(self._quote_snapshot_cache_ttl_sec):
            snapshot = {
                "current_price": self._to_abs_float(live_snapshot.get("current_price")),
                "ask1": self._to_abs_float(live_snapshot.get("ask1")),
                "current_volume": self._to_abs_float(live_snapshot.get("current_volume") or live_snapshot.get("acc_volume")),
                "current_turnover": self._to_abs_float(live_snapshot.get("current_turnover") or live_snapshot.get("acc_turnover")),
            }
            if any(float(snapshot.get(key) or 0.0) > 0 for key in ["current_price", "ask1", "current_volume", "current_turnover"]):
                return snapshot
        return self._get_cached_snapshot(
            self._quote_snapshot_cache,
            code,
            self._quote_snapshot_cache_ttl_sec,
            now_dt=now_dt,
        )

    def _get_daily_reference_cache_key(self, code, target_dt=None, lookback_days=5):
        target_dt = target_dt or datetime.datetime.now()
        return "{0}|{1}|{2}".format(str(code or "").strip(), target_dt.strftime("%Y%m%d"), int(lookback_days or 5))

    def _get_intraday_reference_cache_key(self, code, target_dt=None, lookback_days=5):
        target_dt = target_dt or datetime.datetime.now()
        return "{0}|{1}|{2}".format(str(code or "").strip(), target_dt.strftime("%Y%m%d%H%M"), int(lookback_days or 5))

    def request_current_price_snapshot(self, code, timeout_ms=1200):
        snapshot = self.request_quote_snapshot(code, timeout_ms=timeout_ms)
        return float(snapshot.get("current_price", 0.0) or 0.0)

    def request_quote_snapshot(self, code, timeout_ms=1200):
        if not self.connected or not self.widget or not code:
            return {"current_price": 0.0, "ask1": 0.0, "current_volume": 0.0, "current_turnover": 0.0}
        cached_snapshot = self._get_cached_quote_snapshot(code)
        if cached_snapshot is not None:
            return cached_snapshot
        rq_name = "SNAP_QUOTE_{0}".format(str(code)[-6:])
        self._quote_snapshot_wait_code = str(code)
        self._quote_snapshot_result = {"current_price": 0.0, "ask1": 0.0, "current_volume": 0.0, "current_turnover": 0.0}
        self.set_input_value("종목코드", code)
        self._rq_context_map[rq_name] = {
            "type": "quote_snapshot",
            "code": str(code),
            "tr_code": "opt10001",
            "screen_no": "6501",
        }
        ok = self.comm_rq_data(rq_name, "opt10001", 0, "6501")
        if not ok:
            self._quote_snapshot_wait_code = ""
            return {"current_price": 0.0, "ask1": 0.0, "current_volume": 0.0, "current_turnover": 0.0}
        loop = QEventLoop(self)
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self._quote_snapshot_loop = loop
        timer.start(int(timeout_ms))
        loop.exec_()
        timer.stop()
        self._quote_snapshot_loop = None
        self._quote_snapshot_wait_code = ""
        result = dict(self._quote_snapshot_result or {"current_price": 0.0, "ask1": 0.0})
        self._set_cached_snapshot(self._quote_snapshot_cache, str(code), result)
        return result


    def request_daily_reference_stats(self, code, target_dt=None, lookback_days=5, timeout_ms=2200):
        target_dt = target_dt or datetime.datetime.now()
        if not self.connected or not self.widget or not code:
            return {
                "target_hhmm": target_dt.strftime("%H%M"),
                "avg_volume": 0.0,
                "avg_turnover": 0.0,
                "days_count": 0,
                "current_volume": 0.0,
                "current_turnover": 0.0,
                "reference_price": 0.0,
                "volume_ratio": 0.0,
                "turnover_ratio": 0.0,
                "metric_mode": "full_day",
                "latest_day": "",
                "volume_compare_label": "최근 5일 일간 평균 거래량",
                "turnover_compare_label": "최근 5일 일간 평균 거래대금",
            }
        cache_key = self._get_daily_reference_cache_key(code, target_dt=target_dt, lookback_days=lookback_days)
        cached_stats = self._get_cached_snapshot(
            self._daily_reference_cache,
            cache_key,
            self._daily_reference_cache_ttl(target_dt),
        )
        if cached_stats is not None:
            return cached_stats
        rq_name = "SNAP_DAY_{0}".format(str(code)[-6:])
        self._daily_snapshot_wait_code = str(code)
        self._daily_snapshot_result = {
            "target_hhmm": target_dt.strftime("%H%M"),
            "avg_volume": 0.0,
            "avg_turnover": 0.0,
            "days_count": 0,
            "current_volume": 0.0,
            "current_turnover": 0.0,
            "reference_price": 0.0,
            "volume_ratio": 0.0,
            "turnover_ratio": 0.0,
            "metric_mode": "full_day",
            "latest_day": "",
            "volume_compare_label": "최근 5일 일간 평균 거래량",
            "turnover_compare_label": "최근 5일 일간 평균 거래대금",
        }
        self.set_input_value("종목코드", code)
        self.set_input_value("기준일자", target_dt.strftime("%Y%m%d"))
        self.set_input_value("수정주가구분", "1")
        self._rq_context_map[rq_name] = {
            "type": "daily_history_snapshot",
            "code": str(code),
            "tr_code": "opt10081",
            "screen_no": "6503",
            "lookback_days": int(lookback_days),
            "target_dt": target_dt,
        }
        ok = self.comm_rq_data(rq_name, "opt10081", 0, "6503")
        if not ok:
            self._daily_snapshot_wait_code = ""
            return dict(self._daily_snapshot_result or {})
        loop = QEventLoop(self)
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self._daily_snapshot_loop = loop
        timer.start(int(timeout_ms))
        loop.exec_()
        timer.stop()
        self._daily_snapshot_loop = None
        self._daily_snapshot_wait_code = ""
        result = dict(self._daily_snapshot_result or {})
        self._set_cached_snapshot(self._daily_reference_cache, cache_key, result)
        return result


    def request_intraday_reference_stats(self, code, target_dt=None, lookback_days=5, timeout_ms=3200, max_pages=4, allow_quote_fallback=True, seed_snapshot=None):
        target_dt = target_dt or __import__("datetime").datetime.now()
        if not self.connected or not self.widget or not code:
            return {
                "target_hhmm": target_dt.strftime("%H%M"),
                "avg_volume": 0.0,
                "avg_turnover": 0.0,
                "days_count": 0,
                "current_volume": 0.0,
                "current_turnover": 0.0,
                "reference_price": 0.0,
                "vwap_intraday": 0.0,
            }
        cache_key = self._get_intraday_reference_cache_key(code, target_dt=target_dt, lookback_days=lookback_days)
        cached_stats = self._get_cached_snapshot(
            self._intraday_reference_cache,
            cache_key,
            self._intraday_reference_cache_ttl_sec,
        )
        if cached_stats is not None:
            return cached_stats
        rq_name = "SNAP_MIN_{0}".format(str(code)[-6:])
        self._minute_snapshot_wait_code = str(code)
        self._minute_snapshot_context = {
            "code": str(code),
            "target_dt": target_dt,
            "lookback_days": int(lookback_days),
            "max_pages": int(max_pages),
            "page_count": 0,
            "rows": [],
            "rq_name": rq_name,
            "screen_no": "6502",
            "done": False,
            "result": {
                "target_hhmm": target_dt.strftime("%H%M"),
                "avg_volume": 0.0,
                "avg_turnover": 0.0,
                "days_count": 0,
                "current_volume": 0.0,
                "current_turnover": 0.0,
                "reference_price": 0.0,
                "vwap_intraday": 0.0,
            },
        }
        self.set_input_value("종목코드", code)
        self.set_input_value("틱범위", "1")
        self.set_input_value("수정주가구분", "1")
        self._rq_context_map[rq_name] = {
            "type": "minute_history_snapshot",
            "code": str(code),
            "tr_code": "opt10080",
            "screen_no": "6502",
            "target_dt": target_dt,
            "lookback_days": int(lookback_days),
        }
        ok = self.comm_rq_data(rq_name, "opt10080", 0, "6502")
        if not ok:
            self._minute_snapshot_wait_code = ""
            return dict(self._minute_snapshot_context.get("result") or {})
        loop = QEventLoop(self)
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self._minute_snapshot_loop = loop
        timer.start(int(timeout_ms))
        loop.exec_()
        timer.stop()
        result = dict(self._minute_snapshot_context.get("result") or {})
        self._minute_snapshot_loop = None
        self._minute_snapshot_wait_code = ""
        self._minute_snapshot_context = {}

        quote = {}
        try:
            quote = dict(seed_snapshot or {})
        except Exception:
            quote = {}
        try:
            need_quote_fallback = (
                self._to_abs_float(result.get("reference_price")) <= 0
                or self._to_abs_float(result.get("current_volume")) <= 0
                or self._to_abs_float(result.get("current_turnover")) <= 0
            )
            if need_quote_fallback and not quote:
                quote = dict(self.get_realtime_snapshot(code) or {})
            if need_quote_fallback and allow_quote_fallback and (
                self._to_abs_float(quote.get("current_price")) <= 0
                or self._to_abs_float(quote.get("current_volume")) <= 0
                or self._to_abs_float(quote.get("current_turnover")) <= 0
            ):
                quote = dict(self.request_quote_snapshot(code, timeout_ms=min(int(timeout_ms), 1500)) or {})
        except Exception as exc:
            self.log_emitted.emit("⚠️ 분봉 기준치 보정용 호가조회 실패: {0} / {1}".format(code, exc))
            quote = dict(quote or {})

        if self._to_abs_float(result.get("reference_price")) <= 0 and self._to_abs_float(quote.get("current_price")) > 0:
            result["reference_price"] = self._to_abs_float(quote.get("current_price"))
        if self._to_abs_float(result.get("current_volume")) <= 0 and self._to_abs_float(quote.get("current_volume")) > 0:
            result["current_volume"] = self._to_abs_float(quote.get("current_volume"))
        if self._to_abs_float(result.get("current_turnover")) <= 0 and self._to_abs_float(quote.get("current_turnover")) > 0:
            result["current_turnover"] = self._to_abs_float(quote.get("current_turnover"))

        avg_volume = self._to_abs_float(result.get("avg_volume"))
        avg_turnover = self._to_abs_float(result.get("avg_turnover"))
        current_volume = self._to_abs_float(result.get("current_volume"))
        current_turnover = self._to_abs_float(result.get("current_turnover"))
        if self._to_abs_float(result.get("vwap_intraday")) <= 0 and current_volume > 0 and current_turnover > 0:
            result["vwap_intraday"] = self._compute_intraday_vwap(current_volume, current_turnover)

        if self._to_abs_float(result.get("volume_ratio")) <= 0 and avg_volume > 0 and current_volume > 0:
            result["volume_ratio"] = current_volume / avg_volume
        if self._to_abs_float(result.get("turnover_ratio")) <= 0 and avg_turnover > 0 and current_turnover > 0:
            result["turnover_ratio"] = current_turnover / avg_turnover

        if (
            self._to_abs_float(result.get("reference_price")) <= 0
            or (self._to_abs_float(result.get("volume_ratio")) <= 0 and self._to_abs_float(result.get("turnover_ratio")) <= 0)
        ):
            self.log_emitted.emit(
                "ℹ️ 분봉 기준치 결과 보정: {0} / ref={1} / cur_vol={2} / cur_turn={3} / avg_vol={4} / avg_turn={5} / days={6} / latest_day={7}".format(
                    code,
                    self._to_abs_float(result.get("reference_price")),
                    current_volume,
                    current_turnover,
                    avg_volume,
                    avg_turnover,
                    int(result.get("days_count") or 0),
                    str(result.get("latest_day") or "-"),
                )
            )
        self._set_cached_snapshot(self._intraday_reference_cache, cache_key, result)
        return result

    def _request_minute_history_next_page(self):
        context = dict(self._minute_snapshot_context or {})
        code = str(context.get("code") or "")
        if not code or self._minute_snapshot_wait_code != code:
            return
        self.set_input_value("종목코드", code)
        self.set_input_value("틱범위", "1")
        self.set_input_value("수정주가구분", "1")
        ok = self.comm_rq_data(context.get("rq_name", "SNAP_MIN_{0}".format(code[-6:])), "opt10080", 2, context.get("screen_no", "6502"))
        if not ok:
            self.log_emitted.emit("⚠️ 분봉 스냅샷 추가 요청 실패: {0}".format(code))
            self._minute_snapshot_context["done"] = True
            self._minute_snapshot_context["result"] = self._calc_intraday_reference_from_rows(
                self._minute_snapshot_context.get("rows", []),
                context.get("target_dt") or datetime.datetime.now(),
                context.get("lookback_days", 5),
            )
            if self._minute_snapshot_loop is not None:
                self._minute_snapshot_loop.quit()

    def _parse_daily_history_rows(self, tr_code, rq_name):
        rows = []
        repeat_cnt = self.get_repeat_cnt(tr_code, rq_name)
        for index in range(repeat_cnt):
            day = str(self.get_comm_data_any(tr_code, rq_name, index, ["일자", "날짜"]) or "").strip()
            price = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["현재가", "종가"]))
            volume = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["거래량"]))
            turnover = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["거래대금", "누적거래대금"]))
            if not day:
                continue
            rows.append({
                "day": day,
                "price": price,
                "volume": volume,
                "turnover": turnover,
            })
        return rows

    def _calc_daily_reference_from_rows(self, rows, target_dt=None, lookback_days=5):
        target_dt = target_dt or datetime.datetime.now()
        result = {
            "target_hhmm": target_dt.strftime("%H%M"),
            "avg_volume": 0.0,
            "avg_turnover": 0.0,
            "days_count": 0,
            "current_volume": 0.0,
            "current_turnover": 0.0,
            "reference_price": 0.0,
            "volume_ratio": 0.0,
            "turnover_ratio": 0.0,
            "metric_mode": "full_day",
            "latest_day": "",
            "volume_compare_label": "최근 5일 일간 평균 거래량",
            "turnover_compare_label": "최근 5일 일간 평균 거래대금",
        }
        rows = list(rows or [])
        if not rows:
            return result

        latest = rows[0]
        result["latest_day"] = str(latest.get("day") or "")
        result["reference_price"] = self._to_abs_float(latest.get("price"))
        result["current_volume"] = self._to_abs_float(latest.get("volume"))
        result["current_turnover"] = self._to_abs_float(latest.get("turnover"))

        compare_rows = []
        for row in rows[1:]:
            if len(compare_rows) >= int(lookback_days):
                break
            compare_rows.append(row)

        avg_volume_rows = [self._to_abs_float(row.get("volume")) for row in compare_rows if self._to_abs_float(row.get("volume")) > 0]
        avg_turnover_rows = [self._to_abs_float(row.get("turnover")) for row in compare_rows if self._to_abs_float(row.get("turnover")) > 0]

        avg_volume = (sum(avg_volume_rows) / float(len(avg_volume_rows))) if avg_volume_rows else 0.0
        avg_turnover = (sum(avg_turnover_rows) / float(len(avg_turnover_rows))) if avg_turnover_rows else 0.0
        current_volume = self._to_abs_float(result.get("current_volume"))
        current_turnover = self._to_abs_float(result.get("current_turnover"))

        result["avg_volume"] = avg_volume
        result["avg_turnover"] = avg_turnover
        result["days_count"] = max(len(avg_volume_rows), len(avg_turnover_rows))
        result["volume_ratio"] = (current_volume / avg_volume) if avg_volume > 0 and current_volume > 0 else 0.0
        result["turnover_ratio"] = (current_turnover / avg_turnover) if avg_turnover > 0 and current_turnover > 0 else 0.0
        return result

    def _parse_minute_history_rows(self, tr_code, rq_name):
        rows = []
        repeat_cnt = self.get_repeat_cnt(tr_code, rq_name)
        for index in range(repeat_cnt):
            dt_raw = str(self.get_comm_data_any(tr_code, rq_name, index, ["체결시간", "일자", "시간"]) or "").strip()
            price = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["현재가", "종가"]))
            volume = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["거래량"]))
            if not dt_raw:
                continue
            rows.append({
                "dt": dt_raw,
                "price": price,
                "volume": volume,
                "turnover": price * volume,
            })
        return rows

    def _sum_bucket_until(self, bucket, cutoff_hhmm):
        volume = 0.0
        turnover = 0.0
        for row in bucket.get("rows", []):
            if str(row.get("hhmm") or "") <= str(cutoff_hhmm or "2359"):
                volume += float(row.get("volume") or 0.0)
                turnover += float(row.get("turnover") or 0.0)
        return volume, turnover

    def _last_price_until(self, bucket, cutoff_hhmm):
        selected = 0.0
        selected_hhmm = ""
        for row in bucket.get("rows", []):
            hhmm = str(row.get("hhmm") or "")
            if hhmm and hhmm <= str(cutoff_hhmm or "2359") and hhmm >= selected_hhmm:
                selected_hhmm = hhmm
                selected = float(row.get("price") or 0.0)
        return selected

    def _calc_intraday_reference_from_rows(self, rows, target_dt, lookback_days=5):
        target_dt = target_dt or datetime.datetime.now()
        target_hhmm = target_dt.strftime("%H%M")
        today = target_dt.strftime("%Y%m%d")
        day_map = {}
        for row in rows:
            raw = str(row.get("dt") or "")
            if len(raw) < 12:
                continue
            day = raw[:8]
            hhmm = raw[8:12]
            bucket = day_map.setdefault(day, {
                "rows": [],
                "total_volume": 0.0,
                "total_turnover": 0.0,
                "close_price": 0.0,
                "last_hhmm": "",
            })
            item = {
                "hhmm": hhmm,
                "price": float(row.get("price") or 0.0),
                "volume": float(row.get("volume") or 0.0),
                "turnover": float(row.get("turnover") or 0.0),
            }
            bucket["rows"].append(item)
            bucket["total_volume"] += item["volume"]
            bucket["total_turnover"] += item["turnover"]
            if hhmm >= bucket["last_hhmm"]:
                bucket["last_hhmm"] = hhmm
                bucket["close_price"] = item["price"]

        ordered_days = sorted(day_map.keys(), reverse=True)
        default_result = {
            "target_hhmm": target_hhmm,
            "avg_volume": 0.0,
            "avg_turnover": 0.0,
            "days_count": 0,
            "current_volume": 0.0,
            "current_turnover": 0.0,
            "reference_price": 0.0,
            "volume_ratio": 0.0,
            "turnover_ratio": 0.0,
            "vwap_intraday": 0.0,
            "metric_mode": "same_time",
            "latest_day": "",
            "volume_compare_label": "최근 5일 동시간 평균",
            "turnover_compare_label": "최근 5일 동시간 평균",
        }
        if not ordered_days:
            return default_result

        latest_day = ordered_days[0]
        latest_bucket = day_map[latest_day]
        is_same_time = latest_day == today and target_dt.weekday() < 5 and "0900" <= target_hhmm <= "1530"

        if is_same_time:
            current_volume, current_turnover = self._sum_bucket_until(latest_bucket, target_hhmm)
            reference_price = self._last_price_until(latest_bucket, target_hhmm) or float(latest_bucket.get("close_price") or 0.0)
            compare_days = ordered_days[1:1 + int(lookback_days)]
            volume_compare_label = "최근 5일 동시간 평균"
            turnover_compare_label = "최근 5일 동시간 평균"
        else:
            current_volume = float(latest_bucket.get("total_volume") or 0.0)
            current_turnover = float(latest_bucket.get("total_turnover") or 0.0)
            reference_price = float(latest_bucket.get("close_price") or 0.0)
            compare_days = ordered_days[1:1 + int(lookback_days)]
            volume_compare_label = "최근 5일 일간 평균 거래량"
            turnover_compare_label = "최근 5일 일간 평균 거래대금"

        avg_volume_rows = []
        avg_turnover_rows = []
        for day in compare_days:
            bucket = day_map.get(day, {})
            if is_same_time:
                day_volume, day_turnover = self._sum_bucket_until(bucket, target_hhmm)
            else:
                day_volume = float(bucket.get("total_volume") or 0.0)
                day_turnover = float(bucket.get("total_turnover") or 0.0)
            if day_volume > 0:
                avg_volume_rows.append(day_volume)
            if day_turnover > 0:
                avg_turnover_rows.append(day_turnover)

        avg_volume = (sum(avg_volume_rows) / float(len(avg_volume_rows))) if avg_volume_rows else 0.0
        avg_turnover = (sum(avg_turnover_rows) / float(len(avg_turnover_rows))) if avg_turnover_rows else 0.0
        volume_ratio = (current_volume / avg_volume) if avg_volume > 0 and current_volume > 0 else 0.0
        turnover_ratio = (current_turnover / avg_turnover) if avg_turnover > 0 and current_turnover > 0 else 0.0
        days_count = max(len(avg_volume_rows), len(avg_turnover_rows))

        return {
            "target_hhmm": target_hhmm,
            "avg_volume": avg_volume,
            "avg_turnover": avg_turnover,
            "days_count": int(days_count),
            "current_volume": current_volume,
            "current_turnover": current_turnover,
            "reference_price": reference_price,
            "volume_ratio": volume_ratio,
            "turnover_ratio": turnover_ratio,
            "vwap_intraday": self._compute_intraday_vwap(current_volume, current_turnover),
            "metric_mode": "same_time" if is_same_time else "full_day",
            "latest_day": latest_day,
            "volume_compare_label": volume_compare_label,
            "turnover_compare_label": turnover_compare_label,
        }

    def load_conditions(self):
        if not self.widget:
            self.log_emitted.emit("❌ 조건검색식 로드 불가: API 미초기화")
            return False
        try:
            result = self.widget.dynamicCall("GetConditionLoad()")
            self.log_emitted.emit("📥 조건검색식 로드 요청: {0}".format(result))
            return bool(result)
        except Exception as exc:
            self.log_emitted.emit("❌ 조건검색식 로드 요청 실패: {0}".format(exc))
            return False

    def _on_receive_condition_ver(self, ret, msg):
        if int(ret) != 1:
            self.log_emitted.emit("❌ 조건검색식 버전 수신 실패: {0}".format(msg))
            return
        raw = str(self.widget.dynamicCall("GetConditionNameList()")) if self.widget else ""
        rows = []
        for item in raw.split(";"):
            if not item:
                continue
            try:
                idx, name = item.split("^")
                rows.append({
                    "condition_id": "cond_{0}".format(idx),
                    "condition_index": int(idx),
                    "condition_name": name,
                })
            except ValueError:
                continue
        self._condition_cache = rows
        self.conditions_loaded.emit(rows)
        self.log_emitted.emit("✅ 조건검색식 {0}개 로드".format(len(rows)))

    def send_condition(self, screen_no, condition_name, condition_index, search_type):
        if not self.widget:
            self.log_emitted.emit("❌ 조건검색 실행 불가: API 미초기화")
            return False
        try:
            result = self.widget.dynamicCall(
                "SendCondition(QString, QString, int, int)",
                str(screen_no),
                str(condition_name),
                int(condition_index),
                int(search_type),
            )
            self.log_emitted.emit(
                "📡 조건검색 실행 [{0}] {1} / realtime={2} / result={3}".format(
                    condition_index, condition_name, search_type, result
                )
            )
            return bool(result)
        except Exception as exc:
            self.log_emitted.emit("❌ 조건검색 실행 실패: {0}".format(exc))
            return False

    def stop_condition(self, screen_no, condition_name, condition_index):
        if not self.widget:
            self.log_emitted.emit("❌ 조건검색 중지 불가: API 미초기화")
            return False
        if not self.connected:
            self.log_emitted.emit("❌ 조건검색 중지 불가: 키움 미로그인 상태")
            return False
        try:
            self.widget.dynamicCall(
                "SendConditionStop(QString, QString, int)",
                str(screen_no),
                str(condition_name),
                int(condition_index),
            )
            self.log_emitted.emit("🛑 조건검색 중지 [{0}] {1} / screen={2}".format(condition_index, condition_name, screen_no))
            return True
        except Exception as exc:
            self.log_emitted.emit("❌ 조건검색 중지 실패: {0}".format(exc))
            return False

    def send_order(self, rq_name, screen_no, account_no, order_type, code, qty, price, hoga_gb, original_order_no=""):
        if not self.widget:
            self.log_emitted.emit("❌ 주문 실행 불가: API 미초기화")
            return False
        try:
            signature = "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)"
            params = [
                str(rq_name),
                str(screen_no),
                str(account_no),
                int(order_type),
                str(code),
                int(qty),
                int(price),
                str(hoga_gb),
                str(original_order_no),
            ]
            try:
                result = self.widget.dynamicCall(
                    signature,
                    tuple(params),
                )
            except Exception:
                result = self.widget.dynamicCall(
                    signature,
                    params,
                )
            result_code = int(result)
            self.log_emitted.emit("📨 주문 요청: {0} / {1} / qty={2} / hoga={3} / result={4}".format(account_no, code, qty, hoga_gb, result_code))
            return result_code == 0
        except Exception as exc:
            self.log_emitted.emit("❌ 주문 실행 실패: {0}".format(exc))
            return False

    def set_input_value(self, key, value):
        if not self.widget:
            return False
        try:
            self.widget.dynamicCall("SetInputValue(QString, QString)", str(key), str(value))
            return True
        except Exception as exc:
            self.log_emitted.emit("❌ SetInputValue 실패: {0} / {1}".format(key, exc))
            return False

    def comm_rq_data(self, rq_name, tr_code, prev_next, screen_no):
        if not self.widget:
            return False
        try:
            result = self.widget.dynamicCall(
                "CommRqData(QString, QString, int, QString)",
                str(rq_name),
                str(tr_code),
                int(prev_next),
                str(screen_no),
            )
            self.log_emitted.emit("📥 TR 요청: {0} / {1} / prev_next={2} / result={3}".format(rq_name, tr_code, prev_next, result))
            return int(result) == 0
        except Exception as exc:
            self.log_emitted.emit("❌ TR 요청 실패: {0} / {1}".format(rq_name, exc))
            return False

    def get_repeat_cnt(self, tr_code, rq_name):
        if not self.widget:
            return 0
        try:
            return int(self.widget.dynamicCall("GetRepeatCnt(QString, QString)", str(tr_code), str(rq_name)))
        except Exception:
            return 0

    def get_comm_data(self, tr_code, rq_name, index, item_name):
        if not self.widget:
            return ""
        try:
            value = self.widget.dynamicCall(
                "GetCommData(QString, QString, int, QString)",
                str(tr_code),
                str(rq_name),
                int(index),
                str(item_name),
            )
            return str(value).strip()
        except Exception:
            return ""

    def get_comm_data_any(self, tr_code, rq_name, index, item_names):
        for item_name in list(item_names or []):
            value = self.get_comm_data(tr_code, rq_name, index, item_name)
            if str(value).strip() != "":
                return value
        return ""

    def get_comm_data_first_match(self, tr_code, rq_name, index, item_names):
        for item_name in list(item_names or []):
            value = self.get_comm_data(tr_code, rq_name, index, item_name)
            if str(value).strip() != "":
                return str(item_name), value
        return "", ""

    def request_account_balance(self, account_no, prev_next=0, screen_no="6001", password="", password_mode="api_saved"):
        if not self.connected:
            self.log_emitted.emit("⚠️ 미연결 상태라 계좌평가잔고 조회를 생략합니다")
            return False
        rq_name = "SYNC_BALANCE_{0}".format(str(account_no)[-4:])
        self.set_input_value("계좌번호", account_no)
        self.set_input_value("비밀번호", password or "")
        self.set_input_value("비밀번호입력매체구분", "00")
        self.set_input_value("조회구분", "2")
        self._rq_context_map[rq_name] = {
            "type": "balance",
            "account_no": str(account_no),
            "tr_code": "opw00018",
            "screen_no": str(screen_no),
            "password_mode": str(password_mode or "api_saved"),
            "password": password or "",
        }
        return self.comm_rq_data(rq_name, "opw00018", int(prev_next), screen_no)

    def request_account_cash(self, account_no, prev_next=0, screen_no="6000", password="", password_mode="api_saved"):
        if not self.connected:
            self.log_emitted.emit("⚠️ 미연결 상태라 예수금 조회를 생략합니다")
            return False
        rq_name = "SYNC_CASH_{0}".format(str(account_no)[-4:])
        self.set_input_value("계좌번호", account_no)
        self.set_input_value("비밀번호", password or "")
        self.set_input_value("비밀번호입력매체구분", "00")
        self.set_input_value("조회구분", "2")
        self._rq_context_map[rq_name] = {
            "type": "cash",
            "account_no": str(account_no),
            "tr_code": "opw00001",
            "screen_no": str(screen_no),
            "password_mode": str(password_mode or "api_saved"),
            "password": password or "",
        }
        return self.comm_rq_data(rq_name, "opw00001", int(prev_next), screen_no)

    def request_outstanding_orders(self, account_no, prev_next=0, screen_no="6002"):
        if not self.connected:
            self.log_emitted.emit("⚠️ 미연결 상태라 미체결 조회를 생략합니다")
            return False
        rq_name = "SYNC_ORDERS_{0}".format(str(account_no)[-4:])
        self.set_input_value("계좌번호", account_no)
        self.set_input_value("전체종목구분", "0")
        self.set_input_value("매매구분", "0")
        self.set_input_value("종목코드", "")
        self.set_input_value("체결구분", "1")
        self._rq_context_map[rq_name] = {
            "type": "outstanding",
            "account_no": str(account_no),
            "tr_code": "opt10075",
            "screen_no": str(screen_no),
        }
        return self.comm_rq_data(rq_name, "opt10075", int(prev_next), screen_no)

    def request_account_daily_realized(self, account_no, prev_next=0, screen_no="6003", password="", password_mode="api_saved", trade_date=""):
        if not self.connected:
            self.log_emitted.emit("⚠️ 미연결 상태라 일자별실현손익 조회를 생략합니다")
            return False
        trade_date = str(trade_date or datetime.datetime.now().strftime("%Y%m%d"))
        rq_name = "SYNC_REALIZED_{0}".format(str(account_no)[-4:])
        self.set_input_value("계좌번호", account_no)
        self.set_input_value("비밀번호", password or "")
        self.set_input_value("비밀번호입력매체구분", "00")
        self.set_input_value("시작일자", trade_date)
        self.set_input_value("종료일자", trade_date)
        self._rq_context_map[rq_name] = {
            "type": "realized",
            "account_no": str(account_no),
            "tr_code": "opt10074",
            "screen_no": str(screen_no),
            "password_mode": str(password_mode or "api_saved"),
            "password": password or "",
            "trade_date": trade_date,
        }
        return self.comm_rq_data(rq_name, "opt10074", int(prev_next), screen_no)

    def is_account_sync_busy(self):
        return (self._current_sync_context is not None) or bool(self._account_sync_queue)

    def request_account_sync(self, account_profiles):
        profiles = []
        for item in list(account_profiles or []):
            if isinstance(item, dict):
                account_no = str(item.get("account_no", "") or "").strip()
                if not account_no:
                    continue
                profiles.append({
                    "account_no": account_no,
                    "query_password_mode": str(item.get("query_password_mode", "api_saved") or "api_saved"),
                    "query_password": str(item.get("query_password", "") or ""),
                    "include_cash": bool(item.get("include_cash", True)),
                    "include_balance": bool(item.get("include_balance", True)),
                    "include_realized": bool(item.get("include_realized", True)),
                    "include_outstanding": bool(item.get("include_outstanding", True)),
                })
            else:
                account_no = str(item).strip()
                if account_no:
                    profiles.append({
                        "account_no": account_no,
                        "query_password_mode": "api_saved",
                        "query_password": "",
                        "include_cash": True,
                        "include_balance": True,
                        "include_realized": True,
                        "include_outstanding": True,
                    })
        if not profiles:
            self.log_emitted.emit("⚠️ 동기화할 활성 계좌가 없습니다")
            return False
        self._account_sync_queue = []
        for idx, profile in enumerate(profiles):
            account_no = profile["account_no"]
            password_mode = profile.get("query_password_mode", "api_saved")
            password = profile.get("query_password", "") if password_mode == "program_input" else ""
            include_cash = bool(profile.get("include_cash", True))
            include_balance = bool(profile.get("include_balance", True))
            include_realized = bool(profile.get("include_realized", True))
            include_outstanding = bool(profile.get("include_outstanding", True))
            if password_mode == "program_input" and not password:
                self.log_emitted.emit("⚠️ 조회 비밀번호 입력 모드이나 비밀번호가 비어 있습니다: {0}".format(account_no))
            if include_cash:
                self._account_sync_queue.append({
                    "type": "cash",
                    "account_no": account_no,
                    "screen_no": "{0}{1:02d}".format(self._sync_screen_prefix, (idx * 4) + 1),
                    "password_mode": password_mode,
                    "password": password,
                })
            if include_balance:
                self._account_sync_queue.append({
                    "type": "balance",
                    "account_no": account_no,
                    "screen_no": "{0}{1:02d}".format(self._sync_screen_prefix, (idx * 4) + 2),
                    "password_mode": password_mode,
                    "password": password,
                })
            if include_realized:
                self._account_sync_queue.append({
                    "type": "realized",
                    "account_no": account_no,
                    "screen_no": "{0}{1:02d}".format(self._sync_screen_prefix, (idx * 4) + 3),
                    "password_mode": password_mode,
                    "password": password,
                })
            if include_outstanding:
                self._account_sync_queue.append({
                    "type": "outstanding",
                    "account_no": account_no,
                    "screen_no": "{0}{1:02d}".format(self._sync_screen_prefix, (idx * 4) + 4),
                    "password_mode": password_mode,
                })
        self._current_sync_context = None
        self.log_emitted.emit("🔄 계좌 동기화 큐 시작: {0}개 계좌".format(len(profiles)))
        QTimer.singleShot(0, self._dispatch_next_sync_request)
        return True

    def _dispatch_next_sync_request(self):
        if self._current_sync_context is not None:
            return
        if not self._account_sync_queue:
            self.account_sync_finished.emit({"finished_at": self.persistence.now_ts()})
            self.log_emitted.emit("✅ 계좌 동기화 큐 완료")
            return
        context = self._account_sync_queue.pop(0)
        self._current_sync_context = dict(context)
        if context["type"] == "cash":
            ok = self.request_account_cash(context["account_no"], 0, context["screen_no"], context.get("password", ""), context.get("password_mode", "api_saved"))
        elif context["type"] == "balance":
            ok = self.request_account_balance(context["account_no"], 0, context["screen_no"], context.get("password", ""), context.get("password_mode", "api_saved"))
        elif context["type"] == "realized":
            ok = self.request_account_daily_realized(context["account_no"], 0, context["screen_no"], context.get("password", ""), context.get("password_mode", "api_saved"))
        else:
            ok = self.request_outstanding_orders(context["account_no"], 0, context["screen_no"])
        if not ok:
            self.log_emitted.emit("❌ 계좌 동기화 요청 실패: {0} / {1}".format(context["type"], context["account_no"]))
            self._current_sync_context = None
            QTimer.singleShot(250, self._dispatch_next_sync_request)

    def _queue_followup_request(self, context, prev_next):
        if context["type"] == "cash":
            ok = self.request_account_cash(context["account_no"], prev_next, context["screen_no"], context.get("password", ""), context.get("password_mode", "api_saved"))
        elif context["type"] == "balance":
            ok = self.request_account_balance(context["account_no"], prev_next, context["screen_no"], context.get("password", ""), context.get("password_mode", "api_saved"))
        elif context["type"] == "realized":
            ok = self.request_account_daily_realized(context["account_no"], prev_next, context["screen_no"], context.get("password", ""), context.get("password_mode", "api_saved"), context.get("trade_date", ""))
        else:
            ok = self.request_outstanding_orders(context["account_no"], prev_next, context["screen_no"])
        if not ok:
            self._current_sync_context = None
            QTimer.singleShot(250, self._dispatch_next_sync_request)

    def _on_receive_tr_condition(self, screen_no, code_list, condition_name, index, next_):
        codes = [x for x in str(code_list).split(";") if x.strip()]
        payload = {
            "source": "tr_condition",
            "screen_no": str(screen_no),
            "condition_name": str(condition_name),
            "condition_index": int(index),
            "codes": codes,
            "next": int(next_),
        }
        self.condition_event_received.emit(payload)

    def _on_receive_real_condition(self, code, event_type, condition_name, condition_index):
        payload = {
            "source": "real_condition",
            "code": str(code),
            "event_type": str(event_type),
            "condition_name": str(condition_name),
            "condition_index": int(condition_index),
        }
        self.condition_event_received.emit(payload)

    def _on_receive_real_data(self, code, real_type, real_data):
        try:
            code = str(code or "").strip()
            if not code:
                return
            if self._market_real_codes and code not in self._market_real_codes:
                return
            price = self._to_abs_float(self.widget.dynamicCall("GetCommRealData(QString, int)", code, 10))
            volume = self._to_abs_float(self.widget.dynamicCall("GetCommRealData(QString, int)", code, 13))
            turnover = self._normalize_realtime_turnover(
                self.widget.dynamicCall("GetCommRealData(QString, int)", code, 14),
                price=price,
                volume=volume,
            )
            sell_hoga_total = self._to_abs_float(self.widget.dynamicCall("GetCommRealData(QString, int)", code, 121))
            buy_hoga_total = self._to_abs_float(self.widget.dynamicCall("GetCommRealData(QString, int)", code, 125))
            if price <= 0 and volume <= 0 and turnover <= 0 and sell_hoga_total <= 0 and buy_hoga_total <= 0:
                return
            payload = {
                "code": code,
                "real_type": str(real_type or ""),
                "current_price": float(price),
                "acc_volume": float(volume),
                "acc_turnover": float(turnover),
                "sell_hoga_total": float(sell_hoga_total),
                "buy_hoga_total": float(buy_hoga_total),
                "received_at": self.persistence.now_ts(),
            }
            cache_row = dict(self._real_snapshot_cache.get(code) or {})
            if float(price) > 0:
                cache_row["current_price"] = float(price)
            if float(volume) > 0:
                cache_row["current_volume"] = float(volume)
                cache_row["acc_volume"] = float(volume)
            if float(turnover) > 0:
                cache_row["current_turnover"] = float(turnover)
                cache_row["acc_turnover"] = float(turnover)
            if float(sell_hoga_total) > 0:
                cache_row["sell_hoga_total"] = float(sell_hoga_total)
            if float(buy_hoga_total) > 0:
                cache_row["buy_hoga_total"] = float(buy_hoga_total)
            acc_volume = self._to_abs_float(cache_row.get("acc_volume") or cache_row.get("current_volume"))
            acc_turnover = self._to_abs_float(cache_row.get("acc_turnover") or cache_row.get("current_turnover"))
            if acc_volume > 0 and acc_turnover > 0:
                cache_row["vwap_intraday"] = float(acc_turnover / acc_volume)
            buy_total = self._to_abs_float(cache_row.get("buy_hoga_total"))
            sell_total = self._to_abs_float(cache_row.get("sell_hoga_total"))
            if buy_total > 0 and sell_total > 0:
                cache_row["sell_pressure_ratio"] = round(float(sell_total / buy_total), 4)
            cache_row["updated_at"] = payload["received_at"]
            self._real_snapshot_cache[code] = cache_row
            if price > 0:
                self.real_price_received.emit(payload)
            self.real_market_data_received.emit(payload)
        except Exception as exc:
            self.log_emitted.emit("⚠️ 실시간 시세 수신 처리 실패: {0}".format(exc))


    def _on_receive_tr_data(self, screen_no, rq_name, tr_code, record_name, prev_next, data_len, error_code, message, splm_msg):
        try:
            context = self._rq_context_map.get(str(rq_name), {})
            ctx_type = context.get("type")
            if ctx_type == "cash":
                summary = self._parse_cash_summary(str(tr_code), str(rq_name))
                payload = {
                    "screen_no": str(screen_no),
                    "rq_name": str(rq_name),
                    "tr_code": str(tr_code),
                    "record_name": str(record_name),
                    "prev_next": str(prev_next).strip(),
                    "account_no": context.get("account_no", ""),
                    "summary": summary,
                    "message": str(message or ""),
                }
                self.account_cash_received.emit(payload)
                self.log_emitted.emit("💰 예수금 수신: {0} / 예수금={1} / 주문가능={2}".format(payload["account_no"], summary.get("deposit_cash", 0), summary.get("orderable_cash", 0)))
            elif ctx_type == "balance":
                rows = self._parse_balance_rows(str(tr_code), str(rq_name))
                summary = self._parse_balance_summary(str(tr_code), str(rq_name))
                payload = {
                    "screen_no": str(screen_no),
                    "rq_name": str(rq_name),
                    "tr_code": str(tr_code),
                    "record_name": str(record_name),
                    "prev_next": str(prev_next).strip(),
                    "account_no": context.get("account_no", ""),
                    "summary": summary,
                    "rows": rows,
                    "message": str(message or ""),
                }
                self.account_positions_received.emit(payload)
                self.log_emitted.emit("📊 계좌평가잔고 수신: {0} / {1}건 / next={2}".format(payload["account_no"], len(rows), payload["prev_next"] or "0"))
            elif ctx_type == "realized":
                summary = self._parse_daily_realized_summary(str(tr_code), str(rq_name))
                payload = {
                    "screen_no": str(screen_no),
                    "rq_name": str(rq_name),
                    "tr_code": str(tr_code),
                    "record_name": str(record_name),
                    "prev_next": str(prev_next).strip(),
                    "account_no": context.get("account_no", ""),
                    "summary": summary,
                    "message": str(message or ""),
                }
                self.account_realized_received.emit(payload)
                self.log_emitted.emit("💹 일자별실현손익 수신: {0} / 실현손익={1} / field={2} / rows={3}".format(payload["account_no"], summary.get("api_realized_profit", 0), summary.get("matched_field", "-"), summary.get("row_count", 0)))
            elif ctx_type == "outstanding":
                rows = self._parse_outstanding_rows(str(tr_code), str(rq_name))
                payload = {
                    "screen_no": str(screen_no),
                    "rq_name": str(rq_name),
                    "tr_code": str(tr_code),
                    "record_name": str(record_name),
                    "prev_next": str(prev_next).strip(),
                    "account_no": context.get("account_no", ""),
                    "rows": rows,
                    "message": str(message or ""),
                }
                self.outstanding_orders_received.emit(payload)
                self.log_emitted.emit("📋 미체결 조회 수신: {0} / {1}건 / next={2}".format(payload["account_no"], len(rows), payload["prev_next"] or "0"))
            elif ctx_type == "quote_snapshot":
                current_price = self._to_abs_float(self.get_comm_data_any(str(tr_code), str(rq_name), 0, ["현재가"]))
                ask1 = self._to_abs_float(self.get_comm_data_any(str(tr_code), str(rq_name), 0, ["매도최우선호가", "매도1차호가", "매도호가1", "최우선매도호가", "매도호가"]))
                current_volume = self._to_abs_float(self.get_comm_data_any(str(tr_code), str(rq_name), 0, ["거래량"]))
                turnover_item_name, turnover_raw_value = self.get_comm_data_first_match(
                    str(tr_code),
                    str(rq_name),
                    0,
                    ["누적거래대금", "거래대금", "거래대금(백만)"],
                )
                current_turnover = self._to_abs_float(turnover_raw_value)
                if turnover_item_name == "거래대금(백만)" and current_turnover > 0:
                    current_turnover = float(current_turnover * 1000000.0)
                self._quote_snapshot_result = {
                    "current_price": current_price,
                    "ask1": ask1,
                    "current_volume": current_volume,
                    "current_turnover": current_turnover,
                    "turnover_item_name": turnover_item_name,
                }
                self._set_cached_snapshot(self._quote_snapshot_cache, str(context.get("code", "")), self._quote_snapshot_result)
                self._snapshot_result = current_price
                self.log_emitted.emit(
                    "💹 호가 스냅샷 수신: {0} / 현재가={1} / 매도1={2} / 거래량={3} / 거래대금={4} / 항목={5}".format(
                        context.get("code", ""),
                        current_price,
                        ask1,
                        current_volume,
                        current_turnover,
                        turnover_item_name or "-",
                    )
                )
                if self._quote_snapshot_loop is not None and self._quote_snapshot_wait_code == context.get("code", ""):
                    self._quote_snapshot_loop.quit()
                if self._snapshot_loop is not None and self._snapshot_wait_code == context.get("code", ""):
                    self._snapshot_loop.quit()
            elif ctx_type == "minute_history_snapshot":
                rows = self._parse_minute_history_rows(str(tr_code), str(rq_name))
                if self._minute_snapshot_wait_code == context.get("code", "") and self._minute_snapshot_context:
                    self._minute_snapshot_context["page_count"] = int(self._minute_snapshot_context.get("page_count", 0) or 0) + 1
                    self._minute_snapshot_context.setdefault("rows", []).extend(rows)
                    page_count = int(self._minute_snapshot_context.get("page_count", 0) or 0)
                    self.log_emitted.emit("📘 분봉 스냅샷 수신: {0} / rows={1} / page={2} / next={3}".format(context.get("code", ""), len(rows), page_count, str(prev_next).strip() or "0"))
                    has_more_chart = str(prev_next).strip() == '2' and page_count < int(self._minute_snapshot_context.get("max_pages", 4) or 4)
                    if has_more_chart:
                        QTimer.singleShot(120, self._request_minute_history_next_page)
                    else:
                        self._minute_snapshot_context["done"] = True
                        self._minute_snapshot_context["result"] = self._calc_intraday_reference_from_rows(
                            self._minute_snapshot_context.get("rows", []),
                            self._minute_snapshot_context.get("target_dt") or datetime.datetime.now(),
                            self._minute_snapshot_context.get("lookback_days", 5),
                        )
                        self._set_cached_snapshot(
                            self._intraday_reference_cache,
                            self._get_intraday_reference_cache_key(
                                context.get("code", ""),
                                target_dt=self._minute_snapshot_context.get("target_dt") or datetime.datetime.now(),
                                lookback_days=self._minute_snapshot_context.get("lookback_days", 5),
                            ),
                            self._minute_snapshot_context.get("result") or {},
                            now_dt=self._minute_snapshot_context.get("target_dt") or datetime.datetime.now(),
                        )
                        if self._minute_snapshot_loop is not None:
                            self._minute_snapshot_loop.quit()
            elif ctx_type == "daily_history_snapshot":
                rows = self._parse_daily_history_rows(str(tr_code), str(rq_name))
                self.log_emitted.emit("📗 일봉 스냅샷 수신: {0} / rows={1}".format(context.get("code", ""), len(rows)))
                if self._daily_snapshot_wait_code == context.get("code", ""):
                    self._daily_snapshot_result = self._calc_daily_reference_from_rows(
                        rows,
                        context.get("target_dt") or datetime.datetime.now(),
                        context.get("lookback_days", 5),
                    )
                    self._set_cached_snapshot(
                        self._daily_reference_cache,
                        self._get_daily_reference_cache_key(
                            context.get("code", ""),
                            target_dt=context.get("target_dt") or datetime.datetime.now(),
                            lookback_days=context.get("lookback_days", 5),
                        ),
                        self._daily_snapshot_result,
                        now_dt=context.get("target_dt") or datetime.datetime.now(),
                    )
                    if self._daily_snapshot_loop is not None:
                        self._daily_snapshot_loop.quit()
        except Exception as exc:
            self.log_emitted.emit("❌ TR 수신 처리 실패: {0}".format(exc))
            self.log_emitted.emit(traceback.format_exc())
        finally:
            context = self._rq_context_map.get(str(rq_name), {})
            current = self._current_sync_context
            has_more = str(prev_next).strip() == '2' and bool(context)
            if has_more and current and context.get("account_no") == current.get("account_no") and context.get("type") == current.get("type"):
                QTimer.singleShot(350, lambda ctx=dict(context): self._queue_followup_request(ctx, 2))
            else:
                if current and context.get("account_no") == current.get("account_no") and context.get("type") == current.get("type"):
                    self._current_sync_context = None
                    QTimer.singleShot(350, self._dispatch_next_sync_request)

    def _parse_cash_summary(self, tr_code, rq_name):
        deposit_cash = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["예수금", "추정예수금", "D+2추정예수금"]))
        orderable_cash = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["주문가능금액", "주문가능현금", "출금가능금액"]))
        estimated_assets = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["추정예탁자산", "총평가금액", "총자산", "예탁자산평가액"]))
        api_total_buy = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["총매입금액", "총매입", "매입금액합계"]))
        api_total_eval = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["총평가금액", "총자산", "예탁자산평가액", "추정예탁자산"]))
        api_total_profit = self._to_float(self.get_comm_data_any(tr_code, rq_name, 0, ["총평가손익금액", "총손익금액", "총평가손익", "평가손익합계"]))
        api_realized_profit = self._to_float(self.get_comm_data_any(tr_code, rq_name, 0, ["실현손익", "실현손익금액", "당일실현손익", "실현손익합계", "당일매도손익", "금일매도손익", "당일손익금액", "당일손익"]))
        return {
            "deposit_cash": deposit_cash,
            "orderable_cash": orderable_cash,
            "estimated_assets": estimated_assets,
            "api_total_buy": api_total_buy,
            "api_total_eval": api_total_eval,
            "api_total_profit": api_total_profit,
            "api_realized_profit": api_realized_profit,
        }

    def _parse_balance_summary(self, tr_code, rq_name):
        deposit_cash = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["예수금", "추정예수금", "D+2추정예수금"]))
        orderable_cash = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["주문가능금액", "출금가능금액", "주문가능현금"]))
        estimated_assets = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["추정예탁자산", "총평가금액", "총자산", "예탁자산평가액"]))
        api_total_buy = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["총매입금액", "총매입", "매입금액합계"]))
        api_total_eval = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, 0, ["총평가금액", "총자산", "예탁자산평가액", "추정예탁자산"]))
        api_total_profit = self._to_float(self.get_comm_data_any(tr_code, rq_name, 0, ["총평가손익금액", "총손익금액", "총평가손익", "평가손익합계"]))
        api_realized_profit = self._to_float(self.get_comm_data_any(tr_code, rq_name, 0, ["실현손익", "실현손익금액", "당일실현손익", "실현손익합계", "당일매도손익", "금일매도손익", "당일손익금액", "당일손익"]))
        return {
            "deposit_cash": deposit_cash,
            "orderable_cash": orderable_cash,
            "estimated_assets": estimated_assets,
            "api_total_buy": api_total_buy,
            "api_total_eval": api_total_eval,
            "api_total_profit": api_total_profit,
            "api_realized_profit": api_realized_profit,
        }

    def _parse_balance_rows(self, tr_code, rq_name):
        rows = []
        repeat_cnt = self.get_repeat_cnt(tr_code, rq_name)
        for index in range(repeat_cnt):
            code = self._normalize_code(self.get_comm_data_any(tr_code, rq_name, index, ["종목번호", "종목코드"]))
            if not code:
                continue
            name = self.get_comm_data_any(tr_code, rq_name, index, ["종목명"]) or self.get_master_code_name(code)
            qty = self._to_int(self.get_comm_data_any(tr_code, rq_name, index, ["보유수량", "보유량"]))
            avg_price = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["매입가", "평균단가", "매입단가"]))
            current_price = self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["현재가"]))
            eval_profit = self._to_float(self.get_comm_data_any(tr_code, rq_name, index, ["평가손익", "평가손익금액"]))
            eval_rate = self._to_float(self.get_comm_data_any(tr_code, rq_name, index, ["수익률(%)", "수익률", "손익율"]))
            if qty <= 0:
                continue
            rows.append({
                "code": code,
                "name": name,
                "qty": qty,
                "avg_price": avg_price,
                "current_price": current_price,
                "eval_profit": eval_profit,
                "eval_rate": eval_rate,
            })
        return rows

    def _parse_daily_realized_summary(self, tr_code, rq_name):
        item_names = [
            "실현손익",
            "실현손익금액",
            "당일실현손익",
            "실현손익합계",
            "당일매도손익",
            "금일매도손익",
            "당일손익금액",
            "당일손익",
        ]
        matched_field, matched_value = self.get_comm_data_first_match(tr_code, rq_name, 0, item_names)
        realized_total = self._to_float(matched_value)
        row_count = self.get_repeat_cnt(tr_code, rq_name)
        if realized_total == 0 and row_count > 0:
            realized_total = 0.0
            for index in range(row_count):
                row_field, row_value = self.get_comm_data_first_match(tr_code, rq_name, index, item_names)
                if row_field and not matched_field:
                    matched_field = row_field
                realized_total += self._to_float(row_value)
        return {
            "api_realized_profit": realized_total,
            "matched_field": matched_field or "",
            "row_count": row_count,
        }

    def _parse_outstanding_rows(self, tr_code, rq_name):
        rows = []
        repeat_cnt = self.get_repeat_cnt(tr_code, rq_name)
        for index in range(repeat_cnt):
            order_no = self.get_comm_data_any(tr_code, rq_name, index, ["주문번호"])
            code = self._normalize_code(self.get_comm_data_any(tr_code, rq_name, index, ["종목코드", "종목번호"]))
            if not order_no or not code:
                continue
            name = self.get_comm_data_any(tr_code, rq_name, index, ["종목명"]) or self.get_master_code_name(code)
            order_qty = self._to_int(self.get_comm_data_any(tr_code, rq_name, index, ["주문수량"]))
            unfilled_qty = self._to_int(self.get_comm_data_any(tr_code, rq_name, index, ["미체결수량"]))
            filled_qty = max(0, order_qty - unfilled_qty)
            rows.append({
                "order_no": str(order_no).strip(),
                "code": code,
                "name": name,
                "order_status": self.get_comm_data_any(tr_code, rq_name, index, ["주문상태"]),
                "order_qty": order_qty,
                "unfilled_qty": unfilled_qty,
                "filled_qty": filled_qty,
                "order_price": self._to_abs_float(self.get_comm_data_any(tr_code, rq_name, index, ["주문가격"])),
                "order_gubun": self.get_comm_data_any(tr_code, rq_name, index, ["주문구분", "매매구분"]),
                "order_time": self.get_comm_data_any(tr_code, rq_name, index, ["시간", "주문시간"]),
            })
        return rows

    def _on_receive_chejan_data(self, gubun, item_cnt, fid_list):
        payload = {
            "gubun": str(gubun),
            "item_cnt": int(item_cnt),
            "fids": [x for x in str(fid_list).split(";") if x],
            "account_no": self._get_chejan_data(9201),
            "order_no": self._get_chejan_data(9203),
            "manager_no": self._get_chejan_data(9205),
            "code": self._normalize_code(self._get_chejan_data(9001)),
            "job_type": self._get_chejan_data(912),
            "order_state": self._get_chejan_data(913),
            "name": self._get_chejan_data(302),
            "order_qty": self._get_chejan_data(900),
            "order_price": self._get_chejan_data(901),
            "unfilled_qty": self._get_chejan_data(902),
            "cum_amount": self._get_chejan_data(903),
            "original_order_no": self._get_chejan_data(904),
            "order_gubun": self._get_chejan_data(905),
            "hoga_gubun": self._get_chejan_data(906),
            "buy_sell_gubun": self._get_chejan_data(907),
            "order_time": self._get_chejan_data(908),
            "fill_no": self._get_chejan_data(909),
            "fill_price": self._get_chejan_data(910),
            "fill_qty": self._get_chejan_data(911),
            "current_price": self._get_chejan_data(10),
            "ask_price": self._get_chejan_data(27),
            "bid_price": self._get_chejan_data(28),
            "holding_qty": self._get_chejan_data(930),
            "avg_price": self._get_chejan_data(931),
            "total_buy_amount": self._get_chejan_data(932),
            "orderable_qty": self._get_chejan_data(933),
            "today_net_qty": self._get_chejan_data(945),
            "hold_buy_sell": self._get_chejan_data(946),
            "today_realized_pnl": self._get_chejan_data(950),
            "deposit": self._get_chejan_data(951),
            "profit_rate": self._get_chejan_data(8019),
        }
        self.chejan_received.emit(payload)
        self.log_emitted.emit("📬 체잔 수신: {0}".format(payload))

    def _normalize_code(self, value):
        value = str(value or '').strip()
        if value.startswith('A'):
            value = value[1:]
        return value

    def _get_chejan_data(self, fid):
        if not self.widget:
            return ""
        try:
            return str(self.widget.dynamicCall("GetChejanData(int)", int(fid))).strip()
        except Exception:
            return ""

    def _on_receive_msg(self, screen_no, rq_name, tr_code, msg):
        payload = {
            "screen_no": str(screen_no),
            "rq_name": str(rq_name),
            "tr_code": str(tr_code),
            "message": str(msg),
        }
        self.api_message_received.emit(payload)
        self.log_emitted.emit("ℹ️ API 메시지: {0}".format(payload))

    def _to_int(self, value):
        text = str(value or '').replace(',', '').replace('+', '').strip()
        if text == '':
            return 0
        try:
            return int(float(text))
        except Exception:
            return 0

    def _to_float(self, value):
        text = str(value or '').replace(',', '').replace('%', '').strip()
        if text == '':
            return 0.0
        try:
            return float(text)
        except Exception:
            return 0.0

    def _to_abs_float(self, value):
        return abs(float(self._to_int(value)))
