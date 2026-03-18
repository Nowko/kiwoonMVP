# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import datetime
import subprocess
from html import escape

from PyQt5.QtCore import Qt, QTimer, QLocale
from PyQt5.QtGui import QColor, QCursor
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QRadioButton,
    QSizePolicy,
    QSplitter,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QToolTip,
    QVBoxLayout,
    QWidget,
)


class SortableTableWidgetItem(QTableWidgetItem):
    def __init__(self, text="", sort_value=None):
        super(SortableTableWidgetItem, self).__init__(str(text))
        self._sort_value = str(text) if sort_value is None else sort_value
        self.setData(Qt.UserRole, self._sort_value)

    def __lt__(self, other):
        left = self.data(Qt.UserRole)
        right = other.data(Qt.UserRole) if isinstance(other, QTableWidgetItem) else None
        if right is not None:
            try:
                return left < right
            except Exception:
                return str(left) < str(right)
        return super(SortableTableWidgetItem, self).__lt__(other)


class StartupLoadingDialog(QDialog):
    def __init__(self, parent=None):
        super(StartupLoadingDialog, self).__init__(parent)
        self.setWindowTitle("시작 준비 중")
        self.setModal(True)
        self.setWindowModality(Qt.ApplicationModal)
        self.setWindowFlags(Qt.Dialog | Qt.CustomizeWindowHint | Qt.WindowTitleHint)
        self.setMinimumWidth(420)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(10)

        self.lbl_title = QLabel("필수 시작 작업을 불러오는 중입니다.")
        self.lbl_title.setWordWrap(True)
        self.lbl_title.setStyleSheet("font-size: 15px; font-weight: 700;")
        layout.addWidget(self.lbl_title)

        self.progress = QProgressBar(self)
        self.progress.setRange(0, 0)
        self.progress.setTextVisible(False)
        layout.addWidget(self.progress)

        self.lbl_detail = QLabel("")
        self.lbl_detail.setWordWrap(True)
        self.lbl_detail.setStyleSheet("color: #444; line-height: 1.45;")
        layout.addWidget(self.lbl_detail)

    def set_status(self, title, detail_lines):
        self.lbl_title.setText(str(title or "필수 시작 작업을 불러오는 중입니다."))
        self.lbl_detail.setText(str(detail_lines or ""))


class MainWindow(QMainWindow):
    def __init__(
        self,
        paths,
        persistence,
        credential_manager,
        kiwoom_client,
        account_manager,
        condition_manager,
        strategy_manager,
        news_manager,
        telegram_router,
        order_manager,
        pipeline_manager,
        recovery_manager,
        daily_watch_snapshot_manager=None,
        file_log_manager=None,
        startup_context=None,
        parent=None,
    ):
        super(MainWindow, self).__init__(parent)
        self.paths = paths
        self.persistence = persistence
        self.credential_manager = credential_manager
        self.kiwoom_client = kiwoom_client
        self.account_manager = account_manager
        self.condition_manager = condition_manager
        self.strategy_manager = strategy_manager
        self.news_manager = news_manager
        self.telegram_router = telegram_router
        self.order_manager = order_manager
        self.pipeline_manager = pipeline_manager
        self.recovery_manager = recovery_manager
        self.daily_watch_snapshot_manager = daily_watch_snapshot_manager
        self.file_log_manager = file_log_manager
        self.startup_context = dict(startup_context or {})

        self.current_user_id = ""
        self._pending_profile_user_id = ""
        self._profile_accounts_ready = False
        self._profile_conditions_ready = False
        self._restoring_user_profile = False
        self._reloading_account_table = False
        self._profile_save_timer = QTimer(self)
        self._profile_save_timer.setSingleShot(True)
        self._profile_save_timer.timeout.connect(self._save_current_user_profile)
        self._credential_verify_timer = QTimer(self)
        self._credential_verify_timer.setSingleShot(True)
        self._credential_verify_timer.timeout.connect(self._auto_verify_credentials)
        self._credential_verify_cache = {}
        self._credential_verify_queue = []
        self._credential_verify_running = False
        self._connection_watchdog_prev_connected = bool(self.kiwoom_client.get_connect_state() == 1)
        self._auto_restart_in_progress = False
        self._deferred_restart_reason = ""
        self._maintenance_retry_timer = QTimer(self)
        self._maintenance_retry_timer.setSingleShot(True)
        self._maintenance_retry_timer.timeout.connect(self._run_deferred_restart)
        self._auto_reconnect_timer = QTimer(self)
        self._auto_reconnect_timer.setSingleShot(True)
        self._auto_reconnect_timer.timeout.connect(self._attempt_auto_reconnect)
        self._auto_reconnect_attempts = 0
        self._auto_reconnect_reason = ""
        self._manual_api_disconnect = False
        self._last_maintenance_recovery_key = ""
        self._maintenance_watchdog_timer = QTimer(self)
        self._maintenance_watchdog_timer.timeout.connect(self._check_maintenance_watchdog)
        self._connection_watchdog_timer = QTimer(self)
        self._connection_watchdog_timer.timeout.connect(self._check_connection_watchdog)
        self._refresh_operations_timer = QTimer(self)
        self._refresh_operations_timer.setSingleShot(True)
        self._refresh_operations_timer.timeout.connect(self.refresh_operations)
        self._refresh_news_watch_timer = QTimer(self)
        self._refresh_news_watch_timer.setSingleShot(True)
        self._refresh_news_watch_timer.timeout.connect(self.refresh_news_watch)
        self._refresh_policy_logs_timer = QTimer(self)
        self._refresh_policy_logs_timer.setSingleShot(True)
        self._refresh_policy_logs_timer.timeout.connect(self.refresh_policy_logs)
        self._operations_refresh_pending = True
        self._policy_logs_refresh_pending = True
        self._scope_refresh_pending = True
        self._refresh_realtime_reference_timer = QTimer(self)
        self._refresh_realtime_reference_timer.setSingleShot(True)
        self._refresh_realtime_reference_timer.timeout.connect(self._refresh_realtime_strategy_reference_labels)
        self._live_reference_poll_timer = QTimer(self)
        self._live_reference_poll_timer.timeout.connect(self._poll_realtime_strategy_reference_labels)
        self._news_watch_fill_timer = QTimer(self)
        self._news_watch_fill_timer.setSingleShot(True)
        self._news_watch_fill_timer.timeout.connect(self._run_news_watch_after_hours_fill)
        self._pending_news_watch_fill_code = ""
        self._news_watch_rows_sized = False
        self._news_watch_refresh_pending = False
        self._news_watch_refresh_running = False
        self._news_watch_refresh_rows = []
        self._news_watch_refresh_index = 0
        self._news_watch_refresh_restore_code = ""
        self._news_watch_refresh_restore_row = -1
        self._news_watch_batch_size = 10
        self._news_watch_initial_batch_size = 10
        self._news_watch_scroll_threshold = 3
        self._news_watch_refresh_batch_timer = QTimer(self)
        self._news_watch_refresh_batch_timer.setSingleShot(True)
        self._news_watch_refresh_batch_timer.timeout.connect(self._process_news_watch_refresh_batch)
        self._realtime_capture_log_max_rows = 3
        self._realtime_reference_board_rows = [None] * 10
        self._realtime_reference_board_index = 0
        self._realtime_reference_board_count = 0
        self._realtime_reference_board_code_map = {}
        self._realtime_reference_name_cache = {}
        self._realtime_reference_dirty_rows = []
        self._realtime_reference_dirty_row_set = set()
        self._realtime_reference_full_refresh_pending = False
        self._realtime_reference_flush_batch_size = 2
        self._realtime_reference_flush_timer = QTimer(self)
        self._realtime_reference_flush_timer.setSingleShot(True)
        self._realtime_reference_flush_timer.timeout.connect(self._flush_realtime_reference_board_updates)
        self._realtime_reference_min_hold_sec = 10.0
        self._realtime_reference_pending_codes = []
        self._realtime_reference_pending_snapshots = {}
        self._realtime_reference_rotation_timer = QTimer(self)
        self._realtime_reference_rotation_timer.setSingleShot(True)
        self._realtime_reference_rotation_timer.timeout.connect(self._process_pending_realtime_reference_snapshots)
        self._news_tick_running = False
        self._startup_auto_login_attempted = False
        self._startup_loading_dialog = None
        self._startup_loading_message = ""
        self._startup_bootstrap_active = False
        self._startup_bootstrap_steps = {}
        self._startup_loading_timer = QTimer(self)
        self._startup_loading_timer.setSingleShot(True)
        self._startup_loading_timer.timeout.connect(self._on_startup_loading_timeout)
        self._startup_warmup_timer = QTimer(self)
        self._startup_warmup_timer.setSingleShot(True)
        self._startup_warmup_timer.timeout.connect(self._check_startup_warmup)
        self._deferred_account_sync_timer = QTimer(self)
        self._deferred_account_sync_timer.setSingleShot(True)
        self._deferred_account_sync_timer.timeout.connect(self._run_deferred_account_sync)
        self._startup_deferred_sync_pending = False

        self.setWindowTitle("Kiwoom News Trader MVP")
        self._build_ui()
        self._connect_signals()
        self._load_initial_data()

        self.recovery_timer = QTimer(self)
        self.recovery_timer.timeout.connect(self.recovery_manager.save_runtime_snapshot)
        self.recovery_timer.start(30000)

        self.news_timer = QTimer(self)
        self.news_timer.timeout.connect(self._on_news_tick)
        self.news_timer.start(180000)

        self._connection_watchdog_timer.start(2000)
        self._maintenance_watchdog_timer.start(30000)
        self._live_reference_poll_timer.start(700)
        if self.startup_context.get("auto_recover"):
            reason = self.startup_context.get("recover_reason") or "자동복구"
            self.append_log("♻️ 자동복구 재시작 완료: {0}".format(reason))

    def _is_maintenance_window(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        weekday = int(now_dt.weekday())
        if weekday == 6:
            start_dt = now_dt.replace(hour=5, minute=0, second=0, microsecond=0)
            end_dt = now_dt.replace(hour=5, minute=15, second=0, microsecond=0)
            label = "일요일 05:00~05:15"
        else:
            start_dt = now_dt.replace(hour=6, minute=50, second=0, microsecond=0)
            end_dt = now_dt.replace(hour=6, minute=55, second=0, microsecond=0)
            label = "월~토 06:50~06:55"
        return start_dt <= now_dt <= end_dt, start_dt, end_dt, label

    def _maintenance_window_key(self, start_dt, end_dt, label):
        start_text = start_dt.strftime("%Y%m%d%H%M") if isinstance(start_dt, datetime.datetime) else ""
        end_text = end_dt.strftime("%Y%m%d%H%M") if isinstance(end_dt, datetime.datetime) else ""
        return "{0}|{1}|{2}".format(start_text, end_text, str(label or ""))

    def _reset_auto_reconnect(self):
        if self._auto_reconnect_timer.isActive():
            self._auto_reconnect_timer.stop()
        self._auto_reconnect_attempts = 0
        self._auto_reconnect_reason = ""

    def _schedule_auto_reconnect(self, reason, delay_ms=5000, reset_attempts=False):
        if reset_attempts:
            self._auto_reconnect_attempts = 0
        self._auto_reconnect_reason = str(reason or "api_disconnect")
        delay_ms = max(1000, int(delay_ms or 1000))
        if self._auto_reconnect_timer.isActive():
            remaining = int(self._auto_reconnect_timer.remainingTime() or 0)
            if remaining > 0 and remaining <= delay_ms:
                return
            self._auto_reconnect_timer.stop()
        self._auto_reconnect_timer.start(delay_ms)
        self.append_log("🔁 API 재연결 예약: {0} / {1:.1f}s 후 시도".format(self._auto_reconnect_reason, delay_ms / 1000.0))

    def _attempt_auto_reconnect(self):
        if self._manual_api_disconnect:
            return
        if self.kiwoom_client.get_connect_state() == 1:
            self._connection_watchdog_prev_connected = True
            self._auto_restart_in_progress = False
            self._reset_auto_reconnect()
            return

        in_maintenance, _start_dt, end_dt, label = self._is_maintenance_window()
        if in_maintenance:
            self._schedule_restart_after_maintenance("maintenance_retry", end_dt, label)
            return

        self._auto_reconnect_attempts += 1
        attempt = int(self._auto_reconnect_attempts)
        reason = self._auto_reconnect_reason or "api_disconnect"
        self.append_log("🔁 API 재연결 시도 {0}: {1}".format(attempt, reason))

        try:
            ok = bool(self.kiwoom_client.connect_server())
        except Exception as exc:
            self.append_log("❌ API 재연결 호출 실패: {0}".format(exc))
            ok = False

        if not ok:
            if attempt >= 3:
                self.append_log("⚠️ 재연결 호출이 반복 실패하여 프로그램 재시작으로 복구합니다.")
                self.restart_app("api_reconnect_failed", delay_start_sec=5)
                return
            self._schedule_auto_reconnect(reason, delay_ms=15000)
            return

        check_delay_ms = 25000 if attempt <= 1 else 35000
        QTimer.singleShot(check_delay_ms, self._verify_auto_reconnect_result)

    def _verify_auto_reconnect_result(self):
        if self.kiwoom_client.get_connect_state() == 1:
            self._connection_watchdog_prev_connected = True
            self._auto_restart_in_progress = False
            self._reset_auto_reconnect()
            self.append_log("✅ API 재연결 성공")
            return
        if self._auto_reconnect_attempts >= 3:
            self.append_log("⚠️ API 재연결이 완료되지 않아 프로그램 재시작으로 복구합니다.")
            self.restart_app("api_reconnect_timeout", delay_start_sec=5)
            return
        self._schedule_auto_reconnect(self._auto_reconnect_reason or "api_disconnect", delay_ms=15000)

    def _check_maintenance_watchdog(self):
        if self._auto_restart_in_progress:
            return
        if self._manual_api_disconnect:
            return
        if not self.credential_manager.get_auto_login_on_startup():
            return
        now_dt = datetime.datetime.now()
        in_maintenance, start_dt, end_dt, label = self._is_maintenance_window(now_dt)
        warning_start_dt = start_dt - datetime.timedelta(seconds=45)
        if not (warning_start_dt <= now_dt <= end_dt):
            return
        if self.kiwoom_client.get_connect_state() != 1:
            self._schedule_restart_after_maintenance("maintenance_disconnect", end_dt, label)
            return
        self._schedule_restart_after_maintenance("maintenance_preemptive", end_dt, label)

    def _handle_api_message(self, payload):
        message = str((payload or {}).get("message") or "").strip()
        if not message:
            return
        normalized = message.replace(" ", "")
        maintenance_keywords = [
            "시스템점검",
            "점검시간",
            "매일시스템점검",
            "06:50~06:55",
            "05:00~05:15",
            "접속단말이될수있습니다",
        ]
        if not any(keyword in normalized for keyword in maintenance_keywords):
            return
        now_dt = datetime.datetime.now()
        _in_maintenance, start_dt, end_dt, label = self._is_maintenance_window(now_dt)
        warning_start_dt = start_dt - datetime.timedelta(minutes=1)
        if not (warning_start_dt <= now_dt <= end_dt):
            return
        self.append_log("🛠️ 키움 점검 안내 감지: {0}".format(message))
        self._schedule_restart_after_maintenance("maintenance_message", end_dt, label)

    def _build_restart_command(self, reason, delay_start_sec=3):
        delay_value = max(0, int(delay_start_sec or 0))
        args = [
            "--delay-start={0}".format(delay_value),
            "--auto-recover=1",
            "--recover-reason={0}".format(str(reason or "auto_recover")),
        ]
        if getattr(sys, "frozen", False):
            return [sys.executable] + args
        return [sys.executable, os.path.abspath(sys.argv[0])] + args

    def _schedule_restart_after_maintenance(self, reason, end_dt, label):
        now_dt = datetime.datetime.now()
        recovery_key = self._maintenance_window_key(now_dt.replace(second=0, microsecond=0), end_dt, label)
        if recovery_key == self._last_maintenance_recovery_key and self._auto_restart_in_progress:
            return
        self._last_maintenance_recovery_key = recovery_key
        self._deferred_restart_reason = str(reason or "maintenance_wait")
        delay_sec = max(20, int((end_dt - now_dt).total_seconds()) + 20)
        self.append_log("🛠️ API 점검시간 감지({0}). 점검 종료 후 자동 복구를 위해 재시작합니다.".format(label))
        self.restart_app(self._deferred_restart_reason, delay_start_sec=delay_sec)
        return
        delay_ms = int(max(1000.0, ((end_dt - now_dt).total_seconds() + 15.0) * 1000.0))
        self._deferred_restart_reason = str(reason or "maintenance_wait")
        if self._maintenance_retry_timer.isActive():
            self.append_log("⏳ API 점검시간 재시작 보류 유지: {0}".format(label))
            return
        self._maintenance_retry_timer.start(delay_ms)
        self.append_log("⏳ API 점검시간({0})으로 자동복구를 보류합니다. 점검 종료 후 재시작합니다.".format(label))

    def _run_deferred_restart(self):
        if self.kiwoom_client.get_connect_state() == 1:
            self._auto_restart_in_progress = False
            self._connection_watchdog_prev_connected = True
            self._reset_auto_reconnect()
            self.append_log("✅ 점검시간 종료 후 키움 연결이 이미 복구되었습니다")
            return
            self.append_log("✅ 점검시간 종료 후 키움 연결이 이미 복구되었습니다")
            return
        self._schedule_auto_reconnect(self._deferred_restart_reason or "maintenance_wait", delay_ms=1000, reset_attempts=True)

    def _check_connection_watchdog(self):
        if self._manual_api_disconnect:
            return
        current_state = int(self.kiwoom_client.get_connect_state() or 0)
        connected_now = current_state == 1
        if connected_now:
            self._connection_watchdog_prev_connected = True
            self._auto_restart_in_progress = False
            self._reset_auto_reconnect()
            if self._maintenance_retry_timer.isActive():
                self._maintenance_retry_timer.stop()
            return
        if not self._connection_watchdog_prev_connected:
            return
        if self._auto_restart_in_progress:
            return
        self._handle_connection_lost()

    def _handle_connection_lost(self):
        self._connection_watchdog_prev_connected = False
        self.kiwoom_client.notify_connection_lost("운영 중 연결이 끊어졌습니다")
        in_maintenance, _start_dt, end_dt, label = self._is_maintenance_window()
        if in_maintenance:
            self._schedule_restart_after_maintenance("maintenance_disconnect", end_dt, label)
            return
        self._schedule_auto_reconnect("api_disconnect", delay_ms=3000, reset_attempts=True)

    def restart_app(self, reason="manual_restart", delay_start_sec=3):
        if self._auto_restart_in_progress:
            self.append_log("⏳ 자동복구 재시작이 이미 진행 중입니다")
            return False

        self._auto_restart_in_progress = True
        reason = str(reason or "manual_restart")
        self.append_log("♻️ 자동복구 재시작 시작: {0}".format(reason))

        try:
            if hasattr(self, "_profile_save_timer") and self._profile_save_timer.isActive():
                self._profile_save_timer.stop()
            self._save_current_user_profile()
        except Exception as exc:
            self.append_log("⚠️ 재시작 전 사용자 설정 저장 실패: {0}".format(exc))

        try:
            self.recovery_manager.save_runtime_snapshot()
        except Exception as exc:
            self.append_log("⚠️ 재시작 전 런타임 스냅샷 저장 실패: {0}".format(exc))

        if os.name == "nt":
            for exe_name in ["opstarter.exe", "khopenapi.exe"]:
                try:
                    subprocess.call(
                        ["taskkill", "/F", "/IM", exe_name, "/T"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception as exc:
                    self.append_log("⚠️ 프로세스 종료 실패({0}): {1}".format(exe_name, exc))

        cmd = self._build_restart_command(reason, delay_start_sec=delay_start_sec)
        try:
            creationflags = 0
            if os.name == "nt":
                creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
            subprocess.Popen(
                cmd,
                cwd=os.path.dirname(os.path.abspath(sys.argv[0])),
                close_fds=(os.name != "nt"),
                creationflags=creationflags,
            )
        except Exception as exc:
            self._auto_restart_in_progress = False
            self.append_log("❌ 자동복구 재시작 실패: {0}".format(exc))
            return False

        QTimer.singleShot(200, lambda: os._exit(0))
        return True

    def _restart_app_from_ui(self):
        reply = QMessageBox.question(
            self,
            "프로그램 재시작",
            "지금 프로그램을 재시작할까요?\n저장 가능한 설정과 런타임 스냅샷은 먼저 저장합니다.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        self.restart_app("manual_restart", delay_start_sec=2)

    def _build_ui(self):
        root = QWidget(self)
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(8, 8, 8, 8)
        root_layout.setSpacing(8)

        self.header_tabs = self._build_header_widget()
        self.panel_buy_chain = self._build_buy_chain_panel()
        self.panel_sell_chain = self._build_sell_chain_panel()
        self.panel_condition_catalog = self._build_condition_catalog_panel()
        self.panel_active_slots = self._build_active_slots_panel()
        self.panel_right = self._build_right_panel()
        self._normalize_ui_texts()

        row1_widget = QWidget(self)
        row1_layout = QGridLayout(row1_widget)
        row1_layout.setContentsMargins(0, 0, 0, 0)
        row1_layout.setHorizontalSpacing(8)
        row1_layout.setVerticalSpacing(0)
        row1_layout.addWidget(self.header_tabs, 0, 0)
        row1_layout.addWidget(self.panel_buy_chain, 0, 1)
        row1_layout.addWidget(self.panel_sell_chain, 0, 2)
        row1_layout.setColumnStretch(0, 3)
        row1_layout.setColumnStretch(1, 5)
        row1_layout.setColumnStretch(2, 5)

        row2_widget = QWidget(self)
        row2_layout = QGridLayout(row2_widget)
        row2_layout.setContentsMargins(0, 0, 0, 0)
        row2_layout.setHorizontalSpacing(8)
        row2_layout.setVerticalSpacing(0)
        row2_layout.addWidget(self.panel_condition_catalog, 0, 0)
        row2_layout.addWidget(self.panel_active_slots, 0, 1)
        row2_layout.addWidget(self.panel_right, 0, 2)
        row2_layout.setColumnStretch(0, 2)
        row2_layout.setColumnStretch(1, 4)
        row2_layout.setColumnStretch(2, 8)

        root_layout.addWidget(row1_widget, 2)
        root_layout.addWidget(row2_widget, 5)

    def _normalize_ui_texts(self):
        if hasattr(self, "right_tabs"):
            tab_texts = [
                "실시간 참고값",
                "전략 상세",
                "전략별 분석",
                "운영",
                "뉴스감시",
                "스팸 관리",
                "로그",
            ]
            for index, text in enumerate(tab_texts):
                if self.right_tabs.count() > index:
                    self.right_tabs.setTabText(index, text)
        if hasattr(self, "table_realtime_reference"):
            self.table_realtime_reference.setHorizontalHeaderLabels([
                "종목명",
                "종목코드",
                "현재가",
                "VWAP",
                "매도우위",
                "누적거래량",
                "누적거래대금",
                "매도호가합",
                "매수호가합",
                "업데이트시각",
            ])
        for attr_name in [
            "lbl_news_watch_loading",
            "news_watch_loading_label",
            "news_watch_loading_label_actual",
        ]:
            label = getattr(self, attr_name, None)
            if label is not None:
                label.setText("뉴스감시 데이터 로딩 중...")

    def _build_header_widget(self):
        tabs = QTabWidget(self)
        page_specs = [
            (self._build_kiwoom_group(), "?ㅼ?/怨꾩쥖"),
            (self._build_naver_group(), "?ㅼ씠踰??댁뒪 API"),
            (self._build_dart_group(), "DART API"),
            (self._build_telegram_group("?댁뒪 ?붾젅洹몃옩", "news"), "?댁뒪 ?붾젅洹몃옩"),
            (self._build_telegram_group("留ㅻℓ ?붾젅洹몃옩", "trade"), "留ㅻℓ ?붾젅洹몃옩"),
            (self._build_ai_group(), "AI API"),
        ]
        base_height = page_specs[1][0].sizeHint().height()
        for page, title in page_specs:
            page.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
            page.setFixedHeight(base_height)
            tabs.addTab(page, title)
        tabs.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        tabs.setFixedHeight(base_height + tabs.tabBar().sizeHint().height())
        return tabs
        tabs = QTabWidget(self)
        tabs.addTab(self._build_kiwoom_group(), "키움/계좌")
        tabs.addTab(self._build_naver_group(), "네이버 뉴스 API")
        tabs.addTab(self._build_dart_group(), "DART API")
        tabs.addTab(self._build_telegram_group("뉴스 텔레그램", "news"), "뉴스 텔레그램")
        tabs.addTab(self._build_telegram_group("매매 텔레그램", "trade"), "매매 텔레그램")
        tabs.addTab(self._build_ai_group(), "AI API")
        return tabs

    def _build_kiwoom_group(self):
        group = QGroupBox("키움 접속 / 계좌 활성화")
        layout = QVBoxLayout(group)

        top_row = QHBoxLayout()
        self.btn_restart_app = QPushButton("프로그램 재시작")
        self.btn_login = QPushButton("키움 로그인")
        self.btn_disconnect = QPushButton("API 연결종료")
        self.btn_auto_login_settings = QPushButton("자동로그인 설정")
        self.lbl_connect = QLabel("API OFF")
        self.lbl_connect.setStyleSheet("color: #b00020; font-weight: 700;")
        self.btn_disconnect.setEnabled(bool(self.kiwoom_client.get_connect_state() == 1))
        top_row.addWidget(self.btn_restart_app)
        top_row.addWidget(self.btn_login)
        top_row.addWidget(self.btn_disconnect)
        top_row.addWidget(self.btn_auto_login_settings)
        top_row.addWidget(QLabel("상태:"))
        top_row.addWidget(self.lbl_connect)
        self.lbl_auto_login_status = QLabel("자동로그인: 해제")
        top_row.addSpacing(12)
        top_row.addWidget(self.lbl_auto_login_status)
        top_row.addStretch(1)
        layout.addLayout(top_row)

        form_row = QHBoxLayout()
        self.edt_user_id = QLineEdit()
        self.edt_user_id.setReadOnly(True)
        self.edt_user_id.setMaximumWidth(140)
        self.edt_user_name = QLineEdit()
        self.edt_user_name.setReadOnly(True)
        self.edt_user_name.setMaximumWidth(140)
        self.edt_server = QLineEdit()
        self.edt_server.setReadOnly(True)
        self.edt_server.setMaximumWidth(140)
        form_row.addWidget(QLabel("사용자 ID"))
        form_row.addWidget(self.edt_user_id)
        form_row.addSpacing(8)
        form_row.addWidget(QLabel("사용자명"))
        form_row.addWidget(self.edt_user_name)
        form_row.addSpacing(8)
        form_row.addWidget(QLabel("서버구분"))
        form_row.addWidget(self.edt_server)
        form_row.addStretch(1)
        layout.addLayout(form_row)

        self.account_table = QTableWidget(0, 6)
        self.account_table.setHorizontalHeaderLabels(["사용", "계좌", "주문방식", "주문기준", "기준값", "지정가 옵션"])
        self.account_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.account_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.account_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.account_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.account_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.account_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.account_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.account_table.setMaximumHeight(190)
        layout.addWidget(self.account_table)

        advanced_box = QGroupBox("주문 / 조회 고급 설정")
        advanced_layout = QGridLayout(advanced_box)
        self.cbo_execution_mode = QComboBox()
        self.cbo_execution_mode.addItem("Trade ON", "live")
        self.cbo_execution_mode.addItem("Trade OFF", "simulated")
        self.cbo_execution_mode.setCurrentIndex(0)
        self.cbo_global_pw_mode = QComboBox()
        self.cbo_global_pw_mode.addItem("API 저장 사용", "api_saved")
        self.cbo_global_pw_mode.addItem("프로그램 입력 사용", "program_input")
        self.edt_global_query_pw = QLineEdit()
        self.edt_global_query_pw.setEchoMode(QLineEdit.Password)
        self.edt_global_query_pw.setPlaceholderText("조회 전용 / 주문용 아님")
        self.edt_global_query_pw.setEnabled(False)
        self.cbo_unfilled_policy = QComboBox()
        self.cbo_unfilled_policy.addItem("취소", "cancel")
        self.cbo_unfilled_policy.addItem("재호가 후 취소", "reprice")
        self.cbo_unfilled_policy.addItem("시장가 전환", "market")
        self.cbo_unfilled_policy.addItem("재호가 후 시장가 전환", "reprice_then_market")
        self.spin_first_wait = QSpinBox()
        self.spin_first_wait.setRange(1, 60)
        self.spin_first_wait.setValue(5)
        self.spin_first_wait.setSuffix(" 초")
        self.spin_second_wait = QSpinBox()
        self.spin_second_wait.setRange(1, 120)
        self.spin_second_wait.setValue(5)
        self.spin_second_wait.setSuffix(" 초")
        advanced_layout.addWidget(QLabel("Trade"), 0, 0)
        advanced_layout.addWidget(self.cbo_execution_mode, 0, 1)
        advanced_layout.addWidget(QLabel("조회PW 사용방식"), 0, 2)
        advanced_layout.addWidget(self.cbo_global_pw_mode, 0, 3)
        advanced_layout.addWidget(QLabel("조회PW"), 0, 4)
        advanced_layout.addWidget(self.edt_global_query_pw, 0, 5)
        advanced_layout.addWidget(QLabel("지정가 미체결 처리"), 1, 0)
        advanced_layout.addWidget(self.cbo_unfilled_policy, 1, 1)
        advanced_layout.addWidget(QLabel("1차 대기"), 1, 2)
        advanced_layout.addWidget(self.spin_first_wait, 1, 3)
        advanced_layout.addWidget(QLabel("2차 대기"), 1, 4)
        advanced_layout.addWidget(self.spin_second_wait, 1, 5)
        layout.addWidget(advanced_box)
        self._update_execution_mode_visual()

        self.lbl_account_pw_hint = QLabel("조회PW는 계좌조회 TR(opw00018) 전용이며 전 계좌 공통 적용됩니다. 주문 비밀번호로 직접 사용되지 않습니다.")
        layout.addWidget(self.lbl_account_pw_hint)

        apply_row = QHBoxLayout()
        self.btn_apply_accounts = QPushButton("주문/조회 설정 저장")
        self.btn_sync_accounts = QPushButton("계좌 동기화")
        self.btn_restore_runtime = QPushButton("복구 스냅샷 로드")
        apply_row.addWidget(self.btn_apply_accounts)
        apply_row.addWidget(self.btn_sync_accounts)
        apply_row.addWidget(self.btn_restore_runtime)
        apply_row.addStretch(1)
        layout.addLayout(apply_row)
        return group

    def _build_naver_group(self):
        group = QGroupBox("네이버 뉴스 API")
        layout = QGridLayout(group)
        layout.addWidget(QLabel("Set"), 0, 0)
        layout.addWidget(QLabel("Client ID"), 0, 1)
        layout.addWidget(QLabel("Client Secret"), 0, 2)
        layout.addWidget(QLabel("활성"), 0, 3)
        layout.addWidget(QLabel("저장"), 0, 4)
        layout.addWidget(QLabel("상태"), 0, 5)
        self.naver_rows = []
        for i in range(6):
            row_no = i + 1
            lbl = QLabel(str(row_no))
            edt_id = QLineEdit()
            edt_secret = QLineEdit()
            edt_secret.setEchoMode(QLineEdit.Password)
            chk = QCheckBox()
            btn = QPushButton("저장")
            lbl_status = QLabel("키 미입력")
            lbl_status.setWordWrap(True)
            layout.addWidget(lbl, row_no, 0)
            layout.addWidget(edt_id, row_no, 1)
            layout.addWidget(edt_secret, row_no, 2)
            layout.addWidget(chk, row_no, 3)
            layout.addWidget(btn, row_no, 4)
            layout.addWidget(lbl_status, row_no, 5)
            self.naver_rows.append((edt_id, edt_secret, chk, btn, lbl_status))
        self.lbl_naver_summary = QLabel("상태: 미확인")
        self.lbl_naver_summary.setWordWrap(True)
        layout.addWidget(self.lbl_naver_summary, 7, 0, 1, 6)
        return group

    def _build_dart_group(self):
        group = QGroupBox("DART API")
        layout = QGridLayout(group)
        layout.addWidget(QLabel("API Key"), 0, 0)
        layout.addWidget(QLabel("사용"), 0, 1)
        layout.addWidget(QLabel("저장"), 0, 2)
        layout.addWidget(QLabel("상태"), 0, 3)

        self.edt_dart_api_key = QLineEdit()
        self.edt_dart_api_key.setEchoMode(QLineEdit.Password)
        self.chk_dart_api_enabled = QCheckBox()
        self.btn_save_dart_api = QPushButton("저장")
        self.lbl_dart_api_status = QLabel("키 미입력")
        self.lbl_dart_api_status.setWordWrap(True)
        self.lbl_dart_api_summary = QLabel("상태: 미확인")
        self.lbl_dart_api_summary.setWordWrap(True)

        layout.addWidget(self.edt_dart_api_key, 1, 0)
        layout.addWidget(self.chk_dart_api_enabled, 1, 1)
        layout.addWidget(self.btn_save_dart_api, 1, 2)
        layout.addWidget(self.lbl_dart_api_status, 1, 3)
        layout.addWidget(self.lbl_dart_api_summary, 2, 0, 1, 4)
        return group

    def _build_telegram_group(self, title, channel_group):
        group = QGroupBox(title)
        group.setProperty("channel_group", channel_group)
        layout = QGridLayout(group)
        layout.addWidget(QLabel("Slot"), 0, 0)
        layout.addWidget(QLabel("Bot Token"), 0, 1)
        layout.addWidget(QLabel("Chat ID"), 0, 2)
        layout.addWidget(QLabel("활성"), 0, 3)
        layout.addWidget(QLabel("저장"), 0, 4)
        layout.addWidget(QLabel("상태"), 0, 5)
        rows = []
        for i in range(3):
            row_no = i + 1
            edt_token = QLineEdit()
            edt_token.setEchoMode(QLineEdit.Password)
            edt_chat = QLineEdit()
            chk = QCheckBox()
            btn = QPushButton("저장")
            lbl_status = QLabel("토큰 미입력")
            lbl_status.setWordWrap(True)
            layout.addWidget(QLabel(str(row_no)), row_no, 0)
            layout.addWidget(edt_token, row_no, 1)
            layout.addWidget(edt_chat, row_no, 2)
            layout.addWidget(chk, row_no, 3)
            layout.addWidget(btn, row_no, 4)
            layout.addWidget(lbl_status, row_no, 5)
            rows.append((edt_token, edt_chat, chk, btn, lbl_status))
        summary = QLabel("상태: 미확인")
        summary.setWordWrap(True)
        layout.addWidget(summary, 4, 0, 1, 6)
        if channel_group == "news":
            self.news_telegram_rows = rows
            self.lbl_news_telegram_summary = summary
            self.spin_news_send_min_score = QSpinBox()
            self.spin_news_send_min_score.setRange(0, 100)
            self.spin_news_send_min_score.setValue(60)
            self.btn_save_news_send_min_score = QPushButton("점수 저장")
            layout.addWidget(QLabel("뉴스 발송 최소 점수"), 5, 0, 1, 2)
            layout.addWidget(self.spin_news_send_min_score, 5, 2, 1, 1)
            layout.addWidget(self.btn_save_news_send_min_score, 5, 3, 1, 1)
            self.lbl_news_send_min_score_hint = QLabel("매수체인의 뉴스 필터/뉴스 매매 점수와 별개로, 뉴스 텔레그램 발송 기준 점수입니다.")
            self.lbl_news_send_min_score_hint.setWordWrap(True)
            layout.addWidget(self.lbl_news_send_min_score_hint, 5, 4, 1, 2)
        else:
            self.trade_telegram_rows = rows
            self.lbl_trade_telegram_summary = summary
        return group

    def _build_ai_group(self):
        group = QGroupBox("AI API 설정")
        layout = QGridLayout(group)
        layout.addWidget(QLabel("사용"), 0, 0)
        layout.addWidget(QLabel("제공사"), 0, 1)
        layout.addWidget(QLabel("API Key"), 0, 2)
        layout.addWidget(QLabel("Base URL"), 0, 3)
        layout.addWidget(QLabel("모델명"), 0, 4)
        layout.addWidget(QLabel("표시 라벨"), 0, 5)
        layout.addWidget(QLabel("저장"), 0, 6)
        layout.addWidget(QLabel("상태"), 0, 7)
        self.ai_api_rows = []
        for i in range(3):
            row_no = i + 1
            chk = QCheckBox()
            cbo_provider = QComboBox()
            cbo_provider.addItem("OpenAI", "openai")
            cbo_provider.addItem("Gemini", "gemini")
            cbo_provider.addItem("Custom", "custom")
            edt_key = QLineEdit()
            edt_key.setEchoMode(QLineEdit.Password)
            edt_base = QLineEdit()
            edt_base.setPlaceholderText("기본값 사용 시 비워두기")
            edt_model = QLineEdit()
            edt_model.setPlaceholderText("예: gpt-5-mini")
            edt_label = QLineEdit()
            edt_label.setPlaceholderText("예: GPT-5-mini 분석")
            btn_save = QPushButton("저장")
            lbl_status = QLabel("미사용")
            lbl_status.setWordWrap(True)
            layout.addWidget(chk, row_no, 0)
            layout.addWidget(cbo_provider, row_no, 1)
            layout.addWidget(edt_key, row_no, 2)
            layout.addWidget(edt_base, row_no, 3)
            layout.addWidget(edt_model, row_no, 4)
            layout.addWidget(edt_label, row_no, 5)
            layout.addWidget(btn_save, row_no, 6)
            layout.addWidget(lbl_status, row_no, 7)
            self.ai_api_rows.append((chk, cbo_provider, edt_key, edt_base, edt_model, edt_label, btn_save, lbl_status))
        self.lbl_ai_summary = QLabel("상태: AI 비사용")
        self.lbl_ai_summary.setWordWrap(True)
        layout.addWidget(self.lbl_ai_summary, 4, 0, 1, 8)
        return group

    def _build_condition_catalog_panel(self):
        group = QGroupBox("전체 조건검색식 목록")
        layout = QVBoxLayout(group)
        self.edt_condition_search = QLineEdit()
        self.edt_condition_search.setPlaceholderText("조건식 검색")
        self.list_conditions = QListWidget()
        self.list_conditions.setSelectionMode(QAbstractItemView.SingleSelection)
        layout.addWidget(self.edt_condition_search)
        layout.addWidget(self.list_conditions)
        return group

    def _build_active_slots_panel(self):
        group = QGroupBox("활성 조건식 슬롯 10개")
        layout = QVBoxLayout(group)
        assign_row = QHBoxLayout()
        self.cbo_slot_target = QComboBox()
        for i in range(1, 11):
            self.cbo_slot_target.addItem("슬롯 {0}".format(i), i)
        self.btn_assign_slot = QPushButton("선택 조건식 넣기")
        self.btn_clear_slot = QPushButton("선택 슬롯 비우기")
        assign_row.addWidget(self.cbo_slot_target)
        assign_row.addWidget(self.btn_assign_slot)
        assign_row.addWidget(self.btn_clear_slot)
        layout.addLayout(assign_row)

        self.table_slots = QTableWidget(10, 6)
        self.table_slots.setHorizontalHeaderLabels(["슬롯", "조건식", "활성(사용)", "실시간(API)", "편입수", "마지막 이벤트"])
        header = self.table_slots.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.Stretch)
        self.table_slots.verticalHeader().setDefaultSectionSize(28)
        self.table_slots.setWordWrap(False)
        self.table_slots.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_slots.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table_slots)

        row = QHBoxLayout()
        self.btn_start_slot = QPushButton("선택 슬롯 실시간 시작")
        self.btn_stop_slot = QPushButton("선택 슬롯 실시간 중지")
        row.addWidget(self.btn_start_slot)
        row.addWidget(self.btn_stop_slot)
        layout.addLayout(row)
        return group

    def _build_buy_chain_panel(self):
        group = QGroupBox("매수 전략 체인")
        layout = QVBoxLayout(group)
        self.buy_chain_tabs = QTabWidget(self)

        chain_tab = QWidget(self)
        chain_layout = QVBoxLayout(chain_tab)
        self.btn_add_buy = QPushButton("전략 추가")
        self.btn_del_buy = QPushButton("전략 삭제")
        self.btn_up_buy = QPushButton("위로")
        self.btn_down_buy = QPushButton("아래로")
        for btn in [self.btn_add_buy, self.btn_del_buy, self.btn_up_buy, self.btn_down_buy]:
            btn.setVisible(False)
            btn.setEnabled(False)

        self.lbl_buy_default_preview = QLabel("디폴트 매수 전략 : 디폴트")
        self.lbl_buy_default_preview.setWordWrap(True)
        chain_layout.addWidget(self.lbl_buy_default_preview)

        self.table_buy_chain = QTableWidget(0, 4)
        self.table_buy_chain.setHorizontalHeaderLabels(["번호", "전략명", "설명", "기본 구성"])
        header = self.table_buy_chain.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        self.table_buy_chain.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_buy_chain.setEditTriggers(QAbstractItemView.NoEditTriggers)
        chain_layout.addWidget(self.table_buy_chain)

        guide_tab = self._build_strategy_catalog_tab("buy")
        self.buy_chain_tabs.addTab(chain_tab, "체인 구성")
        self.buy_chain_tabs.addTab(guide_tab, "전략 안내")
        layout.addWidget(self.buy_chain_tabs)
        return group

    def _build_sell_chain_panel(self):
        group = QGroupBox("매도 전략 체인")
        layout = QVBoxLayout(group)
        self.sell_chain_tabs = QTabWidget(self)

        chain_tab = QWidget(self)
        chain_layout = QVBoxLayout(chain_tab)
        self.btn_add_sell = QPushButton("전략 추가")
        self.btn_del_sell = QPushButton("전략 삭제")
        self.btn_up_sell = QPushButton("위로")
        self.btn_down_sell = QPushButton("아래로")
        for btn in [self.btn_add_sell, self.btn_del_sell, self.btn_up_sell, self.btn_down_sell]:
            btn.setVisible(False)
            btn.setEnabled(False)

        self.lbl_sell_default_preview = QLabel("디폴트 매도 전략 : 디폴트")
        self.lbl_sell_default_preview.setWordWrap(True)
        chain_layout.addWidget(self.lbl_sell_default_preview)

        self.table_sell_chain = QTableWidget(0, 4)
        self.table_sell_chain.setHorizontalHeaderLabels(["번호", "전략명", "설명", "기본 구성"])
        header = self.table_sell_chain.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        self.table_sell_chain.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_sell_chain.setEditTriggers(QAbstractItemView.NoEditTriggers)
        chain_layout.addWidget(self.table_sell_chain)

        guide_tab = self._build_strategy_catalog_tab("sell")
        self.sell_chain_tabs.addTab(chain_tab, "체인 구성")
        self.sell_chain_tabs.addTab(guide_tab, "전략 안내")
        layout.addWidget(self.sell_chain_tabs)
        return group

    def _build_strategy_catalog_tab(self, kind):
        widget = QWidget(self)
        layout = QVBoxLayout(widget)
        self.news_watch_loading_label = QLabel("뉴스감시 데이터 로딩 중...")
        self.news_watch_loading_label.setStyleSheet("color: #8a5a00; font-weight: 700; background: #fff4d6; padding: 6px 8px; border: 1px solid #f2d28b;")
        self.news_watch_loading_label.setVisible(False)
        layout.addWidget(self.news_watch_loading_label)
        self.news_watch_loading_label.setParent(None)
        table = QTableWidget(0, 4, self)
        table.setHorizontalHeaderLabels(["번호", "전략명", "유형", "설명", "통과 조건"])
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(["번호", "전략명", "설명", "통과 조건"])
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(["번호", "전략명", "설명", "통과 조건"])
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setDefaultSectionSize(28)
        table.setWordWrap(True)
        layout.addWidget(table)

        row = QHBoxLayout()
        btn_add = QPushButton("선택 전략 바로 추가")
        btn_and = QPushButton("AND") if kind == "buy" else None
        btn_or = QPushButton("OR")
        if kind != "buy":
            btn_or.setText("OR 자동연결")
            btn_or.setEnabled(False)
            btn_or.setToolTip("매도 전략은 선택 전략을 추가하면 자동으로 OR 연결됩니다.")
        row.addStretch(1)
        row.addWidget(btn_add)
        if btn_and is not None:
            row.addWidget(btn_and)
        row.addWidget(btn_or)
        layout.addLayout(row)

        if kind == "buy":
            self.table_buy_strategy_catalog = table
            self.btn_add_selected_buy_catalog = btn_add
            self.btn_buy_catalog_and = btn_and
            self.btn_buy_catalog_or = btn_or
        else:
            self.table_sell_strategy_catalog = table
            self.btn_add_selected_sell_catalog = btn_add
            self.btn_sell_catalog_or = btn_or

        self._populate_strategy_catalog_table(kind, table)
        return widget

    def _strategy_catalog_meta(self, kind):
        if kind == "buy":
            return {
                "institution_trend_a": {
                    "description": "기관 순매수 금액이 양수인지 확인하는 기본 수급 필터입니다.",
                    "config": "기관 순매수금액 > 0",
                },
                "institution_trend_b": {
                    "description": "가격대 영향을 줄이기 위해 기관 순매수금액이 아니라 순매수 비율로 판단하는 수급 필터입니다.",
                    "config": "순매수 비율 선택 / 전략상세에서 설정",
                },
                "institution_trend_c": {
                    "description": "기관 순매수가 최근 N구간 연속으로 이어졌는지 확인하는 지속성 수급 필터입니다.",
                    "config": "최근 N구간 연속 순매수 / 구간은 전략상세에서 설정",
                },
                "foreign_trend_a": {
                    "description": "외국인 순매수 금액이 양수인지 확인하는 기본 수급 필터입니다.",
                    "config": "외국인 순매수금액 > 0",
                },
                "foreign_trend_b": {
                    "description": "가격대 영향을 줄이기 위해 외국인 순매수금액이 아니라 순매수 비율로 판단하는 수급 필터입니다.",
                    "config": "순매수 비율 선택 / 전략상세에서 설정",
                },
                "foreign_trend_c": {
                    "description": "외국인 순매수가 최근 N구간 연속으로 이어졌는지 확인하는 지속성 수급 필터입니다.",
                    "config": "최근 N구간 연속 순매수 / 구간은 전략상세에서 설정",
                },
                "vwap": {
                    "description": "현재가와 장중 VWAP 위치를 비교해 진입 적정성을 봅니다.",
                    "config": "현재가 >= 장중 VWAP",
                },
                "vwap_b": {
                    "description": "장중 VWAP이 현재가보다 높은 종목만 통과시키는 역방향 VWAP 필터입니다.",
                    "config": "장중 VWAP > 현재가",
                },
                "sell_pressure_a": {
                    "description": "호가 기준 매도총잔량/매수총잔량 비율을 1차로 확인합니다.",
                    "config": "매도우위 비율 <= 2.0",
                },
                "sell_pressure_b": {
                    "description": "매도 잔량이 오히려 설정값을 초과하는 강한 매도우위 종목만 선택하는 역방향 필터입니다.",
                    "config": "매도우위 비율 > 1.5",
                },
                "news_filter": {
                    "description": "다른 필터 통과 후 뉴스 최종 점수가 슬롯 기준 이상일 때만 매수합니다.",
                    "config": "최종 뉴스 점수 >= 슬롯 뉴스점수",
                },
                "news_trade": {
                    "description": "재평가 후보용 뉴스 매매 전용 전략입니다. 슬롯 체인에는 직접 배치되지 않습니다.",
                    "config": "최종점수 >= 기준, 중요도 >= 60, 빈도 >= 20",
                },
            }
        return {
            "stop_loss": {"description": "평가손익률이 손절 기준 이하로 내려가면 청산합니다.", "config": "기본 -3.0%"},
            "take_profit": {"description": "평가손익률이 익절 기준 이상이면 청산합니다.", "config": "기본 +5.0%"},
            "trailing_stop": {"description": "수익률이 시작 기준을 넘긴 뒤 최고 수익률에서 갭만큼 밀리면 청산합니다.", "config": "기본 시작 3.0% / 갭 1.5%"},
            "time_exit": {"description": "보유 시간이 설정 분을 넘기면 청산합니다.", "config": "기본 30분"},
            "market_close_exit": {"description": "설정된 장마감 시각 이후에는 강제로 청산합니다.", "config": "기본 15:20"},
        }
        if kind == "buy":
            return {
                "institution_trend_a": {"description": "기관 수급 흐름이 양호한 종목을 통과시키는 기본형 필터입니다.", "config": "기본 통과형 / 수급형 진입 1차"},
                "institution_trend_b": {"description": "기관 수급 강도를 더 보수적으로 해석하는 보강형 필터입니다.", "config": "기관 수급 강화형 / 2차 확인"},
                "institution_trend_c": {"description": "기관 수급 방향성과 지속성을 추가로 확인하는 심화형 필터입니다.", "config": "기관 수급 심화형 / 종합 확인"},
                "foreign_trend_a": {"description": "외국인 매수 추세가 유효한 종목을 통과시키는 수급형 필터입니다.", "config": "외국인 수급 기반 / 추세 확인"},
                "vwap": {"description": "현재가가 장중 VWAP 이상인 종목만 통과시키는 추세형 필터입니다.", "config": "현재가 >= 장중 VWAP"},
                "vwap_b": {"description": "장중 VWAP이 현재가보다 높은 눌림 구간만 통과시키는 역방향 필터입니다.", "config": "장중 VWAP > 현재가"},
                "sell_pressure_a": {"description": "호가상의 매도 우위를 설정값 이하로 제한하는 기본 필터입니다.", "config": "매도우위 비율 <= 설정값"},
                "sell_pressure_b": {"description": "매도 주문이 더 많이 쌓인 종목을 찾기 위해 매도우위가 설정값을 초과하는 경우만 통과시킵니다.", "config": "매도우위 비율 > 설정값"},
                "news_filter": {"description": "다른 필터를 통과한 뒤 뉴스 최종점수가 기준 이상일 때만 매수합니다.", "config": "슬롯 뉴스점수 사용"},
                "news_trade": {"description": "재평가 후보용 전용 전략입니다. 슬롯에 직접 부여하지 않습니다.", "config": "뉴스매매 탭 전용"},
            }
        return {
            "stop_loss": {"description": "평가손익률이 손절 기준을 이탈하면 청산하는 전략입니다.", "config": "기본 -3% / 손실 제한"},
            "take_profit": {"description": "목표 수익률 도달 시 이익을 확정하는 전략입니다.", "config": "기본 +5% / 이익 확정"},
            "trailing_stop": {"description": "고점 대비 되돌림 폭을 기준으로 수익 구간을 추적 청산합니다.", "config": "최고가 추적 / 되돌림 청산"},
            "time_exit": {"description": "일정 보유시간이 지나면 자동으로 청산하는 전략입니다.", "config": "보유시간 기준 / 시간 만료 청산"},
            "market_close_exit": {"description": "장 종료 전 미청산 종목을 일괄 정리하는 전략입니다.", "config": "장마감 시간 기준 / 강제 청산"},
        }

    def _populate_strategy_catalog_table(self, kind, table):
        rows = self._get_strategy_catalog_rows(kind)
        previous_rows = table.rowCount()
        for row_index in range(previous_rows):
            old_widget = table.cellWidget(row_index, 1)
            if old_widget is not None:
                table.removeCellWidget(row_index, 1)
                old_widget.deleteLater()
        table.clearContents()
        table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            item_no = QTableWidgetItem(str(row["strategy_no"]))
            item_no.setData(Qt.UserRole, row)
            if not row.get("assignable", True):
                item_no.setForeground(Qt.gray)
            table.setItem(row_index, 0, item_no)
            tooltip = row["name"]
            if not row.get("assignable", True):
                tooltip = "뉴스매매 전용 전략은 뉴스매매 탭에서만 사용합니다."
            self._set_strategy_name_cell(table, row_index, 1, row["name"], tooltip=tooltip)
            item_desc = QTableWidgetItem(self._shorten_table_text(row["description"], 14))
            item_desc.setToolTip(str(row["description"] or ""))
            item_desc.setData(Qt.UserRole + 1, str(row["description"] or ""))
            item_desc.setData(Qt.UserRole + 2, "전략 설명")
            if not row.get("assignable", True):
                item_desc.setForeground(Qt.gray)
            table.setItem(row_index, 2, item_desc)
            item_config = QTableWidgetItem(str(row["config"] or ""))
            item_config.setToolTip(str(row["config"] or ""))
            item_config.setData(Qt.UserRole + 1, str(row["config"] or ""))
            item_config.setData(Qt.UserRole + 2, "통과 조건")
            if not row.get("assignable", True):
                item_config.setForeground(Qt.gray)
            table.setItem(row_index, 3, item_config)

    def _get_strategy_catalog_rows(self, kind, include_hidden=False):
        meta_map = self._strategy_catalog_meta(kind)
        visible_types = set(pair[0] for pair in self.strategy_manager.get_strategy_type_pairs(kind))
        if kind == "buy":
            meta_map = dict(meta_map or {})
            meta_map.setdefault("buy_pressure_a", {
                "description": "매수 총잔량이 매도 총잔량보다 우위인지 1차로 확인하는 호가 필터입니다.",
                "config": "매수우위 비율 >= 1.25",
            })
            meta_map.setdefault("buy_pressure_b", {
                "description": "매수우위가 과도하지 않은 종목을 찾기 위해 설정값 미만일 때만 통과시키는 역방향 호가 필터입니다.",
                "config": "매수우위 비율 < 1.50",
            })
        rows = []
        for row in self.strategy_manager.get_strategy_catalog(kind, include_unassignable=True):
            strategy_type = str(row["strategy_type"] or "")
            if not include_hidden and strategy_type not in visible_types:
                continue
            meta = meta_map.get(strategy_type, {})
            name = row["strategy_name"] or strategy_type
            if not int(row["is_assignable_to_slot"] or 1):
                name = u"{0} (전용)".format(name)
            config_text = meta.get("config", "정보 없음")
            try:
                params = json.loads(row["params_json"] or "{}")
            except Exception:
                params = {}
            if kind == "sell":
                config_text = self._format_sell_strategy_config(strategy_type, params)
            elif kind == "buy":
                config_text = self._format_buy_strategy_config(strategy_type, params)
            rows.append({
                "strategy_no": int(row["strategy_no"] or 0),
                "strategy_id": row["strategy_id"],
                "type": strategy_type,
                "name": name,
                "description": meta.get("description", "전략 설명이 없습니다."),
                "config": config_text,
                "assignable": bool(int(row["is_assignable_to_slot"] or 1)),
                "is_news_filter": bool(int(row["is_news_filter"] or 0)),
                "is_news_trade": bool(int(row["is_news_trade"] or 0)),
            })
        return rows

    def _shorten_table_text(self, text, max_len=14):
        text = str(text or "").strip()
        if len(text) <= max_len:
            return text
        return text[:max(1, max_len - 1)].rstrip() + "…"

    def _strategy_keyword_colors(self):
        return {
            "기관": "#8e24aa",
            "외국인": "#00897b",
            "매수": "#c62828",
            "매도": "#1565c0",
        }

    def _colorize_strategy_name_html(self, text):
        text = str(text or "")
        if not text:
            return ""
        colored = escape(text)
        for keyword, color in [
            ("외국인", self._strategy_keyword_colors().get("외국인")),
            ("기관", self._strategy_keyword_colors().get("기관")),
            ("매수", self._strategy_keyword_colors().get("매수")),
            ("매도", self._strategy_keyword_colors().get("매도")),
        ]:
            colored = colored.replace(
                keyword,
                '<span style="color:{0}; font-weight:700;">{1}</span>'.format(color, keyword),
            )
        return colored

    def _make_strategy_name_label(self, text, tooltip="", parent=None):
        label = QLabel(parent or self)
        label.setTextFormat(Qt.RichText)
        label.setText(self._colorize_strategy_name_html(text))
        label.setToolTip(str(tooltip or text or ""))
        label.setMargin(4)
        label.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        return label

    def _set_strategy_name_cell(self, table, row_index, column_index, text, tooltip=""):
        old_widget = table.cellWidget(row_index, column_index)
        if old_widget is not None:
            table.removeCellWidget(row_index, column_index)
            old_widget.deleteLater()
        table.setCellWidget(
            row_index,
            column_index,
            self._make_strategy_name_label(text, tooltip=tooltip, parent=table),
        )

    def _show_strategy_catalog_item_detail(self, table, item):
        if table is None or item is None:
            return
        detail_text = str(item.data(Qt.UserRole + 1) or "").strip()
        if not detail_text:
            return
        title = str(item.data(Qt.UserRole + 2) or "상세 설명").strip()
        message = "{0}\n\n{1}".format(title, detail_text)
        QToolTip.showText(QCursor.pos(), message, table, table.visualItemRect(item), 15000)

    def _selected_catalog_strategy(self, table):
        row = table.currentRow()
        if row < 0:
            return None
        item = table.item(row, 0)
        if not item:
            return None
        return item.data(Qt.UserRole)

    def _add_selected_catalog_strategy(self, kind, table):
        strategy_row = self._selected_catalog_strategy(table)
        if not strategy_row:
            QMessageBox.warning(self, "전략 추가", "전략 안내 탭에서 전략을 먼저 선택하세요")
            return
        if self.right_tabs.currentWidget() == self.strategy_detail_widget:
            self._append_selected_catalog_strategy_to_policy(kind, strategy_row)
            return
        QMessageBox.information(self, "전략 추가", "전략상세 탭에서 슬롯/디폴트/뉴스매매를 먼저 선택한 뒤 사용해 주세요.")

    def _append_selected_catalog_strategy_to_policy(self, kind, strategy_row):
        policy_key = self._current_policy_editor_key()
        if not policy_key:
            QMessageBox.warning(self, "전략상세", "전략상세 탭에서 슬롯/디폴트/뉴스매매를 먼저 선택하세요.")
            return
        strategy_no = int(strategy_row.get("strategy_no") or 0)
        strategy_type = str(strategy_row.get("type") or "")
        if kind == "buy":
            if policy_key == "news_trade":
                QMessageBox.warning(self, "뉴스매매", "뉴스매매는 점수만으로 동작하므로 매수 전략을 추가할 수 없습니다.")
                return
            if not strategy_row.get("assignable", True):
                QMessageBox.warning(self, "전략 추가", "이 전략은 슬롯/디폴트 탭에 직접 부여할 수 없습니다. 뉴스매매 탭에서 사용하세요.")
                return
            editor = self._policy_editor_for_key(policy_key)
            editor["buy_items"].append({"kind": "strategy", "no": strategy_no, "type": strategy_type})
            self._refresh_policy_editor_display(editor)
            return
        editor = self._policy_editor_for_key(policy_key)
        sell_items = list(editor.get("sell_items") or [])
        if sell_items:
            last_kind = str((sell_items[-1] or {}).get("kind") or "")
            if last_kind == "strategy":
                sell_items.append({"kind": "op", "value": "OR"})
        sell_items.append({"kind": "strategy", "no": strategy_no})
        editor["sell_items"] = sell_items
        self._refresh_policy_editor_display(editor)

    def _append_policy_operator(self, kind, op_value):
        policy_key = self._current_policy_editor_key()
        if not policy_key:
            QMessageBox.warning(self, "전략상세", "전략상세 탭에서 슬롯/디폴트/뉴스매매를 먼저 선택하세요.")
            return
        op_value = str(op_value or "").upper()
        if kind == "buy":
            if policy_key == "news_trade":
                QMessageBox.warning(self, "뉴스매매", "뉴스매매는 점수만으로 동작하므로 AND/OR를 추가할 수 없습니다.")
                return
            editor = self._policy_editor_for_key(policy_key)
            editor["buy_items"].append({"kind": "op", "value": op_value})
            self._refresh_policy_editor_display(editor)
            return
        if op_value != "OR":
            QMessageBox.warning(self, "매도 전략", "매도 전략은 OR만 사용할 수 있습니다.")
            return
        editor = self._policy_editor_for_key(policy_key)
        editor["sell_items"].append({"kind": "op", "value": "OR"})
        self._refresh_policy_editor_display(editor)

    def _build_right_panel(self):
        self.right_tabs = QTabWidget()
        self.right_tabs.addTab(self._build_realtime_reference_tab(), "실시간 참고값")
        self.right_tabs.addTab(self._build_strategy_detail_tab(), "전략 상세")
        self.right_tabs.addTab(self._build_scope_tab(), "전략별 분석")
        self.right_tabs.addTab(self._build_operations_tab(), "운영")
        self.right_tabs.addTab(self._build_news_watch_tab(), "뉴스감시")
        self.right_tabs.addTab(self._build_spam_tab(), "스팸 관리")
        self.right_tabs.addTab(self._build_log_tab(), "로그")
        return self.right_tabs

    def _build_realtime_reference_tab(self):
        widget = QWidget(self)
        self.realtime_reference_tab_widget = widget
        layout = QVBoxLayout(widget)
        self.table_realtime_reference = QTableWidget(10, 10, self)
        self.table_realtime_reference.setHorizontalHeaderLabels([
            "종목명",
            "종목코드",
            "현재가",
            "VWAP",
            "매도우위",
            "누적거래량",
            "누적거래대금",
            "매도호가합",
            "매수호가합",
            "업데이트시각",
        ])
        header = self.table_realtime_reference.horizontalHeader()
        for col_index in range(10):
            header.setSectionResizeMode(col_index, QHeaderView.Fixed)
        self.table_realtime_reference.setColumnWidth(0, 138)
        self.table_realtime_reference.setColumnWidth(1, 88)
        self.table_realtime_reference.setColumnWidth(2, 92)
        self.table_realtime_reference.setColumnWidth(3, 92)
        self.table_realtime_reference.setColumnWidth(4, 84)
        self.table_realtime_reference.setColumnWidth(5, 104)
        self.table_realtime_reference.setColumnWidth(6, 118)
        self.table_realtime_reference.setColumnWidth(7, 104)
        self.table_realtime_reference.setColumnWidth(8, 104)
        self.table_realtime_reference.setColumnWidth(9, 120)
        self.table_realtime_reference.verticalHeader().setVisible(False)
        self.table_realtime_reference.verticalHeader().setDefaultSectionSize(28)
        self.table_realtime_reference.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_realtime_reference.setSelectionMode(QAbstractItemView.NoSelection)
        self.table_realtime_reference.setFocusPolicy(Qt.NoFocus)
        self.table_realtime_reference.setAlternatingRowColors(True)
        self.table_realtime_reference.setStyleSheet(
            "QTableWidget {"
            "background-color: #08111b;"
            "alternate-background-color: #0d1825;"
            "color: #f5d36a;"
            "gridline-color: #1f3042;"
            "font-family: Consolas, 'Courier New';"
            "font-size: 11pt;"
            "}"
            "QHeaderView::section {"
            "background-color: #13263a;"
            "color: #f6f8fb;"
            "padding: 6px 8px;"
            "border: 1px solid #1f3042;"
            "font-weight: 700;"
            "}"
        )
        for row_index in range(10):
            for col_index in range(10):
                item = QTableWidgetItem("")
                if col_index >= 2:
                    item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
                self.table_realtime_reference.setItem(row_index, col_index, item)
        layout.addWidget(self.table_realtime_reference)
        self.table_realtime_capture_log = QTableWidget(int(self._realtime_capture_log_max_rows or 3), 6, self)
        self.table_realtime_capture_log.setHorizontalHeaderLabels([
            "종목명",
            "종목코드",
            "현재가",
            "판정",
            "검색식 + 전략",
            "시각",
        ])
        capture_header = self.table_realtime_capture_log.horizontalHeader()
        capture_header.setSectionResizeMode(0, QHeaderView.Fixed)
        capture_header.setSectionResizeMode(1, QHeaderView.Fixed)
        capture_header.setSectionResizeMode(2, QHeaderView.Fixed)
        capture_header.setSectionResizeMode(3, QHeaderView.Fixed)
        capture_header.setSectionResizeMode(4, QHeaderView.Stretch)
        capture_header.setSectionResizeMode(5, QHeaderView.Fixed)
        self.table_realtime_capture_log.setColumnWidth(0, 110)
        self.table_realtime_capture_log.setColumnWidth(1, 78)
        self.table_realtime_capture_log.setColumnWidth(2, 88)
        self.table_realtime_capture_log.setColumnWidth(3, 88)
        self.table_realtime_capture_log.setColumnWidth(5, 88)
        self.table_realtime_capture_log.verticalHeader().setVisible(False)
        self.table_realtime_capture_log.verticalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_realtime_capture_log.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_realtime_capture_log.setSelectionMode(QAbstractItemView.NoSelection)
        self.table_realtime_capture_log.setFocusPolicy(Qt.NoFocus)
        self.table_realtime_capture_log.setAlternatingRowColors(True)
        self.table_realtime_capture_log.setStyleSheet(
            "QTableWidget {"
            "background-color: #08111b;"
            "alternate-background-color: #0d1825;"
            "color: #f5d36a;"
            "gridline-color: #1f3042;"
            "font-family: Consolas, 'Courier New';"
            "font-size: 10pt;"
            "}"
            "QHeaderView::section {"
            "background-color: #13263a;"
            "color: #f6f8fb;"
            "padding: 5px 7px;"
            "border: 1px solid #1f3042;"
            "font-weight: 700;"
            "}"
        )
        self.table_realtime_capture_log.setMaximumHeight(128)
        for row_index in range(int(self._realtime_capture_log_max_rows or 3)):
            for col_index in range(6):
                item = QTableWidgetItem("")
                if col_index == 2:
                    item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
                elif col_index in [3, 5]:
                    item.setTextAlignment(int(Qt.AlignCenter))
                self.table_realtime_capture_log.setItem(row_index, col_index, item)
        layout.addWidget(self.table_realtime_capture_log)
        return widget

    def _build_log_tab(self):
        widget = QWidget()
        self.log_tab_widget = widget
        layout = QVBoxLayout(widget)
        self.lbl_policy_logs_title = QLabel("주문 정책 로그")
        self.lbl_policy_logs_title.setStyleSheet("font-weight: 700;")
        layout.addWidget(self.lbl_policy_logs_title)
        self.lbl_policy_logs_empty = QLabel("미체결 재호가, 취소, 시장가 전환 같은 주문 정책 동작이 있을 때 표시됩니다.")
        self.lbl_policy_logs_empty.setStyleSheet("color: #666;")
        self.lbl_policy_logs_empty.setWordWrap(True)
        layout.addWidget(self.lbl_policy_logs_empty)
        self.lbl_news_watch_loading = QLabel("뉴스감시 데이터 로딩 중...")
        self.lbl_news_watch_loading.setStyleSheet("color: #8a5a00; font-weight: 700; background: #fff4d6; padding: 6px 8px; border: 1px solid #f2d28b;")
        self.lbl_news_watch_loading.setMinimumHeight(self.lbl_news_watch_loading.sizeHint().height())
        self.lbl_news_watch_loading.setText("")
        self.lbl_news_watch_loading.setVisible(True)
        layout.addWidget(self.lbl_news_watch_loading)
        self.table_policy_logs = QTableWidget(0, 8)
        self.table_policy_logs.setHorizontalHeaderLabels(["시각", "계좌", "종목명", "코드", "정책", "단계", "액션", "상세"])
        header = self.table_policy_logs.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.Stretch)
        self.table_policy_logs.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_policy_logs.setMaximumHeight(220)
        layout.addWidget(self.table_policy_logs)

        self.log_view = QPlainTextEdit(self)
        self.log_view.setReadOnly(True)
        self.log_view.document().setMaximumBlockCount(3000)
        layout.addWidget(self.log_view)
        return widget

    def _build_right_panel(self):
        self.right_tabs = QTabWidget()
        self.right_tabs.addTab(self._build_realtime_reference_tab(), "실시간 참고값")
        self.right_tabs.addTab(self._build_strategy_detail_tab(), "전략 상세")
        self.right_tabs.addTab(self._build_scope_tab(), "전략별 분석")
        self.right_tabs.addTab(self._build_operations_tab(), "운영")
        self.right_tabs.addTab(self._build_news_watch_tab(), "뉴스감시")
        self.right_tabs.addTab(self._build_spam_tab(), "스팸")
        self.right_tabs.addTab(self._build_log_tab(), "로그")
        return self.right_tabs

    def _normalize_ui_texts(self):
        if hasattr(self, "right_tabs"):
            tab_texts = [
                "실시간 참고값",
                "전략 상세",
                "전략별 분석",
                "운영",
                "뉴스감시",
                "스팸",
                "로그",
            ]
            for index, text in enumerate(tab_texts):
                if self.right_tabs.count() > index:
                    self.right_tabs.setTabText(index, text)

    def _create_policy_slot_tab(self, policy_key, condition_label):
        widget = QWidget(self)
        widget.setProperty("policy_key", policy_key)
        layout = QVBoxLayout(widget)

        lbl_condition = QLabel(condition_label)
        lbl_state = QLabel("")
        lbl_condition.setWordWrap(True)
        lbl_state.setWordWrap(True)
        layout.addWidget(lbl_condition)
        layout.addWidget(lbl_state)

        buy_group = QGroupBox("매수 전략")
        buy_layout = QVBoxLayout(buy_group)
        buy_list = QListWidget(self)
        buy_list.setMinimumHeight(120)
        buy_layout.addWidget(buy_list, 1)
        buy_table = QTableWidget(0, 3, self)
        buy_table.setHorizontalHeaderLabels(["번호", "전략명", "설정값"])
        buy_header = buy_table.horizontalHeader()
        buy_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        buy_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        buy_header.setSectionResizeMode(2, QHeaderView.Stretch)
        buy_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        buy_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        buy_table.setMinimumHeight(200)
        buy_layout.addWidget(buy_table, 2)
        buy_btn_row = QHBoxLayout()
        btn_del_buy = QPushButton("선택 항목 삭제")
        buy_btn_row.addStretch(1)
        buy_btn_row.addWidget(btn_del_buy)
        buy_layout.addLayout(buy_btn_row)

        sell_group = QGroupBox("매도 전략")
        sell_layout = QVBoxLayout(sell_group)
        sell_table = QTableWidget(0, 3, self)
        sell_table.setHorizontalHeaderLabels(["번호", "전략명", "설정값"])
        sell_header = sell_table.horizontalHeader()
        sell_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        sell_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        sell_header.setSectionResizeMode(2, QHeaderView.Stretch)
        sell_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        sell_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        sell_table.setMinimumHeight(200)
        sell_layout.addWidget(sell_table, 1)
        sell_btn_row = QHBoxLayout()
        btn_del_sell = QPushButton("선택 항목 삭제")
        sell_btn_row.addStretch(1)
        sell_btn_row.addWidget(btn_del_sell)
        sell_layout.addLayout(sell_btn_row)

        strategy_row = QHBoxLayout()
        strategy_row.setSpacing(10)
        strategy_row.addWidget(buy_group, 1)
        strategy_row.addWidget(sell_group, 1)
        layout.addLayout(strategy_row)

        news_row = QHBoxLayout()
        lbl_news = QLabel("뉴스점수")
        spin_news = QSpinBox(self)
        spin_news.setRange(0, 100)
        spin_news.setEnabled(False)
        news_row.addWidget(lbl_news)
        news_row.addWidget(spin_news)
        news_row.addStretch(1)
        layout.addLayout(news_row)

        btn_save = QPushButton("전략 저장")
        layout.addWidget(btn_save)
        layout.addStretch(1)

        editor = {
            "policy_key": policy_key,
            "widget": widget,
            "condition_label": lbl_condition,
            "state_label": lbl_state,
            "buy_list": buy_list,
            "buy_table": buy_table,
            "sell_table": sell_table,
            "spin_news": spin_news,
            "btn_save": btn_save,
            "btn_del_buy": btn_del_buy,
            "btn_del_sell": btn_del_sell,
            "buy_items": [],
            "sell_items": [],
            "buy_param_editors": {},
            "sell_param_editors": {},
        }
        btn_del_buy.clicked.connect(lambda: self._delete_selected_policy_token(policy_key, "buy"))
        btn_del_sell.clicked.connect(lambda: self._delete_selected_policy_token(policy_key, "sell"))
        btn_save.clicked.connect(lambda: self._save_strategy_policy_tab(policy_key))
        return widget, editor

    def _create_news_trade_policy_tab(self):
        widget = QWidget(self)
        widget.setProperty("policy_key", "news_trade")
        layout = QVBoxLayout(widget)

        intro = QLabel("재평가 후보 전체에 적용됩니다. 점수만 통과하면 바로 매수하며, 매도 전략은 개별 설정이 없으면 디폴트 매도 전략을 따릅니다.")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        chk_enabled = QCheckBox("뉴스매매 사용")
        layout.addWidget(chk_enabled)

        score_row = QHBoxLayout()
        lbl_score = QLabel("뉴스매매 점수")
        spin_score = QSpinBox(self)
        spin_score.setRange(0, 100)
        score_row.addWidget(lbl_score)
        score_row.addWidget(spin_score)
        score_row.addStretch(1)
        layout.addLayout(score_row)

        lbl_sell_state = QLabel("")
        lbl_sell_state.setWordWrap(True)
        layout.addWidget(lbl_sell_state)

        sell_group = QGroupBox("뉴스매매 매도 전략")
        sell_layout = QVBoxLayout(sell_group)
        sell_table = QTableWidget(0, 3, self)
        sell_table.setHorizontalHeaderLabels(["번호", "전략명", "설정값"])
        sell_header = sell_table.horizontalHeader()
        sell_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        sell_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        sell_header.setSectionResizeMode(2, QHeaderView.Stretch)
        sell_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        sell_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        sell_table.setMinimumHeight(180)
        sell_layout.addWidget(sell_table)
        sell_btn_row = QHBoxLayout()
        btn_del_sell = QPushButton("선택 항목 삭제")
        sell_btn_row.addStretch(1)
        sell_btn_row.addWidget(btn_del_sell)
        sell_layout.addLayout(sell_btn_row)
        layout.addWidget(sell_group)

        btn_save = QPushButton("전략 저장")
        layout.addWidget(btn_save)
        layout.addStretch(1)

        editor = {
            "policy_key": "news_trade",
            "widget": widget,
            "enabled": chk_enabled,
            "min_score": spin_score,
            "sell_state_label": lbl_sell_state,
            "sell_table": sell_table,
            "btn_del_sell": btn_del_sell,
            "btn_save": btn_save,
            "sell_items": [],
            "sell_param_editors": {},
        }
        btn_del_sell.clicked.connect(lambda: self._delete_selected_policy_token("news_trade", "sell"))
        btn_save.clicked.connect(lambda: self._save_strategy_policy_tab("news_trade"))
        return widget, editor

    def _build_strategy_detail_tab(self):
        self.strategy_detail_widget = QWidget(self)
        layout = QVBoxLayout(self.strategy_detail_widget)
        intro = QLabel("슬롯별 매수/매도 전략과 디폴트, 뉴스매매 정책을 설정합니다. 개별 정책이 없으면 디폴트가 적용됩니다.")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self.lbl_strategy_detail_live_reference = QLabel("실시간 참고값 : 뉴스감시에서 종목을 선택하세요")
        self.lbl_strategy_detail_live_reference.setWordWrap(True)
        layout.addWidget(self.lbl_strategy_detail_live_reference)

        self.strategy_policy_tabs = QTabWidget(self)
        self.slot_policy_editors = {}
        for slot_no in range(1, 11):
            tab, editor = self._create_policy_slot_tab("slot:{0}".format(slot_no), "조건식 이름 : 미지정")
            self.slot_policy_editors[slot_no] = editor
            self.strategy_policy_tabs.addTab(tab, "슬롯 {0}".format(slot_no))

        default_tab, self.default_policy_editor = self._create_policy_slot_tab("default", "조건식 이름 : 디폴트")
        self.strategy_policy_tabs.addTab(default_tab, "디폴트")

        news_trade_tab, self.news_trade_editor = self._create_news_trade_policy_tab()
        self.strategy_policy_tabs.addTab(news_trade_tab, "뉴스매매")
        layout.addWidget(self.strategy_policy_tabs)
        return self.strategy_detail_widget

    def _policy_editor_for_key(self, policy_key):
        if policy_key == "default":
            return self.default_policy_editor
        if policy_key == "news_trade":
            return self.news_trade_editor
        if str(policy_key).startswith("slot:"):
            return self.slot_policy_editors.get(int(str(policy_key).split(":", 1)[1]))
        return None

    def _current_policy_editor_key(self):
        if not hasattr(self, "strategy_policy_tabs"):
            return ""
        widget = self.strategy_policy_tabs.currentWidget()
        if widget is None:
            return ""
        return str(widget.property("policy_key") or "")

    def _normalize_buy_expression_items(self, items):
        normalized = []
        expect_strategy = True
        for token in list(items or []):
            kind = str((token or {}).get("kind") or "")
            if kind == "strategy":
                normalized.append(dict(token))
                expect_strategy = False
                continue
            if kind == "op":
                op_value = str((token or {}).get("value") or "").upper()
                if op_value not in ["AND", "OR"] or expect_strategy:
                    continue
                normalized.append({"kind": "op", "value": op_value})
                expect_strategy = True
        while normalized and str((normalized[-1] or {}).get("kind") or "") == "op":
            normalized.pop()
        return normalized

    def _delete_selected_policy_token(self, policy_key, target_kind):
        editor = self._policy_editor_for_key(policy_key)
        if not editor:
            return
        items_key = "buy_items" if target_kind == "buy" else "sell_items"
        items = editor.get(items_key, [])
        if not items:
            return
        if target_kind == "sell":
            sell_table = editor.get("sell_table")
            selected_row = sell_table.currentRow() if sell_table is not None else -1
            strategy_nos = self._expression_strategy_nos(items)
            if selected_row < 0 or selected_row >= len(strategy_nos):
                return
            strategy_nos.pop(selected_row)
            editor[items_key] = self._build_sell_expression_items(strategy_nos)
        else:
            buy_list = editor.get("buy_list")
            selected_row = buy_list.currentRow() if buy_list is not None else -1
            if selected_row < 0 or selected_row >= len(items):
                return
            delete_index = selected_row
            token = dict(items[selected_row] or {})
            if str(token.get("kind") or "") == "strategy":
                if selected_row > 0 and str((items[selected_row - 1] or {}).get("kind") or "") == "op":
                    delete_index = selected_row - 1
                    del items[delete_index:selected_row + 1]
                elif (selected_row + 1) < len(items) and str((items[selected_row + 1] or {}).get("kind") or "") == "op":
                    del items[selected_row:selected_row + 2]
                else:
                    items.pop(selected_row)
            else:
                items.pop(selected_row)
            editor[items_key] = self._normalize_buy_expression_items(items)
        self._refresh_policy_editor_display(editor)

    def _strategy_label_by_no(self, kind, strategy_no):
        row = self.strategy_manager.get_strategy_by_no(kind, strategy_no)
        if not row:
            return u"[{0}] 알수없음".format(int(strategy_no or 0))
        return u"[{0}] {1}".format(int(row["strategy_no"] or 0), row["strategy_name"] or row["strategy_type"])

    def _strategy_name_by_no(self, kind, strategy_no):
        row = self.strategy_manager.get_strategy_by_no(kind, strategy_no)
        if not row:
            return u"알수없음"
        return str(row["strategy_name"] or row["strategy_type"] or u"알수없음")

    def _render_policy_items(self, kind, items):
        rendered = []
        for item in list(items or []):
            if str(item.get("kind") or "") == "strategy":
                rendered.append(self._strategy_label_by_no(kind, int(item.get("no") or 0)))
            else:
                rendered.append(str(item.get("value") or "").upper())
        return rendered

    def _build_sell_expression_items(self, strategy_nos):
        result = []
        normalized = self.strategy_manager.normalize_strategy_nos("sell", strategy_nos)
        for idx, number in enumerate(normalized):
            if idx > 0:
                result.append({"kind": "op", "value": "OR"})
            result.append({"kind": "strategy", "no": int(number)})
        return result

    def _sell_expression_to_nos(self, sell_items):
        items = list(sell_items or [])
        if not items:
            return {"ok": True, "message": "", "nos": []}
        strategy_nos = []
        for item in items:
            kind = str(item.get("kind") or "").strip().lower()
            if kind == "strategy":
                try:
                    strategy_nos.append(int(item.get("no") or 0))
                except Exception:
                    return {"ok": False, "message": "유효하지 않은 매도 전략 번호가 포함되어 있습니다.", "nos": []}
                continue
            if kind == "op":
                value = str(item.get("value") or "").strip().upper()
                if value != "OR":
                    return {"ok": False, "message": "매도 전략식에는 OR만 사용할 수 있습니다.", "nos": []}
                continue
            return {"ok": False, "message": "매도 전략식 형식이 올바르지 않습니다.", "nos": []}
        if not strategy_nos:
            return {"ok": False, "message": "매도 전략을 하나 이상 추가해 주세요.", "nos": []}
        normalized = self.strategy_manager.normalize_strategy_nos("sell", strategy_nos)
        return {"ok": True, "message": "", "nos": normalized}

    def _policy_uses_news_filter(self, buy_items):
        for item in list(buy_items or []):
            if str(item.get("kind") or "") != "strategy":
                continue
            row = self.strategy_manager.get_strategy_by_no("buy", item.get("no"))
            if row and int(row["is_news_filter"] or 0):
                return True
        return False

    def _normalize_institution_interval_type(self, value):
        value = str(value or "day").strip().lower()
        mapping = {
            "day": "day",
            "daily": "day",
            "1d": "day",
            "60m": "60m",
            "60min": "60m",
            "hour": "60m",
            "1h": "60m",
            "5m": "5m",
            "5min": "5m",
        }
        return mapping.get(value, "day")

    def _institution_interval_label(self, value):
        value = self._normalize_institution_interval_type(value)
        if value == "60m":
            return "60분봉"
        if value == "5m":
            return "5분봉"
        return "일간"

    def _format_buy_strategy_config(self, strategy_type, params):
        strategy_type = str(strategy_type or "")
        params = dict(params or {})
        if strategy_type == "institution_trend_a":
            return "기관 순매수금액 > 0"
        if strategy_type == "institution_trend_b":
            value = float(params.get("min_net_buy_ratio_pct", 1.0) or 1.0)
            return "기관 순매수 비율 >= {0:.1f}%".format(value)
        if strategy_type == "institution_trend_c":
            streak_count = int(params.get("streak_count", 2) or 2)
            interval_label = self._institution_interval_label(params.get("interval_type"))
            return "{0} 최근 {1}구간 연속 순매수".format(interval_label, streak_count)
        if strategy_type == "foreign_trend_a":
            return "외국인 순매수금액 > 0"
        if strategy_type == "foreign_trend_b":
            value = float(params.get("min_net_buy_ratio_pct", 0.5) or 0.5)
            return "외국인 순매수 비율 >= {0:.1f}%".format(value)
        if strategy_type == "foreign_trend_c":
            streak_count = int(params.get("streak_count", 2) or 2)
            interval_label = self._institution_interval_label(params.get("interval_type") or "60m")
            return "{0} 최근 {1}구간 연속 순매수".format(interval_label, streak_count)
        if strategy_type == "vwap":
            return "현재가 >= 장중 VWAP"
        if strategy_type == "vwap_b":
            return "장중 VWAP > 현재가"
        if strategy_type == "sell_pressure_a":
            value = float(params.get("max_ratio", 2.0) or 2.0)
            return "매도우위 비율 <= {0:.2f}".format(value)
        if strategy_type == "sell_pressure_b":
            value = float(params.get("min_ratio", params.get("max_ratio", 1.5)) or 1.5)
            return "매도우위 비율 > {0:.2f}".format(value)
        if strategy_type == "sell_pressure_c":
            value = float(params.get("max_ratio", 1.2) or 1.2)
            return "매도우위 비율 <= {0:.2f}".format(value)
        if strategy_type == "sell_pressure_d":
            value = float(params.get("max_ratio", 1.0) or 1.0)
            return "매도우위 비율 <= {0:.2f}".format(value)
        if strategy_type == "buy_pressure_a":
            value = float(params.get("min_buy_ratio", 1.25) or 1.25)
            return "매수우위 비율 >= {0:.2f}".format(value)
        if strategy_type == "buy_pressure_b":
            value = float(params.get("max_buy_ratio", params.get("min_buy_ratio", 1.5)) or 1.5)
            return "매수우위 비율 < {0:.2f}".format(value)
        if strategy_type == "buy_pressure_c":
            value = float(params.get("min_buy_ratio", 2.0) or 2.0)
            return "매수우위 비율 >= {0:.2f}".format(value)
        if strategy_type == "news_filter":
            return "최종 뉴스 점수 >= 슬롯 뉴스점수"
        if strategy_type == "news_trade":
            return "최종점수 >= 기준, 중요도 >= 60, 빈도 >= 20"
        return "설정 없음"

    def _format_sell_strategy_config(self, strategy_type, params):
        strategy_type = str(strategy_type or "")
        params = dict(params or {})
        if strategy_type == "stop_loss":
            value = abs(float(params.get("stop_loss_pct", -3.0) or -3.0))
            return "손절 {0:.1f}%".format(value)
        if strategy_type == "take_profit":
            value = float(params.get("take_profit_pct", 5.0) or 5.0)
            return "익절 +{0:.1f}%".format(value)
        if strategy_type == "trailing_stop":
            start_value = float(params.get("trail_start_pct", 3.0) or 3.0)
            gap_value = float(params.get("trail_gap_pct", 1.5) or 1.5)
            return "시작 +{0:.1f}% / 갭 {1:.1f}%".format(start_value, gap_value)
        if strategy_type == "time_exit":
            hold_minutes = int(params.get("hold_minutes", 30) or 30)
            return "보유 {0}분".format(hold_minutes)
        if strategy_type == "market_close_exit":
            exit_hhmm = str(params.get("exit_hhmm", "1520") or "1520").zfill(4)
            return "청산 {0}:{1}".format(exit_hhmm[:2], exit_hhmm[2:])
        return "설정값 없음"

    def _create_sell_param_widget(self, strategy_row):
        try:
            strategy_row = dict(strategy_row or {})
        except Exception:
            strategy_row = {}
        strategy_type = str(strategy_row.get("strategy_type") or "")
        params = self._safe_json_dict(strategy_row.get("params_json") or "{}")
        meta = {
            "strategy_id": str(strategy_row.get("strategy_id") or ""),
            "strategy_no": int(strategy_row.get("strategy_no") or 0),
            "strategy_type": strategy_type,
        }
        container = QWidget(self)
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        if strategy_type == "stop_loss":
            spin = QDoubleSpinBox(container)
            spin.setRange(0.1, 99.9)
            spin.setDecimals(1)
            spin.setSingleStep(0.1)
            spin.setSuffix(" %")
            spin.setValue(abs(float(params.get("stop_loss_pct", -3.0) or -3.0)))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = "stop_loss"
            meta["stop_spin"] = spin
            return container, meta

        if strategy_type == "take_profit":
            spin = QDoubleSpinBox(container)
            spin.setRange(0.1, 300.0)
            spin.setDecimals(1)
            spin.setSingleStep(0.1)
            spin.setSuffix(" %")
            spin.setValue(float(params.get("take_profit_pct", 5.0) or 5.0))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = "take_profit"
            meta["take_spin"] = spin
            return container, meta

        if strategy_type == "trailing_stop":
            start_spin = QDoubleSpinBox(container)
            start_spin.setRange(0.1, 300.0)
            start_spin.setDecimals(1)
            start_spin.setSingleStep(0.1)
            start_spin.setSuffix(" %")
            start_spin.setValue(float(params.get("trail_start_pct", 3.0) or 3.0))
            gap_spin = QDoubleSpinBox(container)
            gap_spin.setRange(0.1, 100.0)
            gap_spin.setDecimals(1)
            gap_spin.setSingleStep(0.1)
            gap_spin.setSuffix(" %")
            gap_spin.setValue(float(params.get("trail_gap_pct", 1.5) or 1.5))
            layout.addWidget(QLabel("시작", container))
            layout.addWidget(start_spin)
            layout.addWidget(QLabel("갭", container))
            layout.addWidget(gap_spin)
            layout.addStretch(1)
            meta["kind"] = "trailing_stop"
            meta["start_spin"] = start_spin
            meta["gap_spin"] = gap_spin
            return container, meta

        if strategy_type == "time_exit":
            spin = QSpinBox(container)
            spin.setRange(1, 1440)
            spin.setSuffix(" 분")
            spin.setValue(int(params.get("hold_minutes", 30) or 30))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = "time_exit"
            meta["time_spin"] = spin
            return container, meta

        if strategy_type == "market_close_exit":
            try:
                exit_value = int(str(params.get("exit_hhmm", "1520") or "1520"))
            except Exception:
                exit_value = 1520
            spin = QSpinBox(container)
            spin.setRange(0, 2359)
            spin.setValue(exit_value)
            layout.addWidget(QLabel("HHMM", container))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = "market_close_exit"
            meta["close_spin"] = spin
            return container, meta

        layout.addWidget(QLabel("설정 없음", container))
        layout.addStretch(1)
        meta["kind"] = "unknown"
        return container, meta

    def _create_buy_param_widget(self, strategy_row):
        try:
            strategy_row = dict(strategy_row or {})
        except Exception:
            strategy_row = {}
        strategy_type = str(strategy_row.get("strategy_type") or "")
        params = self._safe_json_dict(strategy_row.get("params_json") or "{}")
        meta = {
            "strategy_id": str(strategy_row.get("strategy_id") or ""),
            "strategy_no": int(strategy_row.get("strategy_no") or 0),
            "strategy_type": strategy_type,
        }
        container = QWidget(self)
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        if strategy_type in ["institution_trend_b", "foreign_trend_b"]:
            spin = QDoubleSpinBox(container)
            spin.setRange(0.1, 100.0)
            spin.setDecimals(1)
            spin.setSingleStep(0.1)
            spin.setSuffix(" %")
            default_ratio = 1.0 if strategy_type == "institution_trend_b" else 0.5
            spin.setValue(float(params.get("min_net_buy_ratio_pct", default_ratio) or default_ratio))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = strategy_type
            meta["ratio_spin"] = spin
            return container, meta

        if strategy_type in ["institution_trend_c", "foreign_trend_c"]:
            combo = QComboBox(container)
            combo.addItem("일간", "day")
            combo.addItem("60분봉", "60m")
            combo.addItem("5분봉", "5m")
            default_interval = "day" if strategy_type == "institution_trend_c" else "60m"
            interval_type = self._normalize_institution_interval_type(params.get("interval_type") or default_interval)
            current_index = max(0, combo.findData(interval_type))
            combo.setCurrentIndex(current_index)
            spin = QSpinBox(container)
            spin.setRange(1, 60)
            spin.setSuffix(" 구간")
            spin.setValue(int(params.get("streak_count", 2) or 2))
            layout.addWidget(combo)
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = strategy_type
            meta["interval_combo"] = combo
            meta["streak_spin"] = spin
            return container, meta

        if strategy_type in ["sell_pressure_a", "sell_pressure_b", "sell_pressure_c", "sell_pressure_d"]:
            default_map = {
                "sell_pressure_a": 2.0,
                "sell_pressure_b": 1.5,
                "sell_pressure_c": 1.2,
                "sell_pressure_d": 1.0,
            }
            spin = QDoubleSpinBox(container)
            spin.setRange(0.1, 10.0)
            spin.setDecimals(2)
            spin.setSingleStep(0.1)
            if strategy_type == "sell_pressure_b":
                spin.setSuffix(" 배 초과")
                spin.setValue(float(params.get("min_ratio", params.get("max_ratio", default_map.get(strategy_type, 1.5))) or default_map.get(strategy_type, 1.5)))
            else:
                spin.setSuffix(" 배 이하")
                spin.setValue(float(params.get("max_ratio", default_map.get(strategy_type, 1.5)) or default_map.get(strategy_type, 1.5)))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = strategy_type
            if strategy_type == "sell_pressure_b":
                meta["min_ratio_spin"] = spin
            else:
                meta["max_ratio_spin"] = spin
            return container, meta

        if strategy_type in ["buy_pressure_a", "buy_pressure_b", "buy_pressure_c"]:
            default_map = {
                "buy_pressure_a": 1.25,
                "buy_pressure_b": 1.5,
                "buy_pressure_c": 2.0,
            }
            spin = QDoubleSpinBox(container)
            spin.setRange(0.1, 20.0)
            spin.setDecimals(2)
            spin.setSingleStep(0.1)
            if strategy_type == "buy_pressure_b":
                spin.setSuffix(" 배 미만")
                spin.setValue(float(params.get("max_buy_ratio", params.get("min_buy_ratio", default_map.get(strategy_type, 1.25))) or default_map.get(strategy_type, 1.25)))
            else:
                spin.setSuffix(" 배 이상")
                spin.setValue(float(params.get("min_buy_ratio", default_map.get(strategy_type, 1.25)) or default_map.get(strategy_type, 1.25)))
            layout.addWidget(spin)
            layout.addStretch(1)
            meta["kind"] = strategy_type
            if strategy_type == "buy_pressure_b":
                meta["max_ratio_spin"] = spin
            else:
                meta["min_ratio_spin"] = spin
            return container, meta

        summary = QLabel(self._format_buy_strategy_config(strategy_type, params), container)
        summary.setWordWrap(True)
        layout.addWidget(summary)
        layout.addStretch(1)
        meta["kind"] = "readonly"
        return container, meta

    def _save_buy_strategy_params_from_editor(self, editor):
        updated = []
        for strategy_id, meta in dict(editor.get("buy_param_editors") or {}).items():
            strategy_type = str(meta.get("strategy_type") or "")
            row = self.persistence.fetchone(
                "SELECT params_json, strategy_name FROM strategy_definitions WHERE strategy_id=?",
                (strategy_id,),
            )
            if not row:
                continue
            params = self._safe_json_dict(row["params_json"] or "{}")
            if meta.get("kind") in ["institution_trend_b", "foreign_trend_b"]:
                params["min_net_buy_ratio_pct"] = float(meta["ratio_spin"].value())
            elif meta.get("kind") in ["institution_trend_c", "foreign_trend_c"]:
                params["streak_count"] = int(meta["streak_spin"].value())
                params["interval_type"] = self._normalize_institution_interval_type(
                    meta["interval_combo"].currentData()
                )
            elif meta.get("kind") in ["sell_pressure_a", "sell_pressure_c", "sell_pressure_d"]:
                params["max_ratio"] = float(meta["max_ratio_spin"].value())
            elif meta.get("kind") == "sell_pressure_b":
                params["min_ratio"] = float(meta["min_ratio_spin"].value())
            elif meta.get("kind") in ["buy_pressure_a", "buy_pressure_c"]:
                params["min_buy_ratio"] = float(meta["min_ratio_spin"].value())
            elif meta.get("kind") == "buy_pressure_b":
                params["max_buy_ratio"] = float(meta["max_ratio_spin"].value())
            else:
                continue
            self.persistence.execute(
                "UPDATE strategy_definitions SET params_json=?, updated_at=? WHERE strategy_id=?",
                (
                    json.dumps(params, ensure_ascii=False),
                    self.persistence.now_ts(),
                    strategy_id,
                ),
            )
            updated.append({
                "strategy_id": strategy_id,
                "strategy_type": strategy_type,
                "strategy_name": row["strategy_name"] or strategy_type,
                "config": self._format_buy_strategy_config(strategy_type, params),
            })
        return {"ok": True, "updated": updated}

    def _save_sell_strategy_params_from_editor(self, editor):
        updated = []
        for strategy_id, meta in dict(editor.get("sell_param_editors") or {}).items():
            strategy_type = str(meta.get("strategy_type") or "")
            row = self.persistence.fetchone("SELECT params_json, strategy_name FROM strategy_definitions WHERE strategy_id=?", (strategy_id,))
            if not row:
                continue
            params = self._safe_json_dict(row["params_json"] or "{}")
            if meta.get("kind") == "stop_loss":
                params["stop_loss_pct"] = -abs(float(meta["stop_spin"].value()))
            elif meta.get("kind") == "take_profit":
                params["take_profit_pct"] = float(meta["take_spin"].value())
            elif meta.get("kind") == "trailing_stop":
                params["trail_start_pct"] = float(meta["start_spin"].value())
                params["trail_gap_pct"] = float(meta["gap_spin"].value())
            elif meta.get("kind") == "time_exit":
                params["hold_minutes"] = int(meta["time_spin"].value())
            elif meta.get("kind") == "market_close_exit":
                hhmm_value = int(meta["close_spin"].value() or 0)
                hour = int(hhmm_value / 100)
                minute = int(hhmm_value % 100)
                if hour > 23 or minute > 59:
                    return {
                        "ok": False,
                        "message": "장마감 강제청산 시간은 HHMM 형식으로 입력해 주세요. 예: 1520",
                    }
                params["exit_hhmm"] = "{0:02d}{1:02d}".format(hour, minute)
            else:
                continue
            self.persistence.execute(
                "UPDATE strategy_definitions SET params_json=?, updated_at=? WHERE strategy_id=?",
                (
                    json.dumps(params, ensure_ascii=False),
                    self.persistence.now_ts(),
                    strategy_id,
                ),
            )
            updated.append({
                "strategy_id": strategy_id,
                "strategy_type": strategy_type,
                "strategy_name": row["strategy_name"] or strategy_type,
                "config": self._format_sell_strategy_config(strategy_type, params),
            })
        return {"ok": True, "updated": updated}

    def _refresh_strategy_views_after_policy_save(self):
        self.refresh_buy_chain()
        self.refresh_sell_chain()
        self._populate_strategy_catalog_table("buy", self.table_buy_strategy_catalog)
        self._populate_strategy_catalog_table("sell", self.table_sell_strategy_catalog)
        self._refresh_strategy_policy_ui()
        self._schedule_user_profile_save()

    def _current_buy_params_from_meta(self, meta):
        meta = dict(meta or {})
        params = {}
        kind = str(meta.get("kind") or "")
        if kind in ["institution_trend_b", "foreign_trend_b"]:
            params["min_net_buy_ratio_pct"] = float(meta["ratio_spin"].value())
        elif kind in ["institution_trend_c", "foreign_trend_c"]:
            params["streak_count"] = int(meta["streak_spin"].value())
            params["interval_type"] = self._normalize_institution_interval_type(
                meta["interval_combo"].currentData()
            )
        elif kind in ["sell_pressure_a", "sell_pressure_c", "sell_pressure_d"]:
            params["max_ratio"] = float(meta["max_ratio_spin"].value())
        elif kind == "sell_pressure_b":
            params["min_ratio"] = float(meta["min_ratio_spin"].value())
        elif kind in ["buy_pressure_a", "buy_pressure_c"]:
            params["min_buy_ratio"] = float(meta["min_ratio_spin"].value())
        elif kind == "buy_pressure_b":
            params["max_buy_ratio"] = float(meta["max_ratio_spin"].value())
        return params

    def _current_buy_config_overrides(self, editor):
        overrides = {}
        for meta in dict(editor.get("buy_param_editors") or {}).values():
            try:
                strategy_no = int(meta.get("strategy_no") or 0)
            except Exception:
                strategy_no = 0
            strategy_type = str(meta.get("strategy_type") or "")
            if strategy_no <= 0 or not strategy_type:
                continue
            overrides[strategy_no] = self._format_buy_strategy_config(
                strategy_type,
                self._current_buy_params_from_meta(meta),
            )
        return overrides

    def _current_sell_params_from_meta(self, meta):
        meta = dict(meta or {})
        params = {}
        kind = str(meta.get("kind") or "")
        if kind == "stop_loss":
            params["stop_loss_pct"] = -abs(float(meta["stop_spin"].value()))
        elif kind == "take_profit":
            params["take_profit_pct"] = float(meta["take_spin"].value())
        elif kind == "trailing_stop":
            params["trail_start_pct"] = float(meta["start_spin"].value())
            params["trail_gap_pct"] = float(meta["gap_spin"].value())
        elif kind == "time_exit":
            params["hold_minutes"] = int(meta["time_spin"].value())
        elif kind == "market_close_exit":
            hhmm_value = int(meta["close_spin"].value() or 0)
            hour = int(hhmm_value / 100)
            minute = int(hhmm_value % 100)
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                params["exit_hhmm"] = "{0:02d}{1:02d}".format(hour, minute)
        return params

    def _current_sell_config_overrides(self, editor):
        overrides = {}
        for meta in dict(editor.get("sell_param_editors") or {}).values():
            try:
                strategy_no = int(meta.get("strategy_no") or 0)
            except Exception:
                strategy_no = 0
            strategy_type = str(meta.get("strategy_type") or "")
            if strategy_no <= 0 or not strategy_type:
                continue
            overrides[strategy_no] = self._format_sell_strategy_config(
                strategy_type,
                self._current_sell_params_from_meta(meta),
            )
        return overrides

    def _sync_default_chain_panels(self, *_args):
        editor = getattr(self, "default_policy_editor", None)
        if not editor:
            return
        buy_items = list(editor.get("buy_items") or [])
        sell_items = list(editor.get("sell_items") or [])
        if hasattr(self, "lbl_buy_default_preview"):
            self.lbl_buy_default_preview.setText(
                u"디폴트 매수 전략 : {0}".format(self._format_expression_preview("buy", buy_items))
            )
        if hasattr(self, "lbl_sell_default_preview"):
            self.lbl_sell_default_preview.setText(
                u"디폴트 매도 전략 : {0}".format(self._format_expression_preview("sell", sell_items))
            )
        if hasattr(self, "table_buy_chain"):
            self._set_policy_table_rows(
                self.table_buy_chain,
                "buy",
                self._expression_strategy_nos(buy_items),
                config_overrides=self._current_buy_config_overrides(editor),
            )
        if hasattr(self, "table_sell_chain"):
            self._set_policy_table_rows(
                self.table_sell_chain,
                "sell",
                self._expression_strategy_nos(sell_items),
                config_overrides=self._current_sell_config_overrides(editor),
            )

    def _connect_default_editor_live_sync(self, editor):
        if str(editor.get("policy_key") or "") != "default":
            return
        spin_news = editor.get("spin_news")
        if spin_news is not None:
            try:
                spin_news.valueChanged.disconnect(self._sync_default_chain_panels)
            except Exception:
                pass
            spin_news.valueChanged.connect(self._sync_default_chain_panels)
        for meta in dict(editor.get("buy_param_editors") or {}).values():
            ratio_spin = meta.get("ratio_spin")
            if ratio_spin is not None:
                try:
                    ratio_spin.valueChanged.disconnect(self._sync_default_chain_panels)
                except Exception:
                    pass
                ratio_spin.valueChanged.connect(self._sync_default_chain_panels)
            streak_spin = meta.get("streak_spin")
            if streak_spin is not None:
                try:
                    streak_spin.valueChanged.disconnect(self._sync_default_chain_panels)
                except Exception:
                    pass
                streak_spin.valueChanged.connect(self._sync_default_chain_panels)
            interval_combo = meta.get("interval_combo")
            if interval_combo is not None:
                try:
                    interval_combo.currentIndexChanged.disconnect(self._sync_default_chain_panels)
                except Exception:
                    pass
                interval_combo.currentIndexChanged.connect(self._sync_default_chain_panels)
        for meta in dict(editor.get("sell_param_editors") or {}).values():
            for key in ["stop_spin", "take_spin", "start_spin", "gap_spin", "time_spin", "close_spin"]:
                widget = meta.get(key)
                if widget is None:
                    continue
                try:
                    widget.valueChanged.disconnect(self._sync_default_chain_panels)
                except Exception:
                    pass
                widget.valueChanged.connect(self._sync_default_chain_panels)

    def _refresh_policy_editor_display(self, editor):
        if "buy_list" in editor:
            editor["buy_list"].clear()
            for text_value in self._render_policy_items("buy", editor.get("buy_items", [])):
                editor["buy_list"].addItem(QListWidgetItem(text_value))
            uses_news_filter = self._policy_uses_news_filter(editor.get("buy_items", []))
            editor["spin_news"].setEnabled(uses_news_filter)
            if not uses_news_filter:
                editor["spin_news"].setValue(0)
        if "buy_table" in editor:
            buy_table = editor["buy_table"]
            strategy_nos = self._expression_strategy_nos(editor.get("buy_items", []))
            buy_table.setRowCount(len(strategy_nos))
            editor["buy_param_editors"] = {}
            for row_index, strategy_no in enumerate(strategy_nos):
                strategy_row = self.strategy_manager.get_strategy_by_no("buy", strategy_no)
                if not strategy_row:
                    continue
                params = self._safe_json_dict(strategy_row["params_json"] or "{}")
                item_no = QTableWidgetItem(str(int(strategy_no or 0)))
                buy_table.setItem(row_index, 0, item_no)
                self._set_strategy_name_cell(
                    buy_table,
                    row_index,
                    1,
                    str(strategy_row["strategy_name"] or strategy_row["strategy_type"] or ""),
                    tooltip=self._format_buy_strategy_config(strategy_row["strategy_type"], params),
                )
                widget, meta = self._create_buy_param_widget(strategy_row)
                editor["buy_param_editors"][str(strategy_row["strategy_id"] or "")] = meta
                buy_table.setCellWidget(row_index, 2, widget)
            buy_table.resizeRowsToContents()
        if "sell_table" in editor:
            sell_table = editor["sell_table"]
            strategy_nos = self._expression_strategy_nos(editor.get("sell_items", []))
            sell_table.setRowCount(len(strategy_nos))
            editor["sell_param_editors"] = {}
            for row_index, strategy_no in enumerate(strategy_nos):
                strategy_row = self.strategy_manager.get_strategy_by_no("sell", strategy_no)
                if not strategy_row:
                    continue
                params = self._safe_json_dict(strategy_row["params_json"] or "{}")
                item_no = QTableWidgetItem(str(int(strategy_no or 0)))
                sell_table.setItem(row_index, 0, item_no)
                self._set_strategy_name_cell(
                    sell_table,
                    row_index,
                    1,
                    str(strategy_row["strategy_name"] or strategy_row["strategy_type"] or ""),
                    tooltip=self._format_sell_strategy_config(strategy_row["strategy_type"], params),
                )
                widget, meta = self._create_sell_param_widget(strategy_row)
                editor["sell_param_editors"][str(strategy_row["strategy_id"] or "")] = meta
                sell_table.setCellWidget(row_index, 2, widget)
            sell_table.resizeRowsToContents()
        if str(editor.get("policy_key") or "") == "default":
            self._connect_default_editor_live_sync(editor)
            self._sync_default_chain_panels()

    def _format_expression_preview(self, kind, items, empty_text=u"디폴트"):
        rendered = self._render_policy_items(kind, items)
        return u" ".join(rendered) if rendered else empty_text

    def _expression_strategy_nos(self, items):
        numbers = []
        for item in list(items or []):
            if str(item.get("kind") or "") != "strategy":
                continue
            try:
                numbers.append(int(item.get("no") or 0))
            except Exception:
                continue
        return numbers

    def _catalog_row_by_no(self, kind, strategy_no):
        for row in self._get_strategy_catalog_rows(kind, include_hidden=True):
            if int(row.get("strategy_no") or 0) == int(strategy_no or 0):
                return row
        return None

    def _set_policy_table_rows(self, table, kind, strategy_nos, config_overrides=None):
        numbers = []
        for value in list(strategy_nos or []):
            try:
                number = int(value or 0)
            except Exception:
                continue
            if number > 0:
                numbers.append(number)
        table.setRowCount(len(numbers))
        for row_index, strategy_no in enumerate(numbers):
            row = self._catalog_row_by_no(kind, strategy_no) or {}
            config_text = str(row.get("config") or "기본 구성 정보 없음")
            if isinstance(config_overrides, dict):
                config_text = str(
                    config_overrides.get(strategy_no)
                    or config_overrides.get(str(strategy_no))
                    or config_text
                )
            item_no = QTableWidgetItem(str(int(strategy_no or 0)))
            table.setItem(row_index, 0, item_no)
            strategy_name = str(row.get("name") or self._strategy_label_by_no(kind, strategy_no))
            self._set_strategy_name_cell(table, row_index, 1, strategy_name, tooltip=strategy_name)
            table.setItem(row_index, 2, QTableWidgetItem(str(row.get("description") or "전략 설명이 없습니다.")))
            table.setItem(row_index, 3, QTableWidgetItem(config_text))

    def _find_slot_condition_row(self, slot_no):
        for row in self.condition_manager.get_slots():
            if int(row["slot_no"] or 0) == int(slot_no):
                return row
        return None

    def _refresh_strategy_policy_ui(self):
        if not hasattr(self, "strategy_policy_tabs"):
            return
        default_row = self.strategy_manager.get_default_strategy_policy()
        default_buy_items = json.loads(default_row["buy_expression_json"] or "[]")
        default_sell_nos = json.loads(default_row["sell_strategy_nos_json"] or "[]")
        self.default_policy_editor["condition_label"].setText("조건식 이름 : 디폴트")
        self.default_policy_editor["state_label"].setText("기본 정책 편집")
        self.default_policy_editor["buy_items"] = list(default_buy_items)
        self.default_policy_editor["sell_items"] = self._build_sell_expression_items(default_sell_nos)
        self.default_policy_editor["spin_news"].setValue(int(default_row["news_min_score"] or 0))
        self._refresh_policy_editor_display(self.default_policy_editor)

        self.lbl_buy_default_preview.setText(u"디폴트 매수 전략 : {0}".format(self._format_expression_preview("buy", default_buy_items)))
        self.lbl_sell_default_preview.setText(u"디폴트 매도 전략 : {0}".format(self._format_expression_preview("sell", self._build_sell_expression_items(default_sell_nos))))

        for slot_no, editor in self.slot_policy_editors.items():
            slot_row = self._find_slot_condition_row(slot_no)
            condition_name = (slot_row["condition_name"] if slot_row else "") or ""
            condition_id = (slot_row["condition_id"] if slot_row else "") or ""
            slot_policy = self.strategy_manager.get_slot_strategy_policy(slot_no)
            effective = self.strategy_manager.resolve_slot_strategy_policy(slot_no)
            buy_items = json.loads(effective.get("buy_expression_json") or "[]")
            sell_nos = json.loads(effective.get("sell_strategy_nos_json") or "[]")
            editor["buy_items"] = list(buy_items)
            editor["sell_items"] = self._build_sell_expression_items(sell_nos)
            editor["spin_news"].setValue(int(effective.get("news_min_score") or 0))
            if not condition_id:
                editor["condition_label"].setText("조건식 이름 : 미지정")
                editor["state_label"].setText("조건식 미지정")
                editor["widget"].setEnabled(False)
            else:
                editor["widget"].setEnabled(True)
                editor["condition_label"].setText(u"조건식 이름 : {0}".format(condition_name))
                if slot_policy:
                    editor["state_label"].setText("개별 전략 사용 중")
                else:
                    editor["state_label"].setText("디폴트 적용 중")
            self._refresh_policy_editor_display(editor)

        news_trade_row = self.strategy_manager.get_news_trade_policy()
        news_trade_sell_nos = json.loads(news_trade_row["sell_strategy_nos_json"] or "[]")
        if news_trade_sell_nos:
            self.news_trade_editor["sell_state_label"].setText("뉴스매매 전용 매도 전략 사용 중")
            effective_sell_nos = news_trade_sell_nos
        else:
            self.news_trade_editor["sell_state_label"].setText("디폴트 매도 전략 적용 중")
            effective_sell_nos = default_sell_nos
        self.news_trade_editor["enabled"].setChecked(bool(int(news_trade_row["enabled"] or 0)))
        self.news_trade_editor["min_score"].setValue(int(news_trade_row["min_score"] or 0))
        self.news_trade_editor["sell_items"] = self._build_sell_expression_items(effective_sell_nos)
        self._refresh_policy_editor_display(self.news_trade_editor)

    def _validate_news_filter_vs_news_trade(self, filter_score):
        policy = self.strategy_manager.get_news_trade_policy()
        if not bool(int(policy["enabled"] or 0)):
            return {"ok": True, "message": ""}
        trade_score = int(policy["min_score"] or 0)
        if int(filter_score or 0) >= trade_score:
            return {"ok": False, "message": u"뉴스매매 점수는 뉴스필터 점수보다 높아야 합니다. 현재 뉴스매매 점수는 {0}점입니다.".format(trade_score)}
        return {"ok": True, "message": ""}

    def _validate_news_trade_score_against_filters(self, news_trade_score):
        active_scores = self.strategy_manager.collect_active_news_filter_scores()
        max_score = 0
        for row in active_scores:
            try:
                max_score = max(max_score, int(row.get("score") or 0))
            except Exception:
                pass
        if max_score > 0 and int(news_trade_score or 0) <= max_score:
            return {"ok": False, "message": u"현재 가장 높은 뉴스필터 점수는 {0}점입니다. 뉴스매매 점수는 {1}점보다 커야 합니다.".format(max_score, max_score)}
        return {"ok": True, "message": ""}

    def _save_strategy_policy_tab(self, policy_key):
        if policy_key == "news_trade":
            param_save = self._save_sell_strategy_params_from_editor(self.news_trade_editor)
            if not param_save.get("ok"):
                QMessageBox.warning(self, "뉴스매매", param_save.get("message") or "매도 전략 설정값이 올바르지 않습니다.")
                return
            enabled = bool(self.news_trade_editor["enabled"].isChecked())
            min_score = int(self.news_trade_editor["min_score"].value() or 0)
            check = self._validate_news_trade_score_against_filters(min_score)
            if enabled and not check.get("ok"):
                QMessageBox.warning(self, "뉴스매매", check.get("message") or "뉴스매매 점수가 유효하지 않습니다.")
                return
            sell_check = self._sell_expression_to_nos(self.news_trade_editor.get("sell_items", []))
            if not sell_check.get("ok"):
                QMessageBox.warning(self, "뉴스매매", sell_check.get("message") or "뉴스매매 매도 전략식이 올바르지 않습니다.")
                return
            self.strategy_manager.save_news_trade_policy(enabled, min_score, sell_check.get("nos", []))
            self.append_log(u"💾 뉴스매매 정책 저장")
            self._refresh_strategy_views_after_policy_save()
            return

        editor = self._policy_editor_for_key(policy_key)
        if not editor:
            return
        buy_param_save = self._save_buy_strategy_params_from_editor(editor)
        if not buy_param_save.get("ok"):
            QMessageBox.warning(self, "전략상세", buy_param_save.get("message") or "매수 전략 설정값이 올바르지 않습니다.")
            return
        param_save = self._save_sell_strategy_params_from_editor(editor)
        if not param_save.get("ok"):
            QMessageBox.warning(self, "전략상세", param_save.get("message") or "매도 전략 설정값이 올바르지 않습니다.")
            return
        if policy_key.startswith("slot:"):
            slot_no = int(policy_key.split(":", 1)[1])
            slot_row = self._find_slot_condition_row(slot_no)
            if not slot_row or not (slot_row["condition_id"] or ""):
                QMessageBox.warning(self, "전략상세", "조건식이 배정된 슬롯에서만 저장할 수 있습니다.")
                return
        buy_items = list(editor.get("buy_items", []))
        sell_check = self._sell_expression_to_nos(editor.get("sell_items", []))
        if not sell_check.get("ok"):
            QMessageBox.warning(self, "전략상세", sell_check.get("message") or "매도 전략식이 올바르지 않습니다.")
            return
        uses_news_filter = self._policy_uses_news_filter(buy_items)
        news_score = int(editor["spin_news"].value() or 0)
        if uses_news_filter and news_score <= 0:
            QMessageBox.warning(self, "전략상세", "뉴스필터 전략을 사용 중이므로 뉴스점수를 입력해 주세요.")
            return
        if uses_news_filter:
            check = self._validate_news_filter_vs_news_trade(news_score)
            if not check.get("ok"):
                QMessageBox.warning(self, "전략상세", check.get("message") or "뉴스 점수 검증에 실패했습니다.")
                return
        if policy_key == "default":
            self.strategy_manager.save_default_strategy_policy(buy_items, sell_check.get("nos", []), news_score if uses_news_filter else 0)
            self.append_log(u"💾 디폴트 전략 정책 저장")
        else:
            slot_no = int(policy_key.split(":", 1)[1])
            self.strategy_manager.save_slot_strategy_policy(slot_no, buy_items, sell_check.get("nos", []), news_score if uses_news_filter else 0)
            self.append_log(u"💾 슬롯 {0} 전략 정책 저장".format(slot_no))
        self._refresh_strategy_views_after_policy_save()

    def _build_scope_tab(self):
        widget = QWidget()
        self.scope_tab_widget = widget
        layout = QVBoxLayout(widget)
        summary = QLabel("이 표는 HTS/API 잔고가 아니라 프로그램 체결기록 기준의 전략별 내부 복기입니다. 운영 탭 값과 다를 수 있습니다.")
        summary.setWordWrap(True)
        layout.addWidget(summary)

        self.table_strategy_analysis = QTableWidget(0, 9)
        self.table_strategy_analysis.setHorizontalHeaderLabels([
            "조건슬롯",
            "전략명",
            "진입유형",
            "진입건수",
            "청산건수",
            "승",
            "패",
            "승률",
            "내부누적",
        ])
        header = self.table_strategy_analysis.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.Stretch)
        self.table_strategy_analysis.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_strategy_analysis.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_strategy_analysis.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table_strategy_analysis)

        review_group = QGroupBox("날짜별 거래 복기")
        review_layout = QVBoxLayout(review_group)

        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("기준일"))
        self.cbo_daily_review_date = QComboBox()
        self.cbo_daily_review_date.setMinimumWidth(220)
        top_row.addWidget(self.cbo_daily_review_date)
        top_row.addSpacing(12)
        top_row.addWidget(QLabel("기록 상태"))
        self.lbl_daily_review_status = QLabel("복기 기록 확인 중")
        self.lbl_daily_review_status.setStyleSheet("color: #555; font-weight: 700;")
        top_row.addWidget(self.lbl_daily_review_status, 1)
        review_layout.addLayout(top_row)

        help_label = QLabel("이 구역은 프로그램 복기 스냅샷입니다. 당일 보유 종목의 총평가금액과 실현손익, 그리고 그 합산 값을 함께 확인합니다.")
        help_label.setWordWrap(True)
        review_layout.addWidget(help_label)

        self.table_daily_review_summary = QTableWidget(0, 7)
        self.table_daily_review_summary.setHorizontalHeaderLabels([
            "계좌",
            "기록시각",
            "보유평가",
            "실현손익",
            "합산금액",
            "보유종목 수",
            "매도종목 수",
        ])
        summary_header = self.table_daily_review_summary.horizontalHeader()
        summary_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        summary_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        summary_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        summary_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        summary_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        summary_header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        summary_header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.table_daily_review_summary.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_daily_review_summary.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_daily_review_summary.setEditTriggers(QAbstractItemView.NoEditTriggers)
        tables_row = QHBoxLayout()
        tables_row.addWidget(self.table_daily_review_summary, 1)

        self.table_daily_review_items = QTableWidget(0, 10)
        self.table_daily_review_items.setHorizontalHeaderLabels([
            "계좌",
            "종목명",
            "코드",
            "구분",
            "매수 전략",
            "조건식",
            "기준가",
            "보유평가손익",
            "내부실현",
            "합산기여",
        ])
        self.table_daily_review_items.setColumnCount(5)
        self.table_daily_review_items.setHorizontalHeaderLabels([
            "계좌",
            "종목명(코드)",
            "기준가",
            "보유평가",
            "실현손익",
        ])
        item_header = self.table_daily_review_items.horizontalHeader()
        item_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        item_header.setSectionResizeMode(1, QHeaderView.Stretch)
        item_header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        item_header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        item_header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table_daily_review_items.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_daily_review_items.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_daily_review_items.setEditTriggers(QAbstractItemView.NoEditTriggers)
        tables_row.addWidget(self.table_daily_review_items, 1)
        review_layout.addLayout(tables_row)

        layout.addWidget(review_group)
        return widget

    def _build_operations_tab(self):
        widget = QWidget()
        self.operations_tab_widget = widget
        layout = QVBoxLayout(widget)
        top_row = QHBoxLayout()
        top_row.addStretch(1)
        self.btn_operations_refresh = QPushButton("재조회")
        top_row.addWidget(self.btn_operations_refresh)
        layout.addLayout(top_row)
        self.table_accounts_summary = QTableWidget(0, 9)
        self.table_accounts_summary.setHorizontalHeaderLabels(["계좌", "예수금", "주문 가능 현금", "총평가", "추정자산", "보유종목 수", "총매입", "총손익", "실현손익"])
        self.table_accounts_summary.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_accounts_summary.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table_accounts_summary)

        self.table_positions = QTableWidget(0, 10)
        self.table_positions.setHorizontalHeaderLabels(["계좌", "종목명", "코드", "매입가", "현재가", "평가손익", "수익률", "수량", "매수 전략", "뉴스 상태"])
        header = self.table_positions.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Fixed)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.Stretch)
        header.setSectionResizeMode(9, QHeaderView.ResizeToContents)
        self.table_positions.setColumnWidth(1, 90)
        self.table_positions.setColumnWidth(8, 260)
        self.table_positions.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_positions.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_positions.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_positions.setSortingEnabled(True)
        layout.addWidget(self.table_positions)

        position_action_row = QHBoxLayout()
        self.btn_manual_sell_position = QPushButton("선택 매도")
        position_action_row.addWidget(self.btn_manual_sell_position)
        position_action_row.addStretch(1)
        layout.addLayout(position_action_row)

        self.lbl_open_order_hint = QLabel("미체결 주문내역입니다. 아래 버튼으로 선택 주문을 바로 처리할 수 있습니다.")
        layout.addWidget(self.lbl_open_order_hint)

        self.table_open_orders = QTableWidget(0, 9)
        self.table_open_orders.setHorizontalHeaderLabels(["계좌", "주문번호", "종목명", "코드", "상태", "주문수량", "미체결", "주문가", "진행단계"])
        header = self.table_open_orders.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.Stretch)
        self.table_open_orders.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_open_orders.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_open_orders.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table_open_orders)

        manual_row = QHBoxLayout()
        self.btn_manual_cancel_open_order = QPushButton("선택 미체결 즉시 취소")
        self.btn_manual_reprice_open_order = QPushButton("선택 미체결 즉시 재호가")
        self.btn_manual_market_open_order = QPushButton("선택 미체결 즉시 시장가 전환")
        manual_row.addWidget(self.btn_manual_cancel_open_order)
        manual_row.addWidget(self.btn_manual_reprice_open_order)
        manual_row.addWidget(self.btn_manual_market_open_order)
        manual_row.addStretch(1)
        layout.addLayout(manual_row)
        return widget

    def _build_news_watch_tab(self):
        widget = QWidget()
        self.news_watch_tab_widget = widget
        layout = QVBoxLayout(widget)
        self.news_watch_loading_label_actual = QLabel("뉴스감시 데이터 로딩 중...")
        self.news_watch_loading_label_actual.setStyleSheet("color: #8a5a00; font-weight: 700; background: #fff4d6; padding: 6px 8px; border: 1px solid #f2d28b;")
        self.news_watch_loading_label_actual.setMinimumHeight(self.news_watch_loading_label_actual.sizeHint().height())
        self.news_watch_loading_label_actual.setText("")
        self.news_watch_loading_label_actual.setVisible(True)
        layout.addWidget(self.news_watch_loading_label_actual)
        self.lbl_news_watch_live_reference = QLabel("실시간 참고값 : 뉴스감시에서 종목을 선택하세요")
        self.lbl_news_watch_live_reference.setWordWrap(True)
        layout.addWidget(self.lbl_news_watch_live_reference)
        self.table_news_watch = QTableWidget(0, 9)
        self.table_news_watch.setHorizontalHeaderLabels(["종목명", "코드", "마지막편입", "만료시각", "상태", "제외 사유", "뉴스확인", "중요뉴스", "스팸"])
        header = self.table_news_watch.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.Stretch)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeToContents)
        self.table_news_watch.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_news_watch.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_news_watch.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_news_watch.setSortingEnabled(False)
        self.table_news_watch.setWordWrap(False)
        self.table_news_watch.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.table_news_watch.verticalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.table_news_watch.verticalHeader().setDefaultSectionSize(24)
        self.table_news_watch.verticalScrollBar().valueChanged.connect(self._on_news_watch_scroll_changed)
        layout.addWidget(self.table_news_watch)
        row = QHBoxLayout()
        self.btn_recheck_news = QPushButton("선택 종목 뉴스 재검색")
        self.btn_mark_spam = QPushButton("선택 종목 스팸 등록")
        row.addWidget(self.btn_recheck_news)
        row.addWidget(self.btn_mark_spam)
        layout.addLayout(row)
        return widget

    def _build_spam_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        self.table_spam = QTableWidget(0, 4)
        self.table_spam.setHorizontalHeaderLabels(["종목명", "코드", "등록시각", "사유"])
        self.table_spam.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_spam.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table_spam)
        self.btn_unspam = QPushButton("선택 스팸 해제")
        layout.addWidget(self.btn_unspam)
        return widget

    def _connect_signals(self):
        for manager in [
            self.credential_manager,
            self.kiwoom_client,
            self.account_manager,
            self.condition_manager,
            self.strategy_manager,
            self.news_manager,
            self.telegram_router,
            self.order_manager,
            self.pipeline_manager,
            self.recovery_manager,
        ]:
            if hasattr(manager, "log_emitted"):
                manager.log_emitted.connect(self.append_log)

        self.btn_restart_app.clicked.connect(self._restart_app_from_ui)
        self.btn_login.clicked.connect(self._request_login_from_ui)
        self.btn_disconnect.clicked.connect(self._request_disconnect_from_ui)
        self.btn_auto_login_settings.clicked.connect(self._open_auto_login_settings_dialog)
        self.kiwoom_client.connection_changed.connect(self._on_connection_changed)
        self.kiwoom_client.accounts_loaded.connect(self._on_accounts_loaded)
        self.kiwoom_client.account_sync_finished.connect(self._on_account_sync_finished)
        self.kiwoom_client.api_message_received.connect(self._handle_api_message)

        self.cbo_execution_mode.currentIndexChanged.connect(self._on_execution_mode_changed)
        self.cbo_global_pw_mode.currentIndexChanged.connect(self._on_global_pw_mode_changed)
        self.btn_apply_accounts.clicked.connect(self._save_account_advanced_settings)
        self.btn_sync_accounts.clicked.connect(self._sync_active_accounts)
        self.btn_restore_runtime.clicked.connect(self._restore_runtime)
        self.account_manager.accounts_changed.connect(self._on_accounts_state_changed)

        self.condition_manager.catalog_changed.connect(self._on_catalog_changed)
        self.condition_manager.slots_changed.connect(self._on_slots_changed)
        self.condition_manager.tracked_symbol_changed.connect(self._schedule_refresh_news_watch)
        self.condition_manager.symbol_detected.connect(self._on_realtime_capture_log_symbol_detected)
        self.edt_condition_search.textChanged.connect(self.refresh_condition_catalog)
        self.btn_assign_slot.clicked.connect(self._assign_selected_condition)
        self.table_slots.itemSelectionChanged.connect(self._sync_slot_target_from_table_selection)
        self.table_news_watch.itemSelectionChanged.connect(self._on_news_watch_selection_changed)
        self.btn_clear_slot.clicked.connect(self._clear_selected_slot)
        self.btn_start_slot.clicked.connect(self._start_selected_slot)
        self.btn_stop_slot.clicked.connect(self._stop_selected_slot)

        for idx, widgets in enumerate(self.naver_rows, 1):
            widgets[3].clicked.connect(lambda checked=False, row_no=idx: self._save_naver_row(row_no))
        self.btn_save_dart_api.clicked.connect(self._save_dart_api)
        for idx, widgets in enumerate(self.news_telegram_rows, 1):
            widgets[3].clicked.connect(lambda checked=False, row_no=idx: self._save_telegram_row("news", row_no))
        self.btn_save_news_send_min_score.clicked.connect(self._save_news_send_min_score)
        for idx, widgets in enumerate(self.trade_telegram_rows, 1):
            widgets[3].clicked.connect(lambda checked=False, row_no=idx: self._save_telegram_row("trade", row_no))
        for idx, widgets in enumerate(self.ai_api_rows, 1):
            widgets[6].clicked.connect(lambda checked=False, row_no=idx: self._save_ai_row(row_no))

        self.strategy_manager.strategies_changed.connect(self._on_strategies_changed)
        self.btn_add_buy.clicked.connect(lambda: self._add_strategy("buy"))
        self.btn_add_sell.clicked.connect(lambda: self._add_strategy("sell"))
        self.btn_add_selected_buy_catalog.clicked.connect(
            lambda: self._add_selected_catalog_strategy("buy", self.table_buy_strategy_catalog)
        )
        self.btn_add_selected_sell_catalog.clicked.connect(
            lambda: self._add_selected_catalog_strategy("sell", self.table_sell_strategy_catalog)
        )
        self.btn_buy_catalog_and.clicked.connect(lambda: self._append_policy_operator("buy", "AND"))
        self.btn_buy_catalog_or.clicked.connect(lambda: self._append_policy_operator("buy", "OR"))
        self.btn_sell_catalog_or.clicked.connect(lambda: self._append_policy_operator("sell", "OR"))
        self.table_buy_strategy_catalog.itemDoubleClicked.connect(
            lambda _item: self._add_selected_catalog_strategy("buy", self.table_buy_strategy_catalog)
        )
        self.table_sell_strategy_catalog.itemDoubleClicked.connect(
            lambda _item: self._add_selected_catalog_strategy("sell", self.table_sell_strategy_catalog)
        )
        self.table_buy_strategy_catalog.itemClicked.connect(
            lambda item: self._show_strategy_catalog_item_detail(self.table_buy_strategy_catalog, item)
        )
        self.table_sell_strategy_catalog.itemClicked.connect(
            lambda item: self._show_strategy_catalog_item_detail(self.table_sell_strategy_catalog, item)
        )
        self.btn_del_buy.clicked.connect(lambda: self._delete_selected_strategy(self.table_buy_chain))
        self.btn_del_sell.clicked.connect(lambda: self._delete_selected_strategy(self.table_sell_chain))
        self.btn_up_buy.clicked.connect(lambda: self._move_selected_strategy(self.table_buy_chain, -1))
        self.btn_down_buy.clicked.connect(lambda: self._move_selected_strategy(self.table_buy_chain, 1))
        self.btn_up_sell.clicked.connect(lambda: self._move_selected_strategy(self.table_sell_chain, -1))
        self.btn_down_sell.clicked.connect(lambda: self._move_selected_strategy(self.table_sell_chain, 1))

        self.btn_recheck_news.clicked.connect(self._recheck_selected_news_symbol)
        self.btn_mark_spam.clicked.connect(self._spam_selected_symbol)
        self.btn_unspam.clicked.connect(self._unspam_selected_symbol)
        self.btn_operations_refresh.clicked.connect(self._sync_active_accounts)
        self.cbo_daily_review_date.currentIndexChanged.connect(self.refresh_daily_review_view)
        self.btn_manual_sell_position.clicked.connect(self._manual_sell_selected_position)
        self.btn_manual_cancel_open_order.clicked.connect(self._manual_cancel_selected_open_order)
        self.btn_manual_reprice_open_order.clicked.connect(self._manual_reprice_selected_open_order)
        self.btn_manual_market_open_order.clicked.connect(self._manual_market_selected_open_order)
        self.order_manager.positions_changed.connect(self._schedule_refresh_operations)
        self.order_manager.summaries_changed.connect(self._schedule_refresh_operations)
        self.order_manager.trade_cycles_changed.connect(self._schedule_refresh_operations)
        self.order_manager.trade_cycles_changed.connect(self._schedule_refresh_policy_logs)
        self.pipeline_manager.pipeline_changed.connect(self._schedule_refresh_news_watch)
        self.pipeline_manager.pipeline_changed.connect(self._schedule_refresh_operations)
        self.pipeline_manager.pipeline_changed.connect(self._schedule_refresh_policy_logs)
        self.right_tabs.currentChanged.connect(self._on_right_tab_changed)
        self.strategy_policy_tabs.currentChanged.connect(self._schedule_refresh_realtime_strategy_reference_labels)
        manager = getattr(self.strategy_manager, "realtime_market_state_manager", None)
        if manager is not None and hasattr(manager, "market_state_changed"):
            manager.market_state_changed.connect(self._on_realtime_market_state_changed)

    def _load_initial_data(self):
        self._load_credentials_to_ui()
        self._refresh_auto_login_status()
        self.refresh_condition_catalog()
        self.refresh_slots()
        self.refresh_buy_chain()
        self.refresh_sell_chain()
        self._refresh_strategy_policy_ui()
        self._load_account_advanced_settings()
        self._operations_refresh_pending = True
        self._scope_refresh_pending = True
        self._schedule_refresh_news_watch(80)
        self.refresh_spam_table()
        self._policy_logs_refresh_pending = True
        if self._is_realtime_reference_tab_active():
            self.refresh_realtime_capture_log()
        self._schedule_refresh_realtime_strategy_reference_labels(40)
        self._schedule_credential_verification()
        if self.credential_manager.get_auto_login_on_startup() and self.kiwoom_client.is_available():
            QTimer.singleShot(0, lambda: self._begin_startup_loading("프로그램 시작 준비 중입니다."))
        QTimer.singleShot(800, self._attempt_startup_auto_login)

    def _refresh_auto_login_status(self):
        enabled = bool(self.credential_manager.get_auto_login_on_startup())
        if hasattr(self, "lbl_auto_login_status"):
            self.lbl_auto_login_status.setText("자동로그인: 사용" if enabled else "자동로그인: 해제")

    def _open_auto_login_settings_dialog(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("자동로그인 설정")
        dialog.resize(420, 180)

        layout = QVBoxLayout(dialog)
        lbl_help = QLabel(
            "프로그램 시작 시 키움 로그인 창을 자동으로 호출할지 설정합니다.\n"
            "키움 계정/인증서 자체 자동로그인은 키움 OpenAPI 환경 설정을 따릅니다."
        )
        lbl_help.setWordWrap(True)
        chk_auto_login = QCheckBox("프로그램 시작 시 키움 로그인 자동 시도")
        chk_auto_login.setChecked(bool(self.credential_manager.get_auto_login_on_startup()))
        layout.addWidget(lbl_help)
        layout.addWidget(chk_auto_login)
        layout.addStretch(1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=dialog)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec_() != QDialog.Accepted:
            return

        enabled = bool(chk_auto_login.isChecked())
        self.credential_manager.set_auto_login_on_startup(enabled)
        self._refresh_auto_login_status()
        self.append_log("⚙️ 자동로그인 설정 변경: {0}".format("사용" if enabled else "해제"))

    def _attempt_startup_auto_login(self):
        if self._startup_auto_login_attempted:
            return
        self._startup_auto_login_attempted = True
        if self._manual_api_disconnect:
            return
        if not self.credential_manager.get_auto_login_on_startup():
            return
        if self.kiwoom_client.get_connect_state() == 1:
            return
        if not self.kiwoom_client.is_available():
            self.append_log("⚠️ 자동로그인 시도 불가: 키움 API 컨트롤이 준비되지 않았습니다.")
            return
        self.append_log("🔐 자동로그인 설정이 활성화되어 키움 로그인 창을 호출합니다.")
        self._request_login_with_loading(auto_trigger=True)

    def _request_login_from_ui(self):
        self._request_login_with_loading(auto_trigger=False)

    def _request_disconnect_from_ui(self):
        if not self.kiwoom_client.is_available():
            self.append_log("⚠️ 키움 API 컨트롤이 준비되지 않아 연결 종료를 진행할 수 없습니다.")
            return
        if self.kiwoom_client.get_connect_state() != 1 and not bool(self.kiwoom_client.connected):
            self._manual_api_disconnect = True
            self._connection_watchdog_prev_connected = False
            self.btn_disconnect.setEnabled(False)
            self.append_log("ℹ️ 이미 API 연결이 종료되어 있습니다.")
            return
        answer = QMessageBox.question(
            self,
            "API 연결종료",
            "키움 API 연결을 종료할까요?\n자동 재연결은 다시 로그인하기 전까지 중단됩니다.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        self._manual_api_disconnect = True
        self._connection_watchdog_prev_connected = False
        self._auto_restart_in_progress = False
        self._reset_auto_reconnect()
        if self._maintenance_retry_timer.isActive():
            self._maintenance_retry_timer.stop()
        if self._startup_loading_timer.isActive():
            self._startup_loading_timer.stop()
        if self._startup_warmup_timer.isActive():
            self._startup_warmup_timer.stop()
        if self._startup_bootstrap_active:
            self._finish_startup_loading()
        try:
            for row in list(self.condition_manager.get_slots() or []):
                if int(row["is_realtime"] or 0):
                    try:
                        self.condition_manager.stop_realtime_slot(int(row["slot_no"] or 0))
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            self.kiwoom_client.set_market_realtime_codes([])
        except Exception:
            pass
        self.refresh_slots()
        self.kiwoom_client.disconnect_server("사용자 요청")

    def _create_startup_loading_dialog(self):
        if self._startup_loading_dialog is None:
            self._startup_loading_dialog = StartupLoadingDialog(self)
        return self._startup_loading_dialog

    def _startup_step_labels(self):
        return {
            "login": "키움 로그인",
            "conditions": "조건식 로드",
            "profile": "사용자 설정 복원",
            "account_sync": "계좌 동기화",
        }

    def _startup_status_prefix(self, status):
        return {
            "pending": "[대기]",
            "in_progress": "[진행]",
            "done": "[완료]",
            "skipped": "[생략]",
            "failed": "[실패]",
        }.get(str(status or "pending"), "[대기]")

    def _render_startup_loading_detail(self):
        lines = []
        labels = self._startup_step_labels()
        for key in ["login", "conditions", "profile", "account_sync"]:
            step = dict(self._startup_bootstrap_steps.get(key) or {})
            line = "{0} {1}".format(
                self._startup_status_prefix(step.get("status", "pending")),
                labels.get(key, key),
            )
            detail = str(step.get("detail") or "").strip()
            if detail:
                line = "{0} - {1}".format(line, detail)
            lines.append(line)
        lines.append("")
        lines.append("로딩 중에는 탭 전환이나 패널 클릭이 잠시 제한됩니다.")
        return "\n".join(lines)

    def _refresh_startup_loading_dialog(self):
        if not self._startup_bootstrap_active:
            return
        dialog = self._create_startup_loading_dialog()
        dialog.set_status(
            self._startup_loading_message or "필수 시작 작업을 불러오는 중입니다.",
            self._render_startup_loading_detail(),
        )
        if not dialog.isVisible():
            dialog.show()
        dialog.raise_()
        dialog.activateWindow()
        QApplication.processEvents()

    def _begin_startup_loading(self, message):
        self._startup_bootstrap_active = True
        self._startup_loading_message = str(message or "필수 시작 작업을 불러오는 중입니다.")
        self._startup_bootstrap_steps = {
            "login": {"status": "pending", "detail": ""},
            "conditions": {"status": "pending", "detail": ""},
            "profile": {"status": "pending", "detail": ""},
            "account_sync": {"status": "pending", "detail": ""},
        }
        self._startup_loading_timer.start(120000)
        self._refresh_startup_loading_dialog()

    def _set_startup_step(self, key, status, detail=""):
        if not self._startup_bootstrap_active:
            return
        self._startup_bootstrap_steps[str(key or "")] = {
            "status": str(status or "pending"),
            "detail": str(detail or ""),
        }
        self._refresh_startup_loading_dialog()

    def _set_startup_loading_message(self, message):
        if not self._startup_bootstrap_active:
            return
        self._startup_loading_message = str(message or "")
        self._refresh_startup_loading_dialog()

    def _finish_startup_loading(self):
        self._startup_bootstrap_active = False
        self._startup_loading_message = ""
        self._startup_bootstrap_steps = {}
        if self._startup_loading_timer.isActive():
            self._startup_loading_timer.stop()
        if self._startup_warmup_timer.isActive():
            self._startup_warmup_timer.stop()
        if self._startup_loading_dialog is not None:
            self._startup_loading_dialog.hide()

    def _on_startup_loading_timeout(self):
        if not self._startup_bootstrap_active:
            return
        self.append_log("?좑툘 ?쒖옉 濡쒕뵫 ?湲? ?쒓컙??珥덇낵?섏뿬 濡쒕뵫 李쎌쓣 ?뺣━?⑸땲??")
        self._finish_startup_loading()

    def _get_startup_warmup_counts(self):
        snapshot_count = 0
        detection_count = 0
        try:
            snapshot_count = int(self.condition_manager.get_pending_snapshot_job_count())
        except Exception:
            snapshot_count = 0
        try:
            detection_count = int(self.pipeline_manager.get_pending_detection_job_count())
        except Exception:
            detection_count = 0
        return max(0, snapshot_count), max(0, detection_count)

    def _check_startup_warmup(self):
        if not self._startup_bootstrap_active:
            return
        snapshot_count, detection_count = self._get_startup_warmup_counts()
        if snapshot_count <= 0 and detection_count <= 0:
            self._finish_startup_loading()
            return
        pending_parts = []
        if snapshot_count > 0:
            pending_parts.append("기준치 {0}건".format(snapshot_count))
        if detection_count > 0:
            pending_parts.append("파이프라인 {0}건".format(detection_count))
        pending_text = ", ".join(pending_parts) if pending_parts else "후속 작업"
        self._set_startup_loading_message("초기 종목 준비 작업을 마무리하는 중입니다.")
        self._set_startup_step("account_sync", "done", "계좌 동기화 완료 / 후속 작업 {0}".format(pending_text))
        self._startup_warmup_timer.start(700)

    def _request_login_with_loading(self, auto_trigger=False):
        self._manual_api_disconnect = False
        if hasattr(self.condition_manager, "set_startup_background_mode"):
            self.condition_manager.set_startup_background_mode(True, duration_sec=150)
        if hasattr(self.pipeline_manager, "set_startup_background_mode"):
            self.pipeline_manager.set_startup_background_mode(True, duration_sec=150)
        if not self._startup_bootstrap_active:
            self._begin_startup_loading("필수 시작 작업을 준비하는 중입니다.")
        self._set_startup_loading_message(
            "자동 로그인으로 시작 작업을 불러오는 중입니다." if auto_trigger else "로그인 후 시작 작업을 불러오는 중입니다."
        )
        self._set_startup_step("login", "in_progress", "로그인 창을 호출하고 있습니다.")
        ok = self.kiwoom_client.connect_server()
        if not ok:
            self._set_startup_step("login", "failed", "로그인 창 호출에 실패했습니다.")
            self._finish_startup_loading()

    def append_log(self, text):
        line = str(text or "")
        if not line:
            return
        self.log_view.appendPlainText(line)
        if self.file_log_manager is not None:
            try:
                self.file_log_manager.write_line(line)
            except Exception:
                pass

    def _schedule_refresh_operations(self, delay_ms=250):
        self._operations_refresh_pending = True
        self._scope_refresh_pending = True
        if not (self._is_operations_tab_active() or self._is_scope_tab_active()):
            return
        self._refresh_operations_timer.start(max(50, int(delay_ms or 250)))

    def _is_scope_tab_active(self):
        return (
            hasattr(self, "right_tabs")
            and getattr(self, "scope_tab_widget", None) is not None
            and self.right_tabs.currentWidget() == self.scope_tab_widget
        )

    def _is_operations_tab_active(self):
        return (
            hasattr(self, "right_tabs")
            and getattr(self, "operations_tab_widget", None) is not None
            and self.right_tabs.currentWidget() == self.operations_tab_widget
        )

    def _is_log_tab_active(self):
        return (
            hasattr(self, "right_tabs")
            and getattr(self, "log_tab_widget", None) is not None
            and self.right_tabs.currentWidget() == self.log_tab_widget
        )

    def _is_news_watch_tab_active(self):
        return (
            hasattr(self, "right_tabs")
            and getattr(self, "news_watch_tab_widget", None) is not None
            and self.right_tabs.currentWidget() == self.news_watch_tab_widget
        )

    def _is_realtime_reference_tab_active(self):
        return (
            hasattr(self, "right_tabs")
            and getattr(self, "realtime_reference_tab_widget", None) is not None
            and self.right_tabs.currentWidget() == self.realtime_reference_tab_widget
        )

    def _set_news_watch_loading(self, visible, message=None):
        label = (
            getattr(self, "news_watch_loading_label_actual", None)
            or getattr(self, "news_watch_loading_label", None)
            or getattr(self, "lbl_news_watch_loading", None)
        )
        if label is None:
            return
        if message is not None:
            label.setText(str(message or "뉴스감시 데이터 로딩 중..."))
        label.setVisible(bool(visible))
        QApplication.processEvents()

    def _schedule_refresh_news_watch(self, delay_ms=400):
        self._news_watch_refresh_pending = True
        if self._news_watch_refresh_running:
            return
        if not self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, "뉴스감시 데이터 준비 중...")
            return
        if self._refresh_news_watch_timer.isActive():
            return
        self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
        self._refresh_news_watch_timer.start(max(160, int(delay_ms or 400)))

    def _populate_news_watch_row(self, row_index, row):
        extra = {}
        try:
            extra = json.loads(row["extra_json"] or "{}")
        except Exception:
            extra = {}
        code = str(row["code"] or "")
        item_name = QTableWidgetItem(row["name"] or code)
        item_name.setData(Qt.UserRole, code)
        item_code = QTableWidgetItem(code)
        item_code.setData(Qt.UserRole, code)
        self.table_news_watch.setItem(row_index, 0, item_name)
        self.table_news_watch.setItem(row_index, 1, item_code)
        self.table_news_watch.setItem(row_index, 2, QTableWidgetItem(row["last_detected_at"] or ""))
        self.table_news_watch.setItem(row_index, 3, QTableWidgetItem(row["expire_at"] or ""))
        self.table_news_watch.setItem(row_index, 4, QTableWidgetItem(self._translate_watch_state(row["current_state"] or "")))
        self.table_news_watch.setItem(row_index, 5, QTableWidgetItem(self._translate_buy_block_reason(extra.get("buy_block_reason", ""))))
        self.table_news_watch.setItem(row_index, 6, QTableWidgetItem(row["last_news_checked_at"] or ""))
        self.table_news_watch.setItem(row_index, 7, QTableWidgetItem(row["last_important_news_at"] or ""))
        self.table_news_watch.setItem(row_index, 8, QTableWidgetItem(self._translate_watch_spam(row["is_spam"] or 0)))
        if self._news_watch_refresh_restore_code and code == self._news_watch_refresh_restore_code:
            self._news_watch_refresh_restore_row = row_index

    def _finalize_news_watch_refresh(self):
        try:
            if self._news_watch_refresh_restore_row >= 0:
                self.table_news_watch.selectRow(self._news_watch_refresh_restore_row)
        finally:
            self.table_news_watch.blockSignals(False)
            self.table_news_watch.setUpdatesEnabled(True)
            self._news_watch_refresh_running = False
        if not self._news_watch_rows_sized:
            self._news_watch_rows_sized = True
        self._schedule_refresh_realtime_strategy_reference_labels(80)
        if self._news_watch_refresh_pending and self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, "?댁뒪媛먯떆 ?곗씠??濡쒕뵫 以?..")
            QTimer.singleShot(80, lambda: self._schedule_refresh_news_watch(80))
            return
        self._set_news_watch_loading(False)

    def _pause_news_watch_refresh(self):
        if self._refresh_news_watch_timer.isActive():
            self._refresh_news_watch_timer.stop()
        if self._news_watch_refresh_batch_timer.isActive():
            self._news_watch_refresh_batch_timer.stop()
        if self._news_watch_refresh_running:
            self._news_watch_refresh_running = False
            self._news_watch_refresh_pending = True
            self.table_news_watch.blockSignals(False)
            self.table_news_watch.setUpdatesEnabled(True)
        self._set_news_watch_loading(False)

    def _process_news_watch_refresh_batch(self):
        if not self._news_watch_refresh_running:
            return
        if not self._is_news_watch_tab_active():
            self._pause_news_watch_refresh()
            return
        rows = list(self._news_watch_refresh_rows or [])
        total_count = len(rows)
        if total_count <= 0:
            self._finalize_news_watch_refresh()
            return
        start_index = int(self._news_watch_refresh_index or 0)
        end_index = min(start_index + int(self._news_watch_batch_size or 60), total_count)
        for row_index in range(start_index, end_index):
            self._populate_news_watch_row(row_index, rows[row_index])
        self._news_watch_refresh_index = end_index
        self._set_news_watch_loading(True, "?댁뒪媛먯떆 ?곗씠??濡쒕뵫 以?.. ({0}/{1})".format(end_index, total_count))
        if end_index < total_count:
            self._news_watch_refresh_batch_timer.start(1)
            return
        self._finalize_news_watch_refresh()

    def _schedule_refresh_realtime_strategy_reference_labels(self, *_args, **_kwargs):
        delay_ms = 180
        if _args:
            try:
                candidate = int(_args[0] or 0)
                if candidate > 0:
                    delay_ms = candidate
            except Exception:
                pass
        if self._refresh_realtime_reference_timer.isActive():
            return
        self._refresh_realtime_reference_timer.start(max(120, int(delay_ms or 180)))

    def _on_realtime_market_state_changed(self, payload=None):
        code = str(((payload or {}) if isinstance(payload, dict) else {}).get("code") or "").strip()
        selected_code, _selected_name = self._selected_watch_symbol()
        selected_code = str(selected_code or "").strip()
        if selected_code:
            if code and code != selected_code:
                return
            self._schedule_refresh_realtime_strategy_reference_labels(120)
            return
        self._schedule_refresh_realtime_strategy_reference_labels(220)

    def _poll_realtime_strategy_reference_labels(self):
        if not self.isVisible():
            return
        self._schedule_refresh_realtime_strategy_reference_labels(120)

    def _on_right_tab_changed(self, index):
        current_widget = self.right_tabs.widget(int(index)) if hasattr(self, "right_tabs") else None
        if current_widget == getattr(self, "news_watch_tab_widget", None):
            if self._news_watch_refresh_pending and not self._refresh_news_watch_timer.isActive():
                self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
                QTimer.singleShot(40, lambda: self._schedule_refresh_news_watch(120))
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        self._set_news_watch_loading(False)

    def _set_news_watch_loading(self, visible, message=None):
        label = (
            getattr(self, "news_watch_loading_label_actual", None)
            or getattr(self, "news_watch_loading_label", None)
            or getattr(self, "lbl_news_watch_loading", None)
        )
        if label is None:
            return
        label.setText(str(message or "뉴스감시 데이터 로딩 중..."))
        label.setVisible(bool(visible))
        QApplication.processEvents()

    def _schedule_refresh_news_watch(self, delay_ms=400):
        self._news_watch_refresh_pending = True
        if self._news_watch_refresh_running:
            return
        if not self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, "뉴스감시 데이터 준비 중...")
            return
        if self._refresh_news_watch_timer.isActive():
            return
        self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
        self._refresh_news_watch_timer.start(max(160, int(delay_ms or 400)))

    def _finalize_news_watch_refresh(self):
        try:
            if self._news_watch_refresh_restore_row >= 0:
                self.table_news_watch.selectRow(self._news_watch_refresh_restore_row)
        finally:
            self.table_news_watch.blockSignals(False)
            self.table_news_watch.setUpdatesEnabled(True)
            self._news_watch_refresh_running = False
        if not self._news_watch_rows_sized:
            self.table_news_watch.resizeRowsToContents()
            self._news_watch_rows_sized = True
        self._schedule_refresh_realtime_strategy_reference_labels(80)
        if self._news_watch_refresh_pending and self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
            QTimer.singleShot(80, lambda: self._schedule_refresh_news_watch(80))
            return
        self._set_news_watch_loading(False)

    def _process_news_watch_refresh_batch(self):
        if not self._news_watch_refresh_running:
            return
        rows = list(self._news_watch_refresh_rows or [])
        total_count = len(rows)
        if total_count <= 0:
            self._finalize_news_watch_refresh()
            return
        start_index = int(self._news_watch_refresh_index or 0)
        end_index = min(start_index + int(self._news_watch_batch_size or 60), total_count)
        for row_index in range(start_index, end_index):
            self._populate_news_watch_row(row_index, rows[row_index])
        self._news_watch_refresh_index = end_index
        self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중... ({0}/{1})".format(end_index, total_count))
        if end_index < total_count:
            self._news_watch_refresh_batch_timer.start(1)
            return
        self._finalize_news_watch_refresh()

    def _on_realtime_market_state_changed(self, payload=None):
        self._record_realtime_reference_snapshot(payload)
        code = str(((payload or {}) if isinstance(payload, dict) else {}).get("code") or "").strip()
        selected_code, _selected_name = self._selected_watch_symbol()
        selected_code = str(selected_code or "").strip()
        if selected_code:
            if code and code != selected_code:
                return
            self._schedule_refresh_realtime_strategy_reference_labels(120)
            return
        self._schedule_refresh_realtime_strategy_reference_labels(220)

    def _on_right_tab_changed(self, index):
        current_widget = self.right_tabs.widget(int(index)) if hasattr(self, "right_tabs") else None
        if current_widget == getattr(self, "realtime_reference_tab_widget", None):
            self._refresh_realtime_reference_table()
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        if current_widget == getattr(self, "news_watch_tab_widget", None):
            if self._news_watch_refresh_pending and not self._refresh_news_watch_timer.isActive():
                self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
                QTimer.singleShot(40, lambda: self._schedule_refresh_news_watch(120))
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        self._set_news_watch_loading(False)

    def _set_news_watch_loading(self, visible, message=None):
        label = (
            getattr(self, "news_watch_loading_label_actual", None)
            or getattr(self, "news_watch_loading_label", None)
            or getattr(self, "lbl_news_watch_loading", None)
        )
        if label is None:
            return
        label.setText(str(message or "뉴스감시 데이터 로딩 중..."))
        label.setVisible(bool(visible))
        QApplication.processEvents()

    def _schedule_refresh_news_watch(self, delay_ms=400):
        self._news_watch_refresh_pending = True
        if self._news_watch_refresh_running:
            return
        if not self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, "뉴스감시 데이터 준비 중...")
            return
        if self._refresh_news_watch_timer.isActive():
            return
        self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
        self._refresh_news_watch_timer.start(max(160, int(delay_ms or 400)))

    def _finalize_news_watch_refresh(self):
        try:
            if self._news_watch_refresh_restore_row >= 0:
                self.table_news_watch.selectRow(self._news_watch_refresh_restore_row)
        finally:
            self.table_news_watch.blockSignals(False)
            self.table_news_watch.setUpdatesEnabled(True)
            self._news_watch_refresh_running = False
        if not self._news_watch_rows_sized:
            self.table_news_watch.resizeRowsToContents()
            self._news_watch_rows_sized = True
        self._schedule_refresh_realtime_strategy_reference_labels(80)
        if self._news_watch_refresh_pending and self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
            QTimer.singleShot(80, lambda: self._schedule_refresh_news_watch(80))
            return
        self._set_news_watch_loading(False)

    def _process_news_watch_refresh_batch(self):
        if not self._news_watch_refresh_running:
            return
        rows = list(self._news_watch_refresh_rows or [])
        total_count = len(rows)
        if total_count <= 0:
            self._finalize_news_watch_refresh()
            return
        start_index = int(self._news_watch_refresh_index or 0)
        end_index = min(start_index + int(self._news_watch_batch_size or 60), total_count)
        for row_index in range(start_index, end_index):
            self._populate_news_watch_row(row_index, rows[row_index])
        self._news_watch_refresh_index = end_index
        self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중... ({0}/{1})".format(end_index, total_count))
        if end_index < total_count:
            self._news_watch_refresh_batch_timer.start(1)
            return
        self._finalize_news_watch_refresh()

    def _on_realtime_market_state_changed(self, payload=None):
        self._record_realtime_reference_snapshot(payload)
        code = str(((payload or {}) if isinstance(payload, dict) else {}).get("code") or "").strip()
        selected_code, _selected_name = self._selected_watch_symbol()
        selected_code = str(selected_code or "").strip()
        if selected_code:
            if code and code != selected_code:
                return
            self._schedule_refresh_realtime_strategy_reference_labels(120)
            return
        self._schedule_refresh_realtime_strategy_reference_labels(220)

    def _on_right_tab_changed(self, index):
        current_widget = self.right_tabs.widget(int(index)) if hasattr(self, "right_tabs") else None
        if current_widget == getattr(self, "realtime_reference_tab_widget", None):
            self._refresh_realtime_reference_table()
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        if current_widget == getattr(self, "news_watch_tab_widget", None):
            if self._news_watch_refresh_pending and not self._refresh_news_watch_timer.isActive():
                self._set_news_watch_loading(True, "뉴스감시 데이터 로딩 중...")
                QTimer.singleShot(40, lambda: self._schedule_refresh_news_watch(120))
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        self._set_news_watch_loading(False)

    def _schedule_news_watch_after_hours_fill(self, code, delay_ms=220):
        code = str(code or "").strip()
        self._pending_news_watch_fill_code = code
        if not code:
            self._news_watch_fill_timer.stop()
            return
        self._news_watch_fill_timer.start(max(120, int(delay_ms or 220)))

    def _run_news_watch_after_hours_fill(self):
        code = str(self._pending_news_watch_fill_code or "").strip()
        self._pending_news_watch_fill_code = ""
        if not code or self.daily_watch_snapshot_manager is None:
            return
        if self._is_regular_market_hours():
            return
        try:
            self._ensure_news_watch_daily_snapshot(code, request_on_missing=True)
        except Exception as exc:
            self.append_log("?좑툘 ?댁뒪媛먯떆 ?대┃ 蹂닿컯 ?ㅽ뙣: {0} / {1}".format(code, exc))
        self._schedule_refresh_realtime_strategy_reference_labels(80)

    def _schedule_refresh_policy_logs(self, delay_ms=350):
        self._policy_logs_refresh_pending = True
        if not self._is_log_tab_active():
            return
        self._refresh_policy_logs_timer.start(max(50, int(delay_ms or 350)))

    def _get_periodic_recheck_limit(self):
        now_dt = datetime.datetime.now()
        hhmm = now_dt.strftime("%H%M")
        if now_dt.weekday() < 5 and "0900" <= hhmm <= "1530":
            return 3
        return 10

    def _on_accounts_state_changed(self):
        self._reload_account_table()
        self._schedule_user_profile_save()

    def _on_catalog_changed(self):
        self.refresh_condition_catalog()
        self._profile_conditions_ready = bool(self.condition_manager.get_catalog())
        if self._startup_bootstrap_active:
            self._set_startup_step("conditions", "done", "조건검색식을 불러왔습니다.")
        self._try_restore_user_profile()

    def _on_slots_changed(self):
        self.refresh_slots()
        self.refresh_condition_catalog()
        self._refresh_strategy_policy_ui()
        self._schedule_user_profile_save()

    def _on_strategies_changed(self):
        self.refresh_buy_chain()
        self.refresh_sell_chain()
        self._populate_strategy_catalog_table("buy", self.table_buy_strategy_catalog)
        self._populate_strategy_catalog_table("sell", self.table_sell_strategy_catalog)
        self._refresh_strategy_policy_ui()
        self._schedule_user_profile_save()

    def _schedule_user_profile_save(self):
        if not self.current_user_id or self._restoring_user_profile:
            return
        self._profile_save_timer.start(600)

    def _profile_row(self, user_id):
        row = self.persistence.fetchone("SELECT profile_json FROM user_runtime_profiles WHERE user_id=?", (user_id,))
        if not row or not row["profile_json"]:
            return None
        try:
            return json.loads(row["profile_json"] or '{}')
        except Exception:
            return None

    def _current_advanced_settings(self):
        return {
            "execution_mode": self.cbo_execution_mode.currentData() or "live",
            "query_password_mode": self.cbo_global_pw_mode.currentData() or "api_saved",
            "query_password": self.edt_global_query_pw.text().strip(),
            "unfilled_policy": self.cbo_unfilled_policy.currentData() or "reprice_then_market",
            "first_wait_sec": int(self.spin_first_wait.value() or 5),
            "second_wait_sec": int(self.spin_second_wait.value() or 5),
        }

    def _current_credential_profile(self):
        return {
            "naver_keys": [
                {
                    "key_set_id": idx,
                    "client_id": widgets[0].text().strip(),
                    "client_secret": widgets[1].text().strip(),
                    "enabled": bool(widgets[2].isChecked()),
                }
                for idx, widgets in enumerate(self.naver_rows, 1)
            ],
            "dart_api": {
                "api_key": self.edt_dart_api_key.text().strip(),
                "enabled": bool(self.chk_dart_api_enabled.isChecked()),
            },
            "telegram": {
                "news_send_min_score": int(self.spin_news_send_min_score.value() or 60),
                "news": [
                    {
                        "slot_no": idx,
                        "bot_token": widgets[0].text().strip(),
                        "chat_id": widgets[1].text().strip(),
                        "enabled": bool(widgets[2].isChecked()),
                    }
                    for idx, widgets in enumerate(self.news_telegram_rows, 1)
                ],
                "trade": [
                    {
                        "slot_no": idx,
                        "bot_token": widgets[0].text().strip(),
                        "chat_id": widgets[1].text().strip(),
                        "enabled": bool(widgets[2].isChecked()),
                    }
                    for idx, widgets in enumerate(self.trade_telegram_rows, 1)
                ],
            },
            "ai_apis": [
                {
                    "slot_no": idx,
                    "provider": widgets[1].currentData() or "openai",
                    "api_key": widgets[2].text().strip(),
                    "base_url": widgets[3].text().strip(),
                    "model_name": widgets[4].text().strip(),
                    "analysis_label": widgets[5].text().strip(),
                    "enabled": bool(widgets[0].isChecked()),
                }
                for idx, widgets in enumerate(self.ai_api_rows, 1)
            ],
        }

    def _clear_credential_profile(self):
        for idx, widgets in enumerate(self.naver_rows, 1):
            widgets[0].clear()
            widgets[1].clear()
            widgets[2].setChecked(False)
            self._update_naver_row_detected_status(idx)
        self.edt_dart_api_key.clear()
        self.chk_dart_api_enabled.setChecked(False)
        self._update_dart_api_detected_status()
        self.spin_news_send_min_score.setValue(60)
        for group_name, rows in [("news", self.news_telegram_rows), ("trade", self.trade_telegram_rows)]:
            for idx, widgets in enumerate(rows, 1):
                widgets[0].clear()
                widgets[1].clear()
                widgets[2].setChecked(False)
                self._update_telegram_row_detected_status(group_name, idx)
        for idx, widgets in enumerate(self.ai_api_rows, 1):
            widgets[0].setChecked(False)
            widgets[1].setCurrentIndex(0)
            widgets[2].clear()
            widgets[3].clear()
            widgets[4].clear()
            widgets[5].clear()
            self._update_ai_row_status(idx)
        self._refresh_credential_status_summaries()
        for idx in range(1, len(self.naver_rows) + 1):
            self.credential_manager.set_naver_key(idx, "", "", False)
        self.credential_manager.set_dart_api("", False)
        self.credential_manager.set_news_send_min_score(60)
        for channel_group, rows in [("news", self.news_telegram_rows), ("trade", self.trade_telegram_rows)]:
            for idx, _widgets in enumerate(rows, 1):
                self.credential_manager.set_telegram_channel(channel_group, idx, "", "", False)
        for idx in range(1, len(self.ai_api_rows) + 1):
            self.credential_manager.set_ai_api(idx, "openai", "", "", "", "", False)

    def _apply_credential_profile(self, profile):
        if not profile:
            self._clear_credential_profile()
            return
        naver_map = {}
        for row in list(profile.get("naver_keys") or []):
            try:
                naver_map[int(row.get("key_set_id", 0))] = row
            except Exception:
                pass
        for idx, widgets in enumerate(self.naver_rows, 1):
            data = naver_map.get(idx, {})
            client_id = str(data.get("client_id", "") or "")
            client_secret = str(data.get("client_secret", "") or "")
            enabled = bool(data.get("enabled", False))
            widgets[0].setText(client_id)
            widgets[1].setText(client_secret)
            widgets[2].setChecked(enabled)
            self.credential_manager.set_naver_key(idx, client_id, client_secret, enabled)

        dart_row = dict(profile.get("dart_api") or {})
        dart_api_key = str(dart_row.get("api_key", "") or "")
        dart_enabled = bool(dart_row.get("enabled", False))
        self.edt_dart_api_key.setText(dart_api_key)
        self.chk_dart_api_enabled.setChecked(dart_enabled)
        self.credential_manager.set_dart_api(dart_api_key, dart_enabled)
        self._update_dart_api_detected_status()

        tg_root = dict(profile.get("telegram") or {})
        self.spin_news_send_min_score.setValue(int(tg_root.get("news_send_min_score", 60) or 60))
        self.credential_manager.set_news_send_min_score(int(tg_root.get("news_send_min_score", 60) or 60))
        for channel_group, rows in [("news", self.news_telegram_rows), ("trade", self.trade_telegram_rows)]:
            group_map = {}
            for row in list(tg_root.get(channel_group) or []):
                try:
                    group_map[int(row.get("slot_no", 0))] = row
                except Exception:
                    pass
            for idx, widgets in enumerate(rows, 1):
                data = group_map.get(idx, {})
                bot_token = str(data.get("bot_token", "") or "")
                chat_id = str(data.get("chat_id", "") or "")
                enabled = bool(data.get("enabled", False))
                widgets[0].setText(bot_token)
                widgets[1].setText(chat_id)
                widgets[2].setChecked(enabled)
                self.credential_manager.set_telegram_channel(channel_group, idx, bot_token, chat_id, enabled)

        ai_map = {}
        for row in list(profile.get("ai_apis") or []):
            try:
                ai_map[int(row.get("slot_no", 0))] = row
            except Exception:
                pass
        for idx, widgets in enumerate(self.ai_api_rows, 1):
            data = ai_map.get(idx, {})
            provider = str(data.get("provider", "openai") or "openai")
            api_key = str(data.get("api_key", "") or "")
            base_url = str(data.get("base_url", "") or "")
            model_name = str(data.get("model_name", "") or "")
            analysis_label = str(data.get("analysis_label", "") or "")
            enabled = bool(data.get("enabled", False))
            provider_idx = widgets[1].findData(provider)
            widgets[1].setCurrentIndex(provider_idx if provider_idx >= 0 else 0)
            widgets[0].setChecked(enabled)
            widgets[2].setText(api_key)
            widgets[3].setText(base_url)
            widgets[4].setText(model_name)
            widgets[5].setText(analysis_label)
            self.credential_manager.set_ai_api(idx, provider, api_key, base_url, model_name, analysis_label, enabled)
            self._update_ai_row_status(idx)

    def _save_current_user_profile(self):
        if not self.current_user_id or self._restoring_user_profile:
            return
        profile = {
            "user_id": self.current_user_id,
            "saved_at": self.persistence.now_ts(),
            "advanced_settings": self._current_advanced_settings(),
            "credentials": self._current_credential_profile(),
            "accounts": self.account_manager.export_account_profile(),
            "slots": self.condition_manager.export_slot_profile(),
            "buy_chain": self.strategy_manager.export_chain_profile("buy"),
            "sell_chain": self.strategy_manager.export_chain_profile("sell"),
        }
        self.persistence.execute(
            """
            INSERT INTO user_runtime_profiles (user_id, profile_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                profile_json=excluded.profile_json,
                updated_at=excluded.updated_at
            """,
            (self.current_user_id, json.dumps(profile, ensure_ascii=False), self.persistence.now_ts()),
        )

    def _apply_default_profile_state(self):
        self._restoring_user_profile = True
        try:
            self.strategy_manager.reset_to_defaults()
            self.condition_manager.import_slot_profile([])
            self.account_manager.apply_account_profile([], emit_signal=True)
            self._clear_credential_profile()
            self.cbo_execution_mode.setCurrentIndex(max(0, self.cbo_execution_mode.findData("live")))
            self.order_manager.set_execution_mode("live")
            self.cbo_global_pw_mode.setCurrentIndex(max(0, self.cbo_global_pw_mode.findData("api_saved")))
            self.edt_global_query_pw.clear()
            self.cbo_unfilled_policy.setCurrentIndex(max(0, self.cbo_unfilled_policy.findData("reprice_then_market")))
            self.spin_first_wait.setValue(5)
            self.spin_second_wait.setValue(5)
            self._on_global_pw_mode_changed()
        finally:
            self._restoring_user_profile = False
        self.append_log("🆕 저장된 사용자 설정이 없어 기본 설정으로 시작합니다: {0}".format(self.current_user_id))
        self._schedule_credential_verification()

    def _apply_profile(self, profile):
        advanced = dict(profile.get("advanced_settings") or {})
        self._restoring_user_profile = True
        try:
            execution_mode = str(advanced.get("execution_mode", "live") or "live")
            idx = self.cbo_execution_mode.findData(execution_mode)
            self.cbo_execution_mode.setCurrentIndex(idx if idx >= 0 else 0)
            self.order_manager.set_execution_mode(self.cbo_execution_mode.currentData() or "live")

            query_mode = str(advanced.get("query_password_mode", "api_saved") or "api_saved")
            idx = self.cbo_global_pw_mode.findData(query_mode)
            self.cbo_global_pw_mode.setCurrentIndex(idx if idx >= 0 else 0)
            self.edt_global_query_pw.setText(str(advanced.get("query_password", "") or ""))
            policy = str(advanced.get("unfilled_policy", "reprice_then_market") or "reprice_then_market")
            idx = self.cbo_unfilled_policy.findData(policy)
            self.cbo_unfilled_policy.setCurrentIndex(idx if idx >= 0 else 0)
            self.spin_first_wait.setValue(int(advanced.get("first_wait_sec", 5) or 5))
            self.spin_second_wait.setValue(int(advanced.get("second_wait_sec", 5) or 5))
            self._on_global_pw_mode_changed()

            self._apply_credential_profile(profile.get("credentials") or {})
            self.strategy_manager.import_chain_profile("buy", profile.get("buy_chain") or [])
            self.strategy_manager.import_chain_profile("sell", profile.get("sell_chain") or [])
            self.account_manager.apply_account_profile(profile.get("accounts") or [], emit_signal=True)
            self.condition_manager.import_slot_profile(profile.get("slots") or [])
        finally:
            self._restoring_user_profile = False
        self.append_log("📂 사용자 설정 복원 완료: {0}".format(self.current_user_id))
        self._schedule_credential_verification()
        if self.account_manager.get_accounts():
            self._sync_active_accounts(startup_sync=self._startup_bootstrap_active)
        elif self._startup_bootstrap_active:
            self._set_startup_step("account_sync", "skipped", "동기화할 계좌가 없습니다.")
            self._finish_startup_loading()

    def _try_restore_user_profile(self):
        if not self._pending_profile_user_id:
            return
        if not (self._profile_accounts_ready and self._profile_conditions_ready):
            return
        user_id = self._pending_profile_user_id
        self.current_user_id = user_id
        self._pending_profile_user_id = ""
        profile = self._profile_row(user_id)
        if profile:
            self._apply_profile(profile)
        else:
            self._apply_default_profile_state()
        self.refresh_condition_catalog()
        self.refresh_slots()
        self.refresh_buy_chain()
        self.refresh_sell_chain()
        self.refresh_operations()
        self._schedule_refresh_news_watch(80)
        self.refresh_spam_table()
        self.refresh_policy_logs()
        if self._startup_bootstrap_active:
            self._set_startup_step("profile", "done", "사용자 설정 복원을 마쳤습니다.")
            if (
                self._startup_bootstrap_steps.get("account_sync", {}).get("status") == "pending"
                and not self.account_manager.get_accounts()
            ):
                self._set_startup_step("account_sync", "skipped", "동기화할 계좌가 없습니다.")
                self._finish_startup_loading()

    def _on_connection_changed(self, connected, message):
        self.lbl_connect.setText(self._format_connection_status(connected, message))
        self.lbl_connect.setStyleSheet("color: #0a7a28; font-weight: 700;" if connected else "color: #b00020; font-weight: 700;")
        self.btn_disconnect.setEnabled(bool(connected))
        self.edt_user_id.setText(self._masked_user_id(self.kiwoom_client.user_id))
        self.edt_user_name.setText(self.kiwoom_client.user_name)
        self.edt_server.setText(self.kiwoom_client.server_gubun)
        self._reload_account_table()
        if connected:
            self._manual_api_disconnect = False
            self._connection_watchdog_prev_connected = True
            self._auto_restart_in_progress = False
            self._reset_auto_reconnect()
            if self._maintenance_retry_timer.isActive():
                self._maintenance_retry_timer.stop()
            self._pending_profile_user_id = self.kiwoom_client.user_id or ""
            self._profile_accounts_ready = False
            self._profile_conditions_ready = False
            if self._startup_bootstrap_active:
                self._set_startup_step("login", "done", "로그인에 성공했습니다.")
                self._set_startup_step("conditions", "in_progress", "조건검색식을 불러오는 중입니다.")
                self._set_startup_step("profile", "in_progress", "사용자 설정 복원을 준비하는 중입니다.")
                self._set_startup_loading_message("로그인 이후 필수 시작 작업을 진행 중입니다.")
            ok = self.condition_manager.request_load_conditions()
            if not ok:
                self._profile_conditions_ready = True
                if self._startup_bootstrap_active:
                    self._set_startup_step("conditions", "skipped", "조건검색식 로드를 건너뛰었습니다.")
                self._try_restore_user_profile()
        elif self._startup_bootstrap_active:
            self._set_startup_step("login", "failed", str(message or "로그인에 실패했습니다."))
            self._finish_startup_loading()

    def _on_accounts_loaded(self, account_numbers):
        self.account_manager.sync_accounts(account_numbers)
        self._load_account_advanced_settings()
        self._profile_accounts_ready = True
        if self._startup_bootstrap_active:
            self._set_startup_loading_message("계좌 목록과 사용자 설정을 불러오는 중입니다.")
        self._try_restore_user_profile()

    def _on_account_sync_finished(self, _payload):
        if not self._startup_bootstrap_active:
            return
        self._set_startup_step("account_sync", "done", "계좌 동기화를 마쳤습니다.")
        self._finish_startup_loading()

    def _on_account_sync_finished(self, _payload):
        if not self._startup_bootstrap_active:
            return
        self._set_startup_step("account_sync", "done", "계좌 동기화를 마쳤고 후속 시작 작업을 정리 중입니다.")
        self._schedule_deferred_account_sync()
        snapshot_count, detection_count = self._get_startup_warmup_counts()
        if snapshot_count > 0 or detection_count > 0:
            self.append_log(
                "⏳ 시작 후 후속작업을 백그라운드로 이어갑니다: 기준치 {0}건 / 파이프라인 {1}건".format(
                    int(snapshot_count or 0),
                    int(detection_count or 0),
                )
            )
        self._finish_startup_loading()

    def _on_execution_mode_changed(self):
        self.order_manager.set_execution_mode(self.cbo_execution_mode.currentData())
        self._update_execution_mode_visual()
        self._schedule_user_profile_save()

    def get_trade_enabled(self):
        return str(self.cbo_execution_mode.currentData() or "live") == "live"

    def set_trade_enabled(self, enabled):
        target_mode = "live" if bool(enabled) else "simulated"
        index = self.cbo_execution_mode.findData(target_mode)
        if index >= 0 and self.cbo_execution_mode.currentIndex() != index:
            self.cbo_execution_mode.setCurrentIndex(index)
            return
        self.order_manager.set_execution_mode(target_mode)
        self._update_execution_mode_visual()
        self._schedule_user_profile_save()

    def stop_all_condition_realtime(self):
        try:
            for row in list(self.condition_manager.get_slots() or []):
                if int(row["is_realtime"] or 0):
                    self.condition_manager.stop_realtime_slot(int(row["slot_no"] or 0))
        except Exception:
            pass
        self.refresh_slots()

    def resume_enabled_condition_realtime(self):
        try:
            for row in list(self.condition_manager.get_slots() or []):
                if int(row["is_enabled"] or 0):
                    self.condition_manager.start_realtime_slot(int(row["slot_no"] or 0))
        except Exception:
            pass
        self.refresh_slots()

    def _on_global_pw_mode_changed(self):
        self.edt_global_query_pw.setEnabled(self.cbo_global_pw_mode.currentData() == "program_input")
        self._schedule_user_profile_save()

    def _update_execution_mode_visual(self):
        mode = self.cbo_execution_mode.currentData() or "live"
        if mode == "live":
            self.cbo_execution_mode.setStyleSheet(
                "QComboBox { background-color: #ffe8e8; color: #b00020; font-weight: 700; border: 1px solid #b00020; padding: 2px 8px; }"
            )
            self.cbo_execution_mode.setToolTip("Trade ON: 서버로 주문을 전송합니다.")
        else:
            self.cbo_execution_mode.setStyleSheet(
                "QComboBox { background-color: #e8f1ff; color: #0b57d0; font-weight: 700; border: 1px solid #0b57d0; padding: 2px 8px; }"
            )
            self.cbo_execution_mode.setToolTip("Trade OFF: 서버 주문 없이 프로그램 내부에서만 처리합니다.")

    def _configure_budget_spin(self, combo, spin):
        mode = combo.currentData()
        spin.blockSignals(True)
        spin.setLocale(QLocale(QLocale.Korean, QLocale.SouthKorea))
        if hasattr(spin, "setGroupSeparatorShown"):
            spin.setGroupSeparatorShown(True)
        current_value = spin.value()
        if mode == "cash_ratio":
            spin.setDecimals(2)
            spin.setMinimum(1.0)
            spin.setMaximum(100.0)
            spin.setSingleStep(1.0)
            spin.setSuffix(" %")
            spin.setValue(min(max(current_value if current_value > 0 else 10.0, 1.0), 100.0))
        else:
            spin.setDecimals(0)
            spin.setMinimum(10000.0)
            spin.setMaximum(1000000000.0)
            spin.setSingleStep(10000.0)
            spin.setSuffix(" 원")
            spin.setValue(max(current_value if current_value > 0 else 300000.0, 10000.0))
        spin.blockSignals(False)

    def _masked_account_tail(self, account_no):
        account_no = str(account_no or "")
        return account_no[-6:] if len(account_no) >= 6 else account_no

    def _masked_user_id(self, user_id):
        user_id = str(user_id or "").strip()
        if not user_id:
            return ""
        if len(user_id) <= 4:
            return user_id
        return ("*" * (len(user_id) - 4)) + user_id[-4:]

    def _format_connection_status(self, connected, message):
        message = str(message or "").strip()
        if connected:
            return "API ON" if not message else "API ON / {0}".format(message)
        return "API OFF" if not message else "API OFF / {0}".format(message)

    def _reload_account_table(self):
        rows = self.account_manager.get_accounts()
        self._reloading_account_table = True
        try:
            self.account_table.clearContents()
            self.account_table.setRowCount(len(rows))
            for row_index, row in enumerate(rows):
                settings = row.get("settings", {})
                opt_use = QRadioButton()
                opt_use.setAutoExclusive(False)
                opt_use.setChecked(bool(row.get("is_enabled")))

                item_account = QTableWidgetItem(self._masked_account_tail(row["account_no"]))
                item_account.setData(Qt.UserRole, row["account_no"])

                cbo_hoga = QComboBox()
                cbo_hoga.addItem("시장가", "03")
                cbo_hoga.addItem("지정가", "00")
                idx = cbo_hoga.findData(str(settings.get("hoga_gb", "03") or "03"))
                cbo_hoga.setCurrentIndex(idx if idx >= 0 else 0)

                cbo_budget_mode = QComboBox()
                cbo_budget_mode.addItem("주문금액", "fixed_amount")
                cbo_budget_mode.addItem("예수금비중", "cash_ratio")
                mode_idx = cbo_budget_mode.findData(str(settings.get("order_budget_mode", "fixed_amount") or "fixed_amount"))
                cbo_budget_mode.setCurrentIndex(mode_idx if mode_idx >= 0 else 0)

                spin_budget = QDoubleSpinBox()
                self._configure_budget_spin(cbo_budget_mode, spin_budget)
                spin_budget.setValue(float(settings.get("order_budget_value", 300000.0) or 300000.0))

                cbo_limit_option = QComboBox()
                cbo_limit_option.addItem("현재가", "current_price")
                cbo_limit_option.addItem("매도1호가", "ask1")
                cbo_limit_option.addItem("현재가+1틱", "current_plus_1tick")
                limit_idx = cbo_limit_option.findData(str(settings.get("limit_price_option", "current_price") or "current_price"))
                cbo_limit_option.setCurrentIndex(limit_idx if limit_idx >= 0 else 0)
                cbo_limit_option.setEnabled(cbo_hoga.currentData() == "00")

                self.account_table.setCellWidget(row_index, 0, opt_use)
                self.account_table.setItem(row_index, 1, item_account)
                self.account_table.setCellWidget(row_index, 2, cbo_hoga)
                self.account_table.setCellWidget(row_index, 3, cbo_budget_mode)
                self.account_table.setCellWidget(row_index, 4, spin_budget)
                self.account_table.setCellWidget(row_index, 5, cbo_limit_option)

                cbo_hoga.currentIndexChanged.connect(
                    lambda _=0, acc=row["account_no"], use=opt_use, hoga=cbo_hoga, combo=cbo_budget_mode, spin=spin_budget, limit_opt=cbo_limit_option: self._on_account_hoga_changed(acc, use, hoga, combo, spin, limit_opt)
                )
                cbo_budget_mode.currentIndexChanged.connect(
                    lambda _=0, acc=row["account_no"], use=opt_use, hoga=cbo_hoga, combo=cbo_budget_mode, spin=spin_budget, limit_opt=cbo_limit_option: (
                        self._configure_budget_spin(combo, spin),
                        self._save_account_row_widgets(acc, use, hoga, combo, spin, limit_opt)
                    )
                )
                spin_budget.valueChanged.connect(
                    lambda _=0.0, acc=row["account_no"], use=opt_use, hoga=cbo_hoga, combo=cbo_budget_mode, spin=spin_budget, limit_opt=cbo_limit_option: self._save_account_row_widgets(acc, use, hoga, combo, spin, limit_opt)
                )
                cbo_limit_option.currentIndexChanged.connect(
                    lambda _=0, acc=row["account_no"], use=opt_use, hoga=cbo_hoga, combo=cbo_budget_mode, spin=spin_budget, limit_opt=cbo_limit_option: self._save_account_row_widgets(acc, use, hoga, combo, spin, limit_opt)
                )
                opt_use.toggled.connect(
                    lambda _checked=False, acc=row["account_no"], use=opt_use, hoga=cbo_hoga, combo=cbo_budget_mode, spin=spin_budget, limit_opt=cbo_limit_option: self._save_account_row_widgets(acc, use, hoga, combo, spin, limit_opt)
                )
        finally:
            self._reloading_account_table = False

    def _on_account_hoga_changed(self, account_no, opt_use, cbo_hoga, cbo_budget_mode, spin_budget, cbo_limit_option):
        cbo_limit_option.setEnabled(cbo_hoga.currentData() == "00")
        self._save_account_row_widgets(account_no, opt_use, cbo_hoga, cbo_budget_mode, spin_budget, cbo_limit_option)

    def _save_account_row_widgets(self, account_no, opt_use, cbo_hoga, cbo_budget_mode, spin_budget, cbo_limit_option):
        if self._reloading_account_table:
            return
        self.account_manager.set_account_live_settings(
            account_no,
            order_budget_mode=cbo_budget_mode.currentData() if cbo_budget_mode else "fixed_amount",
            order_budget_value=spin_budget.value() if spin_budget else 300000.0,
            hoga_gb=cbo_hoga.currentData() if cbo_hoga else "03",
            limit_price_option=cbo_limit_option.currentData() if cbo_limit_option else "current_price",
            emit_signal=False,
        )
        active_accounts = []
        for row in range(self.account_table.rowCount()):
            use_widget = self.account_table.cellWidget(row, 0)
            item = self.account_table.item(row, 1)
            if use_widget and use_widget.isChecked() and item:
                active_accounts.append(item.data(Qt.UserRole) or item.text())
        self.account_manager.set_active_accounts(active_accounts)
        self.refresh_operations()

    def _load_account_advanced_settings(self):
        rows = self.account_manager.get_accounts()
        first_settings = rows[0].get("settings", {}) if rows else {}
        mode_idx = self.cbo_execution_mode.findData(self.order_manager.execution_mode)
        self.cbo_execution_mode.setCurrentIndex(mode_idx if mode_idx >= 0 else 0)
        pw_mode = str(first_settings.get("query_password_mode", "api_saved") or "api_saved")
        pw_idx = self.cbo_global_pw_mode.findData(pw_mode)
        self.cbo_global_pw_mode.setCurrentIndex(pw_idx if pw_idx >= 0 else 0)
        self.edt_global_query_pw.setText(first_settings.get("query_password", ""))
        unfilled_policy = str(first_settings.get("unfilled_policy", "reprice_then_market") or "reprice_then_market")
        policy_idx = self.cbo_unfilled_policy.findData(unfilled_policy)
        self.cbo_unfilled_policy.setCurrentIndex(policy_idx if policy_idx >= 0 else 0)
        self.spin_first_wait.setValue(int(first_settings.get("first_wait_sec", 5) or 5))
        self.spin_second_wait.setValue(int(first_settings.get("second_wait_sec", 5) or 5))
        self._update_execution_mode_visual()
        self._on_global_pw_mode_changed()

    def _save_account_advanced_settings(self):
        exec_mode = self.cbo_execution_mode.currentData() or "live"
        self.order_manager.set_execution_mode(exec_mode)
        query_mode = self.cbo_global_pw_mode.currentData() or "api_saved"
        query_pw = self.edt_global_query_pw.text().strip() if query_mode == "program_input" else ""
        unfilled_policy = self.cbo_unfilled_policy.currentData() or "reprice_then_market"
        first_wait_sec = int(self.spin_first_wait.value() or 5)
        second_wait_sec = int(self.spin_second_wait.value() or 5)
        for row in self.account_manager.get_accounts():
            self.account_manager.set_account_live_settings(
                row["account_no"],
                unfilled_policy=unfilled_policy,
                first_wait_sec=first_wait_sec,
                second_wait_sec=second_wait_sec,
                query_password_mode=query_mode,
                query_password=query_pw,
                emit_signal=False,
            )
        self.account_manager.accounts_changed.emit()
        self.append_log("💾 주문/조회 고급 설정 저장 완료")
        if self.account_manager.get_accounts():
            self._sync_active_accounts()

    def _restore_runtime(self):
        self.recovery_manager.restore_runtime_snapshot()
        self._reload_account_table()
        self._sync_active_accounts()

    def _update_naver_row_detected_status(self, row_no):
        edt_id, edt_secret, _chk, _btn_save, lbl_status = self.naver_rows[row_no - 1]
        has_id = bool(edt_id.text().strip())
        has_secret = bool(edt_secret.text().strip())
        if has_id and has_secret:
            lbl_status.setText("키 감지됨")
        elif has_id or has_secret:
            lbl_status.setText("일부만 입력됨")
        else:
            lbl_status.setText("키 미입력")

    def _update_dart_api_detected_status(self):
        has_key = bool(self.edt_dart_api_key.text().strip())
        enabled = bool(self.chk_dart_api_enabled.isChecked())
        if has_key and enabled:
            self.lbl_dart_api_status.setText("키 감지 / 사용")
        elif has_key:
            self.lbl_dart_api_status.setText("키 감지 / 미사용")
        else:
            self.lbl_dart_api_status.setText("키 미입력")

    def _update_telegram_row_detected_status(self, channel_group, row_no):
        rows = self.news_telegram_rows if channel_group == "news" else self.trade_telegram_rows
        edt_token, edt_chat, _chk, _btn_save, lbl_status = rows[row_no - 1]
        has_token = bool(edt_token.text().strip())
        has_chat = bool(edt_chat.text().strip())
        if has_token and has_chat:
            lbl_status.setText("토큰/채팅방 감지됨")
        elif has_token:
            lbl_status.setText("토큰 감지 / Chat ID 미입력")
        elif has_chat:
            lbl_status.setText("Chat ID만 입력됨")
        else:
            lbl_status.setText("토큰 미입력")

    def _refresh_credential_status_summaries(self):
        naver_detected = 0
        for row in self.naver_rows:
            if row[0].text().strip() and row[1].text().strip():
                naver_detected += 1
        self.lbl_naver_summary.setText("상태: 키 감지 {0}/6".format(naver_detected))

        dart_has_key = bool(self.edt_dart_api_key.text().strip())
        dart_enabled = bool(self.chk_dart_api_enabled.isChecked())
        if dart_has_key and dart_enabled:
            self.lbl_dart_api_summary.setText("상태: DART API 사용")
        elif dart_has_key:
            self.lbl_dart_api_summary.setText("상태: DART API 저장됨")
        else:
            self.lbl_dart_api_summary.setText("상태: DART API 미입력")

        for group_name, rows, label in [
            ("news", self.news_telegram_rows, self.lbl_news_telegram_summary),
            ("trade", self.trade_telegram_rows, self.lbl_trade_telegram_summary),
        ]:
            detected = 0
            for row in rows:
                if row[0].text().strip():
                    detected += 1
            prefix = "뉴스" if group_name == "news" else "매매"
            if group_name == "news":
                label.setText("상태: {0} 봇 토큰 감지 {1}/3 / 발송 기준 {2}점".format(prefix, detected, int(self.spin_news_send_min_score.value() or 60)))
            else:
                label.setText("상태: {0} 봇 토큰 감지 {1}/3".format(prefix, detected))

        ai_enabled = 0
        ai_configured = 0
        for row in self.ai_api_rows:
            if row[2].text().strip():
                ai_configured += 1
            if row[0].isChecked() and row[2].text().strip():
                ai_enabled += 1
        self.lbl_ai_summary.setText("상태: AI 설정 {0}/3, 사용 {1}/3".format(ai_configured, ai_enabled))

    def _schedule_credential_verification(self):
        self._credential_verify_timer.start(800)

    def _cache_verify_result(self, key, status_text, ttl_sec=300, cooldown_until=0):
        self._credential_verify_cache[key] = {
            "ts": time.time(),
            "status_text": status_text,
            "cooldown_until": float(cooldown_until or 0),
        }

    def _cached_verify_result(self, key, ttl_sec=300):
        row = self._credential_verify_cache.get(key) or {}
        ts = float(row.get("ts") or 0)
        if ts <= 0:
            return None
        if time.time() - ts > float(ttl_sec):
            return None
        return row

    def _auto_verify_credentials(self):
        if self._credential_verify_running:
            return
        queue = []
        for idx, row in enumerate(self.naver_rows, 1):
            has_id = bool(row[0].text().strip())
            has_secret = bool(row[1].text().strip())
            enabled = bool(row[2].isChecked())
            cache_key = ("naver", idx)
            cached = self._cached_verify_result(cache_key, ttl_sec=600)
            if not (enabled and has_id and has_secret):
                continue
            if cached and float(cached.get("cooldown_until") or 0) > time.time():
                row[4].setText(cached.get("status_text", "요청 과다 / 잠시 대기"))
                continue
            if cached and cached.get("status_text"):
                row[4].setText(cached.get("status_text"))
                continue
            queue.append(("naver", idx))
        for channel_group, rows in [("news", self.news_telegram_rows), ("trade", self.trade_telegram_rows)]:
            for idx, row in enumerate(rows, 1):
                has_token = bool(row[0].text().strip())
                enabled = bool(row[2].isChecked())
                cache_key = ("telegram", channel_group, idx)
                cached = self._cached_verify_result(cache_key, ttl_sec=600)
                if not (enabled and has_token):
                    continue
                if cached and cached.get("status_text"):
                    row[4].setText(cached.get("status_text"))
                    continue
                queue.append(("telegram", channel_group, idx))
        self._credential_verify_queue = queue
        self._process_next_credential_verification()

    def _process_next_credential_verification(self):
        if not self._credential_verify_queue:
            self._credential_verify_running = False
            self._refresh_credential_status_summaries()
            return
        self._credential_verify_running = True
        item = self._credential_verify_queue.pop(0)
        if item[0] == "naver":
            self._test_naver_row(item[1], log_result=False)
            QTimer.singleShot(1400, self._process_next_credential_verification)
            return
        _tag, channel_group, row_no = item
        self._test_telegram_row(channel_group, row_no, log_result=False)
        QTimer.singleShot(200, self._process_next_credential_verification)

    def _test_naver_row(self, row_no, log_result=True):
        edt_id, edt_secret, _chk, _btn_save, lbl_status = self.naver_rows[row_no - 1]
        result = self.news_manager.test_api_key(edt_id.text().strip(), edt_secret.text().strip())
        cache_key = ("naver", row_no)
        if result.get("ok"):
            status_text = "API 인증 성공"
            lbl_status.setText(status_text)
            self.lbl_naver_summary.setText("상태: Set {0} 인증 성공".format(row_no))
            self._cache_verify_result(cache_key, status_text, ttl_sec=600)
            if log_result:
                self.append_log("✅ 네이버 API 확인 성공: Set {0}".format(row_no))
        else:
            base = "키 감지됨" if result.get("key_detected") else "키 미입력"
            if int(result.get("status_code") or 0) == 429:
                status_text = "요청 과다 / 잠시 대기"
                self._cache_verify_result(cache_key, status_text, ttl_sec=120, cooldown_until=time.time() + 180)
                lbl_status.setText(status_text)
            else:
                status_text = "{0} / {1}".format(base, result.get("message", "실패"))
                self._cache_verify_result(cache_key, status_text, ttl_sec=180)
                lbl_status.setText(status_text)
            self.lbl_naver_summary.setText("상태: Set {0} 확인 실패".format(row_no))
            if log_result:
                self.append_log("❌ 네이버 API 확인 실패: Set {0} / {1}".format(row_no, result.get("message", "")))
        self._refresh_credential_status_summaries()

    def _test_telegram_row(self, channel_group, row_no, log_result=True):
        rows = self.news_telegram_rows if channel_group == "news" else self.trade_telegram_rows
        summary_label = self.lbl_news_telegram_summary if channel_group == "news" else self.lbl_trade_telegram_summary
        edt_token, edt_chat, _chk, _btn_save, lbl_status = rows[row_no - 1]
        token = edt_token.text().strip()
        chat_id = edt_chat.text().strip()
        cache_key = ("telegram", channel_group, row_no)
        bot_result = self.telegram_router.test_bot_identity(token)
        if not bot_result.get("ok"):
            status_text = "봇 확인 실패: {0}".format(bot_result.get("message", ""))
            lbl_status.setText(status_text)
            summary_label.setText("상태: Slot {0} 봇 확인 실패".format(row_no))
            self._cache_verify_result(cache_key, status_text, ttl_sec=180)
            if log_result:
                self.append_log("❌ 텔레그램 봇 확인 실패: {0} / {1} / {2}".format(channel_group, row_no, bot_result.get("message", "")))
            self._refresh_credential_status_summaries()
            return
        bot_name = bot_result.get("bot_name", "")
        username = bot_result.get("username", "")
        status_text = "봇: {0}{1}".format(bot_name or "이름없음", " (@{0})".format(username) if username else "")
        if chat_id:
            chat_result = self.telegram_router.test_chat_delivery(token, chat_id, channel_group)
            if chat_result.get("ok"):
                chat_title = str(chat_result.get("chat_title", "") or "")
                status_text += " / 채팅방: {0}".format(chat_title) if chat_title else " / 채팅방 연결 확인"
                summary_label.setText("상태: Slot {0} 연결 성공".format(row_no))
                if log_result:
                    self.append_log("✅ 텔레그램 연결 확인 성공: {0} / {1} / {2}".format(channel_group, row_no, status_text))
            else:
                status_text += " / 채팅방 실패: {0}".format(chat_result.get("message", ""))
                summary_label.setText("상태: Slot {0} 채팅방 실패".format(row_no))
                if log_result:
                    self.append_log("❌ 텔레그램 채팅방 확인 실패: {0} / {1} / {2}".format(channel_group, row_no, chat_result.get("message", "")))
        else:
            summary_label.setText("상태: Slot {0} 봇 확인 성공".format(row_no))
            if log_result:
                self.append_log("✅ 텔레그램 봇 확인 성공: {0} / {1} / {2}".format(channel_group, row_no, status_text))
        lbl_status.setText(status_text)
        self._cache_verify_result(cache_key, status_text, ttl_sec=600)
        self._refresh_credential_status_summaries()

    def _update_ai_row_status(self, row_no):
        chk, cbo_provider, edt_key, edt_base, edt_model, edt_label, _btn_save, lbl_status = self.ai_api_rows[row_no - 1]
        provider = cbo_provider.currentText()
        has_key = bool(edt_key.text().strip())
        enabled = bool(chk.isChecked())
        model = edt_model.text().strip()
        if not has_key:
            lbl_status.setText("미입력")
        elif enabled:
            lbl_status.setText("사용 예정 / {0}{1}".format(provider, " / {0}".format(model) if model else ""))
        else:
            lbl_status.setText("저장됨 / 미사용")

    def _save_ai_row(self, row_no):
        chk, cbo_provider, edt_key, edt_base, edt_model, edt_label, _btn_save, _lbl_status = self.ai_api_rows[row_no - 1]
        provider = cbo_provider.currentData() or "openai"
        api_key = edt_key.text().strip()
        base_url = edt_base.text().strip()
        model_name = edt_model.text().strip()
        analysis_label = edt_label.text().strip()
        self.credential_manager.set_ai_api(row_no, provider, api_key, base_url, model_name, analysis_label, chk.isChecked())
        self._update_ai_row_status(row_no)
        self._refresh_credential_status_summaries()
        self.append_log(u"💾 AI API 설정 저장: {0} / {1}".format(row_no, provider))
        self._schedule_user_profile_save()

    def _save_naver_row(self, row_no):
        edt_id, edt_secret, chk, _btn, _lbl_status = self.naver_rows[row_no - 1]
        self.credential_manager.set_naver_key(row_no, edt_id.text().strip(), edt_secret.text().strip(), chk.isChecked())
        self._update_naver_row_detected_status(row_no)
        self._refresh_credential_status_summaries()
        self.append_log(u"💾 네이버 키 세트 저장: {0}".format(row_no))
        self._schedule_user_profile_save()

    def _save_dart_api(self):
        api_key = self.edt_dart_api_key.text().strip()
        enabled = bool(self.chk_dart_api_enabled.isChecked())
        self.credential_manager.set_dart_api(api_key, enabled)
        self._update_dart_api_detected_status()
        self._refresh_credential_status_summaries()
        self.append_log(u"💶 DART API 설정 저장")
        self._schedule_user_profile_save()

    def _save_news_send_min_score(self):
        score = int(self.spin_news_send_min_score.value() or 60)
        self.credential_manager.set_news_send_min_score(score)
        self.lbl_news_telegram_summary.setText("상태: 뉴스 발송 최소 점수 {0}".format(score))
        self.append_log(u"💾 뉴스 발송 최소 점수 저장: {0}".format(score))
        self._schedule_user_profile_save()

    def _save_telegram_row(self, channel_group, row_no):
        rows = self.news_telegram_rows if channel_group == "news" else self.trade_telegram_rows
        edt_token, edt_chat, chk, _btn, _lbl_status = rows[row_no - 1]
        self.credential_manager.set_telegram_channel(channel_group, row_no, edt_token.text().strip(), edt_chat.text().strip(), chk.isChecked())
        self._test_telegram_row(channel_group, row_no)
        self._refresh_credential_status_summaries()
        self.append_log(u"💾 텔레그램 설정 저장: {0} / {1}".format(channel_group, row_no))
        self._schedule_user_profile_save()

    def _load_credentials_to_ui(self):
        naver_keys = self.credential_manager.get_naver_keys(include_secret=True)
        key_map = dict((int(row["key_set_id"]), row) for row in naver_keys)
        for idx, widgets in enumerate(self.naver_rows, 1):
            data = key_map.get(idx, {})
            widgets[0].setText(data.get("client_id", ""))
            widgets[1].setText(data.get("client_secret", ""))
            widgets[2].setChecked(bool(data.get("enabled", False)))
            self._update_naver_row_detected_status(idx)

        dart_row = self.credential_manager.get_dart_api(include_key=True)
        self.edt_dart_api_key.setText(dart_row.get("api_key", ""))
        self.chk_dart_api_enabled.setChecked(bool(dart_row.get("enabled", False)))
        self._update_dart_api_detected_status()

        self.spin_news_send_min_score.setValue(int(self.credential_manager.get_news_send_min_score() or 60))
        for group_name, rows in [("news", self.news_telegram_rows), ("trade", self.trade_telegram_rows)]:
            items = self.credential_manager.get_telegram_channels(group_name, include_token=True)
            item_map = dict((int(row["slot_no"]), row) for row in items)
            for idx, widgets in enumerate(rows, 1):
                data = item_map.get(idx, {})
                widgets[0].setText(data.get("bot_token", ""))
                widgets[1].setText(data.get("chat_id", ""))
                widgets[2].setChecked(bool(data.get("enabled", False)))
                self._update_telegram_row_detected_status(group_name, idx)

        ai_items = self.credential_manager.get_ai_apis(include_key=True)
        ai_map = dict((int(row.get("slot_no", 0)), row) for row in ai_items if int(row.get("slot_no", 0) or 0) > 0)
        for idx, widgets in enumerate(self.ai_api_rows, 1):
            data = ai_map.get(idx, {})
            provider = str(data.get("provider", "openai") or "openai")
            provider_idx = widgets[1].findData(provider)
            widgets[1].setCurrentIndex(provider_idx if provider_idx >= 0 else 0)
            widgets[0].setChecked(bool(data.get("enabled", False)))
            widgets[2].setText(data.get("api_key", ""))
            widgets[3].setText(data.get("base_url", ""))
            widgets[4].setText(data.get("model_name", ""))
            widgets[5].setText(data.get("analysis_label", ""))
            self._update_ai_row_status(idx)
        self._refresh_credential_status_summaries()

    def _sync_active_accounts(self, startup_sync=False):
        if startup_sync and self._startup_bootstrap_active:
            self._set_startup_step("account_sync", "in_progress", "예수금과 보유내역을 동기화하는 중입니다.")
            self._set_startup_loading_message("계좌 예수금과 보유내역을 동기화하는 중입니다.")
        if startup_sync:
            ok = self.order_manager.synchronize_startup_accounts()
        else:
            ok = self.order_manager.synchronize_all_accounts()
        if startup_sync and self._startup_bootstrap_active and not ok:
            self._set_startup_step("account_sync", "skipped", "계좌 동기화를 바로 시작하지 못했습니다.")
            self._finish_startup_loading()
        return ok

    def _schedule_deferred_account_sync(self):
        if self._startup_deferred_sync_pending:
            return
        self._startup_deferred_sync_pending = True
        self._deferred_account_sync_timer.start(12000)

    def _run_deferred_account_sync(self):
        self._startup_deferred_sync_pending = False
        try:
            if self.kiwoom_client.is_account_sync_busy():
                self._schedule_deferred_account_sync()
                return
        except Exception:
            pass
        self.append_log("⏳ 시작 후 백그라운드 계좌 동기화를 진행합니다.")
        self.order_manager.synchronize_active_accounts()

    def refresh_condition_catalog(self):
        keyword = self.edt_condition_search.text().strip().lower()
        rows = self.condition_manager.get_catalog()
        slot_rows = self.condition_manager.get_slots()
        assigned_map = {}
        for slot_row in slot_rows:
            condition_id = str(slot_row["condition_id"] or "")
            if not condition_id:
                continue
            assigned_map[condition_id] = int(slot_row["slot_no"] or 0)

        self.list_conditions.clear()
        for row in rows:
            base_title = u"[{0}] {1}".format(row["condition_index"], row["condition_name"])
            assigned_slot_no = assigned_map.get(str(row["condition_id"] or ""))
            title = base_title
            if assigned_slot_no:
                title = u"{0}  [사용중: 슬롯 {1}]".format(base_title, assigned_slot_no)
            if keyword and keyword not in title.lower():
                continue
            item = QListWidgetItem(title)
            item.setData(Qt.UserRole, row["condition_id"])
            if assigned_slot_no:
                item.setFlags(item.flags() & ~Qt.ItemIsEnabled)
                item.setForeground(Qt.gray)
                item.setToolTip(u"이미 슬롯 {0}에 배치된 조건식입니다".format(assigned_slot_no))
            self.list_conditions.addItem(item)

    def refresh_slots(self):
        rows = self.condition_manager.get_slots()
        self.table_slots.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            self.table_slots.setItem(row_index, 0, QTableWidgetItem(str(row["slot_no"])))
            self.table_slots.setItem(row_index, 1, QTableWidgetItem(row["condition_name"] or ""))
            active_item = QTableWidgetItem("Y" if int(row["is_enabled"] or 0) else "N")
            active_item.setToolTip("조건식을 사용 설정한 상태" if int(row["is_enabled"] or 0) else "조건식을 사용하지 않는 상태")
            realtime_item = QTableWidgetItem("Y" if int(row["is_realtime"] or 0) else "N")
            realtime_item.setToolTip("키움 API 실시간 등록 성공" if int(row["is_realtime"] or 0) else "키움 API 실시간 미등록 또는 등록 실패")
            self.table_slots.setItem(row_index, 2, active_item)
            self.table_slots.setItem(row_index, 3, realtime_item)
            self.table_slots.setItem(row_index, 4, QTableWidgetItem(str(row["current_count"] or 0)))
            self.table_slots.setItem(row_index, 5, QTableWidgetItem(row["last_event_at"] or ""))

    def _assign_selected_condition(self):
        if not getattr(self.kiwoom_client, "connected", False):
            QMessageBox.warning(self, "조건식", "조건식을 슬롯에 배치하려면 키움 API 로그인이 필요합니다.")
            return

        item = self.list_conditions.currentItem()
        if not item:
            QMessageBox.warning(self, "조건식", "조건식을 먼저 선택하세요")
            return
        condition_id = item.data(Qt.UserRole)
        slot_no = int(self.cbo_slot_target.currentData())

        duplicate = self.condition_manager.find_slot_by_condition_id(condition_id, exclude_slot_no=slot_no)
        if duplicate:
            QMessageBox.warning(
                self,
                "조건식",
                "선택한 조건식은 이미 슬롯 {0}에 배치되어 있습니다.".format(int(duplicate["slot_no"] or 0)),
            )
            return

        current = self.persistence.fetchone("SELECT condition_id FROM active_condition_slots WHERE slot_no=?", (slot_no,))
        old_condition_id = str(current["condition_id"] or "") if current else ""
        ok = self.condition_manager.assign_condition_to_slot(slot_no, condition_id)
        if not ok:
            QMessageBox.warning(self, "조건식", "조건식을 슬롯에 배치하지 못했습니다.")
            return
        if old_condition_id and old_condition_id != str(condition_id or ""):
            self.strategy_manager.delete_slot_strategy_policy(slot_no)
            self.append_log(u"🧹 슬롯 {0} 정책 삭제: 조건식 변경으로 디폴트 적용 상태로 전환".format(slot_no))
        self._refresh_strategy_policy_ui()

    def _sync_slot_target_from_table_selection(self):
        row = self.table_slots.currentRow()
        if row < 0:
            return

        slot_item = self.table_slots.item(row, 0)
        condition_item = self.table_slots.item(row, 1)
        if slot_item is None:
            return

        condition_name = (condition_item.text() if condition_item else "").strip()
        if condition_name:
            return

        try:
            slot_no = int(slot_item.text())
        except Exception:
            return

        combo_index = self.cbo_slot_target.findData(slot_no)
        if combo_index >= 0 and combo_index != self.cbo_slot_target.currentIndex():
            self.cbo_slot_target.setCurrentIndex(combo_index)

    def _selected_slot_no(self):
        row = self.table_slots.currentRow()
        if row < 0:
            return int(self.cbo_slot_target.currentData())
        item = self.table_slots.item(row, 0)
        return int(item.text()) if item else int(self.cbo_slot_target.currentData())

    def _clear_selected_slot(self):
        slot_no = self._selected_slot_no()
        self.condition_manager.clear_slot(slot_no)
        self.strategy_manager.delete_slot_strategy_policy(slot_no)
        self.append_log(u"🧹 슬롯 {0} 정책 삭제: 슬롯 비움".format(slot_no))
        self._refresh_strategy_policy_ui()

    def _start_selected_slot(self):
        self.condition_manager.start_realtime_slot(self._selected_slot_no())

    def _stop_selected_slot(self):
        self.condition_manager.stop_realtime_slot(self._selected_slot_no())

    def _add_strategy(self, kind):
        pairs = self.strategy_manager.get_strategy_type_pairs(kind)
        labels = [u"{0} ({1})".format(name, key) for key, name in pairs]
        choice, ok = QInputDialog.getItem(self, "전략 추가", "전략 유형 선택", labels, 0, False)
        if not ok:
            return
        strategy_type = None
        strategy_name = None
        for key, name in pairs:
            label = u"{0} ({1})".format(name, key)
            if label == choice:
                strategy_type = key
                strategy_name = name
                break
        params = {}
        if strategy_type == "news_filter":
            params["min_score"] = 60
        elif strategy_type == "news_trade":
            params["min_score"] = 80
        self.strategy_manager.add_strategy(kind, strategy_type, strategy_name, params=params)

    def _selected_chain_item_id(self, table):
        row = table.currentRow()
        if row < 0:
            return ""
        item = table.item(row, 0)
        if not item:
            return ""
        return item.data(Qt.UserRole)

    def _delete_selected_strategy(self, table):
        chain_item_id = self._selected_chain_item_id(table)
        if not chain_item_id:
            return
        self.strategy_manager.delete_strategy(chain_item_id)

    def _move_selected_strategy(self, table, delta):
        chain_item_id = self._selected_chain_item_id(table)
        if not chain_item_id:
            return
        self.strategy_manager.move_chain_item(chain_item_id, delta)

    def refresh_buy_chain(self):
        default_row = self.strategy_manager.get_default_strategy_policy()
        buy_items = json.loads(default_row["buy_expression_json"] or "[]")
        self.lbl_buy_default_preview.setText(u"디폴트 매수 전략 : {0}".format(self._format_expression_preview("buy", buy_items)))
        self._set_policy_table_rows(self.table_buy_chain, "buy", self._expression_strategy_nos(buy_items))

    def refresh_sell_chain(self):
        default_row = self.strategy_manager.get_default_strategy_policy()
        sell_nos = json.loads(default_row["sell_strategy_nos_json"] or "[]")
        sell_items = self._build_sell_expression_items(sell_nos)
        self.lbl_sell_default_preview.setText(u"디폴트 매도 전략 : {0}".format(self._format_expression_preview("sell", sell_items)))
        self._set_policy_table_rows(self.table_sell_chain, "sell", sell_nos)

    def _load_selected_buy_strategy_detail(self):
        row_idx = self.table_buy_chain.currentRow()
        if row_idx < 0:
            return
        chain_item_id = self._selected_chain_item_id(self.table_buy_chain)
        row = self.persistence.fetchone(
            """
            SELECT s.*
            FROM strategy_chain_items c
            INNER JOIN strategy_definitions s ON c.strategy_id = s.strategy_id
            WHERE c.chain_item_id=?
            """,
            (chain_item_id,),
        )
        if not row:
            return
        params = json.loads(row["params_json"] or "{}")
        self.lbl_strategy_selected.setText(row["strategy_id"])
        self.edt_strategy_name.setText(row["strategy_name"])
        self.cbo_strategy_scope.setCurrentText(row["scope_type"])
        self.spin_strategy_score.setValue(int(params.get("min_score", 0)))

    def _save_selected_strategy_detail(self):
        strategy_id = self.lbl_strategy_selected.text().strip()
        if not strategy_id or strategy_id == u"선택 없음":
            return
        row = self.persistence.fetchone("SELECT * FROM strategy_definitions WHERE strategy_id=?", (strategy_id,))
        if not row:
            return
        params = json.loads(row["params_json"] or "{}")
        params["min_score"] = int(self.spin_strategy_score.value())
        self.persistence.execute(
            "UPDATE strategy_definitions SET strategy_name=?, scope_type=?, params_json=?, updated_at=? WHERE strategy_id=?",
            (
                self.edt_strategy_name.text().strip(),
                self.cbo_strategy_scope.currentText(),
                json.dumps(params, ensure_ascii=False),
                self.persistence.now_ts(),
                strategy_id,
            ),
        )
        self.strategy_manager.strategies_changed.emit()
        self.append_log(u"💾 전략 상세 저장: {0}".format(strategy_id))

    def _safe_json_dict(self, raw):
        if isinstance(raw, dict):
            return dict(raw)
        try:
            return json.loads(raw or '{}')
        except Exception:
            return {}

    def _safe_json_list(self, raw):
        if isinstance(raw, list):
            return list(raw)
        try:
            data = json.loads(raw or '[]')
        except Exception:
            data = []
        return data if isinstance(data, list) else []

    def _profit_loss_color(self, value):
        try:
            number = float(value or 0)
        except Exception:
            number = 0.0
        if number > 0:
            return QColor("#d32f2f")
        if number < 0:
            return QColor("#1565c0")
        return None

    def _make_table_item(self, text, align_right=False, value_for_color=None, sort_value=None, user_data=None):
        item = SortableTableWidgetItem(str(text), sort_value=sort_value)
        if align_right:
            item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
        color = self._profit_loss_color(value_for_color) if value_for_color is not None else None
        if color is not None:
            item.setForeground(color)
        if user_data is not None:
            item.setData(Qt.UserRole + 1, user_data)
        return item

    def _make_number_item(self, value, digits=0, signed=False, rate=False, suffix=""):
        if rate:
            text = self._format_rate_text(value)
        else:
            text = self._format_number_text(value, digits)
        if suffix:
            text = "{0}{1}".format(text, suffix)
        try:
            sort_value = float(value or 0)
        except Exception:
            sort_value = 0.0
        return self._make_table_item(text, align_right=True, value_for_color=(value if signed else None), sort_value=sort_value)

    def _is_strategy_analysis_cycle(self, cycle):
        cycle = dict(cycle or {})
        if str(cycle.get("buy_filled_at") or "").strip():
            return True
        status = str(cycle.get("status") or "")
        return status in [
            "BUY_PARTIAL",
            "HOLDING",
            "SELL_PENDING",
            "SELL_PARTIAL",
            "SIMULATED_HOLDING",
            "CLOSED",
            "SIMULATED_CLOSED",
        ]

    def _resolve_cycle_condition_name(self, cycle, extra, active_state):
        cycle = dict(cycle or {})
        extra = self._safe_json_dict(extra)
        active_state = self._safe_json_dict(active_state)
        entry_source = str(active_state.get("entry_source") or extra.get("entry_source") or "")
        entry_slot_no = active_state.get("entry_slot_no")
        if entry_slot_no in ["", None]:
            entry_slot_no = extra.get("entry_slot_no")
        try:
            entry_slot_no = int(entry_slot_no or 0)
        except Exception:
            entry_slot_no = 0

        if entry_source == "news_trade":
            return {"condition_name": "뉴스매매", "entry_slot_no": 0, "entry_source": entry_source}

        source_conditions = self._safe_json_list(cycle.get("source_conditions_json") or "[]")
        matched = None
        latest = None
        for item in source_conditions:
            if not isinstance(item, dict):
                continue
            try:
                slot_no = int(item.get("slot_no") or 0)
            except Exception:
                slot_no = 0
            condition_name = str(item.get("condition_name") or "").strip()
            ts = str(item.get("ts") or "")
            current = {"slot_no": slot_no, "condition_name": condition_name, "ts": ts}
            if latest is None or ts >= latest.get("ts", ""):
                latest = current
            if entry_slot_no > 0 and slot_no == entry_slot_no and (matched is None or ts >= matched.get("ts", "")):
                matched = current

        chosen = matched or latest or {}
        condition_name = str(chosen.get("condition_name") or "").strip()
        slot_no = int(chosen.get("slot_no") or entry_slot_no or 0)
        if not condition_name and slot_no > 0:
            condition_name = "슬롯{0}".format(slot_no)
        if not condition_name:
            condition_name = entry_source or "알수없음"
        return {"condition_name": condition_name, "entry_slot_no": slot_no, "entry_source": entry_source or "unknown"}

    def _resolve_cycle_strategy_names(self, cycle, extra, active_state):
        cycle = dict(cycle or {})
        extra = self._safe_json_dict(extra)
        active_state = self._safe_json_dict(active_state)
        strategy_names = []

        for item in list(active_state.get("buy_expression_items") or []):
            if str(item.get("kind") or "") != "strategy":
                continue
            try:
                strategy_no = int(item.get("no") or 0)
            except Exception:
                strategy_no = 0
            if strategy_no > 0:
                strategy_names.append(self._strategy_name_by_no("buy", strategy_no))

        if strategy_names:
            return strategy_names

        for item in self._safe_json_list(cycle.get("buy_filters_json") or "[]"):
            if not isinstance(item, dict):
                continue
            name = str(item.get("strategy_name") or "").strip()
            if not name:
                try:
                    strategy_no = int(item.get("strategy_no") or 0)
                except Exception:
                    strategy_no = 0
                if strategy_no > 0:
                    name = self._strategy_name_by_no("buy", strategy_no)
            if not name:
                name = str(item.get("strategy_type") or "").strip()
            if name:
                strategy_names.append(name)

        if strategy_names:
            return strategy_names

        trigger = extra.get("trigger_buy_strategy")
        if isinstance(trigger, dict):
            name = str(trigger.get("strategy_name") or trigger.get("strategy_type") or "").strip()
            if name:
                return [name]

        entry_source = str(active_state.get("entry_source") or extra.get("entry_source") or "")
        if entry_source == "news_trade":
            return ["뉴스매매"]
        return []

    def _resolve_cycle_buy_strategy_summary(self, cycle):
        cycle = dict(cycle or {})
        extra = self._safe_json_dict(cycle.get("extra_json") or "{}")
        summary = {}

        trigger = extra.get("trigger_buy_strategy")
        if isinstance(trigger, dict) and any(trigger.get(key) not in ["", None, 0] for key in ["strategy_no", "strategy_id", "strategy_name", "strategy_type"]):
            summary = dict(trigger)

        if not summary:
            passed_rows = extra.get("passed_buy_strategies")
            if isinstance(passed_rows, list):
                for item in reversed(passed_rows):
                    if isinstance(item, dict) and any(item.get(key) not in ["", None, 0] for key in ["strategy_no", "strategy_id", "strategy_name", "strategy_type"]):
                        summary = dict(item)
                        break

        if not summary:
            for item in self._safe_json_list(cycle.get("buy_filters_json") or "[]"):
                if not isinstance(item, dict) or not bool(item.get("passed")):
                    continue
                summary = {
                    "strategy_no": int(item.get("strategy_no") or 0),
                    "strategy_id": str(item.get("strategy_id") or ""),
                    "strategy_name": str(item.get("strategy_name") or ""),
                    "strategy_type": str(item.get("strategy_type") or ""),
                }

        strategy_no = int(summary.get("strategy_no") or 0)
        strategy_id = str(summary.get("strategy_id") or "")
        strategy_name = str(summary.get("strategy_name") or "")
        strategy_type = str(summary.get("strategy_type") or "")

        if strategy_no > 0:
            row = self.strategy_manager.get_strategy_by_no("buy", strategy_no)
            if row:
                strategy_id = strategy_id or str(row["strategy_id"] or "")
                strategy_name = strategy_name or str(row["strategy_name"] or row["strategy_type"] or "")
                strategy_type = strategy_type or str(row["strategy_type"] or "")

        if not strategy_type:
            strategy_type = str(extra.get("trigger_buy_strategy_type") or extra.get("entry_source") or "")
        if not strategy_id:
            strategy_id = strategy_type or ("buy_{0}".format(strategy_no) if strategy_no > 0 else "unknown")
        if not strategy_name:
            if strategy_no > 0:
                strategy_name = self._strategy_label_by_no("buy", strategy_no).split("] ", 1)[-1]
            elif strategy_type == "news_trade":
                strategy_name = "뉴스매매"
            elif strategy_type == "slot_buy":
                strategy_name = "슬롯매수"
            else:
                strategy_name = strategy_type or "알수없음"

        return {
            "strategy_no": strategy_no,
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "strategy_type": strategy_type or "unknown",
        }

    def _resolve_cycle_buy_decision_summary(self, cycle):
        cycle = dict(cycle or {})
        extra = self._safe_json_dict(cycle.get("extra_json") or "{}")
        active_state = self._safe_json_dict(extra.get("active_sell_state") or {})
        condition_info = self._resolve_cycle_condition_name(cycle, extra, active_state)
        strategy_names = self._resolve_cycle_strategy_names(cycle, extra, active_state)

        condition_name = str(condition_info.get("condition_name") or "").strip()
        joined_names = "-".join([str(name or "").strip() for name in strategy_names if str(name or "").strip()])
        if joined_names:
            display_name = "{0}-{1}".format(condition_name, joined_names) if condition_name else joined_names
        else:
            display_name = condition_name or "알수없음"

        entry_source = str(condition_info.get("entry_source") or active_state.get("entry_source") or extra.get("entry_source") or "unknown")
        entry_slot_no = int(condition_info.get("entry_slot_no") or 0)

        return {
            "strategy_no": entry_slot_no,
            "strategy_id": "{0}|{1}|{2}|{3}".format(entry_source, entry_slot_no, condition_name, joined_names),
            "strategy_name": display_name,
            "strategy_type": entry_source or "unknown",
            "condition_name": condition_name,
            "entry_slot_no": entry_slot_no,
            "strategy_names": strategy_names,
        }

    def _build_strategy_analysis_rows(self):
        rows = self.persistence.fetchall("SELECT * FROM trade_cycles ORDER BY COALESCE(sell_filled_at, buy_filled_at, buy_order_at, entry_detected_at) DESC")
        summary_map = {}
        for row in rows:
            cycle = dict(row)
            if not self._is_strategy_analysis_cycle(cycle):
                continue
            info = self._resolve_cycle_buy_decision_summary(cycle)
            key = str(info.get("strategy_id") or info.get("strategy_type") or info.get("strategy_no") or "unknown")
            bucket = summary_map.setdefault(
                key,
                {
                    "strategy_no": int(info.get("strategy_no") or 0),
                    "strategy_name": str(info.get("strategy_name") or ""),
                    "strategy_type": str(info.get("strategy_type") or ""),
                    "entry_count": 0,
                    "closed_count": 0,
                    "win_count": 0,
                    "loss_count": 0,
                    "cumulative_pnl": 0.0,
                },
            )
            bucket["entry_count"] += 1
            try:
                bucket["cumulative_pnl"] += float(cycle.get("pnl_realized") or 0)
            except Exception:
                pass
            status = str(cycle.get("status") or "")
            if status in ["CLOSED", "SIMULATED_CLOSED"]:
                bucket["closed_count"] += 1
                pnl_value = float(cycle.get("pnl_realized") or 0)
                if pnl_value > 0:
                    bucket["win_count"] += 1
                elif pnl_value < 0:
                    bucket["loss_count"] += 1

        items = []
        for bucket in summary_map.values():
            closed_count = int(bucket.get("closed_count") or 0)
            win_count = int(bucket.get("win_count") or 0)
            bucket["win_rate"] = (float(win_count) / float(closed_count) * 100.0) if closed_count > 0 else 0.0
            items.append(bucket)
        items.sort(key=lambda row: (-float(row.get("cumulative_pnl") or 0), -int(row.get("win_count") or 0), -int(row.get("entry_count") or 0), str(row.get("strategy_name") or "")))
        return items

    def _selected_daily_review_meta(self):
        if not hasattr(self, "cbo_daily_review_date"):
            return {"trade_date": "", "has_record": False, "is_finalized": False, "snapshot_ts": ""}
        data = self.cbo_daily_review_date.currentData()
        if isinstance(data, dict):
            return {
                "trade_date": str(data.get("trade_date") or ""),
                "has_record": bool(data.get("has_record")),
                "is_finalized": bool(data.get("is_finalized")),
                "snapshot_ts": str(data.get("snapshot_ts") or ""),
            }
        return {"trade_date": "", "has_record": False, "is_finalized": False, "snapshot_ts": ""}

    def _set_daily_review_status(self, has_record, is_finalized=False, snapshot_ts=""):
        if not hasattr(self, "lbl_daily_review_status"):
            return
        if not has_record:
            self.lbl_daily_review_status.setText("복기 기록 없음")
            self.lbl_daily_review_status.setStyleSheet("color: #b00020; font-weight: 700;")
            return
        label = "확정 기록" if is_finalized else "임시 기록"
        if snapshot_ts:
            label = "{0} / 최근 저장 {1}".format(label, snapshot_ts)
        color = "#0a7a28" if is_finalized else "#b26a00"
        self.lbl_daily_review_status.setText(label)
        self.lbl_daily_review_status.setStyleSheet("color: {0}; font-weight: 700;".format(color))

    def refresh_daily_review_dates(self, preserve_selected=True):
        if not hasattr(self, "cbo_daily_review_date"):
            return
        selected_meta = self._selected_daily_review_meta() if preserve_selected else {}
        selected_date = str(selected_meta.get("trade_date") or "")
        status_map = {}
        if hasattr(self.order_manager, "get_daily_review_date_status_map"):
            try:
                status_map = dict(self.order_manager.get_daily_review_date_status_map() or {})
            except Exception:
                status_map = {}
        today = datetime.date.today()
        model = self.cbo_daily_review_date.model()
        self.cbo_daily_review_date.blockSignals(True)
        self.cbo_daily_review_date.clear()
        first_enabled_index = -1
        selected_index = -1
        for offset in range(31):
            current_date = today - datetime.timedelta(days=offset)
            trade_date = current_date.isoformat()
            meta = dict(status_map.get(trade_date) or {})
            has_record = bool(meta)
            is_finalized = bool(meta.get("is_finalized"))
            title = trade_date
            if offset == 0:
                title += " (오늘)"
            elif offset == 1:
                title += " (어제)"
            if not has_record:
                title += " / 기록 없음"
            elif is_finalized:
                title += " / 확정"
            else:
                title += " / 임시"
            item_data = {
                "trade_date": trade_date,
                "has_record": has_record,
                "is_finalized": is_finalized,
                "snapshot_ts": str(meta.get("snapshot_ts") or ""),
            }
            self.cbo_daily_review_date.addItem(title, item_data)
            index = self.cbo_daily_review_date.count() - 1
            item = model.item(index) if hasattr(model, "item") else None
            if item is not None:
                if not has_record:
                    item.setEnabled(False)
                    item.setForeground(QColor("#9e9e9e"))
                elif is_finalized:
                    item.setForeground(QColor("#0a7a28"))
                else:
                    item.setForeground(QColor("#b26a00"))
            if has_record and first_enabled_index < 0:
                first_enabled_index = index
            if has_record and trade_date == selected_date:
                selected_index = index
        if selected_index < 0:
            selected_index = first_enabled_index
        if selected_index < 0 and self.cbo_daily_review_date.count() > 0:
            selected_index = 0
        if selected_index >= 0:
            self.cbo_daily_review_date.setCurrentIndex(selected_index)
        self.cbo_daily_review_date.blockSignals(False)

    def refresh_daily_review_view(self):
        if not hasattr(self, "table_daily_review_summary"):
            return
        meta = self._selected_daily_review_meta()
        trade_date = str(meta.get("trade_date") or "")
        has_record = bool(meta.get("has_record"))
        is_finalized = bool(meta.get("is_finalized"))
        snapshot_ts = str(meta.get("snapshot_ts") or "")
        if not has_record or not trade_date:
            self._set_daily_review_status(False)
            self.table_daily_review_summary.setRowCount(0)
            self.table_daily_review_items.setRowCount(0)
            self.table_daily_review_summary.setEnabled(False)
            self.table_daily_review_items.setEnabled(False)
            return
        summary_rows = []
        item_rows = []
        try:
            summary_rows = list(self.order_manager.get_daily_review_summary_rows(trade_date) or [])
            item_rows = list(self.order_manager.get_daily_review_item_rows(trade_date) or [])
        except Exception:
            summary_rows = []
            item_rows = []
        if not summary_rows and not item_rows:
            self._set_daily_review_status(False)
            self.table_daily_review_summary.setRowCount(0)
            self.table_daily_review_items.setRowCount(0)
            self.table_daily_review_summary.setEnabled(False)
            self.table_daily_review_items.setEnabled(False)
            return
        self._set_daily_review_status(True, is_finalized=is_finalized, snapshot_ts=snapshot_ts)
        self.table_daily_review_summary.setEnabled(True)
        self.table_daily_review_items.setEnabled(True)

        if trade_date == self.persistence.today_str() and summary_rows:
            account_settings_map = {}
            for account_row in self.account_manager.get_accounts():
                account_no = str(account_row.get("account_no") or "")
                if not account_no:
                    continue
                settings = dict(account_row.get("settings") or {})
                account_settings_map[account_no] = float(settings.get("api_realized_profit", 0.0) or 0.0)
            patched_rows = []
            for row in summary_rows:
                patched = dict(row)
                account_no = str(patched.get("account_no") or "")
                live_realized = float(account_settings_map.get(account_no, 0.0) or 0.0)
                if live_realized != 0:
                    patched["realized_profit_total"] = live_realized
                    patched["total_pnl"] = float(patched.get("holding_eval_total") or 0.0) + live_realized
                patched_rows.append(patched)
            summary_rows = patched_rows

        display_summary_rows = list(summary_rows)
        if summary_rows:
            display_summary_rows.append(
                {
                    "account_no": "합계",
                    "snapshot_ts": max([str(row.get("snapshot_ts") or "") for row in summary_rows] or [""]),
                    "holding_eval_total": sum([float(row.get("holding_eval_total") or 0.0) for row in summary_rows]),
                    "realized_profit_total": sum([float(row.get("realized_profit_total") or 0.0) for row in summary_rows]),
                    "total_pnl": sum([float(row.get("total_pnl") or 0.0) for row in summary_rows]),
                    "holding_count": sum([int(row.get("holding_count") or 0) for row in summary_rows]),
                    "sold_count": sum([int(row.get("sold_count") or 0) for row in summary_rows]),
                }
            )
        self.table_daily_review_summary.setRowCount(len(display_summary_rows))
        for row_index, row in enumerate(display_summary_rows):
            account_no = str(row.get("account_no") or "")
            account_text = "합계" if account_no == "합계" else self._masked_account_tail(account_no)
            snapshot_text = str(row.get("snapshot_ts") or "")
            if len(snapshot_text) >= 16:
                snapshot_text = snapshot_text[11:16]
            self.table_daily_review_summary.setItem(row_index, 0, self._make_table_item(account_text))
            self.table_daily_review_summary.setItem(row_index, 1, self._make_table_item(snapshot_text))
            self.table_daily_review_summary.setItem(row_index, 2, self._make_number_item(row.get("holding_eval_total") or 0, signed=True))
            self.table_daily_review_summary.setItem(row_index, 3, self._make_number_item(row.get("realized_profit_total") or 0, signed=True))
            self.table_daily_review_summary.setItem(row_index, 4, self._make_number_item(row.get("total_pnl") or 0, signed=True))
            self.table_daily_review_summary.setItem(row_index, 5, self._make_number_item(row.get("holding_count") or 0))
            self.table_daily_review_summary.setItem(row_index, 6, self._make_number_item(row.get("sold_count") or 0))

        self.table_daily_review_items.setRowCount(len(item_rows))
        for row_index, row in enumerate(item_rows):
            account_text = self._masked_account_tail(row.get("account_no") or "")
            ref_price = float(row.get("ref_price") or row.get("avg_price") or 0.0)
            code = str(row.get("code") or "").strip()
            name = str(row.get("name") or "").strip()
            item_status = str(row.get("item_status") or "").strip()
            name_text = name
            if code:
                name_text = f"{name_text} ({code})" if name_text else f"({code})"
            if item_status and item_status != "보유":
                name_text = f"{item_status} | {name_text}" if name_text else item_status
            self.table_daily_review_items.setItem(row_index, 0, self._make_table_item(account_text))
            self.table_daily_review_items.setItem(row_index, 1, self._make_table_item(name_text))
            self.table_daily_review_items.setItem(row_index, 2, self._make_number_item(ref_price or 0))
            self.table_daily_review_items.setItem(row_index, 3, self._make_number_item(row.get("eval_profit") or 0, signed=True))
            self.table_daily_review_items.setItem(row_index, 4, self._make_number_item(row.get("realized_profit") or 0, signed=True))

    def refresh_strategy_analysis(self):
        self._scope_refresh_pending = False
        rows = self._build_strategy_analysis_rows()
        self.table_strategy_analysis.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            strategy_no = int(row.get("strategy_no") or 0)
            self.table_strategy_analysis.setItem(row_index, 0, self._make_table_item(strategy_no if strategy_no > 0 else "-", align_right=True))
            self._set_strategy_name_cell(
                self.table_strategy_analysis,
                row_index,
                1,
                str(row.get("strategy_name") or ""),
                tooltip=str(row.get("strategy_name") or ""),
            )
            self.table_strategy_analysis.setItem(row_index, 2, self._make_table_item(str(row.get("strategy_type") or "")))
            self.table_strategy_analysis.setItem(row_index, 3, self._make_number_item(row.get("entry_count") or 0))
            self.table_strategy_analysis.setItem(row_index, 4, self._make_number_item(row.get("closed_count") or 0))
            self.table_strategy_analysis.setItem(row_index, 5, self._make_number_item(row.get("win_count") or 0))
            self.table_strategy_analysis.setItem(row_index, 6, self._make_number_item(row.get("loss_count") or 0))
            self.table_strategy_analysis.setItem(row_index, 7, self._make_number_item(row.get("win_rate") or 0, digits=2, rate=True, suffix="%"))
            self.table_strategy_analysis.setItem(row_index, 8, self._make_number_item(row.get("cumulative_pnl") or 0, signed=True))
        self.refresh_daily_review_dates(preserve_selected=True)
        self.refresh_daily_review_view()

    def _translate_unfilled_policy(self, policy):
        mapping = {
            "cancel": "취소",
            "reprice": "재호가 후 취소",
            "market": "시장가 전환",
            "reprice_then_market": "재호가 후 시장가 전환",
        }
        return mapping.get(str(policy or ''), str(policy or ''))

    def _translate_limit_option(self, option):
        mapping = {
            "current_price": "현재가",
            "ask1": "매도1호가",
            "current_plus_1tick": "현재가+1틱",
        }
        return mapping.get(str(option or ''), str(option or ''))

    def _translate_unfilled_stage(self, status, extra):
        status = str(status or "")
        if status in ["BUY_REQUESTED", "BUY_PENDING"]:
            return "1차 대기"
        if status == "BUY_PARTIAL":
            return "부분체결 감시"
        if status == "BUY_REPRICE_REQUESTED":
            return "재호가 요청"
        if status == "BUY_MARKET_SWITCH_REQUESTED":
            return "시장가 전환 요청"
        if status == "BUY_CANCEL_REQUESTED":
            return "취소 요청"
        if status == "CANCELLED":
            return "취소 완료"
        if status == "HOLDING":
            return "체결 완료"
        return status

    def _translate_unfilled_action(self, extra):
        action = str(extra.get("unfilled_action", "") or "")
        mapping = {
            "cancel_requested": "취소 요청",
            "market_switch_requested": "시장가 전환 요청",
            "reprice_requested": "재호가 요청",
        }
        return mapping.get(action, "-")

    def _get_position_state_manager(self):
        return getattr(self.order_manager, "position_state_manager", None)

    def _format_number_text(self, value, digits=0):
        try:
            number = float(value or 0)
        except Exception:
            number = 0.0
        if digits <= 0:
            return "{0:,.0f}".format(number)
        return ("{0:,.%df}" % int(digits)).format(number)

    def _format_rate_text(self, value):
        try:
            number = float(value or 0)
        except Exception:
            number = 0.0
        return "{0:.2f}".format(number)

    def _condition_name_by_slot(self, slot_no):
        try:
            slot_no = int(slot_no or 0)
        except Exception:
            slot_no = 0
        if slot_no <= 0:
            return ""
        try:
            for row in self.condition_manager.get_slots():
                try:
                    current_slot = int(row["slot_no"] or 0)
                except Exception:
                    current_slot = 0
                if current_slot == slot_no:
                    return str(row["condition_name"] or "").strip()
        except Exception:
            return ""
        return ""

    def _build_position_strategy_text(self, state):
        state = dict(state or {})
        cycle_row = state.get("cycle_row")
        if cycle_row:
            info = self._resolve_cycle_buy_decision_summary(cycle_row)
            display_name = str(info.get("strategy_name") or "").strip()
            if display_name:
                return display_name

        active_state = self._safe_json_dict(state.get("active_sell_state") or {})
        entry_source = str(active_state.get("entry_source") or state.get("entry_source") or "")
        entry_slot_no = active_state.get("entry_slot_no") or state.get("entry_slot_no")
        condition_name = self._condition_name_by_slot(entry_slot_no)

        strategy_names = []
        for item in list(active_state.get("buy_expression_items") or state.get("buy_expression_items") or []):
            if str(item.get("kind") or "") != "strategy":
                continue
            try:
                strategy_no = int(item.get("no") or 0)
            except Exception:
                strategy_no = 0
            if strategy_no > 0:
                strategy_names.append(self._strategy_name_by_no("buy", strategy_no))

        joined_names = "-".join([str(name or "").strip() for name in strategy_names if str(name or "").strip()])
        if joined_names:
            return "{0}-{1}".format(condition_name, joined_names) if condition_name else joined_names
        if condition_name:
            return condition_name
        if entry_source == "news_trade":
            return "뉴스매매"
        buy_chain_id = str(state.get("buy_chain_id") or "")
        return buy_chain_id or "-"

    def _build_position_news_status_text(self, state):
        state = dict(state or {})
        entry_source = str(state.get("entry_source") or "")
        news_trade_min_score = int(state.get("news_trade_min_score") or 0)
        news_min_score = int(state.get("news_min_score") or 0)
        if entry_source == "news_trade":
            return "뉴스매매 {0}점".format(news_trade_min_score) if news_trade_min_score > 0 else "뉴스매매"
        if news_min_score > 0:
            return "뉴스필터 {0}점".format(news_min_score)
        return "-"

    def _build_position_rows_for_operations(self):
        manager = self._get_position_state_manager()
        if manager is None:
            return []
        rows = list(manager.get_active_position_states() or [])
        rows.sort(key=lambda row: (str(row.get("account_no") or ""), str(row.get("code") or "")))
        return rows

    def _build_live_account_summary_map(self, position_states):
        summary_map = {}
        for state in list(position_states or []):
            account_no = str(state.get("account_no") or "")
            if not account_no:
                continue
            info = summary_map.setdefault(account_no, {"holding_count": 0, "total_buy": 0.0, "total_eval": 0.0, "eval_profit_total": 0.0})
            info["holding_count"] += 1
            try:
                qty = int(state.get("qty") or 0)
            except Exception:
                qty = 0
            try:
                avg_price = float(state.get("avg_price") or 0.0)
            except Exception:
                avg_price = 0.0
            try:
                current_price = float(state.get("current_price") or 0.0)
            except Exception:
                current_price = 0.0
            info["total_buy"] += avg_price * qty
            info["total_eval"] += current_price * qty
            try:
                info["eval_profit_total"] += float(state.get("eval_profit") or 0.0)
            except Exception:
                pass
        return summary_map

    def _build_policy_detail_text(self, extra):
        parts = []
        if extra.get("order_no"):
            parts.append("원주문:%s" % extra.get("order_no"))
        if extra.get("reprice_target_order_no"):
            parts.append("정정대상:%s" % extra.get("reprice_target_order_no"))
        if extra.get("cancel_target_order_no"):
            parts.append("취소대상:%s" % extra.get("cancel_target_order_no"))
        if extra.get("market_switch_target_order_no"):
            parts.append("전환대상:%s" % extra.get("market_switch_target_order_no"))
        if extra.get("reprice_order_price"):
            parts.append("재호가:%s" % int(float(extra.get("reprice_order_price") or 0)))
        if extra.get("price_source"):
            parts.append("기준:%s" % extra.get("price_source"))
        if extra.get("reprice_price_source"):
            parts.append("재호가기준:%s" % extra.get("reprice_price_source"))
        return " / ".join(parts) if parts else "-"

    def _selected_position_context(self):
        row = self.table_positions.currentRow()
        if row < 0:
            return None
        item_account = self.table_positions.item(row, 0)
        item_name = self.table_positions.item(row, 1)
        item_code = self.table_positions.item(row, 2)
        if not item_account or not item_code:
            return None
        account_no = item_account.data(Qt.UserRole + 1) or item_account.text()
        return {
            "account_no": str(account_no or ""),
            "name": item_name.text() if item_name else (item_code.text() if item_code else ""),
            "code": item_code.text(),
        }

    def _manual_sell_selected_position(self):
        context = self._selected_position_context()
        if not context:
            QMessageBox.warning(self, "선택 매도", "운영 탭에서 보유 종목 행을 먼저 선택해 주세요.")
            return
        reply = QMessageBox.question(
            self,
            "선택 매도",
            "{0}({1}) 종목을 즉시 매도하시겠습니까?".format(context["name"], context["code"]),
        )
        if reply != QMessageBox.Yes:
            return
        ok = self.order_manager.manual_sell_position(context["account_no"], context["code"])
        if not ok:
            QMessageBox.warning(self, "선택 매도", "선택 매도 요청을 처리하지 못했습니다.")

    def _selected_open_order_context(self):
        row = self.table_open_orders.currentRow()
        if row < 0:
            return None
        item_account = self.table_open_orders.item(row, 0)
        item_order_no = self.table_open_orders.item(row, 1)
        item_name = self.table_open_orders.item(row, 2)
        item_code = self.table_open_orders.item(row, 3)
        if not item_account or not item_code:
            return None
        account_no = item_account.data(Qt.UserRole + 1) or item_account.text()
        return {
            "account_no": str(account_no or ""),
            "order_no": item_order_no.text() if item_order_no else "",
            "name": item_name.text() if item_name else (item_code.text() if item_code else ""),
            "code": item_code.text(),
        }

    def _manual_cancel_selected_open_order(self):
        context = self._selected_open_order_context()
        if not context:
            QMessageBox.warning(self, "미체결 수동 처리", "운영 탭에서 미체결 주문을 먼저 선택하세요")
            return
        reply = QMessageBox.question(self, "미체결 즉시 취소", "{0}({1}) 미체결 매수를 즉시 취소하시겠습니까?".format(context["name"], context["code"]))
        if reply != QMessageBox.Yes:
            return
        ok = self.order_manager.manual_cancel_open_buy(context["account_no"], context["code"], context["order_no"])
        if not ok:
            QMessageBox.warning(self, "미체결 즉시 취소", "수동 취소 요청을 수행하지 못했습니다")

    def _manual_reprice_selected_open_order(self):
        context = self._selected_open_order_context()
        if not context:
            QMessageBox.warning(self, "미체결 수동 처리", "운영 탭에서 미체결 주문을 먼저 선택하세요")
            return
        reply = QMessageBox.question(self, "미체결 즉시 재호가", "{0}({1}) 미체결 매수를 현재 정책 기준으로 즉시 재호가하시겠습니까?".format(context["name"], context["code"]))
        if reply != QMessageBox.Yes:
            return
        ok = self.order_manager.manual_reprice_open_buy(context["account_no"], context["code"], context["order_no"])
        if not ok:
            QMessageBox.warning(self, "미체결 즉시 재호가", "수동 재호가 요청을 수행하지 못했습니다")

    def _manual_market_selected_open_order(self):
        context = self._selected_open_order_context()
        if not context:
            QMessageBox.warning(self, "미체결 수동 처리", "운영 탭에서 미체결 주문을 먼저 선택하세요")
            return
        reply = QMessageBox.question(self, "미체결 즉시 시장가 전환", "{0}({1}) 미체결 매수를 즉시 시장가로 전환하시겠습니까?".format(context["name"], context["code"]))
        if reply != QMessageBox.Yes:
            return
        ok = self.order_manager.manual_market_switch_open_buy(context["account_no"], context["code"], context["order_no"])
        if not ok:
            QMessageBox.warning(self, "미체결 즉시 시장가 전환", "수동 시장가 전환 요청을 수행하지 못했습니다")

    def refresh_operations(self):
        if not (self._is_operations_tab_active() or self._is_scope_tab_active()):
            self._operations_refresh_pending = True
            self._scope_refresh_pending = True
            return
        self._operations_refresh_pending = False
        rows = self.persistence.fetchall(
            """
            SELECT
                a.account_no,
                COALESCE(s.holding_count, 0) AS holding_count,
                COALESCE(s.eval_profit_total, 0) AS eval_profit_total,
                COALESCE(s.realized_profit_total, 0) AS realized_profit_total
            FROM accounts a
            LEFT JOIN daily_account_summary s
              ON s.account_no = a.account_no
             AND s.trade_date = (
                 SELECT MAX(ds.trade_date)
                 FROM daily_account_summary ds
                 WHERE ds.account_no = a.account_no
             )
            WHERE COALESCE(TRIM(a.account_no), '') <> ''
            ORDER BY a.account_no
            """
        )
        position_states = self._build_position_rows_for_operations()
        live_summary_map = self._build_live_account_summary_map(position_states)
        cycle_realized_map = {}
        for cycle_row in self.persistence.fetchall(
            """
            SELECT account_no, COALESCE(SUM(pnl_realized), 0) AS realized_sum
            FROM trade_cycles
            WHERE status IN ('CLOSED', 'SIMULATED_CLOSED')
            GROUP BY account_no
            """
        ):
            cycle_realized_map[str(cycle_row["account_no"] or "")] = float(cycle_row["realized_sum"] or 0.0)
        account_settings_map = {}
        for row in self.account_manager.get_accounts():
            account_no = str(row.get("account_no") or "")
            if not account_no:
                continue
            settings = dict(row.get("settings") or {})
            account_settings_map[account_no] = {
                "deposit_cash": float(settings.get("deposit_cash", 0.0) or 0.0),
                "orderable_cash": float(settings.get("orderable_cash", 0.0) or 0.0),
                "estimated_assets": float(settings.get("estimated_assets", 0.0) or 0.0),
                "api_total_buy": float(settings.get("api_total_buy", 0.0) or 0.0),
                "api_total_eval": float(settings.get("api_total_eval", 0.0) or 0.0),
                "api_total_profit": float(settings.get("api_total_profit", 0.0) or 0.0),
                "api_realized_profit": float(settings.get("api_realized_profit", 0.0) or 0.0),
            }
        self.table_accounts_summary.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            account_no = str(row["account_no"] or "")
            live_summary = live_summary_map.get(account_no, {})
            account_cash = dict(account_settings_map.get(account_no) or {})
            api_deposit_cash = float(account_cash.get("deposit_cash", 0.0) or 0.0)
            api_orderable_cash = float(account_cash.get("orderable_cash", 0.0) or 0.0)
            api_estimated_assets = float(account_cash.get("estimated_assets", 0.0) or 0.0)
            api_total_buy = float(account_cash.get("api_total_buy", 0.0) or 0.0)
            api_total_eval = float(account_cash.get("api_total_eval", 0.0) or 0.0)
            api_total_profit = float(account_cash.get("api_total_profit", 0.0) or 0.0)
            api_realized_profit = float(account_cash.get("api_realized_profit", 0.0) or 0.0)
            holding_count = int(live_summary.get("holding_count", row["holding_count"] or 0) or 0)
            total_buy = api_total_buy if api_total_buy > 0 else float(live_summary.get("total_buy", 0.0) or 0.0)
            total_eval = api_total_eval if api_total_eval > 0 else float(live_summary.get("total_eval", 0.0) or 0.0)
            total_profit = api_total_profit if api_total_profit != 0 else float(row["eval_profit_total"] or 0)
            holding_profit_total = float(live_summary.get("eval_profit_total", 0.0) or 0.0)
            summary_realized_profit = float(row["realized_profit_total"] or 0)
            cycle_realized_profit = float(cycle_realized_map.get(account_no, 0.0) or 0.0)
            if api_realized_profit != 0:
                realized_profit_total = api_realized_profit
            elif api_total_profit != 0:
                realized_profit_total = api_total_profit - holding_profit_total
            elif summary_realized_profit != 0:
                realized_profit_total = summary_realized_profit
            else:
                realized_profit_total = cycle_realized_profit
            self.table_accounts_summary.setItem(
                row_index,
                0,
                self._make_table_item(self._masked_account_tail(account_no), sort_value=account_no, user_data=account_no),
            )
            self.table_accounts_summary.setItem(row_index, 1, self._make_number_item(api_deposit_cash))
            self.table_accounts_summary.setItem(row_index, 2, self._make_number_item(api_orderable_cash))
            self.table_accounts_summary.setItem(row_index, 3, self._make_number_item(total_eval))
            self.table_accounts_summary.setItem(row_index, 4, self._make_number_item(api_estimated_assets))
            self.table_accounts_summary.setItem(row_index, 5, self._make_number_item(holding_count))
            self.table_accounts_summary.setItem(row_index, 6, self._make_number_item(total_buy))
            self.table_accounts_summary.setItem(row_index, 7, self._make_number_item(total_profit, signed=True))
            self.table_accounts_summary.setItem(row_index, 8, self._make_number_item(realized_profit_total, signed=True))

        self.table_positions.setSortingEnabled(False)
        self.table_positions.setRowCount(len(position_states))
        for row_index, state in enumerate(position_states):
            account_no = str(state.get("account_no") or "")
            self.table_positions.setItem(
                row_index,
                0,
                self._make_table_item(self._masked_account_tail(account_no), sort_value=account_no, user_data=account_no),
            )
            self.table_positions.setItem(row_index, 1, self._make_table_item(str(state.get("name") or state.get("code") or "")))
            self.table_positions.setItem(row_index, 2, self._make_table_item(str(state.get("code") or "")))
            self.table_positions.setItem(row_index, 3, self._make_number_item(state.get("avg_price")))
            self.table_positions.setItem(row_index, 4, self._make_number_item(state.get("current_price")))
            self.table_positions.setItem(row_index, 5, self._make_number_item(state.get("eval_profit"), signed=True))
            self.table_positions.setItem(row_index, 6, self._make_number_item(state.get("eval_rate"), digits=2, rate=True, signed=True))
            self.table_positions.setItem(row_index, 7, self._make_number_item(int(state.get("qty") or 0)))
            self.table_positions.setItem(row_index, 8, self._make_table_item(self._build_position_strategy_text(state)))
            self.table_positions.setItem(row_index, 9, self._make_table_item(self._build_position_news_status_text(state)))
        self.table_positions.setSortingEnabled(True)

        order_rows = self.persistence.fetchall("SELECT * FROM open_orders WHERE unfilled_qty > 0 ORDER BY account_no, updated_at DESC, order_no DESC")
        self.table_open_orders.setRowCount(len(order_rows))
        for row_index, row in enumerate(order_rows):
            cycle = self.persistence.fetchone(
                "SELECT * FROM trade_cycles WHERE account_no=? AND code=? ORDER BY buy_order_at DESC LIMIT 1",
                (row["account_no"], row["code"]),
            )
            extra = self._safe_json_dict(cycle["extra_json"] if cycle else "{}")
            stage_text = self._translate_unfilled_stage(cycle["status"], extra) if cycle else "-"

            self.table_open_orders.setItem(row_index, 0, self._make_table_item(row["account_no"], sort_value=str(row["account_no"] or ""), user_data=str(row["account_no"] or "")))
            self.table_open_orders.setItem(row_index, 1, self._make_table_item(row["order_no"]))
            self.table_open_orders.setItem(row_index, 2, self._make_table_item(row["name"] or row["code"]))
            self.table_open_orders.setItem(row_index, 3, self._make_table_item(row["code"]))
            self.table_open_orders.setItem(row_index, 4, self._make_table_item(row["order_status"] or ""))
            self.table_open_orders.setItem(row_index, 5, self._make_number_item(row["order_qty"]))
            self.table_open_orders.setItem(row_index, 6, self._make_number_item(row["unfilled_qty"]))
            self.table_open_orders.setItem(row_index, 7, self._make_number_item(row["order_price"]))
            self.table_open_orders.setItem(row_index, 8, self._make_table_item(stage_text))

        if self._is_scope_tab_active():
            self.refresh_strategy_analysis()
        else:
            self._scope_refresh_pending = True

    def refresh_policy_logs(self):
        if not self._is_log_tab_active():
            self._policy_logs_refresh_pending = True
            return
        self._policy_logs_refresh_pending = False
        rows = self.persistence.fetchall("SELECT * FROM order_policy_logs ORDER BY log_id DESC LIMIT 100")
        self.table_policy_logs.setRowCount(len(rows))
        if hasattr(self, "lbl_policy_logs_empty"):
            self.lbl_policy_logs_empty.setVisible(len(rows) <= 0)
        for row_index, row in enumerate(rows):
            detail = self._safe_json_dict(row["detail_json"] or "{}")
            detail_text = []
            for key in ["first_wait_sec", "second_wait_sec", "order_no", "unfilled_qty", "new_price", "price_source", "status"]:
                if key in detail and detail.get(key) not in ["", None]:
                    detail_text.append("%s=%s" % (key, detail.get(key)))
            self.table_policy_logs.setItem(row_index, 0, QTableWidgetItem(row["ts"] or ""))
            self.table_policy_logs.setItem(row_index, 1, QTableWidgetItem(row["account_no"] or ""))
            self.table_policy_logs.setItem(row_index, 2, QTableWidgetItem(row["name"] or row["code"] or ""))
            self.table_policy_logs.setItem(row_index, 3, QTableWidgetItem(row["code"] or ""))
            self.table_policy_logs.setItem(row_index, 4, QTableWidgetItem(self._translate_unfilled_policy(row["policy"] or "")))
            self.table_policy_logs.setItem(row_index, 5, QTableWidgetItem(str(row["stage"] or "")))
            self.table_policy_logs.setItem(row_index, 6, QTableWidgetItem(str(row["action"] or "")))
            self.table_policy_logs.setItem(row_index, 7, QTableWidgetItem(" / ".join(detail_text) if detail_text else "-"))

    def _translate_watch_state(self, state):
        return {
            "DETECTED": "조건식 편입",
            "FILTERING": "매수체인 검토 중",
            "BUY_BLOCKED": "매수 제외",
            "BUY_ORDER_PENDING": "매수 주문 대기",
            "HOLDING": "보유 중",
            "SELL_ORDER_PENDING": "매도 주문 대기",
            "CLOSED": "종료",
            "ARCHIVE_READY": "보관 종료 예정",
        }.get(str(state or ""), str(state or "-"))

    def _translate_watch_spam(self, value):
        return "Y" if int(value or 0) else "N"

    def _translate_buy_block_reason(self, reason):
        mapping = {
            "뉴스 점수 부족": "뉴스 점수 부족",
            "뉴스 매매 점수 부족": "뉴스 매매 점수 부족",
            "unknown": "사유 미확인",
            "": "-",
        }
        text = str(reason or "").strip()
        return mapping.get(text, text or "-")

    def refresh_news_watch(self):
        if not self._is_news_watch_tab_active():
            self._news_watch_refresh_pending = True
            self._set_news_watch_loading(True, "?댁뒪媛먯떆 ?곗씠??以鍮?以?..")
            return
        if self._news_watch_refresh_running:
            self._news_watch_refresh_pending = True
            return
        rows = self.condition_manager.get_tracked_symbols()
        selected_code, _selected_name = self._selected_watch_symbol()
        self._news_watch_refresh_rows = list(rows or [])
        self._news_watch_refresh_index = 0
        self._news_watch_refresh_restore_code = str(selected_code or "").strip()
        self._news_watch_refresh_restore_row = -1
        self._news_watch_refresh_pending = False
        self._news_watch_refresh_running = True
        self._set_news_watch_loading(
            True,
            "?댁뒪媛먯떆 ?곗씠??濡쒕뵫 以?.. (0/{0})".format(len(self._news_watch_refresh_rows)),
        )
        self.table_news_watch.setUpdatesEnabled(False)
        self.table_news_watch.blockSignals(True)
        self.table_news_watch.setSortingEnabled(False)
        self.table_news_watch.clearContents()
        self.table_news_watch.setRowCount(len(self._news_watch_refresh_rows))
        self._news_watch_refresh_batch_timer.start(0)

    def refresh_news_watch(self):
        if not self._is_news_watch_tab_active():
            self._news_watch_refresh_pending = True
            self._set_news_watch_loading(True, "뉴스감시 데이터 준비 중...")
            return
        if self._news_watch_refresh_running:
            self._news_watch_refresh_pending = True
            return
        rows = self.condition_manager.get_tracked_symbols()
        selected_code, _selected_name = self._selected_watch_symbol()
        self._news_watch_refresh_rows = list(rows or [])
        self._news_watch_refresh_index = 0
        self._news_watch_refresh_restore_code = str(selected_code or "").strip()
        self._news_watch_refresh_restore_row = -1
        self._news_watch_refresh_pending = False
        self._news_watch_refresh_running = True
        self._set_news_watch_loading(
            True,
            "뉴스감시 데이터 로딩 중... (0/{0})".format(len(self._news_watch_refresh_rows)),
        )
        self.table_news_watch.setUpdatesEnabled(False)
        self.table_news_watch.blockSignals(True)
        self.table_news_watch.setSortingEnabled(False)
        self.table_news_watch.clearContents()
        self.table_news_watch.setRowCount(len(self._news_watch_refresh_rows))
        self._news_watch_refresh_batch_timer.start(0)

    def refresh_news_watch(self):
        if not self._is_news_watch_tab_active():
            self._news_watch_refresh_pending = True
            self._set_news_watch_loading(True, "뉴스감시 데이터 준비 중...")
            return
        if self._news_watch_refresh_running:
            self._news_watch_refresh_pending = True
            return
        rows = self.condition_manager.get_tracked_symbols()
        selected_code, _selected_name = self._selected_watch_symbol()
        self._news_watch_refresh_rows = list(rows or [])
        self._news_watch_refresh_index = 0
        self._news_watch_refresh_restore_code = str(selected_code or "").strip()
        self._news_watch_refresh_restore_row = -1
        self._news_watch_refresh_pending = False
        self._news_watch_refresh_running = True
        self._set_news_watch_loading(
            True,
            "뉴스감시 데이터 로딩 중... (0/{0})".format(len(self._news_watch_refresh_rows)),
        )
        self.table_news_watch.setUpdatesEnabled(False)
        self.table_news_watch.blockSignals(True)
        self.table_news_watch.setSortingEnabled(False)
        self.table_news_watch.clearContents()
        self.table_news_watch.setRowCount(len(self._news_watch_refresh_rows))
        self._news_watch_refresh_batch_timer.start(0)

    def _news_watch_loading_message(self, progress=None, total=None, pending=False):
        if progress is not None and total is not None:
            return "뉴스감시 데이터 로딩 중... ({0}/{1})".format(int(progress), int(total))
        if pending:
            return "뉴스감시 데이터 준비 중..."
        return "뉴스감시 데이터 로딩 중..."

    def _set_news_watch_loading(self, visible, message=None):
        label = (
            getattr(self, "news_watch_loading_label_actual", None)
            or getattr(self, "news_watch_loading_label", None)
            or getattr(self, "lbl_news_watch_loading", None)
        )
        if label is None:
            return
        label.setText(str(message or self._news_watch_loading_message()) if visible else "")
        label.setVisible(True)

    def _schedule_refresh_news_watch(self, delay_ms=400):
        self._news_watch_refresh_pending = True
        if self._news_watch_refresh_running:
            return
        if not self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, self._news_watch_loading_message(pending=True))
            return
        if self._refresh_news_watch_timer.isActive():
            return
        self._set_news_watch_loading(True, self._news_watch_loading_message())
        self._refresh_news_watch_timer.start(max(160, int(delay_ms or 400)))

    def _finalize_news_watch_refresh(self):
        try:
            if self._news_watch_refresh_restore_row >= 0:
                self.table_news_watch.selectRow(self._news_watch_refresh_restore_row)
        finally:
            self.table_news_watch.blockSignals(False)
            self.table_news_watch.setUpdatesEnabled(True)
            self._news_watch_refresh_running = False
        if not self._news_watch_rows_sized:
            self._news_watch_rows_sized = True
        self._schedule_refresh_realtime_strategy_reference_labels(80)
        if self._news_watch_refresh_pending and self._is_news_watch_tab_active():
            self._set_news_watch_loading(True, self._news_watch_loading_message())
            QTimer.singleShot(80, lambda: self._schedule_refresh_news_watch(80))
            return
        self._set_news_watch_loading(False)
        QTimer.singleShot(0, self._maybe_queue_news_watch_next_batch)

    def _pause_news_watch_refresh(self):
        if self._refresh_news_watch_timer.isActive():
            self._refresh_news_watch_timer.stop()
        if self._news_watch_refresh_batch_timer.isActive():
            self._news_watch_refresh_batch_timer.stop()
        if self._news_watch_refresh_running:
            self._news_watch_refresh_running = False
            self._news_watch_refresh_pending = True
            self.table_news_watch.blockSignals(False)
            self.table_news_watch.setUpdatesEnabled(True)
        self._set_news_watch_loading(False)

    def _on_news_watch_scroll_changed(self, _value):
        if not self._is_news_watch_tab_active():
            return
        self._maybe_queue_news_watch_next_batch()

    def _maybe_queue_news_watch_next_batch(self):
        if self._news_watch_refresh_running:
            return
        total_count = len(self._news_watch_refresh_rows or [])
        loaded_count = int(self._news_watch_refresh_index or 0)
        if loaded_count >= total_count:
            return
        scrollbar = self.table_news_watch.verticalScrollBar()
        if scrollbar is None:
            return
        remaining = int(scrollbar.maximum() - scrollbar.value())
        if remaining > int(self._news_watch_scroll_threshold or 3):
            return
        self._news_watch_refresh_running = True
        self.table_news_watch.setUpdatesEnabled(False)
        self.table_news_watch.blockSignals(True)
        self._news_watch_refresh_batch_timer.start(0)

    def _process_news_watch_refresh_batch(self):
        if not self._news_watch_refresh_running:
            return
        if not self._is_news_watch_tab_active():
            self._pause_news_watch_refresh()
            return
        rows = list(self._news_watch_refresh_rows or [])
        total_count = len(rows)
        if total_count <= 0:
            self._finalize_news_watch_refresh()
            return
        start_index = int(self._news_watch_refresh_index or 0)
        batch_size = int(self._news_watch_batch_size or 10)
        if start_index <= 0:
            batch_size = max(batch_size, int(self._news_watch_initial_batch_size or 10))
        end_index = min(start_index + batch_size, total_count)
        current_row_count = self.table_news_watch.rowCount()
        if current_row_count < end_index:
            self.table_news_watch.setRowCount(end_index)
        for row_index in range(start_index, end_index):
            self._populate_news_watch_row(row_index, rows[row_index])
            if self._news_watch_refresh_restore_row < 0 and self._news_watch_refresh_restore_code:
                row_code = str(rows[row_index].get("code") or "").strip()
                if row_code and row_code == self._news_watch_refresh_restore_code:
                    self._news_watch_refresh_restore_row = row_index
        self._news_watch_refresh_index = end_index
        self._set_news_watch_loading(True, self._news_watch_loading_message(end_index, total_count))
        self._finalize_news_watch_refresh()

    def _fetch_realtime_capture_log_rows(self, limit_count=None):
        limit_count = max(1, int(limit_count or self._realtime_capture_log_max_rows or 3))
        rows = self.persistence.fetchall(
            """
            SELECT
                se.event_id,
                se.ts,
                se.code,
                se.name,
                se.source_condition_slot,
                se.source_condition_name,
                se.payload_json,
                ts.current_state,
                ts.detected_price
            FROM symbol_events se
            LEFT JOIN tracked_symbols ts ON ts.code = se.code
            WHERE se.event_type = 'condition_enter'
            ORDER BY se.event_id DESC
            LIMIT ?
            """,
            (max(limit_count * 4, limit_count),),
        )
        return list(reversed(list(rows or [])))

    def _capture_log_price_value(self, fallback_price=0.0):
        try:
            return float(fallback_price or 0.0)
        except Exception:
            return 0.0

    def _translate_capture_log_result(self, current_state, event_type="condition_enter"):
        state = str(current_state or "").strip().upper()
        event_type = str(event_type or "").strip().lower()
        mapping = {
            "DETECTED": "검토 중",
            "BUY_BLOCKED": "매수 제외",
            "BUY_REJECTED": "매수 거부",
            "BUY_PENDING": "매수 진행",
            "BUY_REQUESTED": "매수 진행",
            "BUY_SUBMITTED": "매수 진행",
            "BUY_ORDER_PENDING": "매수 진행",
            "BUY_ORDERED": "매수 완료",
            "BUY_PARTIAL": "매수 진행",
            "SIMULATED_HOLDING": "매수 완료",
            "HOLDING": "매수 완료",
            "ARCHIVE_READY": "이탈 감지",
            "CLOSED": "종료",
        }
        if state in mapping:
            return mapping.get(state, state)
        if event_type == "condition_snapshot":
            return "재검토"
        return "검토 중"

    def _resolve_capture_log_strategy_text(self, slot_no, condition_name):
        try:
            slot_no = int(slot_no or 0)
        except Exception:
            slot_no = 0
        base_text = str(condition_name or "-").strip() or "-"
        if slot_no <= 0:
            return base_text
        try:
            policy = dict(self.strategy_manager.resolve_slot_strategy_policy(slot_no) or {})
        except Exception:
            policy = {}
        source = str(policy.get("source") or "").strip().lower()
        preview = "-"
        try:
            buy_items = json.loads(policy.get("buy_expression_json") or "[]")
        except Exception:
            buy_items = []
        try:
            preview = self._format_expression_preview("buy", buy_items, empty_text=u"전략없음")
        except Exception:
            preview = u"전략없음"
        if source == "slot":
            source_label = u"슬롯"
        elif source == "default":
            source_label = u"기본"
        else:
            source_label = u"미정"
        return u"{0} / {1} / {2}".format(base_text, source_label, preview)

    def _format_capture_log_time_text(self, value):
        text = str(value or "").strip()
        if not text:
            return "-"
        if " " in text:
            text = text.split(" ")[-1]
        if "." in text:
            text = text.split(".", 1)[0]
        if len(text) >= 8 and ":" in text:
            return text[:8]
        return text

    def _build_realtime_capture_log_row_data(self, raw_row):
        raw_row = dict(raw_row or {})
        payload = {}
        try:
            payload = json.loads(raw_row.get("payload_json") or "{}")
        except Exception:
            payload = {}
        code = str(raw_row.get("code") or payload.get("code") or "").strip()
        detected_price = raw_row.get("detected_price") or 0.0
        condition_name = str(raw_row.get("source_condition_name") or payload.get("condition_name") or "-")
        current_price = self._capture_log_price_value(detected_price)
        return {
            "name": str(raw_row.get("name") or payload.get("name") or code),
            "code": code,
            "current_price": current_price,
            "result": self._translate_capture_log_result(raw_row.get("current_state"), payload.get("event_type") or "condition_enter"),
            "condition_strategy": self._resolve_capture_log_strategy_text(raw_row.get("source_condition_slot"), condition_name),
            "detected_at": self._format_capture_log_time_text(raw_row.get("ts") or payload.get("ts") or ""),
        }

    def _compress_realtime_capture_log_rows(self, rows):
        compressed = []
        last_key = None
        for row in list(rows or []):
            row = dict(row or {})
            key = (
                str(row.get("code") or "").strip(),
                str(row.get("condition_strategy") or "").strip(),
            )
            if key == last_key:
                if compressed:
                    compressed[-1] = row
                continue
            compressed.append(row)
            last_key = key
        return compressed

    def _populate_realtime_capture_log_row(self, row_index, row_data):
        table = getattr(self, "table_realtime_capture_log", None)
        if table is None:
            return
        row_data = dict(row_data or {})
        columns = [
            str(row_data.get("name") or ""),
            str(row_data.get("code") or ""),
            "{0:,.0f}".format(float(row_data.get("current_price") or 0.0)) if float(row_data.get("current_price") or 0.0) > 0 else "-",
            str(row_data.get("result") or "-"),
            str(row_data.get("condition_strategy") or "-"),
            str(row_data.get("detected_at") or "-"),
        ]
        for col_index, value_text in enumerate(columns):
            item = table.item(row_index, col_index)
            if item is None:
                item = QTableWidgetItem("")
                if col_index == 2:
                    item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
                elif col_index in [3, 5]:
                    item.setTextAlignment(int(Qt.AlignCenter))
                table.setItem(row_index, col_index, item)
            item.setText(str(value_text))
            item.setForeground(QColor("#f5d36a"))
            if col_index == 1:
                item.setData(Qt.UserRole, str(row_data.get("code") or ""))
        result_text = str(row_data.get("result") or "")
        result_item = table.item(row_index, 3)
        if result_item is not None:
            if result_text == "매수 완료":
                result_item.setForeground(QColor("#4da3ff"))
            elif result_text in ["매수 진행", "검토 중", "재검토"]:
                result_item.setForeground(QColor("#f5d36a"))
            elif result_text in ["매수 제외", "매수 거부", "이탈 감지", "종료"]:
                result_item.setForeground(QColor("#ff6a5a"))

    def _clear_realtime_capture_log_row(self, row_index):
        table = getattr(self, "table_realtime_capture_log", None)
        if table is None:
            return
        for col_index in range(table.columnCount()):
            item = table.item(row_index, col_index)
            if item is None:
                item = QTableWidgetItem("")
                if col_index == 2:
                    item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
                elif col_index in [3, 5]:
                    item.setTextAlignment(int(Qt.AlignCenter))
                table.setItem(row_index, col_index, item)
            item.setText("")
            item.setData(Qt.UserRole, "")
            item.setForeground(QColor("#f5d36a"))

    def _render_realtime_capture_log_rows(self, rows):
        table = getattr(self, "table_realtime_capture_log", None)
        if table is None:
            return
        visible_rows = int(self._realtime_capture_log_max_rows or 3)
        recent_rows = list(rows or [])[-visible_rows:]
        table.setUpdatesEnabled(False)
        try:
            for row_index in range(visible_rows):
                self._clear_realtime_capture_log_row(row_index)
            start_row = max(0, visible_rows - len(recent_rows))
            for offset, row_data in enumerate(recent_rows):
                self._populate_realtime_capture_log_row(start_row + offset, row_data)
        finally:
            table.setUpdatesEnabled(True)

    def _append_realtime_capture_log_row(self, row_data):
        table = getattr(self, "table_realtime_capture_log", None)
        if table is None:
            return
        row_count = int(self._realtime_capture_log_max_rows or 3)
        table.setUpdatesEnabled(False)
        try:
            for row_index in range(0, row_count - 1):
                for col_index in range(table.columnCount()):
                    current_item = table.item(row_index, col_index)
                    next_item = table.item(row_index + 1, col_index)
                    if current_item is None:
                        current_item = QTableWidgetItem("")
                        table.setItem(row_index, col_index, current_item)
                    current_item.setText(next_item.text() if next_item is not None else "")
                    current_item.setData(Qt.UserRole, next_item.data(Qt.UserRole) if next_item is not None else "")
                    current_item.setForeground(next_item.foreground() if next_item is not None else QColor("#f5d36a"))
            self._populate_realtime_capture_log_row(row_count - 1, row_data)
        finally:
            table.setUpdatesEnabled(True)

    def refresh_realtime_capture_log(self):
        rows = [self._build_realtime_capture_log_row_data(row) for row in self._fetch_realtime_capture_log_rows()]
        rows = self._compress_realtime_capture_log_rows(rows)
        self._render_realtime_capture_log_rows(rows)

    def _on_realtime_capture_log_symbol_detected(self, payload):
        if str((payload or {}).get("event_type") or "") != "condition_enter":
            return
        if not self._is_realtime_reference_tab_active():
            return
        symbol_row = self.persistence.fetchone(
            "SELECT current_state, detected_price FROM tracked_symbols WHERE code=?",
            (str(payload.get("code") or "").strip(),),
        )
        row_data = {
            "name": str(payload.get("name") or payload.get("code") or ""),
            "code": str(payload.get("code") or ""),
            "current_price": self._capture_log_price_value(symbol_row["detected_price"] if symbol_row else 0.0),
            "result": self._translate_capture_log_result(
                symbol_row["current_state"] if symbol_row else "",
                payload.get("event_type") or "condition_enter",
            ),
            "condition_strategy": self._resolve_capture_log_strategy_text(
                payload.get("slot_no"),
                payload.get("condition_name"),
            ),
            "detected_at": self._format_capture_log_time_text(payload.get("ts") or self.persistence.now_ts()),
        }
        table = getattr(self, "table_realtime_capture_log", None)
        if table is not None:
            last_row_index = max(0, int(self._realtime_capture_log_max_rows or 3) - 1)
            last_code_item = table.item(last_row_index, 1)
            last_strategy_item = table.item(last_row_index, 4)
            last_code = str(last_code_item.text() if last_code_item is not None else "").strip()
            last_strategy = str(last_strategy_item.text() if last_strategy_item is not None else "").strip()
            if last_code == str(row_data.get("code") or "").strip() and last_strategy == str(row_data.get("condition_strategy") or "").strip():
                self._populate_realtime_capture_log_row(last_row_index, row_data)
                return
        self._append_realtime_capture_log_row(row_data)

    def _on_right_tab_changed(self, index):
        current_widget = self.right_tabs.widget(int(index)) if hasattr(self, "right_tabs") else None
        if current_widget != getattr(self, "news_watch_tab_widget", None):
            self._pause_news_watch_refresh()
        if current_widget == getattr(self, "realtime_reference_tab_widget", None):
            self._refresh_realtime_reference_table()
            self.refresh_realtime_capture_log()
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        if current_widget == getattr(self, "news_watch_tab_widget", None):
            if self._news_watch_refresh_pending and not self._refresh_news_watch_timer.isActive():
                self._set_news_watch_loading(True, self._news_watch_loading_message())
                QTimer.singleShot(40, lambda: self._schedule_refresh_news_watch(120))
            self._schedule_refresh_realtime_strategy_reference_labels(80)
            return
        if current_widget == getattr(self, "operations_tab_widget", None):
            if self._operations_refresh_pending and not self._refresh_operations_timer.isActive():
                QTimer.singleShot(30, lambda: self._schedule_refresh_operations(80))
            return
        if current_widget == getattr(self, "scope_tab_widget", None):
            if self._scope_refresh_pending:
                QTimer.singleShot(30, self.refresh_strategy_analysis)
            return
        if current_widget == getattr(self, "log_tab_widget", None):
            if self._policy_logs_refresh_pending and not self._refresh_policy_logs_timer.isActive():
                QTimer.singleShot(30, lambda: self._schedule_refresh_policy_logs(80))
            return
        self._set_news_watch_loading(False)

    def refresh_news_watch(self):
        if not self._is_news_watch_tab_active():
            self._news_watch_refresh_pending = True
            self._set_news_watch_loading(True, self._news_watch_loading_message(pending=True))
            return
        if self._news_watch_refresh_running:
            self._news_watch_refresh_pending = True
            return
        rows = self.condition_manager.get_tracked_symbols()
        selected_code, _selected_name = self._selected_watch_symbol()
        self._news_watch_refresh_rows = list(rows or [])
        self._news_watch_refresh_index = 0
        self._news_watch_refresh_restore_code = str(selected_code or "").strip()
        self._news_watch_refresh_restore_row = -1
        self._news_watch_refresh_pending = False
        self._news_watch_refresh_running = True
        self._set_news_watch_loading(True, self._news_watch_loading_message(0, len(self._news_watch_refresh_rows)))
        self.table_news_watch.setUpdatesEnabled(False)
        self.table_news_watch.blockSignals(True)
        self.table_news_watch.setSortingEnabled(False)
        self.table_news_watch.clearContents()
        self.table_news_watch.setRowCount(0)
        self._news_watch_refresh_batch_timer.start(0)

    def _selected_watch_symbol(self):
        selected = self.table_news_watch.selectionModel().selectedRows()
        if not selected:
            return None, None
        row = selected[0].row()
        code_item = self.table_news_watch.item(row, 1)
        name_item = self.table_news_watch.item(row, 0)
        code = code_item.data(Qt.UserRole) if code_item else ""
        name = name_item.text() if name_item else ""
        return (code or (code_item.text() if code_item else ""), name)

    def _is_regular_market_hours(self, now_dt=None):
        now_dt = now_dt or datetime.datetime.now()
        hhmm = now_dt.strftime("%H%M")
        return now_dt.weekday() < 5 and "0900" <= hhmm <= "1530"

    def _format_short_datetime(self, value):
        text = str(value or "").strip()
        if not text:
            return "-"
        normalized = text.replace("T", " ")
        for pattern in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S", "%Y%m%d %H:%M:%S"]:
            try:
                parsed = datetime.datetime.strptime(normalized, pattern)
                return parsed.strftime("%m-%d %H:%M:%S")
            except Exception:
                continue
        if len(normalized) >= 16 and normalized[4] == "-" and normalized[7] == "-":
            try:
                return normalized[5:16]
            except Exception:
                pass
        return text

    def _has_missing_watch_snapshot_values(self, snapshot):
        snapshot = dict(snapshot or {})
        current_price = float(snapshot.get("current_price") or 0)
        current_volume = float(snapshot.get("acc_volume") or snapshot.get("current_volume") or 0)
        current_turnover = float(snapshot.get("acc_turnover") or snapshot.get("current_turnover") or 0)
        vwap_intraday = float(snapshot.get("vwap_intraday") or 0)
        return current_price <= 0 or current_volume <= 0 or current_turnover <= 0 or vwap_intraday <= 0

    def _news_watch_snapshot_provider(self, code):
        return self._merged_realtime_snapshot(
            code,
            self.strategy_manager.get_realtime_market_snapshot(code),
        )

    def _ensure_news_watch_daily_snapshot(self, code, request_on_missing=False):
        code = str(code or "").strip()
        if not code or self.daily_watch_snapshot_manager is None:
            return {}
        now_dt = datetime.datetime.now()
        tracked_row = self.persistence.fetchone("SELECT * FROM tracked_symbols WHERE code=?", (code,))
        if not tracked_row:
            return {}

        daily_snapshot = dict(
            self.daily_watch_snapshot_manager.get_live_snapshot(code, target_dt=now_dt) or {}
        )
        if not self._has_missing_watch_snapshot_values(daily_snapshot):
            return daily_snapshot

        live_snapshot = self._news_watch_snapshot_provider(code)
        if request_on_missing and hasattr(self.kiwoom_client, "request_quote_snapshot"):
            try:
                quote_snapshot = dict(self.kiwoom_client.request_quote_snapshot(code, timeout_ms=1200) or {})
            except Exception as exc:
                quote_snapshot = {}
                self.append_log("⚠️ 뉴스감시 단건 시세 보강 실패: {0} / {1}".format(code, exc))
            for key in ["current_price", "ask1", "current_volume", "current_turnover"]:
                try:
                    current_value = float(live_snapshot.get(key) or 0)
                except Exception:
                    current_value = 0.0
                try:
                    quote_value = float(quote_snapshot.get(key) or 0)
                except Exception:
                    quote_value = 0.0
                if current_value <= 0 and quote_value > 0:
                    live_snapshot[key] = quote_value
            if float(live_snapshot.get("current_price") or 0) <= 0 and hasattr(self.kiwoom_client, "get_master_last_price"):
                try:
                    master_last_price = float(self.kiwoom_client.get_master_last_price(code) or 0)
                except Exception:
                    master_last_price = 0.0
                if master_last_price > 0:
                    live_snapshot["current_price"] = master_last_price
        entry = self.daily_watch_snapshot_manager.capture_symbol(
            tracked_row=dict(tracked_row),
            live_snapshot=live_snapshot,
            source="after_hours_click_fill" if request_on_missing else "after_hours_preview",
            target_dt=now_dt,
        )
        return dict(entry.get("live_snapshot") or {})

    def _merged_realtime_snapshot(self, code, base_snapshot=None):
        snapshot = dict(base_snapshot or {})
        code = str(code or "").strip()
        if code and hasattr(self.kiwoom_client, "get_enriched_realtime_snapshot"):
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
        fallback = {}
        if code and hasattr(self.kiwoom_client, "get_realtime_snapshot"):
            try:
                fallback = dict(self.kiwoom_client.get_realtime_snapshot(code) or {})
            except Exception:
                fallback = {}
        for key in [
            "current_price",
            "acc_volume",
            "acc_turnover",
            "current_volume",
            "current_turnover",
            "sell_hoga_total",
            "buy_hoga_total",
            "vwap_intraday",
            "sell_pressure_ratio",
        ]:
            try:
                current_value = float(snapshot.get(key) or 0)
            except Exception:
                current_value = 0.0
            try:
                fallback_value = float(fallback.get(key) or 0)
            except Exception:
                fallback_value = 0.0
            if current_value <= 0 and fallback_value > 0:
                snapshot[key] = fallback_value

        acc_volume = float(snapshot.get("acc_volume") or 0)
        acc_turnover = float(snapshot.get("acc_turnover") or 0)
        if float(snapshot.get("vwap_intraday") or 0) <= 0 and acc_volume > 0 and acc_turnover > 0:
            snapshot["vwap_intraday"] = float(acc_turnover / acc_volume)

        sell_hoga_total = float(snapshot.get("sell_hoga_total") or 0)
        buy_hoga_total = float(snapshot.get("buy_hoga_total") or 0)
        if float(snapshot.get("sell_pressure_ratio") or 0) <= 0 and sell_hoga_total > 0 and buy_hoga_total > 0:
            snapshot["sell_pressure_ratio"] = round(float(sell_hoga_total / buy_hoga_total), 4)

        return snapshot

    def _on_news_watch_selection_changed(self):
        code, _name = self._selected_watch_symbol()
        code = str(code or "").strip()
        if code and self.daily_watch_snapshot_manager is not None and not self._is_regular_market_hours():
            self._schedule_news_watch_after_hours_fill(code)
        else:
            self._schedule_news_watch_after_hours_fill("")
        self._schedule_refresh_realtime_strategy_reference_labels(40)

    def _resolve_realtime_preview_target(self):
        now_dt = datetime.datetime.now()
        code, name = self._selected_watch_symbol()
        code = str(code or "").strip()
        if code:
            if self.daily_watch_snapshot_manager is not None and not self._is_regular_market_hours(now_dt):
                try:
                    daily_snapshot = self._ensure_news_watch_daily_snapshot(code, request_on_missing=False)
                    if daily_snapshot:
                        return code, str(name or code), daily_snapshot
                except Exception:
                    pass
            snapshot = self._merged_realtime_snapshot(
                code,
                self.strategy_manager.get_realtime_market_snapshot(code),
            )
            return code, str(name or code), snapshot
        if self._is_news_watch_tab_active():
            return "", "", {}
        manager = getattr(self.strategy_manager, "realtime_market_state_manager", None)
        if manager is None:
            return "", "", {}
        snapshots = manager.get_snapshots()
        if not snapshots:
            return "", "", {}
        snapshots = sorted(snapshots, key=lambda x: str(x.get("updated_at") or ""), reverse=True)
        snapshot = dict(snapshots[0] or {})
        code = str(snapshot.get("code") or "")
        row = self.persistence.fetchone("SELECT name FROM tracked_symbols WHERE code=?", (code,)) if code else None
        name = str((row["name"] if row else "") or code)
        return code, name, self._merged_realtime_snapshot(code, snapshot)

    def _format_realtime_reference_text(self):
        code, name, snapshot = self._resolve_realtime_preview_target()
        if not code:
            return "실시간 참고값 : 뉴스감시에서 종목을 선택하세요"
        current_price = float((snapshot or {}).get("current_price") or 0)
        acc_volume = float((snapshot or {}).get("acc_volume") or 0)
        acc_turnover = float((snapshot or {}).get("acc_turnover") or 0)
        display_volume = float((snapshot or {}).get("acc_volume") or (snapshot or {}).get("current_volume") or 0)
        display_turnover = float((snapshot or {}).get("acc_turnover") or (snapshot or {}).get("current_turnover") or 0)
        vwap_intraday = float((snapshot or {}).get("vwap_intraday") or 0)
        if vwap_intraday <= 0 and acc_volume > 0 and acc_turnover > 0:
            vwap_intraday = float(acc_turnover / acc_volume)
        sell_pressure_ratio = float((snapshot or {}).get("sell_pressure_ratio") or 0)
        return (
            u"실시간 참고값 [{0} {1}] 현재가:{2:,.0f} / VWAP:{3:,.0f} / 매도우위:{4:.2f} / 거래량:{5:,.0f} / 거래대금:{6:,.0f}".format(
                name,
                code,
                current_price,
                vwap_intraday,
                sell_pressure_ratio,
                display_volume,
                display_turnover,
            )
        )

    def _format_realtime_reference_html(self):
        code, name, snapshot = self._resolve_realtime_preview_target()
        if not code:
            return "실시간 참고값 : 뉴스감시에서 종목을 선택하세요"
        current_price = float((snapshot or {}).get("current_price") or 0)
        acc_volume = float((snapshot or {}).get("acc_volume") or 0)
        acc_turnover = float((snapshot or {}).get("acc_turnover") or 0)
        display_volume = float((snapshot or {}).get("acc_volume") or (snapshot or {}).get("current_volume") or 0)
        display_turnover = float((snapshot or {}).get("acc_turnover") or (snapshot or {}).get("current_turnover") or 0)
        vwap_intraday = float((snapshot or {}).get("vwap_intraday") or 0)
        if vwap_intraday <= 0 and acc_volume > 0 and acc_turnover > 0:
            vwap_intraday = float(acc_turnover / acc_volume)
        sell_pressure_ratio = float((snapshot or {}).get("sell_pressure_ratio") or 0)
        vwap_color = "#111111"
        if current_price > 0 and vwap_intraday > 0:
            vwap_color = "#0057b8" if vwap_intraday > current_price else "#c62828"
        current_price_text = "{0:,.0f}".format(current_price) if current_price > 0 else "-"
        vwap_text = "{0:,.0f}".format(vwap_intraday) if vwap_intraday > 0 else "-"
        volume_text = "{0:,.0f}".format(display_volume) if display_volume > 0 else "-"
        turnover_text = "{0:,.0f}".format(display_turnover) if display_turnover > 0 else "-"
        sell_pressure_text = "{0:.2f}".format(sell_pressure_ratio) if sell_pressure_ratio > 0 else "-"
        return (
            "실시간 참고값 "
            "[{0} {1}] "
            "현재가:{2} / "
            "VWAP:<span style=\"color:{3}; font-weight:700;\">{4}</span> / "
            "매도우위:{5} / "
            "거래량:{6} / "
            "거래대금:{7}".format(
                escape(str(name or code)),
                escape(str(code)),
                current_price_text,
                vwap_color,
                vwap_text,
                sell_pressure_text,
                volume_text,
                turnover_text,
            )
        )

    def _refresh_realtime_reference_table(self):
        table = getattr(self, "table_realtime_reference", None)
        if table is None:
            return
        code, name, snapshot = self._resolve_realtime_preview_target()
        current_price = float((snapshot or {}).get("current_price") or 0)
        acc_volume = float((snapshot or {}).get("acc_volume") or 0)
        acc_turnover = float((snapshot or {}).get("acc_turnover") or 0)
        current_volume = float((snapshot or {}).get("current_volume") or 0)
        current_turnover = float((snapshot or {}).get("current_turnover") or 0)
        vwap_intraday = float((snapshot or {}).get("vwap_intraday") or 0)
        sell_pressure_ratio = float((snapshot or {}).get("sell_pressure_ratio") or 0)
        sell_hoga_total = float((snapshot or {}).get("sell_hoga_total") or 0)
        buy_hoga_total = float((snapshot or {}).get("buy_hoga_total") or 0)
        updated_at = self._format_short_datetime((snapshot or {}).get("updated_at") or "")
        if not code:
            rows = [("상태", "뉴스감시에서 종목을 선택하세요")]
        else:
            rows = [
                ("종목명", str(name or code)),
                ("종목코드", str(code)),
                ("현재가", "{0:,.0f}".format(current_price) if current_price > 0 else "-"),
                ("VWAP", "{0:,.0f}".format(vwap_intraday) if vwap_intraday > 0 else "-"),
                ("매도우위", "{0:.2f}".format(sell_pressure_ratio) if sell_pressure_ratio > 0 else "-"),
                ("누적거래량", "{0:,.0f}".format(acc_volume) if acc_volume > 0 else (("{0:,.0f}".format(current_volume)) if current_volume > 0 else "-")),
                ("누적거래대금", "{0:,.0f}".format(acc_turnover) if acc_turnover > 0 else (("{0:,.0f}".format(current_turnover)) if current_turnover > 0 else "-")),
                ("매도호가합", "{0:,.0f}".format(sell_hoga_total) if sell_hoga_total > 0 else "-"),
                ("매수호가합", "{0:,.0f}".format(buy_hoga_total) if buy_hoga_total > 0 else "-"),
                ("업데이트시각", updated_at or "-"),
            ]
        table.setRowCount(len(rows))
        for row_index, (label_text, value_text) in enumerate(rows):
            label_item = QTableWidgetItem(str(label_text))
            value_item = QTableWidgetItem(str(value_text))
            if row_index >= 2:
                value_item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
            if label_text == "VWAP" and current_price > 0 and vwap_intraday > 0:
                value_item.setForeground(QColor("#0057b8") if vwap_intraday > current_price else QColor("#c62828"))
            table.setItem(row_index, 0, label_item)
            table.setItem(row_index, 1, value_item)

    def _record_realtime_reference_snapshot(self, payload=None):
        payload = dict(payload or {})
        code = str(payload.get("code") or "").strip()
        if not code:
            return
        snapshot = self._merged_realtime_snapshot(code, payload)
        now_epoch = float(datetime.datetime.now().timestamp())
        board_count = int(self._realtime_reference_board_count or 0)
        existing_row = self._realtime_reference_board_code_map.get(code)
        if existing_row is not None:
            previous = dict(self._realtime_reference_board_rows[existing_row] or {})
            name = str(previous.get("name") or "")
            row_index = int(existing_row)
            slot_assigned_at = float(previous.get("slot_assigned_at") or now_epoch)
        elif board_count < 10:
            row_index = board_count
            name = self._lookup_realtime_reference_name(code)
            self._realtime_reference_board_code_map[code] = row_index
            self._realtime_reference_board_count = min(10, board_count + 1)
            if int(self._realtime_reference_board_count or 0) >= 10:
                self._realtime_reference_board_index = 0
            slot_assigned_at = now_epoch
        else:
            row_index = int(self._realtime_reference_board_index or 0) % 10
            previous = dict(self._realtime_reference_board_rows[row_index] or {})
            slot_assigned_at = float(previous.get("slot_assigned_at") or 0)
            if slot_assigned_at > 0 and (now_epoch - slot_assigned_at) < float(self._realtime_reference_min_hold_sec or 10.0):
                self._enqueue_pending_realtime_reference_snapshot(code, payload)
                return
            old_code = str(previous.get("code") or "").strip()
            if old_code:
                self._realtime_reference_board_code_map.pop(old_code, None)
            name = self._lookup_realtime_reference_name(code)
            self._realtime_reference_board_code_map[code] = row_index
            self._realtime_reference_board_index = (row_index + 1) % 10
            slot_assigned_at = now_epoch

        persisted_updated_at = str(snapshot.get("updated_at") or self.persistence.now_ts())
        if self.daily_watch_snapshot_manager is not None:
            try:
                self.daily_watch_snapshot_manager.capture_realtime_reference(
                    code=code,
                    name=str(name or code),
                    live_snapshot={
                        "current_price": float(snapshot.get("current_price") or 0),
                        "vwap_intraday": float(snapshot.get("vwap_intraday") or 0),
                        "sell_pressure_ratio": float(snapshot.get("sell_pressure_ratio") or 0),
                        "acc_volume": float(snapshot.get("acc_volume") or snapshot.get("current_volume") or 0),
                        "acc_turnover": float(snapshot.get("acc_turnover") or snapshot.get("current_turnover") or 0),
                        "sell_hoga_total": float(snapshot.get("sell_hoga_total") or 0),
                        "buy_hoga_total": float(snapshot.get("buy_hoga_total") or 0),
                        "updated_at": persisted_updated_at,
                    },
                    target_dt=datetime.datetime.now(),
                )
            except Exception:
                pass

        self._realtime_reference_board_rows[row_index] = {
            "name": str(name or code),
            "code": code,
            "current_price": float(snapshot.get("current_price") or 0),
            "vwap_intraday": float(snapshot.get("vwap_intraday") or 0),
            "sell_pressure_ratio": float(snapshot.get("sell_pressure_ratio") or 0),
            "acc_volume": float(snapshot.get("acc_volume") or snapshot.get("current_volume") or 0),
            "acc_turnover": float(snapshot.get("acc_turnover") or snapshot.get("current_turnover") or 0),
            "sell_hoga_total": float(snapshot.get("sell_hoga_total") or 0),
            "buy_hoga_total": float(snapshot.get("buy_hoga_total") or 0),
            "updated_at": persisted_updated_at,
            "slot_assigned_at": float(slot_assigned_at or now_epoch),
        }
        self._realtime_reference_pending_snapshots.pop(code, None)
        self._realtime_reference_pending_codes = [queued_code for queued_code in self._realtime_reference_pending_codes if str(queued_code or "").strip() != code]
        self._schedule_realtime_reference_board_refresh(row_index=row_index)

    def _lookup_realtime_reference_name(self, code):
        code = str(code or "").strip()
        if not code:
            return ""
        cached = str(self._realtime_reference_name_cache.get(code) or "").strip()
        if cached:
            return cached
        row = self.persistence.fetchone("SELECT name FROM tracked_symbols WHERE code=?", (code,))
        name = str((row["name"] if row else "") or code)
        self._realtime_reference_name_cache[code] = name
        return name

    def _schedule_realtime_reference_board_refresh(self, row_index=None, force_full=False):
        if force_full:
            self._realtime_reference_full_refresh_pending = True
        elif row_index is not None:
            row_index = int(row_index)
            if row_index not in self._realtime_reference_dirty_row_set:
                self._realtime_reference_dirty_row_set.add(row_index)
                self._realtime_reference_dirty_rows.append(row_index)
        if self._is_realtime_reference_tab_active() and not self._realtime_reference_flush_timer.isActive():
            self._realtime_reference_flush_timer.start(80)

    def _enqueue_pending_realtime_reference_snapshot(self, code, payload):
        code = str(code or "").strip()
        if not code:
            return
        payload = dict(payload or {})
        if code not in self._realtime_reference_pending_snapshots:
            self._realtime_reference_pending_codes.append(code)
        self._realtime_reference_pending_snapshots[code] = payload
        if not self._realtime_reference_rotation_timer.isActive():
            self._realtime_reference_rotation_timer.start(1000)

    def _process_pending_realtime_reference_snapshots(self):
        if not self._realtime_reference_pending_codes:
            return
        code = str(self._realtime_reference_pending_codes.pop(0) or "").strip()
        payload = dict(self._realtime_reference_pending_snapshots.pop(code, {}) or {})
        if payload:
            self._record_realtime_reference_snapshot(payload)
        if self._realtime_reference_pending_codes and not self._realtime_reference_rotation_timer.isActive():
            self._realtime_reference_rotation_timer.start(1000)

    def _flush_realtime_reference_board_updates(self):
        if not self._is_realtime_reference_tab_active():
            return
        table = getattr(self, "table_realtime_reference", None)
        if table is None:
            return
        if self._realtime_reference_full_refresh_pending:
            self._realtime_reference_full_refresh_pending = False
            self._realtime_reference_dirty_rows = list(range(10))
            self._realtime_reference_dirty_row_set = set(range(10))
        if not self._realtime_reference_dirty_rows:
            return
        batch_size = max(1, int(self._realtime_reference_flush_batch_size or 2))
        rows_to_apply = []
        while self._realtime_reference_dirty_rows and len(rows_to_apply) < batch_size:
            row_index = int(self._realtime_reference_dirty_rows.pop(0))
            self._realtime_reference_dirty_row_set.discard(row_index)
            rows_to_apply.append(row_index)
        table.setUpdatesEnabled(False)
        try:
            for row_index in rows_to_apply:
                self._apply_realtime_reference_board_row(row_index, self._realtime_reference_board_rows[row_index] or {})
        finally:
            table.setUpdatesEnabled(True)
        if self._realtime_reference_dirty_rows:
            self._realtime_reference_flush_timer.start(80)

    def _apply_realtime_reference_board_row(self, row_index, row_data=None):
        table = getattr(self, "table_realtime_reference", None)
        if table is None:
            return
        row_data = dict(row_data or {})
        turnover_value = float(row_data.get("acc_turnover") or 0)
        turnover_text = "{0:,}M".format(int(turnover_value / 1000000.0)) if turnover_value > 0 else ""
        columns = [
            str(row_data.get("name") or ""),
            str(row_data.get("code") or ""),
            "{0:,.0f}".format(float(row_data.get("current_price") or 0)) if float(row_data.get("current_price") or 0) > 0 else "",
            "{0:,.0f}".format(float(row_data.get("vwap_intraday") or 0)) if float(row_data.get("vwap_intraday") or 0) > 0 else "",
            "{0:.2f}".format(float(row_data.get("sell_pressure_ratio") or 0)) if float(row_data.get("sell_pressure_ratio") or 0) > 0 else "",
            "{0:,.0f}".format(float(row_data.get("acc_volume") or 0)) if float(row_data.get("acc_volume") or 0) > 0 else "",
            turnover_text,
            "{0:,.0f}".format(float(row_data.get("sell_hoga_total") or 0)) if float(row_data.get("sell_hoga_total") or 0) > 0 else "",
            "{0:,.0f}".format(float(row_data.get("buy_hoga_total") or 0)) if float(row_data.get("buy_hoga_total") or 0) > 0 else "",
            self._format_short_datetime(row_data.get("updated_at") or ""),
        ]
        for col_index, value_text in enumerate(columns):
            item = table.item(row_index, col_index)
            if item is None:
                item = QTableWidgetItem("")
                if col_index >= 2:
                    item.setTextAlignment(int(Qt.AlignRight | Qt.AlignVCenter))
                table.setItem(row_index, col_index, item)
            item.setText(str(value_text))
            item.setForeground(QColor("#f5d36a"))
        current_price = float(row_data.get("current_price") or 0)
        vwap_intraday = float(row_data.get("vwap_intraday") or 0)
        if vwap_intraday > 0 and current_price > 0:
            table.item(row_index, 3).setForeground(QColor("#4da3ff") if vwap_intraday > current_price else QColor("#ff6a5a"))

    def _seed_realtime_reference_board(self):
        if any(row for row in (self._realtime_reference_board_rows or []) if row):
            return
        manager = getattr(self.strategy_manager, "realtime_market_state_manager", None)
        if manager is None:
            return
        snapshots = list(manager.get_snapshots() or [])
        if not snapshots:
            return
        snapshots = sorted(snapshots, key=lambda item: str(item.get("updated_at") or ""), reverse=True)[:10]
        self._realtime_reference_board_rows = [None] * 10
        self._realtime_reference_board_code_map = {}
        self._realtime_reference_board_count = 0
        self._realtime_reference_board_index = 0
        self._realtime_reference_dirty_rows = []
        self._realtime_reference_dirty_row_set = set()
        for snapshot in reversed(list(snapshots)):
            self._record_realtime_reference_snapshot(snapshot)

    def _refresh_realtime_reference_table(self):
        table = getattr(self, "table_realtime_reference", None)
        if table is None:
            return
        self._seed_realtime_reference_board()
        self._schedule_realtime_reference_board_refresh(force_full=True)

    def _refresh_realtime_strategy_reference_labels(self, *_args):
        text_value = self._format_realtime_reference_html()
        for attr_name in [
            "lbl_news_watch_live_reference",
            "lbl_strategy_detail_live_reference",
        ]:
            label = getattr(self, attr_name, None)
            if label is not None:
                label.setTextFormat(Qt.RichText)
                label.setText(text_value)
        if self._is_realtime_reference_tab_active() and int(self._realtime_reference_board_count or 0) <= 0:
            self._refresh_realtime_reference_table()

    def _on_realtime_market_state_changed(self, payload=None):
        self._record_realtime_reference_snapshot(payload)
        if self._is_realtime_reference_tab_active():
            return
        code = str(((payload or {}) if isinstance(payload, dict) else {}).get("code") or "").strip()
        selected_code, _selected_name = self._selected_watch_symbol()
        selected_code = str(selected_code or "").strip()
        if selected_code:
            if code and code != selected_code:
                return
            self._schedule_refresh_realtime_strategy_reference_labels(120)
            return
        self._schedule_refresh_realtime_strategy_reference_labels(220)

    def _recheck_selected_news_symbol(self):
        code, name = self._selected_watch_symbol()
        if not code:
            return
        self.news_manager.search_news_for_symbol_async(code, name, trigger_type="manual_recheck", min_score=None)

    def _spam_selected_symbol(self):
        code, name = self._selected_watch_symbol()
        if not code:
            return
        reason, ok = QInputDialog.getText(self, "스팸 등록", "사유")
        if not ok:
            return
        self.persistence.execute(
            "INSERT OR REPLACE INTO spam_symbols (code, name, added_at, reason, memo, block_trade, block_news_send, block_news_search, extra_json) VALUES (?, ?, ?, ?, '', 1, 1, 1, '{}')",
            (code, name, self.persistence.now_ts(), reason),
        )
        self.persistence.execute("UPDATE tracked_symbols SET is_spam=1, updated_at=? WHERE code=?", (self.persistence.now_ts(), code))
        self.append_log(u"🚫 스팸 등록: {0}".format(code))
        self._schedule_refresh_news_watch(80)

    def refresh_spam_table(self):
        rows = self.persistence.fetchall("SELECT * FROM spam_symbols ORDER BY added_at DESC")
        self.table_spam.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            self.table_spam.setItem(row_index, 0, QTableWidgetItem(row["name"] or row["code"]))
            self.table_spam.setItem(row_index, 1, QTableWidgetItem(row["code"]))
            self.table_spam.setItem(row_index, 2, QTableWidgetItem(row["added_at"] or ""))
            self.table_spam.setItem(row_index, 3, QTableWidgetItem(row["reason"] or ""))

    def _unspam_selected_symbol(self):
        row = self.table_spam.currentRow()
        if row < 0:
            return
        item = self.table_spam.item(row, 1)
        if not item:
            return
        code = item.text()
        self.persistence.execute("DELETE FROM spam_symbols WHERE code=?", (code,))
        self.persistence.execute("UPDATE tracked_symbols SET is_spam=0, updated_at=? WHERE code=?", (self.persistence.now_ts(), code))
        self.refresh_spam_table()
        self._schedule_refresh_news_watch(80)

    def _on_news_tick(self):
        if self._news_tick_running:
            self.append_log("⏳ 뉴스 주기 작업 생략: 이전 작업이 아직 진행 중입니다.")
            return
        self._news_tick_running = True
        try:
            self.news_manager.schedule_periodic_checks()
            self.news_manager.process_recheck_queue(limit=self._get_periodic_recheck_limit())
            self.pipeline_manager.run_periodic_maintenance()
            self._schedule_refresh_news_watch(100)
            self._schedule_refresh_operations(100)
        finally:
            self._news_tick_running = False

    def closeEvent(self, event):
        try:
            if hasattr(self, "_profile_save_timer") and self._profile_save_timer.isActive():
                self._profile_save_timer.stop()
            self._save_current_user_profile()
        except Exception:
            pass
        try:
            self.recovery_manager.save_runtime_snapshot()
        except Exception:
            pass
        try:
            if self.file_log_manager is not None and hasattr(self.file_log_manager, "flush_pending_lines"):
                self.file_log_manager.flush_pending_lines()
        except Exception:
            pass
        super(MainWindow, self).closeEvent(event)
