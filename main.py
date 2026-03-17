# -*- coding: utf-8 -*-
import os
import sys
import time
import ctypes
import subprocess
import traceback

from PyQt5.QtWidgets import QApplication

from app.config import AppPaths
from app.persistence import PersistenceManager
from app.managers.credential_manager import CredentialManager
from app.managers.kiwoom_api import KiwoomApiClient
from app.managers.account_manager import AccountActivationManager
from app.managers.condition_manager import ConditionCatalogManager
from app.managers.strategy_manager import StrategyManager
from app.managers.news_manager import NaverNewsManager
from app.services.telegram_manager import TelegramManager
from app.services.news_analysis_manager import NewsAnalysisManager
from app.services.file_log_manager import FileLogManager
from app.services.daily_watch_snapshot_manager import DailyWatchSnapshotManager
from app.managers.recovery_manager import RecoveryManager
from app.managers.order_manager import OrderManager
from app.managers.signal_pipeline import SignalPipelineManager
from app.managers.position_state_manager import PositionStateManager
from app.managers.realtime_market_state_manager import RealtimeMarketStateManager
from app.ui.main_window import MainWindow


def _startup_log_path(base_dir):
    log_dir = os.path.join(base_dir, "logs", "program")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return os.path.join(log_dir, "program_{0}.log".format(time.strftime("%Y-%m-%d")))


def _append_startup_log(base_dir, text):
    try:
        path = _startup_log_path(base_dir)
        stamp = time.strftime("[%Y-%m-%d %H:%M:%S] ")
        with open(path, "a", encoding="utf-8") as fp:
            fp.write(stamp + str(text or "") + "\n")
    except Exception:
        pass


def _install_global_excepthook(base_dir):
    def _hook(exc_type, exc_value, exc_tb):
        try:
            _append_startup_log(base_dir, "❌ 치명적 예외 발생: {0}".format(exc_value))
            for line in traceback.format_exception(exc_type, exc_value, exc_tb):
                for part in str(line).rstrip().splitlines():
                    _append_startup_log(base_dir, part)
        except Exception:
            pass
        try:
            sys.__excepthook__(exc_type, exc_value, exc_tb)
        except Exception:
            pass

    sys.excepthook = _hook


def _is_running_as_admin():
    if os.name != "nt":
        return True
    try:
        is_admin = bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False
    return is_admin


def _ensure_admin_or_relaunch(_raw_args):
    raw_args = list(_raw_args or [])
    if os.name != "nt":
        return True
    if _is_running_as_admin():
        return True
    try:
        if getattr(sys, "frozen", False):
            params = subprocess.list2cmdline(list(raw_args[1:]))
        else:
            params = subprocess.list2cmdline([os.path.abspath(raw_args[0])] + list(raw_args[1:]))
        result = ctypes.windll.shell32.ShellExecuteW(None, u"runas", sys.executable, params, None, 1)
        if int(result) > 32:
            return False
    except Exception:
        pass
    return True


def _parse_startup_context(raw_args):
    qt_args = [raw_args[0]]
    startup_context = {
        "auto_recover": False,
        "recover_reason": "",
        "delay_start_sec": 0,
    }
    for arg in list(raw_args[1:]):
        if arg.startswith("--delay-start="):
            try:
                startup_context["delay_start_sec"] = max(0, int(arg.split("=", 1)[1] or "0"))
            except Exception:
                startup_context["delay_start_sec"] = 0
        elif arg in ["--auto-recover", "--auto-recover=1"]:
            startup_context["auto_recover"] = True
        elif arg.startswith("--recover-reason="):
            startup_context["recover_reason"] = str(arg.split("=", 1)[1] or "")
        else:
            qt_args.append(arg)
    return qt_args, startup_context


def main():
    raw_args = list(sys.argv)
    base_dir = os.path.abspath(os.path.dirname(__file__))
    _install_global_excepthook(base_dir)
    _append_startup_log(base_dir, "🚀 프로그램 시작")

    if not _ensure_admin_or_relaunch(raw_args):
        _append_startup_log(base_dir, "ℹ️ 관리자 권한 재실행 요청")
        return 0

    _append_startup_log(base_dir, "관리자 권한 실행: {0}".format("YES" if _is_running_as_admin() else "NO"))
    qt_args, startup_context = _parse_startup_context(raw_args)
    delay_start_sec = int(startup_context.get("delay_start_sec", 0) or 0)
    if delay_start_sec > 0:
        time.sleep(delay_start_sec)

    paths = AppPaths(base_dir)
    paths.ensure()

    app = QApplication(qt_args)
    app.setApplicationName("Kiwoom News Trader MVP")

    file_log_manager = FileLogManager(paths, retention_days=7)
    if startup_context.get("auto_recover"):
        file_log_manager.write_line("♻️ 자동복구 시작 인자 감지: {0}".format(startup_context.get("recover_reason") or "auto_recover"))

    persistence = PersistenceManager(paths)
    persistence.initialize()

    credential_manager = CredentialManager(paths, persistence)
    kiwoom_client = KiwoomApiClient(persistence)
    daily_watch_snapshot_manager = DailyWatchSnapshotManager(paths)
    daily_watch_snapshot_manager.log_emitted.connect(file_log_manager.write_line)
    account_manager = AccountActivationManager(persistence)
    condition_manager = ConditionCatalogManager(
        persistence,
        kiwoom_client,
        daily_watch_snapshot_manager=daily_watch_snapshot_manager,
    )
    realtime_market_state_manager = RealtimeMarketStateManager(persistence, kiwoom_client)
    strategy_manager = StrategyManager(persistence, realtime_market_state_manager=realtime_market_state_manager)
    position_state_manager = PositionStateManager(persistence, realtime_market_state_manager=realtime_market_state_manager)
    telegram_router = TelegramManager(credential_manager, persistence)
    news_analysis_manager = NewsAnalysisManager(credential_manager=credential_manager)
    news_manager = NaverNewsManager(
        credential_manager,
        persistence,
        telegram_router,
        kiwoom_client=kiwoom_client,
        analysis_manager=news_analysis_manager,
        daily_watch_snapshot_manager=daily_watch_snapshot_manager,
    )
    order_manager = OrderManager(
        persistence,
        kiwoom_client,
        telegram_router,
        account_manager,
        position_state_manager=position_state_manager,
        strategy_manager=strategy_manager,
        realtime_market_state_manager=realtime_market_state_manager,
    )
    pipeline_manager = SignalPipelineManager(
        persistence=persistence,
        condition_manager=condition_manager,
        strategy_manager=strategy_manager,
        news_manager=news_manager,
        order_manager=order_manager,
        account_manager=account_manager,
    )
    if hasattr(order_manager, "positions_changed"):
        order_manager.positions_changed.connect(realtime_market_state_manager.refresh_watch_codes)
    if hasattr(pipeline_manager, "pipeline_changed"):
        pipeline_manager.pipeline_changed.connect(realtime_market_state_manager.refresh_watch_codes)
    if hasattr(kiwoom_client, "account_sync_finished"):
        kiwoom_client.account_sync_finished.connect(lambda _payload: realtime_market_state_manager.refresh_watch_codes())
    recovery_manager = RecoveryManager(paths, persistence, kiwoom_client, account_manager, condition_manager)

    window = MainWindow(
        paths=paths,
        persistence=persistence,
        credential_manager=credential_manager,
        kiwoom_client=kiwoom_client,
        account_manager=account_manager,
        condition_manager=condition_manager,
        strategy_manager=strategy_manager,
        news_manager=news_manager,
        telegram_router=telegram_router,
        order_manager=order_manager,
        pipeline_manager=pipeline_manager,
        recovery_manager=recovery_manager,
        daily_watch_snapshot_manager=daily_watch_snapshot_manager,
        file_log_manager=file_log_manager,
        startup_context=startup_context,
    )
    daily_watch_snapshot_manager.log_emitted.connect(window.append_log)
    realtime_market_state_manager.refresh_watch_codes()
    window.resize(1800, 980)
    window.show()

    return app.exec_()


if __name__ == "__main__":
    sys.exit(main())
