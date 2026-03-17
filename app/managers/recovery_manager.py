# -*- coding: utf-8 -*-
from PyQt5.QtCore import QObject, pyqtSignal


class RecoveryManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, paths, persistence, kiwoom_client, account_manager, condition_manager, parent=None):
        super(RecoveryManager, self).__init__(parent)
        self.paths = paths
        self.persistence = persistence
        self.kiwoom_client = kiwoom_client
        self.account_manager = account_manager
        self.condition_manager = condition_manager

    def save_runtime_snapshot(self):
        state = {
            "saved_at": self.persistence.now_ts(),
            "activation_mode": self.account_manager.activation_mode,
            "active_accounts": list(self.account_manager.active_accounts),
            "enabled_slots": [int(row["slot_no"]) for row in self.condition_manager.get_slots() if int(row["is_realtime"] or 0)],
        }
        self.persistence.save_runtime_state(state)
        self.log_emitted.emit("💾 런타임 스냅샷 저장")

    def restore_runtime_snapshot(self):
        state = self.persistence.load_runtime_state()
        if not state:
            self.log_emitted.emit("ℹ️ 복구할 런타임 스냅샷이 없습니다")
            return state
        mode = state.get("activation_mode", "single_active")
        self.account_manager.set_activation_mode(mode)
        self.account_manager.set_active_accounts(state.get("active_accounts", []))
        for slot_no in state.get("enabled_slots", []):
            try:
                self.condition_manager.start_realtime_slot(int(slot_no))
            except Exception:
                continue
        self.log_emitted.emit("♻️ 런타임 스냅샷 복구 완료")
        return state
