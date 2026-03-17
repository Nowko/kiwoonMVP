# -*- coding: utf-8 -*-
import os


class AppPaths(object):
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.data_dir = os.path.join(base_dir, "data")
        self.log_dir = os.path.join(base_dir, "logs")
        self.runtime_log_dir = os.path.join(self.log_dir, "program")
        self.db_path = os.path.join(self.data_dir, "runtime.db")
        self.credential_path = os.path.join(self.data_dir, "credentials.json")
        self.runtime_state_path = os.path.join(self.data_dir, "runtime_state.json")
        self.daily_watch_snapshot_dir = os.path.join(self.data_dir, "daily_watch_snapshots")

    def ensure(self):
        for path in [self.data_dir, self.log_dir, self.runtime_log_dir, self.daily_watch_snapshot_dir]:
            if not os.path.exists(path):
                os.makedirs(path)
