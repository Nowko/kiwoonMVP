# -*- coding: utf-8 -*-
import datetime
import os
import traceback

from PyQt5.QtCore import QObject, QTimer, pyqtSignal


class FileLogManager(QObject):
    log_emitted = pyqtSignal(str)

    def __init__(self, paths, retention_days=7, parent=None):
        super(FileLogManager, self).__init__(parent)
        self.paths = paths
        self.retention_days = max(1, int(retention_days or 7))
        self._last_cleanup_date = ""
        self._pending_lines = []
        self._flush_timer = QTimer(self)
        self._flush_timer.setSingleShot(True)
        self._flush_timer.timeout.connect(self.flush_pending_lines)
        self.ensure_log_dir()
        self.cleanup_old_logs()

    def ensure_log_dir(self):
        try:
            if not os.path.exists(self.paths.runtime_log_dir):
                os.makedirs(self.paths.runtime_log_dir)
        except Exception as exc:
            self.log_emitted.emit("⚠️ 로그 폴더 생성 실패: {0}".format(exc))

    def _today_str(self):
        return datetime.datetime.now().strftime("%Y-%m-%d")

    def _log_path(self, dt=None):
        dt = dt or datetime.datetime.now()
        return os.path.join(self.paths.runtime_log_dir, "program_{0}.log".format(dt.strftime("%Y-%m-%d")))

    def cleanup_old_logs(self):
        self.ensure_log_dir()
        today = datetime.date.today()
        today_str = today.isoformat()
        if self._last_cleanup_date == today_str:
            return
        cutoff = today - datetime.timedelta(days=self.retention_days - 1)
        try:
            for filename in os.listdir(self.paths.runtime_log_dir):
                if not filename.lower().endswith(".log"):
                    continue
                if not filename.startswith("program_"):
                    continue
                date_text = filename[8:18]
                try:
                    file_date = datetime.datetime.strptime(date_text, "%Y-%m-%d").date()
                except Exception:
                    continue
                if file_date < cutoff:
                    path = os.path.join(self.paths.runtime_log_dir, filename)
                    try:
                        os.remove(path)
                    except Exception as exc:
                        self.log_emitted.emit("⚠️ 오래된 로그 삭제 실패: {0} / {1}".format(filename, exc))
        finally:
            self._last_cleanup_date = today_str

    def write_line(self, text):
        line = str(text or "")
        if not line:
            return
        self.ensure_log_dir()
        self.cleanup_old_logs()
        timestamp = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
        self._pending_lines.append(timestamp + line)
        if len(self._pending_lines) >= 50:
            self.flush_pending_lines()
        elif not self._flush_timer.isActive():
            self._flush_timer.start(500)

    def flush_pending_lines(self):
        if not self._pending_lines:
            return
        self.ensure_log_dir()
        self.cleanup_old_logs()
        path = self._log_path()
        lines = self._pending_lines[:]
        self._pending_lines = []
        try:
            with open(path, "a", encoding="utf-8") as fp:
                fp.write("\n".join(lines) + "\n")
        except Exception as exc:
            self.log_emitted.emit("⚠️ 로그 파일 기록 실패: {0}".format(exc))

    def write_exception(self, prefix, exc):
        try:
            self.write_line("{0}: {1}".format(prefix, exc))
            self.write_line(traceback.format_exc().strip())
        except Exception:
            pass
