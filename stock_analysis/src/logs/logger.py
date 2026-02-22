import os
from datetime import datetime


def singleton(class_):
    instances = {}

    def get_instance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return get_instance


@singleton
class Logger:
    def __init__(self, log_file_name="unknow_log.log"):
        if not hasattr(self, '_initialized'):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_dir_path = os.path.join(base_dir, 'files')
            self.log_file_name = os.path.join(log_dir_path, log_file_name)
            self._ensure_log_directory(log_dir_path)
            self._initialized = True

    def _ensure_log_directory(self, directory_path):
        if directory_path and not os.path.exists(directory_path):
            try:
                os.makedirs(directory_path)
            except OSError as e:
                print(f"Error creating log directory {directory_path}: {e}")

    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level.upper()}] {message}"

        try:
            with open(self.log_file_name, "a", encoding="utf-8") as f:
                f.write(log_entry + "\n")
        except Exception as e:
            print(
                f"CRITICAL Error writing to log file {self.log_file_name}: {e}")