import re
import polars as pl

class LogParser:
    log_pattern = re.compile(
        r'(?P<ip>\S+) - - \[(?P<datetime>.*?)\] "(?P<method>\S+) (?P<path>\S+) \S+" (?P<status>\d{3}) (?P<size>\d+)'
    )

    def __init__(self, log_path):
        self.log_path = log_path

    def parse_logs(self, limit=None):
        records = []
        with open(self.log_path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                match = self.log_pattern.match(line)
                if match:
                    records.append(match.groupdict())
                if limit and i >= limit:
                    break

        return pl.DataFrame(records)
