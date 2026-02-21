import logging
import time
from abc import ABC, abstractmethod
import requests

class BaseCrawler(ABC):
    def __init__(self, base_url: str, timeout: int = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        handler = logging.FileHandler("stock_analysis/src/logs/crawler.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(handler)

        return logger

    def fetch(self, url: str, params=None):
        try:
            self.logger.info(f"Fetching URL: {url}")
            response = self.session.get(
                url, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            time.sleep(1)
            return response.json()
        except Exception as e:
            self.logger.error(f"Fetch failed: {e}")
            return None

    @abstractmethod
    def crawl(self):
        pass
