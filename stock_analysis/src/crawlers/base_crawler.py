from abc import ABC, abstractmethod
import requests
from logs.logger import Logger


class BaseCrawler(ABC):
    def __init__(self, base_url: str, timeout: int = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = Logger("crawler.log")

    def fetch(self, url: str, params=None):
        try:


            self.logger.log(f"Fetching URL: {url}")
            response = self.session.get(
                url, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            self.logger.log(f"Request timed out for URL: {url}", level="ERROR")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.log(f"Request failed for URL: {url}. Error: {e}", level="ERROR")
            return None
        except Exception as e:
            self.logger.log(
                f"An unexpected error occurred while fetching {url}: {e}", level="ERROR")
            return None

    @abstractmethod
    def crawl(self):
        pass
