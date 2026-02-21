import json
from datetime import datetime
from crawlers.base_crawler import BaseCrawler

class StockPriceCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(base_url="http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceHistory/35425587644337450/20260218")
    
    def crawl(self):
        url = self.base_url
        data = self.fetch(url)

        if not data:
            self.logger.warning("No data fetched")
            return

        output = {
            "fetched_at": datetime.utcnow().isoformat(),
            "data": data
        }

        file_path = f"stock_analysis/src/data/raw/stock_prices_{datetime.now().date()}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        self.logger.info(f"Data saved to {file_path}")
