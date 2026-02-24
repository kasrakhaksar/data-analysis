import json
from datetime import datetime
from crawlers.base_crawler import BaseCrawler
import os


class StockPriceCrawler(BaseCrawler):
    def __init__(self):
        base_url = "http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceHistory/35425587644337450/20260218"
        super().__init__(base_url=base_url)
    
    def crawl(self):
        url = self.base_url
        data = self.fetch(url)

        if not data:
            self.logger.log("No data fetched from the stock price API.")
            return

        output = {
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "data": data
        }


        today_date_for_filename = self.base_url.split('/')[-1]
        file_path = f"stock_analysis/src/data/raw/stock_prices_{today_date_for_filename}.json"
        
        try:
            output_dir = os.path.dirname(file_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                self.logger.log(f"Created directory for output file: {output_dir}")

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)

            self.logger.log(f"Data successfully saved to {file_path}")

        except IOError as e:
            self.logger.log(f"Error saving data to file {file_path}: {e}", level="ERROR")
        except Exception as e:
            self.logger.log(f"An unexpected error occurred during file saving: {e}", level="ERROR")