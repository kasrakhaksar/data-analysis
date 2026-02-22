import json
import pandas as pd
from pathlib import Path
from logs.logger import Logger


def extract_latest_prices() -> pd.DataFrame:

    raw_data_path = Path("stock_analysis/src/data/raw")

    logger = Logger('etl.log')


    files = sorted(raw_data_path.glob("stock_prices_*.json"))
    if not files:
        logger.log("No raw data files found")
        return False

    latest_file = files[-1]
    with open(latest_file, "r", encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw["data"])
    df = pd.json_normalize(df["closingPriceHistory"])
    return df
