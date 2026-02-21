import json
import pandas as pd
from pathlib import Path

RAW_DATA_PATH = Path("stock_analysis/src/data/raw")

def extract_latest_prices() -> pd.DataFrame:
    files = sorted(RAW_DATA_PATH.glob("stock_prices_*.json"))
    if not files:
        raise FileNotFoundError("No raw data files found")

    latest_file = files[-1]
    with open(latest_file, "r", encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw["data"])
    df = pd.json_normalize(df["closingPriceHistory"])
    return df
