from etl.extract import extract_latest_prices
from etl.transform import transform_prices
from etl.data_quality import run_data_quality_checks
from etl.load import load_to_clickhouse
from dotenv import load_dotenv
import os

load_dotenv()

def run():
    CLICKHOUSE_CONNECTION = os.getenv('CLICKHOUSE_CONNECTION')

    df = extract_latest_prices()
    df = transform_prices(df)
    run_data_quality_checks(df)
    load_to_clickhouse(df , CLICKHOUSE_CONNECTION)
    print(f"ETL finished. Data saved to ClickHouse")

if __name__ == "__main__":
    run()
