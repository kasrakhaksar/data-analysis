from etl.extract import extract_latest_prices
from etl.transform import transform_prices
from etl.data_quality import run_data_quality_checks
from etl.load import load_to_clickhouse
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":

    clickhouse_connection = os.getenv('CLICKHOUSE_CONNECTION')

    df = extract_latest_prices()
    df = transform_prices(df)
    run_data_quality_checks(df)
    load_to_clickhouse(df, clickhouse_connection)
