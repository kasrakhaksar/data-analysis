from etl.extract import extract_latest_prices
from etl.transform import transform_prices
from etl.data_quality import run_data_quality_checks
from etl.load import load_to_clickhouse

def run():
    df = extract_latest_prices()
    df = transform_prices(df)
    run_data_quality_checks(df)
    load_to_clickhouse(df)
    print(f"ETL finished. Data saved to ClickHouse")

if __name__ == "__main__":
    run()
