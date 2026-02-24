import pandas as pd
from db.database import ClickHouseDB
from logs.logger import Logger


def load_to_clickhouse(df: pd.DataFrame, clickhouse_connection: str):

    logger = Logger('etl.log')
    engine = ClickHouseDB(clickhouse_connection)

    try:
        df.to_sql(
            "stock_prices",
            engine,
            if_exists="append",
            index=False  
        )
        logger.log("ETL finished successfully. Data saved to ClickHouse")
    except Exception as e:
        logger.log(f"Database Insert Error: {e}", level="ERROR")
