from sqlalchemy import create_engine
import pandas as pd
from logs.logger import Logger


def load_to_clickhouse(df: pd.DataFrame , connection : str):

    logger = Logger('etl.log')

    try:
        engine = create_engine(connection)
    except Exception as e:
        logger.log(f'Database Connection Error : {e}', level="ERROR")

    try:
        df.to_sql(
            "stock_prices",
            engine,
            if_exists="append",
            index=False  
        )

        logger.log("ETL finished. Data saved to ClickHouse")
    except Exception as e:
        logger.log(f'Database Insert Error : {e}', level="ERROR")
