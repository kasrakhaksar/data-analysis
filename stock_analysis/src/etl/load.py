from sqlalchemy import create_engine
import pandas as pd


def load_to_clickhouse(df: pd.DataFrame , connection : str):
    engine = create_engine(connection)

    df.to_sql(
        "stock_prices",
        engine,
        if_exists="append",
        index=False
        
    )
