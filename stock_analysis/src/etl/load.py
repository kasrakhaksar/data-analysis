from sqlalchemy import create_engine
import pandas as pd


def load_to_clickhouse(df: pd.DataFrame):
    engine = create_engine(
        "clickhouse+native://user:user123@localhost:9000/myapp"
    )

    df.to_sql(
        "stock_prices",
        engine,
        if_exists="append",
        index=False
        
    )
