import pandas as pd

class Preprocessor:
    @staticmethod
    def clean_logs(df):
        df["status"] = df["status"].astype(int)
        df["size"] = pd.to_numeric(df["size"], errors="coerce").fillna(0).astype(int)
        df["datetime"] = pd.to_datetime(df["datetime"], format="%d/%b/%Y:%H:%M:%S %z", errors="coerce")
        return df
