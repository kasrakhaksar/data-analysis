import polars as pl
from sklearn.preprocessing import StandardScaler

class Preprocessor:
    @staticmethod
    def clean_teams(df: pl.DataFrame) -> pl.DataFrame:
        df = df.drop_nulls()
        df = df.unique()
        return df

    @staticmethod
    def normalize_features(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
        scaler = StandardScaler()
        data_to_scale = df.select(cols).to_numpy()
        scaled_data = scaler.fit_transform(data_to_scale)
        for i, col in enumerate(cols):
            df = df.with_columns(pl.Series(col, scaled_data[:, i]))
        return df
