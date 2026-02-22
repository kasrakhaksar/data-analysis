import pandas as pd
import numpy as np
from logs.logger import Logger


def run_data_quality_checks(df: pd.DataFrame):
    logger = Logger('etl.log')

    errors = []

    if df.empty:
        errors.append("DataFrame is empty")

    if "inscode" in df.columns and (df["inscode"] == 0).any():
        errors.append("inscode not to be zero")

    if "volume" in df.columns and (df["volume"] <= 0).any() and (df["volume"] == np.nan).any():
        errors.append("volume not to be zero")

    if errors:
        logger.log(f"Data Quality Errors: {errors}")

        return False

    return True
