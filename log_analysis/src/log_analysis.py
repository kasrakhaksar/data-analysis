import polars as pl

class LogAnalysis:
    def __init__(self, df: pl.DataFrame):
        # type casting
        self.df = df.with_columns([
            pl.col("status").cast(pl.Int32),
            pl.col("size").cast(pl.Int64),
            pl.col("datetime").str.strptime(
                pl.Datetime, "%d/%b/%Y:%H:%M:%S %z", strict=False
            )
        ])

    def count_requests_per_ip(self, n=10):
        return (
            self.df.group_by("ip")  # ✅ اصلاح شد
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(n)
        )

    def status_code_distribution(self):
        return (
            self.df.group_by("status")  # ✅ اصلاح شد
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
        )

    def top_requested_paths(self, n=10, filter_static=True):
        df_paths = self.df

        if filter_static:
            # حذف مسیرهای static asset
            df_paths = df_paths.filter(
                ~pl.col("path").str.contains(
                    r"\.(jpg|jpeg|png|gif|ico|css|js|svg|woff|ttf|eot)$"
                )
            )

        return (
            df_paths.group_by("path")  # ✅ اصلاح شد
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(n)
        )

    def requests_per_hour(self):
        return (
            self.df.with_columns([
                pl.col("datetime").dt.hour().alias("hour")
            ])
            .group_by("hour") 
            .agg(pl.count().alias("count"))
            .sort("hour")
        )
