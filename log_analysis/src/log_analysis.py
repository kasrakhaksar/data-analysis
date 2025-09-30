from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, hour

class LogAnalysis:
    def __init__(self, df: DataFrame):
        self.df = df

    def count_requests_per_ip(self):
        return self.df.groupBy("ip").count().orderBy(col("count").desc())

    def status_code_distribution(self):
        return self.df.groupBy("status").count().orderBy(col("count").desc())

    def top_requested_paths(self, n=10, filter_static=True):
        df_paths = self.df
        if filter_static:
            df_paths = df_paths.filter(
                ~col("path").rlike(r"\.(jpg|jpeg|png|gif|ico|css|js|svg|woff|ttf|eot)$")
            )
        return df_paths.groupBy("path").count().orderBy(col("count").desc()).limit(n)

    def requests_per_hour(self):
        return (
            self.df.withColumn("hour", hour("datetime"))
            .groupBy("hour")
            .count()
            .orderBy("hour")
        )
