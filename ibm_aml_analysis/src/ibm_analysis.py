from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class HIAnalysis:
    def __init__(self, df: DataFrame):
        self.df = df

    def transaction_summary(self):


        return self.df.groupBy("pattern_type").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.sum("isFlagged").alias("flagged_count")
        ).orderBy(F.col("total_amount").desc())

    def top_accounts_by_amount(self, n=10):

        return self.df.groupBy("sender").agg(
            F.sum("amount").alias("total_sent"),
            F.count("*").alias("txn_count")
        ).orderBy(F.col("total_sent").desc()).limit(n)

    def suspicious_by_pattern(self):

        return self.df.groupBy("pattern_type").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.sum("isFlagged").alias("flagged_count")
        ).orderBy(F.col("total_amount").desc())

    def transactions_per_hour(self):


        return self.df.withColumn("hour", F.hour("date")).groupBy("hour").agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount")
        ).orderBy("hour")
