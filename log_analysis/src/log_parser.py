from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class LogParser:
    log_pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d{3}|-) (\d+|-)'

    @staticmethod
    def create_spark(app_name="Log Analysis"):
        return (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .getOrCreate()
        )

    @staticmethod
    def parse_logs(spark, log_file):

        df_raw = spark.read.text(log_file)

        df = df_raw.select(
            F.regexp_extract("value", LogParser.log_pattern, 1).alias("ip"),
            F.regexp_extract("value", LogParser.log_pattern, 2).alias("datetime"),
            F.regexp_extract("value", LogParser.log_pattern, 3).alias("request"),
            F.regexp_extract("value", LogParser.log_pattern, 4).alias("status"),
            F.regexp_extract("value", LogParser.log_pattern, 5).alias("size"),
        )

        df = df.withColumn("method", F.try_element_at(F.split(F.col("request"), " "), F.lit(1)))
        df = df.withColumn("path", F.try_element_at(F.split(F.col("request"), " "), F.lit(2)))

        df = df.withColumn(
            "status",
            F.when(F.col("status").rlike("^[0-9]+$"), F.col("status").cast("int")).otherwise(None)
        )
        df = df.withColumn(
            "size",
            F.when(F.col("size").rlike("^[0-9]+$"), F.col("size").cast("long")).otherwise(None)
        )

        df = df.withColumn(
            "datetime",
            F.try_to_timestamp(F.col("datetime"), F.lit("dd/MMM/yyyy:HH:mm:ss Z"))
        )


        return df