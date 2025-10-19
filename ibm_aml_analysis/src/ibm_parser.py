from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re

class HIParser:

    @staticmethod
    def parse_hi_file(spark: SparkSession, file_path: str):

        rdd = spark.sparkContext.textFile(file_path)

        block_id = 0
        pattern_type = None
        txns = []

        for line in rdd.collect():
            line = line.strip()
            if line.startswith("BEGIN LAUNDERING ATTEMPT"):
                block_id += 1
                match = re.match(r'BEGIN LAUNDERING ATTEMPT - (.*)', line)
                if match:
                    pattern_type = match.group(1)
            elif line.startswith("END LAUNDERING ATTEMPT"):
                pattern_type = None
            elif line and pattern_type is not None:
                parts = line.split(",")
                try:
                    date_str = parts[0]
                    sender = parts[1]
                    receiver = parts[3]
                    amount = float(parts[5])
                    currency = parts[6]
                    isFlagged = int(parts[10])
                    txns.append((block_id, pattern_type, date_str, sender, receiver, amount, currency, isFlagged))
                except:
                    continue  

        df = spark.createDataFrame(txns, ["block_id", "pattern_type", "date_str", "sender", "receiver", "amount", "currency", "isFlagged"])
        df = df.withColumn("date", F.to_timestamp("date_str", "yyyy/MM/dd HH:mm"))
        df = df.drop("date_str")
        return df
