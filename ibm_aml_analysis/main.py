from pyspark.sql import SparkSession
from src.ibm_parser import HIParser
from src.ibm_analysis import HIAnalysis
from src.visualization import Visualizer
from dotenv import load_dotenv
import os


load_dotenv()

endpoint = os.getenv('endpoint')
access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')

log_path = f"{os.getenv('log_path')}/HI-Large_Patterns.txt"


def main():


    spark = (
        SparkSession.builder
        .appName("IBM AML Analysis")
        .master("local[*]")
        .config("spark.jars.packages" , "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.paging.maximum", "1000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.cleanup.delay", "86400000") 
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .getOrCreate()
    )

    df = HIParser.parse_hi_file(spark, log_path)

    analysis = HIAnalysis(df)
    summary = analysis.transaction_summary()
    top_accounts = analysis.top_accounts_by_amount(10)
    pattern_summary = analysis.suspicious_by_pattern()
    hourly_activity = analysis.transactions_per_hour()

    os.system('cls' if os.name == 'nt' else 'clear')

    # summary.show()
    # top_accounts.show()
    # pattern_summary.show()
    # hourly_activity.show()

    Visualizer.plot_transaction_summary(summary)
    Visualizer.plot_top_accounts(top_accounts)
    Visualizer.plot_pattern_summary(pattern_summary)
    Visualizer.plot_hourly_activity(hourly_activity)

if __name__ == "__main__":
    main()
