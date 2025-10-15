from pyspark.sql import SparkSession
from src.log_parser import LogParser
from src.log_analysis import LogAnalysis
from src.visualizer import Visualizer
from dotenv import load_dotenv
import os


os.system('cls' if os.name == 'nt' else 'clear')
load_dotenv()

endpoint = os.getenv('endpoint')
access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')

log_path = f"{os.getenv('log_path')}/access.log"


def main():


    spark = (
        SparkSession.builder
        .appName("LocalStack S3 Access")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.696"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )



    df = LogParser.parse_logs(spark, log_path)

    analysis = LogAnalysis(df)
    top_ips = analysis.count_requests_per_ip()
    status_dist = analysis.status_code_distribution()
    top_paths = analysis.top_requested_paths(10)
    requests_by_hour = analysis.requests_per_hour()

    top_ips.show(10)
    status_dist.show()
    top_paths.show()
    requests_by_hour.show()

    Visualizer.plot_status_distribution(status_dist)
    Visualizer.plot_requests_per_hour(requests_by_hour)
    Visualizer.plot_top_paths(top_paths)


if __name__ == "__main__":
    main()
