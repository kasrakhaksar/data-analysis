from src.log_parser import LogParser
from src.log_analysis import LogAnalysis
from src.visualizer import Visualizer
from dotenv import load_dotenv
import os

os.system('cls' if os.name == 'nt' else 'clear')
load_dotenv()
STATIC_PATH = os.getenv('STATIC_PATH')



def main():
    spark = LogParser.create_spark()
    df = LogParser.parse_logs(spark, fr"{STATIC_PATH}\log_analysis\data\access.log")

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
