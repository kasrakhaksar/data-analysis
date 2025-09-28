from src.log_parser import LogParser
from src.log_analysis import LogAnalysis
from src.visualization import Visualizer

def main():
    parser = LogParser(r"D:\Data Analysis Project\log_analysis\data\access.log")
    log_df = parser.parse_logs(limit=2000000)

    # 2. Analysis
    analysis = LogAnalysis(log_df)

    top_ips = analysis.count_requests_per_ip()
    print("Top IPs:\n", top_ips.head(5))

    status_codes = analysis.status_code_distribution()
    print("\nStatus Codes:\n", status_codes)

    top_paths = analysis.top_requested_paths()
    print("\nTop Paths:\n", top_paths)

    hourly = analysis.requests_per_hour()

    # 3. Visualization
    # Visualizer.plot_status_distribution(status_codes)
    # Visualizer.plot_requests_per_hour(hourly)
    Visualizer.plot_top_paths(top_paths)

if __name__ == "__main__":
    main()
