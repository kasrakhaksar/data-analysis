import matplotlib.pyplot as plt

class Visualizer:
    @staticmethod
    def plot_status_distribution(df):
        df_pandas = df.to_pandas()
        df_pandas.plot.bar(x="status", y="count", legend=False, color="skyblue")
        plt.title("Status Code Distribution")
        plt.xlabel("Status")
        plt.ylabel("Count")
        plt.show()

    @staticmethod
    def plot_requests_per_hour(df):
        df_pandas = df.to_pandas()
        df_pandas.plot.bar(x="hour", y="count", legend=False, color="orange")
        plt.title("Requests per Hour")
        plt.xlabel("Hour")
        plt.ylabel("Requests")
        plt.show()

    @staticmethod
    def plot_top_paths(df):
        df_pandas = df.to_pandas()
        df_pandas.plot.bar(x="path", y="count", legend=False, color="green")
        plt.title("Top Requested Paths")
        plt.xlabel("Path")
        plt.ylabel("Count")
        plt.show()
