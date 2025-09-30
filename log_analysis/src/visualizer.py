import matplotlib.pyplot as plt

class Visualizer:
    @staticmethod
    def plot_status_distribution(df):
        pdf = df.toPandas()
        pdf.plot.bar(x="status", y="count", color="skyblue", legend=False)
        plt.title("Status Code Distribution")
        plt.xlabel("Status")
        plt.ylabel("Count")
        plt.show()

    @staticmethod
    def plot_requests_per_hour(df):
        pdf = df.toPandas()
        pdf.plot.bar(x="hour", y="count", color="orange", legend=False)
        plt.title("Requests per Hour")
        plt.xlabel("Hour")
        plt.ylabel("Requests")
        plt.show()

    @staticmethod
    def plot_top_paths(df):
        pdf = df.toPandas()
        pdf.plot.bar(x="path", y="count", color="green", legend=False)
        plt.title("Top Requested Paths")
        plt.xlabel("Path")
        plt.ylabel("Count")
        plt.show()
