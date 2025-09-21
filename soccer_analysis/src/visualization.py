import matplotlib.pyplot as plt
import seaborn as sns
from math import pi

class Visualizer:
    @staticmethod
    def plot_team_clusters(df, x_col, y_col, cluster_col):
        plt.figure(figsize=(8,6))
        sns.scatterplot(data=df, x=x_col, y=y_col, hue=cluster_col, palette="tab10")
        plt.title("Team Style Clusters")
        plt.show()

    @staticmethod
    def plot_radar_chart(row, features, team_name="Team"):
        values = row[features].values.flatten().tolist()
        values += values[:1]  # برای بستن دایره

        angles = [n / float(len(features)) * 2 * pi for n in range(len(features))]
        angles += angles[:1]

        plt.figure(figsize=(6,6))
        ax = plt.subplot(111, polar=True)
        plt.xticks(angles[:-1], features, color='grey', size=8)
        ax.plot(angles, values, linewidth=2, linestyle='solid')
        ax.fill(angles, values, alpha=0.4)
        plt.title(f"Style Profile: {team_name}")
        plt.show()
