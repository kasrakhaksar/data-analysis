from src.data_loader import DataLoader
from src.preprocessing import Preprocessor
from src.team_analysis import TeamAnalysis
from src.visualization import Visualizer
from dotenv import load_dotenv
import os

os.system('cls' if os.name == 'nt' else 'clear')
load_dotenv()

endpoint = os.getenv("endpoint")
region = os.getenv("region")
access_key = os.getenv("access_key")
secret_key = os.getenv("secret_key")


def main():

    loader = DataLoader(endpoint, region, access_key, secret_key)
    team_df = loader.load_team_attributes()

    team_df = Preprocessor.clean_teams(team_df)

    features = [
        "buildUpPlaySpeed",
        "buildUpPlayPassing",
        "chanceCreationPassing",
        "chanceCreationCrossing",
        "chanceCreationShooting",
        "defencePressure",
        "defenceAggression",
        "defenceTeamWidth"
    ]

    team_df = Preprocessor.normalize_features(team_df, features)
    team_analysis = TeamAnalysis(team_df)
    clustered_teams, _ = team_analysis.cluster_teams(features, n_clusters=3)

    Visualizer.plot_team_clusters(clustered_teams, "buildUpPlaySpeed", "defencePressure", "cluster")

    sample_team = clustered_teams[0].to_dict()
    Visualizer.plot_radar_chart(sample_team, features, team_name="Example Team")


if __name__ == "__main__":
    main()
