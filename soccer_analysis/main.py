from src.data_loader import DataLoader
from src.preprocessing import Preprocessor
from src.team_analysis import TeamAnalysis
from src.visualization import Visualizer
from dotenv import load_dotenv
import os

os.system('cls' if os.name == 'nt' else 'clear')
load_dotenv()
STATIC_PATH = os.getenv('STATIC_PATH')


def main():
    loader = DataLoader(fr"{STATIC_PATH}\soccer_analysis\data\database.sqlite")
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

    sample_team = clustered_teams.iloc[0]
    Visualizer.plot_radar_chart(sample_team, features, team_name="Example Team")

if __name__ == "__main__":
    main()
