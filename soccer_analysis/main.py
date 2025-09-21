from src.data_loader import DataLoader
from src.preprocessing import Preprocessor
from src.team_analysis import TeamAnalysis
from src.visualization import Visualizer

def main():
    # 1. Load data
    loader = DataLoader()
    team_df = loader.load_team_attributes()

    # 2. Clean + preprocess
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

    # 3. Analysis
    team_analysis = TeamAnalysis(team_df)
    clustered_teams, _ = team_analysis.cluster_teams(features, n_clusters=3)

    # 4. Visualization
    Visualizer.plot_team_clusters(clustered_teams, "buildUpPlaySpeed", "defencePressure", "cluster")

    # Radar Chart برای یک تیم مثال
    sample_team = clustered_teams.iloc[0]
    Visualizer.plot_radar_chart(sample_team, features, team_name="Example Team")

if __name__ == "__main__":
    main()
