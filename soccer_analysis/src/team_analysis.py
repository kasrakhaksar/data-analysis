from sklearn.cluster import KMeans
import polars as pl

class TeamAnalysis:
    def __init__(self, df: pl.DataFrame):
        self.df = df.clone()

    def cluster_teams(self, features: list[str], n_clusters=4):
        X = self.df.select(features).to_numpy()

        model = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = model.fit_predict(X)

        self.df = self.df.with_columns(pl.Series("cluster", clusters))

        return self.df, model
