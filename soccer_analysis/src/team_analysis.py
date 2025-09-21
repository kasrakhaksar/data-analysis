from sklearn.cluster import KMeans

class TeamAnalysis:
    def __init__(self, df):
        self.df = df.copy()

    def cluster_teams(self, features, n_clusters=4):
        model = KMeans(n_clusters=n_clusters, random_state=42)
        self.df["cluster"] = model.fit_predict(self.df[features])
        return self.df, model
