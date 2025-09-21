from sklearn.preprocessing import StandardScaler

class Preprocessor:
    @staticmethod
    def clean_teams(df):
        # حذف سطرهایی که مقدار null دارن
        df = df.dropna()
        df = df.drop_duplicates()
        return df

    @staticmethod
    def normalize_features(df, cols):
        scaler = StandardScaler()
        df[cols] = scaler.fit_transform(df[cols])
        return df
