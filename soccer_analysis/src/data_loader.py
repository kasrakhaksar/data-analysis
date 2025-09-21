import sqlite3
import pandas as pd

class DataLoader:
    def __init__(self, db_path=r"D:\Data Analysis Project\soccer_analysis\data\database.sqlite"):
        self.conn = sqlite3.connect(db_path)
    
    def load_player_attributes(self):
        query = "SELECT * FROM Player_Attributes"
        return pd.read_sql(query, self.conn)

    def load_team_attributes(self):
        query = "SELECT * FROM Team_Attributes"
        return pd.read_sql(query, self.conn)

    def load_matches(self):
        query = "SELECT * FROM Match"
        return pd.read_sql(query, self.conn)
