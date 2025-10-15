import boto3
import polars as pl
from dotenv import load_dotenv

load_dotenv()

class DataLoader:
    def __init__(self ,endpoint ,region ,access_key ,secret_key):
        self.endpoint = endpoint
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key


        self.dynamodb = boto3.resource(
            "dynamodb",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def load_table(self, table_name: str) -> pl.DataFrame:
        table = self.dynamodb.Table(table_name)
        items = []

        response = table.scan()
        items.extend(response.get("Items", []))

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))

        if not items:
            return pl.DataFrame()

        return pl.DataFrame(items)

    def load_player_attributes(self) -> pl.DataFrame:
        return self.load_table("Player_Attributes")

    def load_team_attributes(self) -> pl.DataFrame:
        return self.load_table("Team_Attributes")

    def load_matches(self) -> pl.DataFrame:
        return self.load_table("Match")