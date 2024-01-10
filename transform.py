"""Transform the downloaded data"""
from pathlib import Path
import json
import datetime
import pandas as pd


def transform_data(file_path: Path) -> pd.DataFrame:
    with open(file_path, encoding="UTF-8") as file:
        data = json.load(file)

        df = pd.json_normalize(data["list"], max_level=1)

        # Refining the column names to remove the parent key from the JSON object
        df.rename(columns=lambda x: x.split(".")[-1], inplace=True)

        # Converting the Unix timestamp to datetime
        df["dt"] = df["dt"].apply(datetime.datetime.fromtimestamp)

        return df
