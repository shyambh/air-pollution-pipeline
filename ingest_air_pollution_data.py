import json
import pandas as pd

with open(".sample_data/historical_air_pollution_ktm.json") as file:
    data = json.load(file)

df = pd.DataFrame.from_dict(data["list"])

df
