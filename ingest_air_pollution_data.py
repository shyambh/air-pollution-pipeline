"""
Ingest the Air Quality Index (AQI) for the selected City
"""

from pathlib import Path
import json
import requests
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from prefect_gcp import GcpCredentials

lat = 27.708317
lon = 85.3205817
start_time = 1606807137
end_time = 1704007137
api_key = "7c66b0105c57c08de6263a0e3ef2fbf6"
city_name = "Kathmandu"
file_name = f"weather_data_{city_name}.json"
file_path = Path(f"./.sample_data/{file_name}")

api_url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_time}&end={end_time}&appid={api_key}"

# Make a request to the endpoint to get the AQI details
# response = requests.get(api_url, timeout=5_000)

# if response.status_code == 200:
#     with open(f"./.sample_data/{file_name}", "wb") as file:
#         file.write(response.content)

with open(file_path, encoding="UTF-8") as file:
    data = json.load(file)
    df = pd.DataFrame(data["list"])
