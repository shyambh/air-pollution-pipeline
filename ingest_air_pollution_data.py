"""
Ingest the Air Quality Index (AQI) for the selected City
"""
import datetime
from pathlib import Path
import json
import requests
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from prefect_gcp import GcpCredentials


# @task(name="Make a request and store the json locally", retries=3)
def call_api_and_save_response():
    """Make a request and store the json locally"""
    lat = 27.708317
    lon = 85.3205817
    start_time = 1606807137
    end_time = 1704007137
    api_key = "7c66b0105c57c08de6263a0e3ef2fbf6"
    city_name = "Kathmandu"
    file_name = f"aqi_data_{city_name}.json"
    file_path = Path(f"./.sample_data/{file_name}")
    table_name = f"aqi_{city_name}"

    api_url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_time}&end={end_time}&appid={api_key}"
    # Make a request to the endpoint to get the AQI details
    # response = requests.get(api_url, timeout=5_000)

    # if response.status_code == 200:
    #     with open(f"./.sample_data/{file_name}", "wb") as file:
    #         file.write(response.content)

    with open(file_path, encoding="UTF-8") as file:
        data = json.load(file)
        # Store to local postgres
        connection_block = SqlAlchemyConnector.load("pg-sql-connector")

        with connection_block.get_connection(begin=False) as db_engine:
            df = pd.json_normalize(data["list"], max_level=1)

            # Refining the column names to remove the parent key from the JSON object
            df.rename(columns=lambda x: x.split(".")[-1], inplace=True)

            # Converting the Unix timestamp to datetime
            df["dt"] = df["dt"].apply(datetime.datetime.fromtimestamp)

            # create the table
            df.head(n=0).to_sql(table_name, con=db_engine, if_exists="replace")

            df.to_sql(table_name, con=db_engine, if_exists="append")


call_api_and_save_response()
