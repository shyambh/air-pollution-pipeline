"""
Extract the Air Quality Index (AQI) data for the selected City
"""

from pathlib import Path
import requests
import os
from dotenv import load_dotenv
from prefect import flow, task


@task(name="Make a request and store the json locally", retries=3)
def call_api_and_save_response(
    lat, lon, start_time, end_time, city_name, api_key
) -> Path:
    """Make a request and store the json locally"""
    file_name = f"aqi_data_{city_name}.json"
    file_path = Path(f"./.sample_data/{file_name}")

    api_url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_time}&end={end_time}&appid={api_key}"

    if not file_path.exists():
        # Make a request to the endpoint to get the AQI details
        response = requests.get(api_url, timeout=5_000)

        if response.status_code == 200:
            with open(f"./.sample_data/{file_name}", "wb") as file:
                file.write(response.content)

    return file_path


@flow(name="Start the Extraction Flow")
def start_extraction_flow(lat, lon, start_time, end_time, city_name, api_key) -> Path:
    """Being the execution of the data extraction

    Args:
        lat (str): Latitude of the city
        lon (str): Longitude of the city
        start_time (str): Initial time of metric gathering
        end_time (str): End time of metric gathering
        city_name (str): Name of the city

    Returns:
        Path
    """
    return call_api_and_save_response(lat, lon, start_time, end_time, city_name, api_key)
