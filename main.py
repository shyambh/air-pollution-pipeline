import os
from extract import call_api_and_save_response
from pathlib import Path
from transform import transform_data
from load import start_load_flow
from prefect import flow, task
from dotenv import load_dotenv


load_dotenv()


@flow(name="ETL Main Flow")
def start_etl_flow():
    """Start the ETL flow"""

    lat = 27.708317
    lon = 85.3205817
    start_time = 1606807137
    end_time = 1704007137
    city_name = "Kathmandu"
    table_name = f"aqi_{city_name}"
    data_path = Path("./.sample_data")
    dataset = os.getenv("AQI_DATASET_NAME")

    # Extract the data
    response_file_path = call_api_and_save_response(
        lat, lon, start_time, end_time, city_name
    )

    # Apply transformations
    df = transform_data(response_file_path)

    # Load the data
    start_load_flow(df, table_name, data_path, dataset)


if __name__ == "__main__":
    start_etl_flow()
