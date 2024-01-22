import os
from utilities.util_methods import *
from extract import start_extraction_flow
from pathlib import Path
from transform import start_transformation_flow
from load import start_load_flow_local, start_load_flow_cloud
from prefect import flow
from dotenv import load_dotenv


load_dotenv()


@flow(name="ETL Main Flow")
def start_etl_flow(city_names: str, start_date: str, end_date: str = "") -> None:
    """Start the ETL flow"""
    table_name = "all_city_aqi"
    data_path = Path("./.sample_data")
    dataset = os.getenv("AQI_DATASET_NAME")

    start_day_unix_time = get_unix_time_from_date(start_date)

    if end_date:
        end_day_unix_time = get_unix_time_from_date(end_date)
    else:
        end_day_unix_time = get_current_time()

    for index, city_names in enumerate(city_names.split(",")):
        print(f"Running the flow for {city_names}")

        # Get coordinates for the city
        if city_names:
            coordinates = get_city_coordinates(city_names)
        else:
            raise ValueError("Please enter the city name.")

        print(f"coordinates : {coordinates}")  # Extract the data
        response_file_path = start_extraction_flow(
            coordinates["lat"],
            coordinates["lon"],
            start_day_unix_time,
            end_day_unix_time,
            city_names,
        )

        # Apply transformations
        df = start_transformation_flow(response_file_path)

        replace_table = index == 0 and len(city_names) > 1

        # Load the data to GCS and BigQuery
        start_load_flow_local(
            df, table_name, data_path, dataset, city_names, replace_table
        )

    start_load_flow_cloud(df, table_name, data_path, dataset, city_names)


if __name__ == "__main__":
    city_name = os.getenv("CITY")
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    start_etl_flow(city_name, start_date, end_date)
