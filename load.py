import pandas as pd
import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from prefect_gcp import GcpCredentials
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="Save to local Postgres", retries=2)
def store_in_local_db(df: pd.DataFrame, table_name: str):
    database_block = SqlAlchemyConnector.load("pg-sql-connector")
    with database_block.get_connection(begin=False) as db_engine:
        # create the table
        df.head(n=0).to_sql(table_name, con=db_engine, if_exists="replace")

        df.to_sql(table_name, con=db_engine, if_exists="append")


@task(name="Store to Google Cloud Storage and Big Query", retries=2)
def store_in_cloud(table_name: str, path: Path, dataset: str) -> None:
    sql_connection_block = SqlAlchemyConnector.load("pg-sql-connector")
    gcp_bucket_block = GcsBucket.load("gcp-zoomcamp-bucket")
    gcp_credentials_block = GcpCredentials.load("gcp-de-zoomcamp-creds")

    with sql_connection_block.get_connection() as sql_con:
        df = pd.read_sql_table(table_name, con=sql_con)

        df.to_parquet(
            f"./{path}/{table_name}.parquet",
            compression="gzip",
        )
    # Store the local parquet file to GCP Bucket
    gcp_bucket_block.upload_from_dataframe(
        df,
        to_path=f"aqi/{table_name}.parquet",
        serialization_format=DataFrameSerializationFormat.PARQUET_GZIP,
    )

    # Store the local parquet file to BigQuery
    df.to_gbq(
        f"{dataset}.{table_name}",
        os.getenv("GCP_PROJECT_NAME"),
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )


@flow(name="Main Load Flow")
def start_load_flow(df: pd.DataFrame, table_name: str, path: Path, dataset: str):
    store_in_local_db(df, table_name)
    store_in_cloud(table_name, path, dataset)
