import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from prefect_gcp import GcpCredentials
from prefect_sqlalchemy import SqlAlchemyConnector


def save_to_local(df: pd.DataFrame, table_name: str):
    database_block = SqlAlchemyConnector.load("pg-sql-connector")
    with database_block.get_connection(begin=False) as db_engine:
        # create the table
        df.head(n=0).to_sql(table_name, con=db_engine, if_exists="replace")

        df.to_sql(table_name, con=db_engine, if_exists="append")
