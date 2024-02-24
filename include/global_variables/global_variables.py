# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from minio import Minio
from pendulum import duration
import json

# -------------------- #
# Enter your own info! #
# -------------------- #

# MY_NAME = "Friend"
# MY_CITY = "Portland"

# ----------------------- #
# Configuration variables #
# ----------------------- #

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
FIXTURES_BUCKET_NAME = "fixtures"
# ARCHIVE_BUCKET_NAME = "archive"

# Source file path climate data
FIXTURES_DATA_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/fixtures_data/all_fixtures_combined.csv"
)

RESULTS_DATA_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/results/"
)

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
FIXTURES_IN_TABLE_NAME = "in_fixtures"
REPORTING_TABLE_NAME = "reporting_table"
CONN_ID_DUCKDB = "duckdb_default"

# Datasets
DS_FIXTURES_DATA_MINIO = Dataset(f"minio://{FIXTURES_BUCKET_NAME}")
DS_DUCKDB_IN_FIXTURES = Dataset(f"duckdb://{FIXTURES_IN_TABLE_NAME}")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")



# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": "Edun Joshua",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=1),
}

# default coordinates
# default_coordinates = {"city": "No city provided", "lat": 0, "long": 0}


# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client


# command to run streamlit app within codespaces/docker
# modifications are necessary to support double-port-forwarding
STREAMLIT_COMMAND = "streamlit run weather_v_climate_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"