# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from airflow.models import Variable
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

FIXTURES_DATA_FOLDER = (
    f"{os.environ['AIRFLOW_HOME']}/include/fixtures_data/"
)

RESULTS_DATA_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/results/"
)

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
FIXTURES_IN_TABLE_NAME = "in_fixtures"
REPORTING_TABLE_NAME_1 = "reporting_table_1"
REPORTING_TABLE_NAME_2 = "reporting_table_2"
CONN_ID_DUCKDB = "duckdb_default"

# Datasets
DS_FIXTURES_DATA_MINIO = Dataset(f"minio://{FIXTURES_BUCKET_NAME}")
DS_DUCKDB_IN_FIXTURES = Dataset(f"duckdb://{FIXTURES_IN_TABLE_NAME}")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")
DS_INGEST = Dataset("ingest")

# API KEYS

API_ENDPOINT = Variable.get("API_ENDPOINT")
API_KEY = Variable.get("API_KEY")
API_HOST = Variable.get("API_HOST")

# API_ENDPOINT = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
# API_KEY = "8feeed635dmshbab9080dc4f248bp112d75jsn9661eab3597b"
# API_HOST = "api-football-v1.p.rapidapi.com"



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