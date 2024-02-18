from airflow import Dataset
import logging
import os
from minio import Minio
from pendulum import duration
import json

DS_START = Dataset("start")

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
# WEATHER_BUCKET_NAME = "weather"
FIXTURES_BUCKET_NAME = "fixtures"
ARCHIVE_BUCKET_NAME = "archive"

# DAG default arguments
default_args = {
    "owner": "Edun Joshua",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
}

LEAGUE_IDS= [39, 40]
# league_ids = [39, 40,
#               41, 42, 43, 50, 51, 59, 60,
#              78, 79, 83, 84, 85, 86, 61, 62, 135, 136,
#              88, 89, 106, 107,  144, 283, 94, 95, 96,
#              140, 141, 142, 876, 435, 436, 210, 318, 345,
#              203, 204, 271, 272, 235, 373, 307, 308, 301, 303,
#              207, 208, 179, 180, 183, 184,
#             305, 233, 290, 218, 219, 419, 172,
#              119, 120, 236, 288, 128, 129, 134, 71, 72, 383, 382
#               ]


# Source file path fixtures data
TEMP_GLOBAL_PATH = f"{os.environ['AIRFLOW_HOME']}/include/fixtures/"

# Datasets
DS_FIXTURES_DATA_MINIO = Dataset(f"minio://{FIXTURES_BUCKET_NAME}")
# DS_WEATHER_DATA_MINIO = Dataset(f"minio://{WEATHER_BUCKET_NAME}")
# DS_DUCKDB_IN_WEATHER = Dataset("duckdb://in_weather")
DS_DUCKDB_IN_FIXTURES = Dataset("duckdb://in_fixtures")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
# CLIMATE_TABLE_NAME = "temp_global_table"
REPORTING_TABLE_NAME = "reporting_table"
CONN_ID_DUCKDB = "duckdb_default"

# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# API Config variables - hide them later in AIRFLOW UI
API_ENDPOINT = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
API_KEY = "8feeed635dmshbab9080dc4f248bp112d75jsn9661eab3597b"
API_HOST="api-football-v1.p.rapidapi.com"

#
CHOSEN_SEASON = "2023"