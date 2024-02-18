""" DAG that kicks off the pipeline by producing to the start dataset"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime
from include.global_variables import global_variables as gv


@dag(
    start_date=datetime(2023,1,1),
    schedule="@once",
    catchup=False,
    default_args=gv.default_args,
    description="Run this DAG to start the pipeline",
    tags=["start","setup"]
)
def start():
    # this task uses the BashOperator to run a bash command creating an Airflow
    # pool called 'duckdb' which contains one worker slot. All tasks running queries
    # against DuckDB will be assigned to this pool, preventing parallel requests to DuckDB.

    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool"
        bash_command="airflow pools list | grep -q 'duckdb' || airflow pools set duckdb 1 'Pool for duckdb'"
        outlets=[gv.DS_START]
    )

start_dag = start()