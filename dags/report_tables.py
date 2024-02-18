"""DAG that runs a transformation on data in DuckDB using the Astro SDK"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime

# import tools from the Astro SDK
# from astro import sql as aql
from astro.sql.table import Table

# from airflow.operators.python_operator import PythonOperator


# from astro.sql import connection
# from astro.sql.table import Table


# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.utils import apply_filtering_logic

# ----------------- #
# Astro SDK Queries #
# ----------------- #


# -------


@dag(
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=gv.default_args,
    description="Runs a transformation on climate data in DuckDB.",
    tags=["transform"],
)
def create_reporting_table():

    # input the raw climate data and save the outcome of the transformation to a
    # permanent reporting table
    apply_filtering_logic(
        output_table=Table(name=gv.REPORTING_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB),
    )


create_reporting_table()
