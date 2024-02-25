"DAG that runs a transformation on data in DuckDB using the Astro SDK."

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import pandas as pd

# import tools from the Astro SDK
from astro import sql as aql
from astro.sql.table import Table

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

from include.logic import apply_filtering_logic, won_last_5_matches

# -------- #
# Datasets #
# -------- #

# in_fixtures_dataset = Table(
#     name=gv.FIXTURES_IN_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
# )

# ----------------- #
# Astro SDK Queries #
# ----------------- #


# Create a reporting table that counts heat days per year for each city location



# ---------- #
# Exercise 3 #
# ---------- #
# Use pandas to transform the 'historical_weather_reporting_table' into a table
# showing the hottest day in your year of birth (or a year of your choice, if your year
# of birth is not available for your city). Make sure the function returns a pandas dataframe
# Tip: the returned dataframe will be shown in your streamlit App.

# SOLUTION: One of many possible solutions to retrieve the warmest day by year by city
@aql.dataframe(pool="duckdb")
def find_fixtures_c1(in_table: pd.DataFrame):
    # print ingested df to the logs
    gv.task_log.info(in_table)

    df = in_table
# import transformation function from utils.py
    output_df = apply_filtering_logic(df)
    

    # print result table to the logs
    gv.task_log.info(output_df)

    return output_df

@aql.dataframe(pool="duckdb")
def find_fixtures_c2(in_table: pd.DataFrame):
    # print ingested df to the logs
    gv.task_log.info(in_table)

    df = in_table
# import transformation function from utils.py
    output_df = won_last_5_matches(df)
    

    # print result table to the logs
    gv.task_log.info(output_df)

    return output_df


# --- #
# DAG #
# --- #

# ---------- #
# Exercise 1 #
# ---------- #
# Schedule this DAG to run as soon as the 'extract_historical_weather_data' DAG has finished running.
# Tip: You can either add your own Dataset as an outlet in the last task of the previous DAG or
# use the a Astro Python SDK Table based Dataset as seen in the 'transform_climate_data' DAG.


@dag(
    start_date=datetime(2023, 1, 1),
    # SOLUTION: Run this DAG as soon as the Astro Python SDK Table where ingested historical weather data is stored is updated
    schedule=[gv.DS_DUCKDB_IN_FIXTURES],
    catchup=False,
    default_args=gv.default_args,
    description="Runs transformations on fixtures data in DuckDB.",
    tags=["transformation"],
)
def transform_fixtures():


    find_fixtures_c1(
        in_table=Table(
            name=gv.FIXTURES_IN_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
        output_table=Table(
            name=gv.REPORTING_TABLE_NAME_1, conn_id=gv.CONN_ID_DUCKDB
        ),
    )

    find_fixtures_c2(
        in_table=Table(
            name=gv.FIXTURES_IN_TABLE_NAME, conn_id=gv.CONN_ID_DUCKDB
        ),
        output_table=Table(
            name=gv.REPORTING_TABLE_NAME_2, conn_id=gv.CONN_ID_DUCKDB
        ),
    )


transform_fixtures()