# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st
import duckdb
import pandas as pd
from datetime import date, datetime
import altair as alt
# from global_variables import global_variables as gv




duck_db_instance_path = (
    "/app/include/dwh"  # when changing this value also change the db name in .env
)


# -------------- #
# DuckDB Queries #
# -------------- #


def list_currently_available_tables(db=duck_db_instance_path):
    cursor = duckdb.connect(db)
    tables = cursor.execute("SHOW TABLES;").fetchall()
    cursor.close()
    return [table[0] for table in tables]







def get_fixtures(db=duck_db_instance_path):

    cursor = duckdb.connect(db)
    fixtures_data = cursor.execute(
        f"""SELECT * FROM reporting_table;"""
    ).fetchall()

    fixtures_data_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = 'reporting_table';"""
    ).fetchall()

    df = pd.DataFrame(
        fixtures_data, columns=[x[0] for x in fixtures_data_col_names]
    )
    cursor.close()

    return df




# ------------ #
# Query DuckDB #
# ------------ #


tables = list_currently_available_tables()


if "reporting_table" in tables:
    fixtures_result_table = get_fixtures()



# ------------- #
# STREAMLIT APP #
# ------------- #

st.title("Fixtures Transformation Results")

st.markdown(f"Hello Joshua :wave: Welcome to your Streamlit App! :blush:")

st.subheader("Hey")
# Get the DataFrame

# Display the DataFrame as a table in Streamlit
st.dataframe(fixtures_result_table)




