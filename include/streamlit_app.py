import streamlit as st
import duckdb
import pandas as pd

import global_variables as gv


# duck_db_instance_path = (
#     "/app/include/dwh"  # when changing this value also change the db name in .env
# )



# -------------- #
# DuckDB Queries #
# -------------- #

def get_fixtures_info(db=gv.DUCKDB_INSTANCE_NAME):

    cursor = duckdb.connect(db)
    fixtures_data = cursor.execute(
        f"""SELECT * FROM {gv.REPORTING_TABLE_NAME};"""
    ).fetchall()

    fixtures_data_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = '{gv.REPORTING_TABLE_NAME}';"""
    ).fetchall()
   
    df = pd.DataFrame(
        fixtures_data, columns=[x[0] for x in fixtures_data_col_names]
    )
    cursor.close()


    return df

# -------------- #
# STREAMLIT APP #
# -------------- #

st.title("Football API App")

# st.markdown(f"Hello {user_name} :wave: Welcome to your Streamlit App! :blush:")

# st.subheader("Surface temperatures")

data = get_fixtures_info()
st.dataframe()

st.button("Re-run")