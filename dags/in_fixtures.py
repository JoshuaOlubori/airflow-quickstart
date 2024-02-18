"""DAG that loads climate ingests from local csv files into MinIO."""
from airflow.decorators import dag
from pendulum import datetime
# import io
import os

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the start Dataset has been updated
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Loads fixtures data form local storage to DuckDB.",
    tags=["ingestion"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_fixtures_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_fixtures_bucket", bucket_name=gv.FIXTURES_BUCKET_NAME
    )

    # use the custom LocalCSVToMinIOOperator to read the contents in /include/fixtures
    # into MinIO. This task uses dynamic task allowing you to add additional files to
    # the folder and reading them in without changing any DAG code


    # files_to_ingest = []
    # for file_name in os.listdir(gv.TEMP_GLOBAL_PATH):
    #     file_path = os.path.join(gv.TEMP_GLOBAL_PATH, file_name)
    #     files_to_ingest.append({
    #         "local_file_path": file_path,
    #         "object_name": file_name,
    #     })

    files_to_ingest = []
    for root, dirs, files in os.walk(gv.TEMP_GLOBAL_PATH):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(file_path, gv.TEMP_GLOBAL_PATH)
            files_to_ingest.append({
                "local_file_path": file_path,
                "object_name": relative_path,
            })


    ingest_fixtures_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_fixtures_data",
        bucket_name=gv.FIXTURES_BUCKET_NAME,
        outlets=[gv.DS_FIXTURES_DATA_MINIO],
    ).expand_kwargs(files_to_ingest)

    # set dependencies
    create_bucket_tg >> ingest_fixtures_data


in_fixtures_data()
