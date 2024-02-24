# import os
# from airflow.decorators import dag, task

# @task
# def dump_data_to_bucket(tweet_list: list):
#     import pandas as pd
#     from minio import Minio
#     from io import BytesIO
#     MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
#     MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
#     MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

#     df = pd.DataFrame(tweet_list)
#     csv = df.to_csv(index=False).encode("utf-8")

#     client = Minio("minio:9000", access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

#     # Make MINIO_BUCKET_NAME if not exist.
#     found = client.bucket_exists(MINIO_BUCKET_NAME)
#     if not found:
#         client.make_bucket(MINIO_BUCKET_NAME)
#     else:
#         print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

#     # Put csv data in the bucket
#     client.put_object(
#         "airflow-bucket", "twitter_elon_musk.csv", data=BytesIO(csv), length=len(csv), content_type="application/csv"
#     )