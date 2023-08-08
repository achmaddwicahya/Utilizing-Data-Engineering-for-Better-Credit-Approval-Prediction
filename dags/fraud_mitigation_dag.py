import os
import sys
import logging
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

DATASET_ID = Variable.get("DATASET_ID")
BASE_PATH = Variable.get("BASE_PATH")
BUCKET_NAME = Variable.get("BUCKET_NAME")
GOOGLE_CLOUD_CONN_ID = Variable.get("GOOGLE_CLOUD_CONN_ID")
BIGQUERY_TABLE_NAME = "application_record"
GCS_OBJECT_NAME = "application_record.csv"
SOURCE_DATA = f"https://drive.google.com/uc?export=download&id=1uNWpKM6seqOgrnuPkMZKycLvko8-rqhO"
DATA_RAW_PATH = f"{BASE_PATH}/data_raw"
DATA_CLEAN_PATH = f"{BASE_PATH}/data_clean"
# OUTPUT_RAW_PATH = f"{DATA_RAW_PATH}/{GCS_OBJECT_NAME}"

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    return pq.write_table(table, src_file.replace('.csv', '.parquet'))

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# CREATE DAG 

with DAG(
    dag_id="data_ingestion_rek",
    default_args={
        "owner": "Kelompok 3",
        "retries": 2,
        "start_date": days_ago(1),
    },
    schedule="@daily",
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')

    #Download data from source
    download_data_task = BashOperator(
        task_id="download_data_task",
        bash_command="gdrive_connect.sh"
    )

    format_raw_data_to_parquet_task = DummyOperator(task_id='format_raw_data_to_parquet_task')

    # format_raw_data_to_parquet_task = PythonOperator(
    #     task_id="format_raw_data_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{DATA_RAW_PATH}/{GCS_OBJECT_NAME}", 
    #     },
    # )

    stored_data_raw_csv_to_gcs_task = PythonOperator(
        task_id="stored_data_raw_csv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": f"raw_data/application_record.csv",          
            "local_file": f"{DATA_RAW_PATH}/application_record.csv"      
        },
    )

    #Clean and Transform Raw Data using Spark
    spark_cleansing_task = BashOperator(
        task_id="spark_cleansing_task",
        bash_command= "cd /opt/airflow/spark && python3 spark_final_project.py"
    )

    stored_data_clean_to_gcs_task = PythonOperator(
        task_id="stored_data_clean_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": f"clean_data/application_record_clean.csv",         
            "local_file": f"{DATA_CLEAN_PATH}/application_record_clean.csv"        
        },
    )

    stored_ml_result_to_gcs_task = PythonOperator(
        task_id="stored_ml_result_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": f"clean_data/ml_result.csv",         
            "local_file": f"{DATA_CLEAN_PATH}/ml_result.csv"        
        },
    )
    
    #Load data to Big Query
    loaded_data_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket= BUCKET_NAME,
        source_objects=f"clean_data/application_record_clean.csv",
        source_format='csv',
        destination_project_dataset_table=f"{DATASET_ID}.application_record_clean",
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )

    #Run dbt deps
    dbt_deps_task = BashOperator(
        task_id="dbt_deps_task",
        bash_command= "cd /opt/airflow/dbt/fraud_mitigation_dwh && dbt deps --profiles-dir ."
    )

    # Run all script in dbt folder
    run_dbt_task = BashOperator(
        task_id="run_dbt_task",
        bash_command= "cd /opt/airflow/dbt/fraud_mitigation_dwh &&  dbt run --profiles-dir ."
    )

    end = DummyOperator(task_id='end')

# WORKFLOW DAG RUNNING
    start >> download_data_task >> stored_data_raw_csv_to_gcs_task 
    download_data_task >> format_raw_data_to_parquet_task
    format_raw_data_to_parquet_task >>  spark_cleansing_task
    spark_cleansing_task >> stored_data_clean_to_gcs_task
    spark_cleansing_task >> stored_ml_result_to_gcs_task
    stored_data_clean_to_gcs_task >> loaded_data_bigquery
    loaded_data_bigquery >> dbt_deps_task >> run_dbt_task >> end


