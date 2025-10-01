# dataproc_nyc_taxi_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "your-gcp-project"
REGION = "us-central1"            # adjust
CLUSTER_NAME = "nyc-dataproc-cluster"
GCS_BUCKET = "your-bucket"
PYTHON_FILE_URI = f"gs://{GCS_BUCKET}/jobs/spark_job.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "nyc_taxi_dataproc_pipeline",
    default_args=default_args,
    description="Run NYC taxi transformation on Dataproc",
    schedule_interval="@monthly",   # your monthly orchestration
    start_date=days_ago(1),
    catchup=False,
) as dag:

    dataproc_spark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYTHON_FILE_URI,
            "args": [
                "--source_parquet", "gs://datalake-grp-03/nyc-merged-raw-data/part-00000-a7fd998c-ac15-4237-b9b0-684f953b2375-c000.snappy.parquet",
                "--zone_csv", "gs://nycfinalp/taxi_zone_lookup.csv",
                "--out_gcs_parquet", "gs://raw-data-grp-3/cleaned-data/transformeddata/",
                "--bq_table", "your_project.your_dataset.nyc_taxi_transformed"  # optional
            ],
            # Optionally include jars/packages if your Dataproc image doesn't include them:
            # "archive_uris": [],
            # "jar_file_uris": [],
            # "file_uris": [],
        },
    }

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_nyc_taxi_spark_job",
        job=dataproc_spark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    submit_job
