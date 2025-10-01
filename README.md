# NYC Taxi ELT Pipeline — GCP Dataproc, BigQuery, Cloud Composer

This project implements a scalable ELT pipeline on Google Cloud Platform (GCP) for NYC taxi trip records. It leverages Dataproc (PySpark), BigQuery, Cloud Storage (GCS), and Cloud Composer (Airflow) for end-to-end orchestration, transformation, and analytics.

---

## Pipeline Architecture

- **Input**: Parquet files on GCS (taxi trip data), CSV (zone lookup) on GCS
- **Transform**: PySpark job (`spark_job.py`) on Dataproc
- **Output**: Partitioned Parquet on GCS and optionally BigQuery table
- **Orchestration**: Airflow DAG in Cloud Composer
- **Testing/Ad-hoc runs**: gcloud CLI

---

## 1. PySpark Job (`spark_job.py`)

- Reads Parquet & CSV from GCS
- Cleans and enriches data (feature engineering, joins, filtering)
- Writes partitioned Parquet to GCS and/or to BigQuery
- Uses distributed-safe ID generation (`monotonically_increasing_id`)

**How to use:**
- Upload `spark_job.py` to `gs://<YOUR_BUCKET>/jobs/spark_job.py`
- Example CLI args:
  ```bash
  --source_parquet gs://<YOUR_BUCKET>/input/nyc_trips.parquet \
  --zone_csv gs://<YOUR_BUCKET>/input/taxi_zone_lookup.csv \
  --out_gcs_parquet gs://<YOUR_BUCKET>/output/transformed/ \
  --bq_table your_project.your_dataset.nyc_taxi_transformed
  ```

---

## 2. Airflow DAG (Cloud Composer)

- Submits the PySpark job to Dataproc
- Schedule: monthly (customizable)
- Place the DAG in your Composer environment’s `dags/` folder

**Config to edit in the DAG:**
- `PROJECT_ID`
- `REGION`
- `CLUSTER_NAME`
- GCS paths and BigQuery table

---

## 3. Manual Run with gcloud

**Create Dataproc Cluster:**
```bash
gcloud dataproc clusters create nyc-dataproc-cluster \
  --region=us-central1 \
  --zone=us-central1-a \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4 \
  --num-workers=2 \
  --image-version=2.1-debian10 \
  --optional-components=ANACONDA \
  --project=your-gcp-project \
  --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/connectors/install-bigquery-connector.sh
```

**Submit PySpark Job:**
```bash
gcloud dataproc jobs submit pyspark gs://<YOUR_BUCKET>/jobs/spark_job.py \
  --region=us-central1 \
  --cluster=nyc-dataproc-cluster \
  -- \
  --source_parquet gs://<YOUR_BUCKET>/input/nyc_trips.parquet \
  --zone_csv gs://<YOUR_BUCKET>/input/taxi_zone_lookup.csv \
  --out_gcs_parquet gs://<YOUR_BUCKET>/output/transformed/ \
  --bq_table your_project.your_dataset.nyc_taxi_transformed
```

---

## 4. Best Practices & Notes

- **Connectors**: Ensure Dataproc has GCS and BigQuery connectors (default or via init action)
- **IAM**:  
  - Dataproc service account: roles/bigquery.dataEditor, roles/storage.objectAdmin  
  - Composer service account: roles/dataproc.editor or roles/dataproc.jobRunner
- **Cluster Sizing**: 3 nodes (n1-standard-4) for ~20GB/77M rows; tune as needed
- **Partitioning**: Output is partitioned by `year` for performance
- **Broadcast Joins**: Zone CSV is broadcast; efficient for small lookups
- **ID Generation**: Uses `monotonically_increasing_id()` for distributed safety
- **Retries**: Use Composer & Dataproc retries for robustness
- **Costs**: Use ephemeral clusters or autoscaling to optimize spend

---

## 5. Quick Checklist

- [ ] Upload `spark_job.py` to GCS
- [ ] Ensure Dataproc cluster has BigQuery connector
- [ ] Configure IAM for Dataproc & Composer service accounts
- [ ] Place Airflow DAG in Composer’s `dags/`
- [ ] Edit all paths and project/table names in code and DAG

---

## Example Resume Achievement

> Built a production GCP ELT pipeline processing 77M+ NYC taxi trips monthly using PySpark on Dataproc, BigQuery, GCS, and Composer. Automated data ingestion, partitioned outputs for analytics, and engineered robust operational workflows.

---

## Project Structure

```
jobs/
  spark_job.py      # PySpark ETL job
composer/
  dags/
    dataproc_nyc_taxi_dag.py
README.md           # This file
```

---

## References

- [Dataproc documentation](https://cloud.google.com/dataproc/docs)
- [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
- [Cloud Composer docs](https://cloud.google.com/composer/docs)
