# spark_job.py
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    lit, col, when, expr, date_format, dayofmonth, floor,
    row_number, hour, monotonically_increasing_id
)
from pyspark.sql.types import IntegerType

def build_spark(app_name="nyc_taxi_transformation"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def main(args):
    spark = build_spark()

    # Input paths (GCS)
    source_parquet = args.source_parquet  # e.g. gs://datalake-grp-03/.../part-00000...parquet
    zone_csv = args.zone_csv             # e.g. gs://nycfinalp/taxi_zone_lookup.csv

    # Output locations
    out_gcs_parquet = args.out_gcs_parquet  # e.g. gs://raw-data-grp-3/cleaned-data/transformeddata/
    bq_table = args.bq_table                 # optional BigQuery table: project.dataset.table

    # Read inputs
    master_df = spark.read.option("header", True).parquet(source_parquet)
    zone_df = spark.read.option("header", True).csv(zone_csv)

    # Transformations (kept same logic as your Glue job)
    transformed_df = (master_df
       .filter((col("passenger_count").isNotNull()) & (col("passenger_count") != 0))
       .filter((col("RatecodeID").isNotNull()) & (col("RatecodeID") != 99))
       .drop("store_and_fwd_flag", "airport_fee", "congestion_surcharge")
       .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
       .withColumn("week_of_month", (floor((dayofmonth("tpep_pickup_datetime") - 1) / 7) + 1).cast("int"))
       .withColumn("day_name", date_format("tpep_pickup_datetime", "EEEE"))
       .withColumn("time_of_day",
                   when((hour(col("tpep_pickup_datetime")) >= 6) & (hour(col("tpep_pickup_datetime")) < 18), "Day")
                   .otherwise("Night"))
       .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
       .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
       .join(zone_df.select(col("LocationID").alias("PULocationID"),
                            col("Zone").alias("pickup_zone"),
                            col("Borough").alias("pickup_borough")),
             on="PULocationID", how="left")
       .join(zone_df.select(col("LocationID").alias("DOLocationID"),
                            col("Zone").alias("dropoff_zone"),
                            col("Borough").alias("dropoff_borough")),
             on="DOLocationID", how="left")
       # Use monotonically_increasing_id to avoid global shuffle/ordering issues
       .withColumn("Id", monotonically_increasing_id())
       .withColumn("tip_percentage",
                   when(col("fare_amount").cast("double") > 0,
                        (col("tip_amount").cast("double") / col("fare_amount").cast("double")) * 100)
                   .otherwise(0.0))
       .filter((col("trip_distance").cast("double") <= 100) & (col("fare_amount").cast("double") <= 500))
       .withColumn("distance_bucket",
                   when(col("trip_distance").cast("double") < 1, "0-1 miles")
                   .when((col("trip_distance").cast("double") >= 1) & (col("trip_distance").cast("double") < 5), "1-5 miles")
                   .when((col("trip_distance").cast("double") >= 5) & (col("trip_distance").cast("double") < 10), "5-10 miles")
                   .otherwise("10+ miles"))
       .withColumn("payment_type_desc",
                   expr("""
                       CASE payment_type
                           WHEN 1 THEN 'Credit Card'
                           WHEN 2 THEN 'Cash'
                           WHEN 3 THEN 'No Charge'
                           WHEN 4 THEN 'Dispute'
                           WHEN 5 THEN 'Unknown'
                           WHEN 6 THEN 'Voided'
                           ELSE 'Other'
                       END
                   """))
       .withColumn("ratecode_desc",
                   when(col("RatecodeID") == 1, "Standard rate")
                   .when(col("RatecodeID") == 2, "JFK")
                   .when(col("RatecodeID") == 3, "Newark")
                   .when(col("RatecodeID") == 4, "Nassau or Westchester")
                   .when(col("RatecodeID") == 5, "Negotiated fare")
                   .when(col("RatecodeID") == 6, "Group ride")
                   .otherwise("Other"))
       .withColumn("vendor_desc",
                   when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
                   .when(col("VendorID") == 2, "Curb Mobility, LLC")
                   .when(col("VendorID") == 6, "Myle Technologies Inc")
                   .when(col("VendorID") == 7, "Helix")
                   .when(col("VendorID").isin(3, 4, 5), "Third Party")
                   .otherwise(None))
       .drop("service_zone")
    )

    # Add 'year' column if not present (used for partitioning) - derived from pickup timestamp
    if 'year' not in transformed_df.columns:
        from pyspark.sql.functions import year as spark_year
        transformed_df = transformed_df.withColumn('year', spark_year(col('tpep_pickup_datetime')))

    # Write out to GCS as partitioned parquet
    transformed_df.write.mode("overwrite").partitionBy("year").parquet(out_gcs_parquet)

    # Optionally write to BigQuery (if bq_table provided)
    if bq_table:
        # Requires Dataproc cluster with the spark-bigquery connector or providing the connector jar
        transformed_df.write \
            .format("bigquery") \
            .option("table", bq_table) \
            .mode("overwrite") \
            .save()

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC taxi Dataproc Spark job")
    parser.add_argument("--source_parquet", required=True)
    parser.add_argument("--zone_csv", required=True)
    parser.add_argument("--out_gcs_parquet", required=True)
    parser.add_argument("--bq_table", required=False, default=None)
    parsed = parser.parse_args()
    main(parsed)
