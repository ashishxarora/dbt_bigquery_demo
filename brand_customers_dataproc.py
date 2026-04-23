#!/usr/bin/env python3
"""
PySpark script for Dataproc to read brand customer data from BigQuery.
Reads brand_a and brand_b customer tables, excludes PII columns,
counts customers in each table, and produces combined total.

PII Columns Excluded:
- brand_a: email_id, name
- brand_b: cc_token, display_name
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_spark_session() -> SparkSession:
    """
    Create Dataproc-optimized Spark session with BigQuery connector.
    Uses Dataproc-specific configurations for GCS staging and BigQuery.
    """
    spark = (
        SparkSession.builder.appName("BrandCustomersAnalysis")
        .config("spark.jars", "gs://spark-lib/bigquery/bigquery-connector.jar")
        .config("spark.datasource.bigquery.projectId", "your-project-id")
        .config("spark.datasource.bigquery.parentProject", "your-project-id")
        .config("spark.datasource.bigquery.gcsBucket", "your-staging-bucket")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.jsonKeyFile",
            "/path/to/service-account.json",
        )
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "4")
        .config("spark.dataproc.pool.enable", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_brand_a_customers(spark: SparkSession) -> "DataFrame":
    """
    Read brand_a customers from BigQuery, excluding PII columns.
    Excluded: email_id, name
    """
    df = (
        spark.read.format("bigquery")
        .option("table", "your-project.demo_dataset.bronze_brand_a_customers")
        .load()
    )

    # Exclude PII columns: email_id, name
    pii_columns = ["email_id", "name"]
    non_pi_columns = [c for c in df.columns if c not in pii_columns]

    return df.select(*non_pi_columns)


def read_brand_b_customers(spark: SparkSession) -> "DataFrame":
    """
    Read brand_b customers from BigQuery, excluding PII columns.
    Excluded: cc_token (payment token), display_name
    """
    df = (
        spark.read.format("bigquery")
        .option("table", "your-project.demo_dataset.bronze_brand_b_customers")
        .load()
    )

    # Exclude PII columns: cc_token (credit card token), display_name
    pii_columns = ["cc_token", "display_name"]
    non_pi_columns = [c for c in df.columns if c not in pii_columns]

    return df.select(*non_pi_columns)


def count_customers(df: "DataFrame") -> int:
    """Count total customers in a DataFrame."""
    return df.count()


def main():
    """Main execution: read both tables, count customers, output combined total."""

    # Create Dataproc-optimized Spark session
    spark = create_spark_session()

    try:
        # Read brand_a customers (PII excluded: email_id, name)
        brand_a_df = read_brand_a_customers(spark)
        brand_a_count = count_customers(brand_a_df)

        # Read brand_b customers (PII excluded: cc_token, display_name)
        brand_b_df = read_brand_b_customers(spark)
        brand_b_count = count_customers(brand_b_df)

        # Combined total customer count
        combined_total = brand_a_count + brand_b_count

        # Output results
        print("=" * 60)
        print("Brand Customer Analysis Results")
        print("=" * 60)
        print(f"Brand A Customers: {brand_a_count:,}")
        print(f"Brand B Customers: {brand_b_count:,}")
        print(f"Combined Total:   {combined_total:,}")
        print("=" * 60)

        # Verify schema
        print("\nSchema Verification:")
        print(f"Brand A columns (PII excluded): {brand_a_df.columns}")
        print(f"Brand B columns (PII excluded): {brand_b_df.columns}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
