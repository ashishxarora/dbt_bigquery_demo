"""
PySpark script for Dataproc to read brand_a and brand_b customer tables from BigQuery.
Excludes PII columns and calculates customer counts.

This script uses the Dataproc-specific Spark Session API with spark-bigquery-connector.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def create_spark_session() -> SparkSession:
    """
    Create a Dataproc SparkSession with BigQuery connector configuration.
    Uses Dataproc-specific optimizations.
    """
    return (
        SparkSession.builder.appName("BrandCustomerCount")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.max.result.size", "2g")
        .config("spark.sql.shuffle.partitions", "200")
        # BigQuery Connector configurations
        .config("spark.datasource.bigquery.projectId", "YOUR_PROJECT_ID")
        .config("spark.datasource.bigquery.parentProject", "YOUR_PROJECT_ID")
        .config("spark.datasource.bigquery.gcsBucket", "YOUR_TEMP_BUCKET")
        .config("spark.bigquery.connector.enabled", "true")
        .config("viewsEnabled", "true")
        .config("materializationDataset", "temp_dataset")
        .getOrCreate()
    )


def read_brand_table(spark: SparkSession, table_name: str, dataset: str) -> "DataFrame":
    """
    Read a brand customer table from BigQuery using spark-bigquery-connector.

    Args:
        spark: SparkSession instance
        table_name: Name of the table (e.g., 'bronze_brand_a_customers')
        dataset: BigQuery dataset name

    Returns:
        DataFrame with PII columns excluded
    """
    # Full table path
    full_table_path = f"{dataset}.{table_name}"

    # Read directly from BigQuery
    df = spark.read.format("bigquery").option("table", full_table_path).load()

    return df


def exclude_pii_columns(df: "DataFrame", table_name: str) -> "DataFrame":
    """
    Exclude PII columns based on table schema.

    brand_a: exclude email_id, name
    brand_b: exclude cc_token, display_name
    """
    pii_columns = {
        "bronze_brand_a_customers": ["email_id", "name"],
        "bronze_brand_b_customers": ["cc_token", "display_name"],
    }

    columns_to_exclude = pii_columns.get(table_name, [])
    non_pii_columns = [c for c in df.columns if c not in columns_to_exclude]

    return df.select(*non_pii_columns)


def calculate_customer_counts(brand_a_df: "DataFrame", brand_b_df: "DataFrame") -> dict:
    """
    Calculate customer counts for each brand and combined total.

    Args:
        brand_a_df: DataFrame for brand_a customers
        brand_b_df: DataFrame for brand_b customers

    Returns:
        Dictionary with count metrics
    """
    # Count individual brands
    brand_a_count = brand_a_df.count()
    brand_b_count = brand_b_df.count()

    # Combined total
    combined_count = brand_a_count + brand_b_count

    return {
        "brand_a_customers": brand_a_count,
        "brand_b_customers": brand_b_count,
        "combined_total": combined_count,
    }


def main():
    """Main execution function."""
    # Configuration - replace with your actual values
    PROJECT_ID = "YOUR_PROJECT_ID"
    TEMP_BUCKET = "gs://YOUR_TEMP_BUCKET"
    DATASET = "demo_dataset"

    # Create Spark session
    print("Creating Dataproc Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Configure BigQuery for this session
        spark.conf.set("spark.datasource.bigquery.projectId", PROJECT_ID)
        spark.conf.set("spark.datasource.bigquery.parentProject", PROJECT_ID)
        spark.conf.set("spark.datasource.bigquery.gcsBucket", TEMP_BUCKET)

        # Read brand tables
        print("Reading brand_a_customers from BigQuery...")
        brand_a_df = read_brand_table(spark, "bronze_brand_a_customers", DATASET)
        brand_a_df = exclude_pii_columns(brand_a_df, "bronze_brand_a_customers")

        print("Reading brand_b_customers from BigQuery...")
        brand_b_df = read_brand_table(spark, "bronze_brand_b_customers", DATASET)
        brand_b_df = exclude_pii_columns(brand_b_df, "bronze_brand_b_customers")

        # Calculate counts
        print("Calculating customer counts...")
        counts = calculate_customer_counts(brand_a_df, brand_b_df)

        # Print results
        print("\n" + "=" * 50)
        print("CUSTOMER COUNT RESULTS")
        print("=" * 50)
        print(f"Brand A Customers: {counts['brand_a_customers']:,}")
        print(f"Brand B Customers: {counts['brand_b_customers']:,}")
        print(f"Combined Total:    {counts['combined_total']:,}")
        print("=" * 50)

        # Show sample data (PII excluded)
        print("\nSample data from Brand A (PII excluded):")
        brand_a_df.show(5, truncate=False)

        print("Sample data from Brand B (PII excluded):")
        brand_b_df.show(5, truncate=False)

    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session terminated.")


if __name__ == "__main__":
    main()
