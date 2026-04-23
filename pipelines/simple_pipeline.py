"""
Simple BigQuery PySpark Pipeline - Direct SQL Version
Use this in Dataproc Jupyter notebook or spark-shell

Since Dataproc has BigQuery connector pre-configured,
this version uses direct SQL queries.
"""

from pyspark.sql import SparkSession

# Get the existing Spark session (don't create a new one!)
spark = SparkSession.getActiveSession()

if spark is None:
    raise RuntimeError("No active Spark session found. Run in Dataproc with BigQuery.")

# Configuration
PROJECT_ID = "project-962ed868-ab8d-4995-821"
DATASET = "demo_dataset"

print("🚀 BigQuery Customer Count Pipeline")
print("=" * 50)

# Read Brand A customers
print("\n📥 Reading Brand A customers...")
brand_a_df = (
    spark.read.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.bronze_brand_a_customers")
    .load()
)
brand_a_df.createOrReplaceTempView("brand_a")
print(f"   ✅ Brand A: {brand_a_df.count()} customers")
brand_a_df.show()

# Read Brand B customers
print("\n📥 Reading Brand B customers...")
brand_b_df = (
    spark.read.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.bronze_brand_b_customers")
    .load()
)
brand_b_df.createOrReplaceTempView("brand_b")
print(f"   ✅ Brand B: {brand_b_df.count()} customers")
brand_b_df.show()

# Count total customers
print("\n📊 CUSTOMER COUNT RESULTS")
print("=" * 50)

total_a = brand_a_df.count()
total_b = brand_b_df.count()
total = total_a + total_b

print(f"Brand A Customers: {total_a}")
print(f"Brand B Customers: {total_b}")
print(f"{'─' * 30}")
print(f"TOTAL Customers:   {total}")
print("=" * 50)

# Optional: Create summary table and write to BigQuery
print("\n💾 Writing summary to BigQuery...")

summary_df = spark.createDataFrame(
    [
        {"brand": "brand_a", "customer_count": total_a},
        {"brand": "brand_b", "customer_count": total_b},
        {"brand": "total", "customer_count": total},
    ]
)

(
    summary_df.write.format("bigquery")
    .option("table", f"{PROJECT_ID}.{DATASET}.customer_count_summary")
    .option("writeMethod", "overwrite")
    .mode("overwrite")
    .save()
)

print("✅ Summary written to demo_dataset.customer_count_summary")

print("\n🎉 Pipeline completed successfully!")
