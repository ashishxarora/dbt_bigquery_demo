"""
BigQuery PySpark Pipeline - Using DataprocSparkSession
Optimized for Google Cloud Dataproc Spark Connect
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count

# Configuration
PROJECT_ID = "project-962ed868-ab8d-4995-821"

# ============================================================
# OPTION A: Use DataprocSparkSession (for Dataproc with Spark Connect)
# ============================================================
try:
    # Try the Dataproc-specific session
    from google.cloud.dataproc_v1 import DataprocSparkSession

    spark = DataprocSparkSession.builder().getOrCreate()
    print("✅ Using DataprocSparkSession")

except ImportError:
    # Fallback to standard SparkSession
    print("⚠️ DataprocSparkSession not available, using standard SparkSession")
    spark = SparkSession.builder.getOrCreate()

# ============================================================
# PIPELINE
# ============================================================

print(f"\n🚀 Project: {PROJECT_ID}")

# Read Brand A
print("\n📥 Reading Brand A...")
brand_a_df = (
    spark.read.format("bigquery")
    .option("table", f"{PROJECT_ID}.demo_dataset.bronze_brand_a_customers")
    .load()
)
brand_a_count = brand_a_df.count()
print(f"   ✅ Brand A: {brand_a_count}")
brand_a_df.show()

# Read Brand B
print("\n📥 Reading Brand B...")
brand_b_df = (
    spark.read.format("bigquery")
    .option("table", f"{PROJECT_ID}.demo_dataset.bronze_brand_b_customers")
    .load()
)
brand_b_count = brand_b_df.count()
print(f"   ✅ Brand B: {brand_b_count}")
brand_b_df.show()

# Union and count
print("\n" + "=" * 50)
print("📊 RESULTS")
print("=" * 50)
combined_df = brand_a_df.unionByName(brand_b_df, allowMissingColumns=True)
print(f"Total Customers: {combined_df.count()}")
print(f"  Brand A:      {brand_a_count}")
print(f"  Brand B:      {brand_b_count}")
print("=" * 50)

print("\n✅ Pipeline completed!")
