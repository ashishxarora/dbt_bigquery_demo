# BigQuery PySpark Customer Count Pipeline

A simple data pipeline that reads customer data from multiple BigQuery tables and counts total customers across brands.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    CUSTOMER COUNT PIPELINE                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │ Brand A      │    │ Brand B      │    │ Unified      │   │
│  │ Customers    │    │ Customers    │    │ Customer     │   │
│  │ (email_id)   │    │ (cc_token)   │    │ Count        │   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘   │
│         │                  │                   │            │
│         └────────┬─────────┘                   │            │
│                  ▼                             ▼            │
│         ┌────────────────┐           ┌──────────────────┐   │
│         │ PySpark Union  │──────────▶│ Count + Summary  │   │
│         └────────────────┘           └──────────────────┘   │
│                                                     │        │
│                                                     ▼        │
│                                            ┌────────────────┐ │
│                                            │ BigQuery       │ │
│                                            │ Summary Table  │ │
│                                            └────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | Table | Identifier | Attributes |
|--------|-------|------------|------------|
| Brand A | `demo_dataset.bronze_brand_a_customers` | email_id | name, loyalty_tier |
| Brand B | `demo_dataset.bronze_brand_b_customers` | cc_token | display_name, reward_points |

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure BigQuery Access

The pipeline uses Application Default Credentials. Ensure you're authenticated:

```bash
# Option 1: Service Account JSON
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Option 2: gcloud CLI (for local development)
gcloud auth application-default login
```

### 3. Run the Pipeline

```bash
# Local development (requires Spark installation)
spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.1 \
  customer_count_pipeline.py

# Or run directly with Python
python customer_count_pipeline.py
```

## Output

The pipeline outputs:

```
==================================================
📊 CUSTOMER COUNT RESULTS
==================================================
Total Customers: 4
  - Brand A:      2
  - Brand B:      2
==================================================
```

## Deployment Options

### Google Dataproc (Recommended for production)

```bash
# Create a Dataproc cluster
gcloud dataproc clusters create customer-pipeline \
  --region=us-central1 \
  --single-node \
  --scopes=bigquery

# Submit the job
gcloud dataproc jobs submit pyspark \
  gs://your-bucket/pipelines/customer_count_pipeline.py \
  --region=us-central1 \
  --cluster=customer-pipeline
```

### Apache Airflow (for orchestration)

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator

with DAG('customer_count') as dag:
    run_pipeline = DataprocSubmitPySparkJobOperator(
        task_id='count_customers',
        main=f'gs://your-bucket/pipelines/customer_count_pipeline.py',
        cluster_name='customer-pipeline',
        region='us-central1',
    )
```

### Scheduled Execution (Cloud Scheduler + Cloud Functions)

1. Deploy the pipeline as a Cloud Function
2. Use Cloud Scheduler to trigger at desired intervals

## Configuration

Edit these variables in `customer_count_pipeline.py`:

```python
PROJECT_ID = "project-962ed868-ab8d-4995-821"  # Your GCP project
DATASET = "demo_dataset"                        # BigQuery dataset
OUTPUT_TABLE = "customer_count_summary"          # Summary output table
```

## Pipeline Features

- ✅ Reads from multiple BigQuery tables
- ✅ Handles different column schemas via standardization
- ✅ Provides customer count by brand
- ✅ Writes summary to BigQuery (optional)
- ✅ Structured logging
- ✅ Error handling

## Extending the Pipeline

### Add More Brands

```python
def read_brand_c_customers(spark, project_id):
    df = spark.read.format("bigquery")...
    return df.select(...)

# In main():
combined_df = brand_a.unionByName(brand_b, allowMissingColumns=True)
combined_df = combined_df.unionByName(brand_c, allowMissingColumns=True)
```

### Add Data Quality Checks

```python
from pyspark.sql import DataFrame

def validate_data(df: DataFrame) -> bool:
    # Check for null identifiers
    null_count = df.filter(col("customer_identifier").isNull()).count()
    if null_count > 0:
        logger.warning(f"Found {null_count} rows with null identifiers")
        return False
    return True
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Access Denied` | Check BigQuery permissions for service account |
| `Table not found` | Verify project ID and dataset name |
| `Credential issues` | Run `gcloud auth application-default login` |
| `Spark version mismatch` | Update the BigQuery connector version for your Spark version |
