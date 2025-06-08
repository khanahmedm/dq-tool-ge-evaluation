# Great Expectations Data Quality Evaluation (Genomic Data)

This repository provides a hands-on evaluation of [Great Expectations](https://greatexpectations.io/) for data quality testing using both **Pandas** and **PySpark** workflows. It focuses on validating synthetic genomic data and includes examples for:

- Simple Pandas-based validation
- PySpark with Delta Lake and validation logging
- Logging validation results as Delta tables

---

## Repository Purpose

This project demonstrates the application of **Great Expectations** in a genomic data context. It includes:

1. **Basic Example**: Runs expectations using Pandas DataFrame.
2. **Advanced Spark Example**: Uses Spark + Delta Lake to validate data, log results, and query expectations that failed.

---

## Project Structure


├── Dockerfile
├── generate\_genomic\_data.py

├── requirements.txt

└── README.md



- `Dockerfile`: Sets up the container with Spark, Delta Lake, and Great Expectations.
- `generate_genomic_data.py`: Generates synthetic genomic data saved as a Parquet file.
- `requirements.txt`: Python dependencies.

---

## ⚙️ How to Build and Run (Windows 10 PowerShell)

### 1. Clone this Repository

```powershell
git clone https://github.com/<your-username>/great-expectations-genomic-dq.git
cd great-expectations-genomic-dq
````

### 2. Build the Docker Image

```powershell
docker build -t spark-lab .
```

### 3. Run the Container

Mount your current working directory to access notebooks and data:

```powershell
docker run -it --rm -p 8887:8887 -v "${PWD}:/workspace" spark-lab
```

> **Note**: If you're using Git Bash or WSL, use `$(pwd)` instead of `${PWD}`

### 4. Open JupyterLab

Navigate to:

```
http://localhost:8887
```

No token or password is required.

---

## 🧪 Example 1 – Pandas with Great Expectations

```python
import great_expectations as gx
import pandas as pd

context = gx.get_context()

df = pd.read_parquet("output/genomic_data.parquet")
batch = context.sources.pandas_default.read_dataframe(df)

batch.expect_column_values_to_not_be_null("genome_id")
batch.expect_column_values_to_match_regex("genome_id", r"^G\d{4}$")
batch.expect_column_values_to_be_between("start_position", min_value=0)
batch.expect_column_values_to_be_unique("genome_id")

batch.validate()
```

---

## 🔬 Example 2 – Spark + Delta + Great Expectations + Logging

```python
from pyspark.sql import SparkSession
import great_expectations as gx
import pandas as pd

# Start Spark with Delta support
spark = SparkSession.builder \
    .appName("GenomicDQ") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Load and write Delta
df = spark.read.parquet("output/genomic_data.parquet")
df.write.format("delta").mode("overwrite").save("/delta/genomic_data")
df_delta = spark.read.format("delta").load("/delta/genomic_data")

# Convert to Pandas and validate
pdf = df_delta.toPandas()
context = gx.get_context()
batch = context.sources.pandas_default.read_dataframe(pdf)
batch.expect_column_values_to_be_unique("genome_id")
batch.expect_column_values_to_not_be_null("sequence")
results = batch.validate()
```

### 🔁 Log Validation Results to Delta

```python
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import uuid

run_id = str(uuid.uuid4())
run_time = datetime.utcnow().isoformat()

schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_time", StringType(), False),
    StructField("expectation", StringType(), True),
    StructField("column", StringType(), True),
    StructField("success", BooleanType(), True),
    StructField("observed_value", StringType(), True),
])

rows = []
for r in results["results"]:
    observed = r["result"].get("observed_value")
    rows.append({
        "run_id": run_id,
        "run_time": run_time,
        "expectation": r["expectation_config"]["expectation_type"],
        "column": r["expectation_config"]["kwargs"].get("column"),
        "success": r["success"],
        "observed_value": str(observed) if observed is not None else None
    })

log_df = spark.createDataFrame(rows, schema=schema)
log_df.write.format("delta").mode("append").save("/delta/validation_log")
```

---

## 🔍 Query Failed Validations

```python
df_log = spark.read.format("delta").load("/delta/validation_log")
df_log.filter("success = false").show(truncate=False)
```

---

## 📌 Notes

* Designed to run fully inside a Docker container with JupyterLab UI.
* Delta tables are stored locally at `/delta/*` in the container.
* The examples can be extended for pipeline integration and CI/CD validation.





