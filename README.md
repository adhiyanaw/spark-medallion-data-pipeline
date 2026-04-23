
# 🧱 Medallion Data Pipeline Using PySpark

## 📌 Overview
This project implements a **Medallion Architecture (Bronze → Silver → Gold)** data pipeline using **PySpark**.

The pipeline processes raw event data, performs data cleaning and validation, enriches the dataset, and produces aggregated business-ready outputs.

---

## 🏗️ Architecture

Raw Data (JSON)
    ↓
Bronze Layer
(Raw data ingestion, validation, and cleaning)
    ↓
Silver Layer
(Data enrichment and transformation)
    ↓
Gold Layer
(Aggregated Metrics)

---

## ⚙️ Tech Stack

* Python
* PySpark
* YAML (configuration-driven pipeline)
* Parquet (storage format)

---

## 📂 Project Structure

```
project/
├── job/
│   └── pipeline.py
│   └── Log/
├── config/
│   └── pipeline.yaml
├── data/
│   ├── raw/
│   ├── reference/
│   └── output/
│       └── bronze/
│       └── silver/
│       └── gold/
└── README.md
└── pyproject.toml
```

---

## 🚀 How to Run

### 1. Install dependencies

```bash
pip install pyspark pyyaml
```

### 2. Run pipeline

```bash
python job/pipeline.py --config config/pipeline.yaml
```

### 3. See the results
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/bronze/rejected_events") # adjust path's location
df.show(truncate=False)
```

---

## 🥉 Bronze Layer (Data Cleaning)

### Steps:

* Read raw JSON events
* Apply schema
* Normalize:

  * Lowercase `event_type`
  * Parse timestamp
  * Cast `value` to double
* Handle null values (`value → 0`)
* Filter valid vs rejected records
* Remove duplicates

### Output:

* `clean_events`
* `rejected_events`

Partitioned by:

```
event_date
```

---

## 🥈 Silver Layer (Data Enrichment)

### Steps:

* Join events with user data (LEFT JOIN)
* Handle missing dimensions (`country → UNKNOWN`)
* Handle invalid dates using `try_cast`
* Add derived columns:

  * `event_date`
  * `is_purchase`
  * `days_since_signup`

---

## 🥇 Gold Layer (Aggregation)

### Aggregation Level:

```
event_date, country
```

### Metrics:

* total_events
* total_value
* total_purchases
* unique_users

---

## ⚡ Incremental Processing

* Implemented using partition-based overwrite
* Spark config:

```python
spark.sql.sources.partitionOverwriteMode = dynamic
```

This ensures only affected partitions are updated.

---

## 📊 Logging

* Pipeline logs stored in `logs/`
* Includes:

  * Pipeline start & end
  * Record counts (valid, rejected, gold)
  * Execution time
  * Error handling

---

## 🧠 Design Decisions

### 1. Use LEFT JOIN

To preserve all event data even if user info is missing.

### 2. Use `try_cast`

To safely handle malformed date values without breaking the pipeline.

### 3. Fill NULL numeric values

`value` column is filled with `0` to ensure accurate aggregation.

### 4. Partitioning

Partitioning by `event_date` improves performance and enables incremental processing.

---

## 📈 Output Example

| event_date | country | total_events | total_value | total_purchases | unique_users |
| ---------- | ------- | ------------ | ----------- | --------------- | ------------ |

---

## ✅ Conclusion

This pipeline demonstrates:

* Data cleaning & validation
* Handling bad data safely
* Data enrichment
* Business-level aggregation
* Incremental processing strategy


---

