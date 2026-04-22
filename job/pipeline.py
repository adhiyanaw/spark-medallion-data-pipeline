import argparse
import yaml
from pyspark.sql import SparkSession


# ----------------------------
# Config
# ----------------------------
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ----------------------------
# Spark
# ----------------------------
def get_spark(app_name: str = "de-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

import logging
from datetime import datetime
print("Pipeline Starting")

# for pipeline log
def setup_logger():
    log_date = datetime.now().strftime("%Y%m%d")
    log_filename = f"job/Log/{log_date}_pipeline.log"

    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return logging

# ----------------------------
# Main
# ----------------------------

# ------------------------------- Bronze------------------
def main(config_path: str):

 # logtime start
    import time
    logger = setup_logger()

    start_time = time.time()
    logger.info("Pipeline started")
 # logtime start

    try:
        config = load_config(config_path)
        spark = get_spark()

        # TODO: implement your pipeline here

        # 1. Define Schema
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType

        event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_ts", StringType(), True),  
            StructField("value", StringType(), True),     
        ])

        # 2. Read Data
        raw_events_path = config["paths"]["raw_events"]

        df_raw = (
            spark.read
            .schema(event_schema)
            .json(raw_events_path)
        )

        # 3. Normalize
        from pyspark.sql.functions import col, lower, to_timestamp, expr, to_date, coalesce, lit

        df_cleaned = (
            df_raw
            .withColumn("event_type", lower(col("event_type")))
            .withColumn("event_timestamp", expr("try_to_timestamp(event_ts)"))
            .withColumn("value", col("value").cast("double"))
            .withColumn("value", col("value").cast("double"))
            .withColumn("value",coalesce(col("value"), lit(0.0))) #  ==> change NULL to 0
            .withColumn("event_date",to_date(col("event_timestamp"))) #  ==> partition
        )

        # 4. Accept | reject
        df_valid = df_cleaned.filter(
            col("event_id").isNotNull() &
            col("user_id").isNotNull() & (col("user_id") != "") &
            col("event_type").isNotNull() &
            col("event_timestamp").isNotNull()
        )
        valid_count = df_valid.count()
        logger.info(f"Valid count: {valid_count}")

        df_rejected = df_cleaned.subtract(df_valid)
        rejected_count = df_rejected.count()
        logger.info(f"Rejected count: {rejected_count}")

        # 5. Drop redundant records
        df_valid = df_valid.dropDuplicates(["event_id"])

        # 6. Bronze output
        bronze_path = config["paths"]["output"]["bronze"]
        
        logger.info("Writing Bronze outputs...")

        df_valid.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(f"{bronze_path}/clean_events")
        df_rejected.write.mode("overwrite").parquet(f"{bronze_path}/rejected_events")

# ------------------------------- Silver------------------
        # 1. read bronze data
        df_bronze = spark.read.parquet(f"{bronze_path}/clean_events")
        users_path = config["paths"]["users"]
        df_users = spark.read.csv(users_path, header=True, inferSchema=True)

        # 2. clean users data
        from pyspark.sql.functions import to_date, expr

        df_users_clean = df_users.withColumn(
            "signup_date",
            expr("try_cast(signup_date as date)")
        )

        # 3. join with user
        df_silver = df_bronze.join(
            df_users_clean,
            on="user_id",
            how="left"
        )
        # 4. handle missing dimension
        from pyspark.sql.functions import coalesce, lit

        df_silver = df_silver.withColumn(
            "country",
            coalesce(col("country"), lit("UNKNOWN"))
        )

        # 5. add is_purchase
        from pyspark.sql.functions import when

        df_silver = df_silver.withColumn(
            "is_purchase",
            when(col("event_type") == "purchase", 1).otherwise(0)
        )

        # 6. add days_since_signup
        from pyspark.sql.functions import datediff

        df_silver = df_silver.withColumn(
            "days_since_signup",
            datediff(col("event_date"), col("signup_date"))
        )

        # 7. delete event_ts
        df_silver = df_silver.drop("event_ts")


        # 8. write silver
        logger.info("Writing Silver outputs...")
        silver_path = config["paths"]["output"]["silver"]

        df_silver.write \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .parquet(f"{silver_path}/events_enriched")


# ------------------------------- Gold|Incremental------------------
        # 1. read silver data
        df_silver = spark.read.parquet(f"{silver_path}/events_enriched")

        # 2. aggregation
        from pyspark.sql.functions import count, sum, countDistinct

        df_gold = (
            df_silver
            .groupBy("event_date", "country")
            .agg(
                count("*").alias("total_events"),
                sum("value").alias("total_value"),
                sum("is_purchase").alias("total_purchases"),
                countDistinct("user_id").alias("unique_users")
            )
        )
        
        logger.info("Writing Gold outputs...")
        gold_path = config["paths"]["output"]["gold"]

        df_gold.write \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .parquet(f"{gold_path}/daily_metrics")

          
        # pipeline duration/info
        end_time = time.time()
        duration = end_time - start_time
        print(f"Pipeline finished in {duration:.2f} seconds")

        logger.info("Pipeline completed successfully")
        logger.info(f"Pipeline finished in {duration:.2f} seconds")
        spark.stop()
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)
