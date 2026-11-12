import argparse
from datetime import datetime

from pyspark.sql import SparkSession

from config.settings import KAFKA_CONFIG, CHECKPOINT_PATH, logger
from read_stream import get_kafka_stream, transform_stream
from utils.postgres_handler import (
    build_batch_merge_statements,
    execute_transaction,
    read_from_postgres,
    upsert_batch_status,
    write_to_postgres,
)
from utils.surrogate_keys import (
    customer_sk_expr,
    product_sk_expr,
    store_sk_expr,
    location_sk_expr,
    normalize_unix_timestamp,
)

from transforms.dim.dim_customer import transform_to_dim_customer
from transforms.dim.dim_product import transform_to_dim_product
from transforms.dim.dim_store import transform_to_dim_store
from transforms.dim.dim_location import process_dim_location as transform_to_dim_location
from transforms.dim.dim_date import transform_to_dim_date
from transforms.fact.fact_product_view import transform_to_fact_product_view

COLUMNS_MAPPING = {
    "dim_customer": ["customer_sk", "user_id_db", "email_address", "device_id", "user_agent", "resolution"],
    "dim_product": ["product_sk", "product_id", "collection", "alloy", "diamond"],
    "dim_store": ["store_sk", "store_id"],
    "dim_location": ["location_sk", "ip", "country", "region", "city", "latitude", "longitude"],
    "dim_date": ["date_sk", "full_date", "year", "month", "day", "quarter"],
    "fact_product_view": [
        "event_id", "user_id_db", "product_id", "store_id", "time_stamp",
        "date_sk", "customer_sk", "product_sk", "store_sk", "location_sk",
        "referrer_url", "current_url", "domain", "country_domain", "browser", "os",
    ],
}


def _is_empty(df):
    return df.limit(1).count() == 0


def _upsert_dim(spark, df, table_name, sk_col, columns):
    """Append only rows whose surrogate key is not already in Postgres."""
    if df is None or _is_empty(df):
        logger.info(f"Skip {table_name}: empty")
        return

    mapped = df.select(*[c for c in columns if c in df.columns]).dropDuplicates([sk_col])
    if _is_empty(mapped):
        return

    try:
        existing = read_from_postgres(spark, f"SELECT {sk_col} FROM {table_name}")
        new_rows = mapped.join(existing, sk_col, "left_anti")
    except Exception as e:
        logger.warning(f"Could not read existing keys from {table_name}: {e}. Writing full batch.")
        new_rows = mapped

    if _is_empty(new_rows):
        logger.info(f"Skip {table_name}: no new keys")
        return

    write_to_postgres(new_rows, table_name, mode="append")


def _write_staging_batch(df, table_name, batch_id):
    if df is None or _is_empty(df):
        logger.info(f"Skip staging {table_name}: empty batch {batch_id}")
        return

    staged = df.withColumn("batch_id", SparkSession.getActiveSession().lit(batch_id))
    write_to_postgres(staged, table_name, mode="append")


def process_batch(df, batch_id):
    spark = SparkSession.getActiveSession()
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    if _is_empty(df):
        logger.info(f"Batch {batch_id}: empty, skip")
        upsert_batch_status(batch_id, "SUCCEEDED", rows_in=0)
        return

    row_count = df.count()
    logger.info(f"batch_id={batch_id} status=STARTED rows_in={row_count} start_ts={datetime.utcnow().isoformat()}Z")
    upsert_batch_status(batch_id, "STARTED", rows_in=row_count)

    try:
        df_enriched = (
            df
            .withColumn("customer_sk", customer_sk_expr())
            .withColumn("product_sk", product_sk_expr(df.columns))
            .withColumn("store_sk", store_sk_expr())
            .withColumn("location_sk", location_sk_expr())
            .withColumn("time_stamp", normalize_unix_timestamp("time_stamp"))
            .cache()
        )

        _ = df_enriched.count()

        logger.info(f"batch_id={batch_id} status=BUILDING_DIMS rows_in={row_count}")
        df_cust = transform_to_dim_customer(df_enriched)
        df_prod = transform_to_dim_product(df_enriched)
        df_store = transform_to_dim_store(df_enriched)
        df_loc = transform_to_dim_location(df_enriched)
        df_date = transform_to_dim_date(df_enriched)
        df_fact = transform_to_fact_product_view(
            df_enriched, df_cust, df_prod, df_store, df_loc, df_date
        )

        logger.info(f"batch_id={batch_id} status=WRITING_STAGING rows_in={row_count}")
        _write_staging_batch(df_cust, "stg_dim_customer", batch_id)
        _write_staging_batch(df_prod, "stg_dim_product", batch_id)
        _write_staging_batch(df_store, "stg_dim_store", batch_id)
        _write_staging_batch(df_loc, "stg_dim_location", batch_id)
        _write_staging_batch(df_date, "stg_dim_date", batch_id)

        fact_cols = [c for c in COLUMNS_MAPPING["fact_product_view"] if c in df_fact.columns]
        _write_staging_batch(df_fact.select(*fact_cols), "stg_fact_product_view", batch_id)

        logger.info(f"batch_id={batch_id} status=MERGING rows_in={row_count}")
        merge_statements = build_batch_merge_statements(batch_id)
        execute_transaction(merge_statements)

        upsert_batch_status(batch_id, "SUCCEEDED", rows_in=row_count)

        df_enriched.unpersist()
        logger.info(f"batch_id={batch_id} status=SUCCEEDED rows_in={row_count} end_ts={datetime.utcnow().isoformat()}Z")

    except Exception as e:
        logger.error(f"batch_id={batch_id} status=FAILED rows_in={row_count} error={str(e)}")
        upsert_batch_status(batch_id, "FAILED", rows_in=row_count, error_message=str(e))
        raise e


def build_run_config(backfill_days=0, dry_run=False, batch_id=None):
    """Build run metadata for live or backfill mode with separate checkpoint paths."""
    backfill_days = int(backfill_days or 0)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    if backfill_days > 0:
        checkpoint_path = "hdfs://namenode:9000/user/spark/checkpoints/streaming_job_backfill"
        run_batch_id = batch_id or f"backfill-{timestamp}"
    else:
        checkpoint_path = CHECKPOINT_PATH
        run_batch_id = batch_id or f"live-{timestamp}"

    return {
        "backfill_days": backfill_days,
        "dry_run": bool(dry_run),
        "checkpoint_path": checkpoint_path,
        "batch_id": run_batch_id,
    }


def main():
    parser = argparse.ArgumentParser(description="Glamira streaming pipeline")
    parser.add_argument("--backfill-days", type=int, default=0, help="Backfill last N days from Kafka snapshot")
    parser.add_argument("--dry-run", action="store_true", help="Validate config without starting the stream")
    args = parser.parse_args()

    run_config = build_run_config(backfill_days=args.backfill_days, dry_run=args.dry_run)
    logger.info(f"Starting pipeline with config: {run_config}")

    if run_config["dry_run"]:
        logger.info("Dry run enabled; pipeline start skipped.")
        return

    spark = (
        SparkSession.builder
        .appName("GlamiraStreamingPipeline")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = get_kafka_stream(spark, KAFKA_CONFIG)
    df_transformed = transform_stream(raw_df)

    query = (
        df_transformed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", run_config["checkpoint_path"])
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
