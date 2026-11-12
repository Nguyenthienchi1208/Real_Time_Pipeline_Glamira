import json
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json
from schemas.data_schema import GLAMIRA_EVENT_SCHEMA


def _safe_json_loads(value: Any):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def validate_event_payloads(raw_df, batch_id="unknown"):
    """Validate, parse, and quarantine malformed Kafka payloads before publish."""
    parsed = raw_df.selectExpr("CAST(value AS STRING) AS json_payload")
    data_df = parsed.withColumn("data", from_json(col("json_payload"), GLAMIRA_EVENT_SCHEMA))

    valid_expr = (
        col("json_payload").isNotNull()
        & (F.length(F.trim(col("json_payload"))) > 0)
        & col("data").isNotNull()
        & col("data._id").isNotNull()
        & (F.trim(col("data._id")) != "")
        & col("data.product_id").isNotNull()
        & (F.trim(col("data.product_id")) != "")
        & col("data.store_id").isNotNull()
        & (F.trim(col("data.store_id")) != "")
        & col("data.time_stamp").isNotNull()
    )

    valid_df = (
        data_df
        .filter(valid_expr)
        .select("data.*")
        .withColumnRenamed("_id", "event_id")
        .withColumn("time_stamp", F.col("time_stamp").cast("timestamp"))
    )

    invalid_df = (
        data_df
        .withColumn(
            "reason",
            F.when(
                col("json_payload").isNull() | (F.length(F.trim(col("json_payload"))) == 0),
                "invalid_json"
            )
            .when(col("data").isNull(), "invalid_json")
            .when(
                (
                    col("data._id").isNull() | (F.trim(col("data._id")) == "")
                    | col("data.product_id").isNull() | (F.trim(col("data.product_id")) == "")
                    | col("data.store_id").isNull() | (F.trim(col("data.store_id")) == "")
                    | col("data.time_stamp").isNull()
                ),
                "missing_required_field"
            )
            .otherwise("invalid_payload")
        )
        .filter(~valid_expr)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("payload", col("json_payload"))
        .select("batch_id", "payload", "reason")
    )

    rows_in = raw_df.count()
    rows_valid = valid_df.count()
    rows_rejected = invalid_df.count()
    pass_rate = (rows_valid / rows_in) if rows_in else 0.0

    summary = {
        "rows_in": rows_in,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "pass_rate": pass_rate,
    }

    return valid_df, invalid_df, summary


def get_kafka_stream(spark, kafka_config):
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", kafka_config["topic"])
        .option("startingOffsets", kafka_config["starting_offsets"])
        .option("maxOffsetsPerTrigger", kafka_config["max_offsets_per_trigger"])
        .option("failOnDataLoss", kafka_config.get("fail_on_data_loss", "false"))
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    )

    return reader.load()


def transform_stream(raw_df):
    """Parse Kafka JSON and rename `_id` → `event_id` for the warehouse contract."""
    return (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), GLAMIRA_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumnRenamed("_id", "event_id")
    )