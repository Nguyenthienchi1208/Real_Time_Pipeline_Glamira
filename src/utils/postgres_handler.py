from config.settings import POSTGRES_CONFIG, logger
import os
from datetime import datetime
import psycopg2
from pyspark.sql import functions as F


def build_monitoring_summary(batch_id, rows_in, rows_out, started_at, finished_at, last_event_at, now_ts):
    start_dt = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(str(finished_at).replace("Z", "+00:00"))
    event_dt = datetime.fromisoformat(str(last_event_at).replace("Z", "+00:00"))
    now_dt = datetime.fromisoformat(str(now_ts).replace("Z", "+00:00"))

    duration_seconds = int((end_dt - start_dt).total_seconds())
    freshness_lag_minutes = max(0, int((now_dt - event_dt).total_seconds() // 60))
    sla_ok = freshness_lag_minutes <= 5

    return {
        "batch_id": batch_id,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duration_seconds": duration_seconds,
        "freshness_lag_minutes": freshness_lag_minutes,
        "sla_ok": sla_ok,
    }


def get_pipeline_health():
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    MAX(CASE WHEN status = 'SUCCEEDED' THEN finished_at END) AS last_success,
                    MAX(CASE WHEN status = 'FAILED' THEN finished_at END) AS last_failed,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_batches
                FROM batch_runs
                """
            )
            row = cur.fetchone()
            return {
                "last_success": row[0],
                "last_failed": row[1],
                "failed_batches": row[2] or 0,
            }
    finally:
        conn.close()


def dedupe_event_batch(df):
    if df is None:
        return df
    return df.dropDuplicates(["event_id"]).withColumn("event_hash", F.sha2(F.to_json(F.struct([F.col(c) for c in df.columns])), 256))


def _get_config():
    if not POSTGRES_CONFIG or "url" not in POSTGRES_CONFIG:
        return {
            "url": os.getenv("DB_URL"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "driver": "org.postgresql.Driver",
        }
    return POSTGRES_CONFIG


def get_postgres_connection():
    config = _get_config()
    if not config.get("user") or not config.get("password"):
        raise ValueError("Postgres connection credentials are missing")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=config["user"],
        password=config["password"],
    )


def execute_transaction(statements):
    conn = None
    cursor = None
    total_affected = 0

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        for sql, params in statements:
            cursor.execute(sql, params or ())
            if cursor.rowcount is not None:
                total_affected += cursor.rowcount

        conn.commit()
        return total_affected
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if cursor is not None and hasattr(cursor, "close"):
            cursor.close()
        if conn is not None:
            conn.close()


def upsert_batch_status(batch_id, status, rows_in=0, error_message=None):
    sql = """
        INSERT INTO batch_runs (batch_id, status, rows_in, started_at, finished_at, error_message, updated_at)
        VALUES (%s, %s, %s, NOW(), NULL, %s, NOW())
        ON CONFLICT (batch_id) DO UPDATE
        SET status = EXCLUDED.status,
            rows_in = EXCLUDED.rows_in,
            error_message = EXCLUDED.error_message,
            finished_at = CASE
                WHEN EXCLUDED.status IN ('SUCCEEDED', 'FAILED') THEN NOW()
                ELSE batch_runs.finished_at
            END,
            updated_at = NOW()
    """

    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        cursor.execute(sql, (batch_id, status, rows_in, error_message))
        conn.commit()
        return True
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if cursor is not None and hasattr(cursor, "close"):
            cursor.close()
        if conn is not None:
            conn.close()


def build_batch_merge_statements(batch_id):
    return [        (
            """
            INSERT INTO dim_customer (
                customer_sk, user_id_db, email_address, device_id, user_agent, resolution
            )
            SELECT DISTINCT ON (customer_sk)
                customer_sk, user_id_db, email_address, device_id, user_agent, resolution
            FROM stg_dim_customer
            WHERE batch_id = %s
            ORDER BY customer_sk
            ON CONFLICT (customer_sk) DO NOTHING
            """,
            (batch_id,),
        ),
        (
            """
            INSERT INTO dim_product (
                product_sk, product_id, collection, alloy, diamond
            )
            SELECT DISTINCT ON (product_sk)
                product_sk, product_id, collection, alloy, diamond
            FROM stg_dim_product
            WHERE batch_id = %s
            ORDER BY product_sk
            ON CONFLICT (product_sk) DO NOTHING
            """,
            (batch_id,),
        ),
        (
            """
            INSERT INTO dim_store (
                store_sk, store_id
            )
            SELECT DISTINCT ON (store_sk)
                store_sk, store_id
            FROM stg_dim_store
            WHERE batch_id = %s
            ORDER BY store_sk
            ON CONFLICT (store_sk) DO NOTHING
            """,
            (batch_id,),
        ),
        (
            """
            INSERT INTO dim_location (
                location_sk, ip, country, region, city, latitude, longitude
            )
            SELECT DISTINCT ON (location_sk)
                location_sk, ip, country, region, city, latitude, longitude
            FROM stg_dim_location
            WHERE batch_id = %s
            ORDER BY location_sk
            ON CONFLICT (location_sk) DO NOTHING
            """,
            (batch_id,),
        ),
        (
            """
            INSERT INTO dim_date (
                date_sk, full_date, year, month, day, quarter
            )
            SELECT DISTINCT ON (date_sk)
                date_sk, full_date, year, month, day, quarter
            FROM stg_dim_date
            WHERE batch_id = %s
            ORDER BY date_sk
            ON CONFLICT (date_sk) DO NOTHING
            """,
            (batch_id,),
        ),
        (
            """
            INSERT INTO fact_product_view (
                event_id, user_id_db, product_id, store_id, time_stamp,
                date_sk, customer_sk, product_sk, store_sk, location_sk,
                referrer_url, current_url, domain, country_domain, browser, os
            )
            SELECT
                event_id, user_id_db, product_id, store_id, time_stamp,
                date_sk, customer_sk, product_sk, store_sk, location_sk,
                referrer_url, current_url, domain, country_domain, browser, os
            FROM stg_fact_product_view
            WHERE batch_id = %s
            ON CONFLICT (event_id) DO NOTHING
            """,
            (batch_id,),
        ),
        (
            "DELETE FROM stg_dim_customer WHERE batch_id = %s",
            (batch_id,),
        ),
        (
            "DELETE FROM stg_dim_product WHERE batch_id = %s",
            (batch_id,),
        ),
        (
            "DELETE FROM stg_dim_store WHERE batch_id = %s",
            (batch_id,),
        ),
        (
            "DELETE FROM stg_dim_location WHERE batch_id = %s",
            (batch_id,),
        ),
        (
            "DELETE FROM stg_dim_date WHERE batch_id = %s",
            (batch_id,),
        ),
        (
            "DELETE FROM stg_fact_product_view WHERE batch_id = %s",
            (batch_id,),
        ),
    ]


def write_to_postgres(df, table_name, mode="append"):
    if df is None or df.rdd.isEmpty():
        return

    config = _get_config()

    if not config.get("url") or not config.get("user") or not config.get("password"):
        error_msg = f"Postgres config is missing: {config}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        df.write.format("jdbc") \
            .option("url", config["url"]) \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", config.get("driver", "org.postgresql.Driver")) \
            .mode(mode) \
            .save()
        logger.info(f"Write completed for table {table_name}")
    except Exception as e:
        logger.error(f"Write failed for table {table_name}: {str(e)}")
        raise e


def read_from_postgres(spark, query):
    config = _get_config()
    try:
        return spark.read.format("jdbc") \
            .option("url", config["url"]) \
            .option("dbtable", f"({query}) AS temp_query") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", config.get("driver", "org.postgresql.Driver")) \
            .load()
    except Exception as e:
        logger.error(f"Read failed from Postgres: {str(e)}")
        raise e