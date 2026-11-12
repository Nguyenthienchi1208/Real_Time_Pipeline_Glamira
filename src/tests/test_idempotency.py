import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Glamira-Idempotency")
        .getOrCreate()
    )


def test_dedupe_event_batch_removes_replayed_rows(spark_session):
    rows = [
        ("evt_001", "p-1", "store-1", "2024-01-01T00:00:00Z"),
        ("evt_001", "p-1", "store-1", "2024-01-01T00:00:00Z"),
        ("evt_002", "p-2", "store-2", "2024-01-01T01:00:00Z"),
    ]
    df = spark_session.createDataFrame(rows, ["event_id", "product_id", "store_id", "time_stamp"])

    from utils.postgres_handler import dedupe_event_batch

    deduped = dedupe_event_batch(df)
    assert deduped.count() == 2
    assert deduped.select("event_id").distinct().count() == 2
