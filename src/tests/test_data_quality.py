import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Glamira-DQ-Validation")
        .getOrCreate()
    )


def test_validate_event_payloads_keeps_valid_and_quarantines_malformed(spark_session):
    raw_rows = [
        (
            b"k1",
            b'{"_id":"evt_001","product_id":"p-100","store_id":"s-10","time_stamp":1700000000,"user_id_db":"u-1"}',
        ),
        (
            b"k2",
            b'{"_id":"evt_002","product_id":"","store_id":"s-10","time_stamp":"bad-ts"}',
        ),
    ]

    raw_df = spark_session.createDataFrame(raw_rows, ["key", "value"])

    from read_stream import validate_event_payloads

    valid_df, invalid_df, summary = validate_event_payloads(raw_df, batch_id="batch-dq-01")

    assert summary["rows_in"] == 2
    assert summary["rows_valid"] == 1
    assert summary["rows_rejected"] == 1
    assert summary["pass_rate"] == 0.5

    assert valid_df.count() == 1
    assert invalid_df.count() == 1

    valid_event = valid_df.collect()[0]
    assert valid_event["event_id"] == "evt_001"
    assert valid_event["product_id"] == "p-100"
    assert valid_event["store_id"] == "s-10"

    invalid_event = invalid_df.collect()[0]
    assert invalid_event["reason"]
    assert invalid_event["batch_id"] == "batch-dq-01"
