from utils.postgres_handler import build_monitoring_summary


def test_build_monitoring_summary_reports_batch_metrics_and_freshness_lag():
    summary = build_monitoring_summary(
        batch_id="batch-006",
        rows_in=120,
        rows_out=112,
        started_at="2026-11-12T00:00:00",
        finished_at="2026-11-12T00:02:00",
        last_event_at="2026-11-12T00:03:00",
        now_ts="2026-11-12T00:05:00",
    )

    assert summary["batch_id"] == "batch-006"
    assert summary["rows_in"] == 120
    assert summary["rows_out"] == 112
    assert summary["duration_seconds"] == 120
    assert summary["freshness_lag_minutes"] == 2
    assert summary["sla_ok"] is True
