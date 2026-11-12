from main import build_run_config


def test_build_run_config_for_backfill_uses_separate_checkpoint_and_dry_run():
    cfg = build_run_config(backfill_days=3, dry_run=True)

    assert cfg["backfill_days"] == 3
    assert cfg["dry_run"] is True
    assert cfg["checkpoint_path"].endswith("backfill")
    assert cfg["checkpoint_path"] != "hdfs://namenode:9000/user/spark/checkpoints/streaming_job"
    assert cfg["batch_id"].startswith("backfill-")
