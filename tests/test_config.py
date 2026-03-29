from goeoview.config import Config


def test_config_parses_runtime_overrides(monkeypatch):
    monkeypatch.setenv("FIREHOSE_INSERT_EVERY", "42")
    monkeypatch.setenv("PLC_CATCHUP_WORKERS", "7")
    config = Config()
    assert config.firehose_insert_every == 42
    assert config.plc_catchup_workers == 7


def test_config_firehose_workers_and_batch_size_defaults():
    config = Config()
    assert config.firehose_workers == 1
    assert config.firehose_batch_size == 128


def test_gap_filler_config_defaults():
    import os
    config = Config()
    assert config.gap_queue_size == 64
    assert config.gap_max_pds_concurrency == 10
    assert config.http_dns_validation_timeout == 3.0
    assert config.gap_parse_workers == (os.cpu_count() or 4)


def test_gap_filler_config_overrides(monkeypatch):
    monkeypatch.setenv("GAP_QUEUE_SIZE", "32")
    monkeypatch.setenv("GAP_MAX_PDS_CONCURRENCY", "5")
    monkeypatch.setenv("HTTP_DNS_VALIDATION_TIMEOUT", "1.5")
    monkeypatch.setenv("GAP_PARSE_WORKERS", "8")
    config = Config()
    assert config.gap_queue_size == 32
    assert config.gap_max_pds_concurrency == 5
    assert config.http_dns_validation_timeout == 1.5
    assert config.gap_parse_workers == 8
