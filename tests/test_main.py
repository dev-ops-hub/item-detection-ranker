from main import parse_args


def test_parse_args_uses_log_level_from_environment(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    parsed = parse_args(
        [
            "--job",
            "sample-job",
            "--detections-path",
            "data/input/detections.parquet",
            "--locations-path",
            "data/input/locations.parquet",
            "--output-path",
            "data/output/results.parquet",
            "--top-x",
            "5",
        ]
    )

    assert parsed.log_level == "DEBUG"


def test_parse_args_prefers_cli_log_level_over_environment(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    parsed = parse_args(
        [
            "--job",
            "sample-job",
            "--detections-path",
            "data/input/detections.parquet",
            "--locations-path",
            "data/input/locations.parquet",
            "--output-path",
            "data/output/results.parquet",
            "--top-x",
            "5",
            "--log-level",
            "DEBUG",
        ]
    )

    assert parsed.log_level == "DEBUG"