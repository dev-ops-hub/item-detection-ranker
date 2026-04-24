"""Unit tests for the project's .env loader.

Targets ``item_ranker.util.environment``.
"""
from item_ranker.util import get_log_level, load_project_env


def test_load_project_env_reads_dotenv_file(tmp_path, monkeypatch):
    """Values defined in a .env file populate os.environ on load."""
    env_file = tmp_path / ".env"
    env_file.write_text("LOG_LEVEL=DEBUG\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    loaded = load_project_env()

    assert loaded is True
    assert get_log_level() == "DEBUG"


def test_load_env_does_not_override_existing(tmp_path, monkeypatch):
    """Pre-existing env vars take precedence over .env file values."""
    env_file = tmp_path / ".env"
    env_file.write_text("LOG_LEVEL=ERROR\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    load_project_env()

    assert get_log_level() == "WARNING"


def test_load_project_env_missing_file_returns_false(tmp_path, monkeypatch):
    """Missing .env files are ignored and reported as not loaded."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    loaded = load_project_env()

    assert loaded is False
    assert get_log_level() == "INFO"
