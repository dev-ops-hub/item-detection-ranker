from common.environment import get_log_level, load_project_env


def test_load_project_env_reads_dotenv_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("LOG_LEVEL=DEBUG\n", encoding="utf-8")
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    loaded = load_project_env(env_file)

    assert loaded is True
    assert get_log_level() == "DEBUG"


def test_load_env_does_not_override_existing(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("LOG_LEVEL=ERROR\n", encoding="utf-8")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    load_project_env(env_file)

    assert get_log_level() == "WARNING"
