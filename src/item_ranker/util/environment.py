import os
from pathlib import Path

from dotenv import load_dotenv

def load_project_env(env_path=None):
    if env_path is None:
        env_path = Path(__file__).resolve().parents[3] / ".env"

    env_file = Path(env_path)
    if not env_file.exists():
        return False

    load_dotenv(env_file, override=False)
    return True


def get_log_level(default="INFO"):
    return os.getenv("LOG_LEVEL", default)