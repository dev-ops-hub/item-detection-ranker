"""Environment-variable helpers for the item-detection-ranker project.

Loads variables from a project-root ``.env`` file (via python-dotenv) and
exposes a small accessor for the log level. Existing process environment
variables are *not* overridden, so CLI/CI overrides always win over the
.env defaults.
"""
import os

from dotenv import load_dotenv


def load_project_env(env_path=None):
    """Load environment variables from the project's ``.env`` file.

    Args:
        env_path: Optional explicit path to a ``.env`` file. When
            omitted, defaults to ``.env`` in the current working
            directory.

    Returns:
        bool: ``True`` if the file was found and loaded, ``False`` if
        the file does not exist (silently ignored to support
        environments where configuration is supplied entirely via the
        process environment).
    """

    # ``override=False`` ensures any pre-existing environment variable
    # (from CI, the shell, or a test fixture) takes precedence over the
    # value declared in the .env file.
    dotenv_path = env_path or ".env"
    return load_dotenv(dotenv_path=dotenv_path, override=False)


def get_log_level(default="INFO"):
    """Return the configured ``LOG_LEVEL`` env var or ``default``."""
    return os.getenv("LOG_LEVEL", default)
