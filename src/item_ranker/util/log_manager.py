"""Centralised logging configuration for the item-detection-ranker.

`LogManager` owns a single named root logger (``item_detection_ranker``)
and vends child loggers via ``LogManager.get_logger(name)``. Handlers are
installed exactly once so repeated configuration calls do not duplicate
log output, which is especially important under pytest where modules can
be imported multiple times.
"""
import logging
import sys


class LogManager:
    """Process-wide logging configuration helper.

    Configures a dedicated ``item_detection_ranker`` logger that does not
    propagate to the root logger (so the root logger's handlers remain
    untouched). All application loggers are obtained as children of this
    base logger so they inherit its handler and level.
    """

    _base_logger_name = "item_detection_ranker"
    _default_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    _configured = False

    @classmethod
    def configure(cls, level="INFO", *, force=False):
        """Configure the base logger.

        Args:
            level: Log level (string name like ``"DEBUG"`` or an int).
            force: When True, removes any existing handlers and applies
                the configuration again. Without ``force`` repeated
                calls are no-ops to prevent duplicate handlers.

        Returns:
            The configured base logger.
        """
        logger = logging.getLogger(cls._base_logger_name)
        if cls._configured and not force:
            return logger

        resolved_level = cls._resolve_level(level)
        if force:
            # Replace any previously installed handlers cleanly.
            for handler in list(logger.handlers):
                logger.removeHandler(handler)
                handler.close()

        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(cls._default_format))
            logger.addHandler(handler)

        logger.setLevel(resolved_level)
        # Disable propagation so the root logger is left untouched and
        # we never produce duplicate log lines.
        logger.propagate = False
        cls._configured = True
        return logger

    @classmethod
    def get_logger(cls, name=None):
        """Return a child logger of the base logger.

        Triggers ``configure()`` lazily if the base logger has not yet
        been initialised, so callers never have to remember to set up
        logging explicitly.
        """
        if not cls._configured:
            cls.configure()

        if not name:
            return logging.getLogger(cls._base_logger_name)

        return logging.getLogger(f"{cls._base_logger_name}.{name}")

    @staticmethod
    def _resolve_level(level):
        """Coerce ``level`` to a numeric ``logging`` level.

        Accepts either an int (returned unchanged) or a case-insensitive
        string matching a standard ``logging`` level name.

        Raises:
            ValueError: If ``level`` cannot be resolved.
        """
        if isinstance(level, int):
            return level

        if isinstance(level, str):
            resolved_level = getattr(logging, level.upper(), None)
            if isinstance(resolved_level, int):
                return resolved_level

        raise ValueError(f"Unsupported log level: {level}")
