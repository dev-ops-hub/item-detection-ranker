import logging
import sys


class LogManager:
    _base_logger_name = "item_detection_ranker"
    _default_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    _configured = False

    @classmethod
    def configure(cls, level="INFO", *, force=False):
        if cls._configured and not force:
            return logging.getLogger(cls._base_logger_name)

        resolved_level = cls._resolve_level(level)
        logging.basicConfig(
            level=resolved_level,
            format=cls._default_format,
            stream=sys.stdout,
            force=force,
        )

        logger = logging.getLogger(cls._base_logger_name)
        logger.setLevel(resolved_level)
        cls._configured = True
        return logger

    @classmethod
    def get_logger(cls, name=None):
        if not cls._configured:
            cls.configure()

        if not name:
            return logging.getLogger(cls._base_logger_name)

        return logging.getLogger(f"{cls._base_logger_name}.{name}")

    @staticmethod
    def _resolve_level(level):
        if isinstance(level, int):
            return level

        if isinstance(level, str):
            resolved_level = getattr(logging, level.upper(), None)
            if isinstance(resolved_level, int):
                return resolved_level

        raise ValueError(f"Unsupported log level: {level}")
