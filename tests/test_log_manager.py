import logging

import pytest

from common import LogManager


def test_get_logger_configures_default_logger():
    logger = LogManager.get_logger("tests")

    assert logger.name == "item_detection_ranker.tests"


def test_configure_sets_requested_level_without_duplicate_handlers():
    LogManager.configure("DEBUG", force=True)
    handler_count = len(logging.getLogger().handlers)

    logger = LogManager.configure("DEBUG")

    assert logger.level == logging.DEBUG
    assert len(logging.getLogger().handlers) == handler_count


def test_configure_rejects_invalid_level():
    with pytest.raises(ValueError, match="Unsupported log level"):
        LogManager.configure("NOT_A_LEVEL", force=True)