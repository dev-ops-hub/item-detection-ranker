"""Unit tests for `item_ranker.util.log_manager.LogManager`."""
import logging

import pytest

from item_ranker.util import LogManager


def test_get_logger_configures_default_logger():
    """`get_logger` returns a child of the package's base logger."""
    logger = LogManager.get_logger("tests")

    assert logger.name == "item_detection_ranker.tests"


def test_configure_sets_requested_level_without_duplicate_handlers():
    """Repeated configure() calls must not stack additional handlers."""
    configured_logger = LogManager.configure("DEBUG", force=True)
    handler_count = len(configured_logger.handlers)

    logger = LogManager.configure("DEBUG")

    assert logger.level == logging.DEBUG
    assert logger.propagate is False
    assert len(logger.handlers) == handler_count


def test_configure_does_not_modify_root_logger_handlers():
    """Reconfiguring our base logger must leave the root logger alone."""
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)

    LogManager.configure("INFO", force=True)

    assert list(root_logger.handlers) == original_handlers


def test_configure_rejects_invalid_level():
    """Unknown level strings raise a clear ValueError."""
    with pytest.raises(ValueError, match="Unsupported log level"):
        LogManager.configure("NOT_A_LEVEL", force=True)
