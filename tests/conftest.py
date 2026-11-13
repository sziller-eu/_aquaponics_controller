import os
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def short_directory():
    """A short-path private temporary directory (Unix socket paths are limited to ~107 bytes)."""
    base = "/tmp" if os.path.isdir("/tmp") else None
    directory = Path(tempfile.mkdtemp(prefix="aq-", dir=base))
    yield directory
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture(autouse=True)
def isolated_diagnostic_logging():
    """Undo what a test does to process-wide logging: root handlers and level, and the correlation context."""
    import logging

    from aquaponics_controller import diagnostic_logging

    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    configured = list(diagnostic_logging._configured_handlers)
    diagnostic_logging.log_context.reset()
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)
    diagnostic_logging._configured_handlers[:] = configured
    diagnostic_logging.log_context.reset()
