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
