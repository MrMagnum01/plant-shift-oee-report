import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

from broker import start_broker, stop_broker  # noqa: E402


@pytest.fixture
def mqtt_broker():
    b = start_broker()
    try:
        yield b
    finally:
        stop_broker(b.container_name)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.duckdb")
