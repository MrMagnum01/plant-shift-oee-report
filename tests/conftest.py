import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

from broker import start_broker, stop_broker, unique_name  # noqa: E402


@pytest.fixture
def mqtt_broker():
    # A fresh, per-test unique container name (item 5: per-run isolation) -
    # never depends on start_broker()'s same-name collision/ownership
    # handling, so parallel test runs on the same host never contend for
    # one fixed container name.
    b = start_broker(name=unique_name())
    try:
        yield b
    finally:
        stop_broker(b.container_name)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.duckdb")
