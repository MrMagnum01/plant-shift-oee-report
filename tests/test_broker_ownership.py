"""
Item 9 (narrowed): src/broker.py must never force-remove a podman container
it doesn't own just because it shares the target name. Verified by starting
a plain, unlabelled container under the demo's fixed name and asserting
start_broker() refuses to touch it.
"""
from __future__ import annotations

import subprocess

import pytest

from broker import CONTAINER_NAME, start_broker, stop_broker


@pytest.fixture
def foreign_container():
    name = CONTAINER_NAME
    subprocess.run(["podman", "rm", "-f", name], capture_output=True)
    subprocess.run(
        ["podman", "run", "-d", "--rm", "--name", name,
         "docker.io/library/eclipse-mosquitto:2", "sh", "-c", "sleep 300"],
        check=True, capture_output=True,
    )
    try:
        yield name
    finally:
        # Teardown removes it directly (not via stop_broker, which would
        # itself refuse - that refusal is exactly what's under test).
        subprocess.run(["podman", "rm", "-f", name], capture_output=True)


def test_start_broker_refuses_to_touch_an_unowned_same_name_container(foreign_container):
    with pytest.raises(RuntimeError, match="not owned by this demo"):
        start_broker(name=foreign_container)
    # The foreign container must still be running - never force-removed.
    r = subprocess.run(
        ["podman", "inspect", "--format", "{{.State.Running}}", foreign_container],
        capture_output=True, text=True,
    )
    assert r.returncode == 0
    assert r.stdout.strip() == "true"


def test_stop_broker_refuses_to_touch_an_unowned_same_name_container(foreign_container):
    with pytest.raises(RuntimeError, match="not owned by this demo"):
        stop_broker(foreign_container)
    r = subprocess.run(
        ["podman", "inspect", "--format", "{{.State.Running}}", foreign_container],
        capture_output=True, text=True,
    )
    assert r.returncode == 0
    assert r.stdout.strip() == "true"


def test_start_broker_cleans_up_its_own_stale_leftover(tmp_path):
    # A previous run of this same demo left a container with our label -
    # start_broker() must clean that up itself (not refuse).
    b = start_broker(name="plant-oee-demo-mosquitto-test-stale")
    # Simulate a stale leftover by NOT calling stop_broker; start again
    # with the same name - must succeed by replacing our own leftover.
    try:
        b2 = start_broker(name="plant-oee-demo-mosquitto-test-stale")
        stop_broker(b2.container_name)
    finally:
        stop_broker(b.container_name)  # no-op if already removed above
