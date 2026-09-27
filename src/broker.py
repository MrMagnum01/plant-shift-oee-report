"""
Starts/stops the Eclipse Mosquitto broker (EPL-2.0/EDL) in a podman
container, bound to 127.0.0.1 on a random free port. Used by scripts and by
the pytest fixtures - never touches any other container on the host.

Ownership and run isolation: containers this module creates are labelled
OWNER_LABEL_KEY=OWNER_LABEL_VALUE (this demo, as a project) AND
RUN_ID_LABEL_KEY=<this process's RUN_ID> (this specific run). If a
container with the target name exists and is NOT project-labelled, it is
left untouched and a RuntimeError is raised - it is never blindly
`podman rm -f`'d, since it might belong to something else on the host
(unchanged from before). If it IS project-labelled but its run-id label
does not match this process's RUN_ID, it belongs to a *different* (possibly
still-active) run of this same demo - start_broker() refuses to touch it
rather than silently killing another run just because the default name
collided; pass an explicit unique `name` (see `unique_name()`) to isolate
concurrent runs instead. Only when the run-id label matches this process's
own RUN_ID (i.e. an earlier, uncommitted call from this exact run/process)
is it treated as this run's own stale leftover and replaced. This is a
narrowed claim: two *separate processes* that both explicitly reuse the
same fixed name (e.g. two manual `python3 src/broker.py` invocations
without ever isolating names) are still two different runs by this
module's definition and will correctly refuse each other, not race.
`tests/conftest.py`'s pytest fixture always passes a fresh unique name so
test runs never depend on this collision handling at all.
"""
from __future__ import annotations

import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

CONF_PATH = Path(__file__).resolve().parent.parent / "scripts" / "mosquitto.conf"
CONTAINER_NAME = "plant-oee-demo-mosquitto"
IMAGE = "docker.io/library/eclipse-mosquitto:2"
OWNER_LABEL_KEY = "com.plant-oee-demo.owner"
OWNER_LABEL_VALUE = "plant-shift-oee-report"
RUN_ID_LABEL_KEY = "com.plant-oee-demo.run-id"
# One UUID per process/import - identifies "this run" for the ownership
# check above. Two separate `python3 ...` invocations (even of the same
# script) get different RUN_IDs; a single process reusing the same name
# twice (e.g. the crash-recovery test below) keeps the same RUN_ID.
RUN_ID = uuid.uuid4().hex[:12]


def unique_name(prefix: str = CONTAINER_NAME) -> str:
    """A fresh container name for a fully name-isolated run - never
    collides with any other run's container, so start_broker() never even
    reaches the ownership-collision logic above. Used by
    tests/conftest.py."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Broker:
    host: str
    port: int
    container_name: str


def _label(name: str, key: str) -> str | None:
    """Return the given label's value on an existing container named
    `name`, or None if no such container exists. Never raises for a
    missing container."""
    r = subprocess.run(
        ["podman", "inspect", "--format", f"{{{{ index .Config.Labels \"{key}\" }}}}", name],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return None
    return r.stdout.strip()


def _owner_label(name: str) -> str | None:
    return _label(name, OWNER_LABEL_KEY)


def start_broker(name: str = CONTAINER_NAME, port: int | None = None) -> Broker:
    owner = _owner_label(name)
    if owner is not None:
        if owner == OWNER_LABEL_VALUE:
            run_id = _label(name, RUN_ID_LABEL_KEY)
            if run_id == RUN_ID:
                # Stale leftover from an earlier call within this exact
                # run/process (e.g. a previous start_broker() that was
                # never stopped) - safe to replace.
                stop_broker(name)
            else:
                raise RuntimeError(
                    f"podman container {name!r} already exists, is owned by this demo "
                    f"({OWNER_LABEL_KEY}={owner!r}), but belongs to a DIFFERENT run "
                    f"({RUN_ID_LABEL_KEY}={run_id!r} != this run's {RUN_ID!r}) - it may "
                    "still be active. Refusing to remove another run's container just "
                    "because the name collided. Pass a unique `name` (see "
                    "`broker.unique_name()`) to isolate concurrent runs, or stop that "
                    f"other run yourself first if you're sure it's done: `podman rm -f {name}`."
                )
        else:
            raise RuntimeError(
                f"podman container {name!r} already exists and is not owned by this "
                f"demo (label {OWNER_LABEL_KEY}={owner!r}); refusing to remove it since "
                "it may belong to something else on the host. Inspect it with "
                f"`podman inspect {name}` and remove it manually if appropriate, or "
                "pass a different `name` to start_broker()."
            )
    port = port or _free_port()
    subprocess.run(
        [
            "podman", "run", "-d", "--rm",
            "--name", name,
            "--label", f"{OWNER_LABEL_KEY}={OWNER_LABEL_VALUE}",
            "--label", f"{RUN_ID_LABEL_KEY}={RUN_ID}",
            "-p", f"127.0.0.1:{port}:1883",
            "-v", f"{CONF_PATH}:/mosquitto/config/mosquitto.conf:ro,Z",
            IMAGE,
        ],
        check=True,
        capture_output=True,
    )
    _wait_ready("127.0.0.1", port, timeout=15)
    return Broker(host="127.0.0.1", port=port, container_name=name)


def stop_broker(name: str = CONTAINER_NAME) -> None:
    """Remove the named container - but only if it carries our ownership
    label (or doesn't exist at all). Never force-removes a container this
    module didn't create."""
    owner = _owner_label(name)
    if owner is None:
        return  # nothing to stop
    if owner != OWNER_LABEL_VALUE:
        raise RuntimeError(
            f"podman container {name!r} is not owned by this demo (label "
            f"{OWNER_LABEL_KEY}={owner!r}); refusing to remove it."
        )
    subprocess.run(["podman", "rm", "-f", name], capture_output=True)


def _wait_ready(host: str, port: int, timeout: float = 15) -> None:
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError as e:
            last_err = e
            time.sleep(0.25)
    raise TimeoutError(f"mosquitto on {host}:{port} not ready: {last_err}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        stop_broker()
        print("stopped")
    else:
        b = start_broker()
        print(f"broker up on {b.host}:{b.port} (container {b.container_name})")
