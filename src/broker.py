"""
Starts/stops the Eclipse Mosquitto broker (EPL-2.0/EDL) in a podman
container, bound to 127.0.0.1 on a random free port. Used by scripts and by
the pytest fixtures - never touches any other container on the host.

Ownership: containers this module creates are labelled OWNER_LABEL_KEY=
OWNER_LABEL_VALUE. start_broker() only ever force-removes a pre-existing
container of the same name if that container carries this exact label
(i.e. it is a stale leftover from a previous run of this same demo). If a
container with the target name exists and is NOT labelled as ours, it is
left untouched and a RuntimeError is raised instead - it is never blindly
`podman rm -f`'d, since it might belong to something else on the host.
"""
from __future__ import annotations

import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

CONF_PATH = Path(__file__).resolve().parent.parent / "scripts" / "mosquitto.conf"
CONTAINER_NAME = "plant-oee-demo-mosquitto"
IMAGE = "docker.io/library/eclipse-mosquitto:2"
OWNER_LABEL_KEY = "com.plant-oee-demo.owner"
OWNER_LABEL_VALUE = "plant-shift-oee-report"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Broker:
    host: str
    port: int
    container_name: str


def _owner_label(name: str) -> str | None:
    """Return the OWNER_LABEL_KEY value of an existing container named
    `name`, or None if no such container exists. Never raises for a
    missing container."""
    r = subprocess.run(
        ["podman", "inspect", "--format", f"{{{{ index .Config.Labels \"{OWNER_LABEL_KEY}\" }}}}", name],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return None
    return r.stdout.strip()


def start_broker(name: str = CONTAINER_NAME, port: int | None = None) -> Broker:
    owner = _owner_label(name)
    if owner is not None:
        if owner == OWNER_LABEL_VALUE:
            stop_broker(name)  # stale leftover from a previous run of this same demo
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
