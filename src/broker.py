"""
Starts/stops the Eclipse Mosquitto broker (EPL-2.0/EDL) in a podman
container, bound to 127.0.0.1 on a random free port. Used by scripts and by
the pytest fixtures - never touches any other container on the host.
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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Broker:
    host: str
    port: int
    container_name: str


def start_broker(name: str = CONTAINER_NAME, port: int | None = None) -> Broker:
    stop_broker(name)  # ensure no stale container with the same name
    port = port or _free_port()
    subprocess.run(
        [
            "podman", "run", "-d", "--rm",
            "--name", name,
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
