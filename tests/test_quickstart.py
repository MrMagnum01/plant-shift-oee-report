"""
MUST-FIX-8: the README's own Quickstart commands must work from a clean
checkout, with no PYTHONPATH set. Astra's review reproduced the previous
`python3 -c "from src.ingester import ..."` one-liner raising
ModuleNotFoundError. This test parses the *exact* commands out of
README.md's Quickstart fenced code block and executes them, verbatim
(only `<port>` and the `out/...` paths are substituted), in independent
subprocesses with cwd=repo root - exactly as a reader following the README
would run them - so the README and the tested behaviour cannot drift apart
silently.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _quickstart_commands() -> list[str]:
    readme = (ROOT / "README.md").read_text()
    m = re.search(r"## Quickstart\n\n```bash\n(.*?)\n```", readme, re.DOTALL)
    assert m, "README.md Quickstart fenced bash code block not found"
    return [line.strip() for line in m.group(1).splitlines() if line.strip().startswith("python3")]


def _clean_env() -> dict:
    return {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}


def test_readme_quickstart_has_expected_commands():
    cmds = _quickstart_commands()
    assert any(c.startswith("python3 src/broker.py") and "stop" not in c for c in cmds)
    assert any(c.startswith("python3 src/ingester.py") for c in cmds)
    assert any(c.startswith("python3 src/simulate.py") for c in cmds)
    assert any(c.startswith("python3 src/report.py") for c in cmds)
    assert "python3 src/broker.py stop" in cmds


def test_readme_quickstart_runs_from_clean_checkout(tmp_path):
    cmds = _quickstart_commands()
    env = _clean_env()

    broker_cmd = next(c for c in cmds if c.startswith("python3 src/broker.py") and "stop" not in c).split()
    broker_proc = subprocess.run(broker_cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
    assert broker_proc.returncode == 0, broker_proc.stderr
    m = re.search(r"broker up on 127\.0\.0\.1:(\d+)", broker_proc.stdout)
    assert m, f"unexpected broker.py output: {broker_proc.stdout!r} / {broker_proc.stderr!r}"
    port = m.group(1)

    try:
        db_path = tmp_path / "plant.duckdb"
        report_out = tmp_path / "report.html"

        def _sub(cmd: str) -> list[str]:
            return [
                tok.replace("<port>", port)
                   .replace("out/plant.duckdb", str(db_path))
                   .replace("out/report.html", str(report_out))
                for tok in cmd.split()
            ]

        ingester_cmd = _sub(next(c for c in cmds if c.startswith("python3 src/ingester.py")))
        simulate_cmd = _sub(next(c for c in cmds if c.startswith("python3 src/simulate.py")))
        report_cmd = _sub(next(c for c in cmds if c.startswith("python3 src/report.py")))

        ingest_proc = subprocess.Popen(
            ingester_cmd, cwd=ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        time.sleep(1.0)
        sim_proc = subprocess.run(simulate_cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
        assert sim_proc.returncode == 0, sim_proc.stderr
        assert "published 9411 events" in sim_proc.stdout

        # Full synthetic day (~9.4k messages) over real MQTT, then a 15s
        # idle-timeout wait before the ingester exits - matches the
        # generous headroom tests/test_reconciliation.py already uses.
        try:
            out, _ = ingest_proc.communicate(timeout=240)
        except subprocess.TimeoutExpired:
            ingest_proc.kill()
            out, _ = ingest_proc.communicate()
            raise
        assert ingest_proc.returncode == 0, out
        assert "ModuleNotFoundError" not in out
        assert "ingest stats" in out

        report_proc = subprocess.run(report_cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
        assert report_proc.returncode == 0, report_proc.stderr
        assert report_out.exists()
        assert "Line A" in report_out.read_text()
        assert report_out.with_suffix(".json").exists()
    finally:
        subprocess.run(["python3", "src/broker.py", "stop"], cwd=ROOT, env=env,
                        capture_output=True, timeout=15)
