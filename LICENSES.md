# Licenses

This repository's own code is MIT licensed - see `LICENSE`.

Third-party components used, all OSI-approved licenses:

| Component | License | SPDX | Role |
|---|---|---|---|
| [Eclipse Mosquitto](https://mosquitto.org/) | Eclipse Public License 2.0 / Eclipse Distribution License 1.0 | EPL-2.0 / EDL-1.0 | MQTT broker (podman container, `docker.io/library/eclipse-mosquitto:2`) |
| [paho-mqtt](https://github.com/eclipse-paho/paho.mqtt.python) | Eclipse Public License 2.0 / Eclipse Distribution License 1.0 | EPL-2.0 / EDL-1.0 | Python MQTT client (simulator + ingester) |
| [DuckDB](https://duckdb.org/) | MIT License | MIT | Embedded time-series store |
| [pytz](https://pythonhosted.org/pytz/) | MIT License | MIT | Timezone support for DuckDB timestamp binding |
| [pytest](https://pytest.org/) | MIT License | MIT | Test runner |

No paid or closed-source dependency is used anywhere in this repository.
