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
| [iniconfig](https://github.com/pytest-dev/iniconfig) 2.3.0 | MIT License | MIT | pytest transitive dependency (ini file parsing) |
| [pluggy](https://github.com/pytest-dev/pluggy) 1.6.0 | MIT License | MIT | pytest transitive dependency (plugin system) |
| [packaging](https://github.com/pypa/packaging) 26.3 | Apache License 2.0 OR BSD 2-Clause | Apache-2.0 OR BSD-2-Clause | pytest transitive dependency (version parsing) |
| [Pygments](https://pygments.org/) 2.21.0 | BSD 2-Clause License | BSD-2-Clause | pytest transitive dependency (traceback syntax highlighting) |

No paid or closed-source dependency is used anywhere in this repository.
