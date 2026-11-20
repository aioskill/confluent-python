"""Load Confluent Cloud credentials and build Kafka/Schema Registry clients."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ENV_FILE = Path(__file__).with_name(".env")


def _read_env_file() -> dict[str, str]:
    """Read simple KEY=VALUE entries without printing or exporting secrets."""
    values = {}
    try:
        lines = ENV_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        name, separator, value = line.partition("=")
        if not separator:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[name.strip()] = value
    return values


_ENV_VALUES = _read_env_file()


def _setting(*names: str, default: str | None = None) -> str | None:
    """Return the first non-empty setting, preferring process environment."""
    for name in names:
        value = os.environ.get(name) or _ENV_VALUES.get(name)
        if value:
            return value
    return default


def default_bootstrap_servers() -> str:
    """Read the Kafka bootstrap server, including the existing env spelling."""
    value = _setting(
        "CONFLUENT_BOOTSTRAP_SERVERS",
        "BOOTSTRAP_SERVERS",
        "BOOTSTRAP_SERVRS",
    )
    if not value:
        raise RuntimeError(
            "Set BOOTSTRAP_SERVRS or CONFLUENT_BOOTSTRAP_SERVERS in .env."
        )
    return value


def default_schema_registry_url() -> str:
    """Read the Confluent Cloud Schema Registry endpoint."""
    value = _setting("SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_ENDPOINT")
    if not value:
        raise RuntimeError("Set SCHEMA_REGISTRY_ENDPOINT in .env.")
    return value


def kafka_client_config(
    bootstrap_servers: str | None = None,
) -> dict[str, str]:
    """Build librdkafka configuration using a Cloud API key and secret."""
    username = _setting("CONFLUENT_CLOUD_API_KEY", "KEY")
    password = _setting("CONFLUENT_CLOUD_API_SECRET", "SECRET")
    if not username or not password:
        raise RuntimeError("Set KEY and SECRET in the Cloud .env file.")

    return {
        "bootstrap.servers": bootstrap_servers or default_bootstrap_servers(),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
    }


def schema_registry_config(endpoint: str | None = None) -> dict[str, str]:
    """Build Schema Registry client config using its resource API key pair."""
    username = _setting("SCHEMA_REGISTRY_KEY")
    password = _setting("SCHEMA_REGISTRY_SECRET")
    if not username or not password:
        raise RuntimeError(
            "Set SCHEMA_REGISTRY_KEY and SCHEMA_REGISTRY_SECRET in .env."
        )
    return {
        "url": endpoint or default_schema_registry_url(),
        "basic.auth.user.info": f"{username}:{password}",
    }


def cloud_rest_request(
    path: str, method: str = "GET", payload: dict | None = None
) -> dict:
    """Call the per-cluster Confluent Cloud Kafka REST API."""
    endpoint = _setting("CONFLUENT_CLOUD_REST_ENDPOINT", "REST_ENDPOINT")
    username = _setting("CONFLUENT_CLOUD_API_KEY", "KEY")
    password = _setting("CONFLUENT_CLOUD_API_SECRET", "SECRET")
    if not endpoint:
        raise RuntimeError("Set REST_ENDPOINT in the Cloud .env file.")
    if not username or not password:
        raise RuntimeError("Set KEY and SECRET in the Cloud .env file.")

    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        endpoint.rstrip("/") + path,
        data=body,
        headers={
            "Authorization": f"Basic {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=30) as response:
            content = response.read()
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Confluent Cloud REST API returned HTTP {error.code}: {detail}"
        ) from error
    except URLError as error:
        raise RuntimeError(
            f"Could not reach Confluent Cloud REST API: {error}"
        ) from error

    if not content:
        return {}
    return json.loads(content.decode("utf-8"))


def cloud_cluster_id() -> str:
    """Get the lkc cluster ID from configuration or the bootstrap hostname."""
    configured = _setting("CONFLUENT_CLOUD_CLUSTER_ID", "KAFKA_CLUSTER_ID")
    if configured:
        return configured
    bootstrap = default_bootstrap_servers().split(",", 1)[0]
    hostname = bootstrap.rsplit(":", 1)[0]
    cluster_id = hostname.split(".", 1)[0]
    if not cluster_id.startswith("lkc-"):
        raise RuntimeError(
            "Could not infer the Cloud cluster ID; set KAFKA_CLUSTER_ID."
        )
    return cluster_id


def ksqldb_config() -> dict[str, str]:
    """Read the ksqlDB cluster endpoint and its API key pair."""
    endpoint = _setting("KSQLDB_ENDPOINT")
    username = _setting("KSQLDB_KEY")
    password = _setting("KSQLDB_SECRET")
    if not endpoint or not username or not password:
        raise RuntimeError(
            "Set KSQLDB_ENDPOINT, KSQLDB_KEY and KSQLDB_SECRET in .env "
            "(see LAB-10-KSQLDB.md)."
        )
    return {"endpoint": endpoint.rstrip("/"), "key": username, "secret": password}
