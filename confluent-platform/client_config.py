"""Shared SASL/SCRAM settings for the Confluent Python tutorials."""

from __future__ import annotations

import os
import re
from pathlib import Path


DEFAULT_CLIENT_CONFIG = Path(
    "/app/confluent/etc/kafka/client.properties"
)


def _client_credentials() -> tuple[str, str]:
    """Read credentials from the environment or the role-installed config."""
    username = os.environ.get("CONFLUENT_SASL_USERNAME", "confluent")
    password = os.environ.get("CONFLUENT_SASL_PASSWORD")
    if password:
        return username, password

    config_path = Path(
        os.environ.get("CONFLUENT_CLIENT_CONFIG", DEFAULT_CLIENT_CONFIG)
    )
    try:
        config_text = config_path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError(
            "Set CONFLUENT_SASL_PASSWORD or run on a broker with "
            "the role-installed client.properties file."
        ) from error

    match = re.search(
        r'sasl\.jaas\.config=.*?username="([^"]+)"\s+'
        r'password="([^"]+)"',
        config_text,
    )
    if match is None:
        raise RuntimeError(
            f"Could not find SASL credentials in {config_path}."
        )
    return match.group(1), match.group(2)


def kafka_client_config(bootstrap_servers: str) -> dict[str, str]:
    """Build librdkafka options using the lab's SCRAM-SHA-512 credentials."""
    username, password = _client_credentials()

    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": os.environ.get(
            "CONFLUENT_SECURITY_PROTOCOL", "SASL_PLAINTEXT"
        ),
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": username,
        "sasl.password": password,
    }


def kafka_command_config() -> str:
    """Return the Java client properties file used by the CLI tools."""
    return os.environ.get(
        "CONFLUENT_CLIENT_CONFIG",
        "/app/confluent/etc/kafka/client.properties",
    )
