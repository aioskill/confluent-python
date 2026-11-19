"""SASL/SCRAM settings shared by the PyFlink Kafka examples.

The Confluent Platform lab cluster (sa01, sa02, sa03) listens on port 9092 with
SASL_PLAINTEXT and SCRAM-SHA-512. Every Kafka client, including the Flink Kafka
connector and the kafka-python producers, must send these settings.

Credentials are read, in this order, from:

1. the environment: CONFLUENT_SASL_USERNAME (default "confluent") and
   CONFLUENT_SASL_PASSWORD;
2. the client file CONFLUENT_CLIENT_CONFIG
   (default /app/confluent/etc/kafka/client.properties), which exists on sa01-sa03.

When a job runs on a machine other than sa01-sa03, export CONFLUENT_SASL_PASSWORD.
"""

import os
import re
from pathlib import Path

DEFAULT_BOOTSTRAP_SERVERS = "sa01:9092,sa02:9092,sa03:9092"
DEFAULT_CLIENT_CONFIG = "/app/confluent/etc/kafka/client.properties"
SECURITY_PROTOCOL = os.environ.get("CONFLUENT_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
SASL_MECHANISM = "SCRAM-SHA-512"


def credentials():
    """Return (username, password) for the SCRAM user."""
    username = os.environ.get("CONFLUENT_SASL_USERNAME", "confluent")
    password = os.environ.get("CONFLUENT_SASL_PASSWORD")
    if password:
        return username, password

    path = Path(os.environ.get("CONFLUENT_CLIENT_CONFIG", DEFAULT_CLIENT_CONFIG))
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError(
            "Set CONFLUENT_SASL_PASSWORD (and CONFLUENT_SASL_USERNAME if it is not "
            f"'confluent'), or run on a node that has {path}."
        ) from error
    match = re.search(r'username="([^"]+)"\s+password="([^"]+)"', text)
    if match is None:
        raise RuntimeError(f"No SCRAM username/password found in {path}.")
    return match.group(1), match.group(2)


# The flink-sql-connector-kafka "uber" JAR relocates the Kafka client classes, so the
# login module must then be named with the relocated package. The unshaded
# flink-connector-kafka + kafka-clients JARs (flinklibs/pom.xml) use the plain name.
SHADED_PREFIX = "org.apache.flink.kafka.shaded."


def jaas_config(shaded=False):
    username, password = credentials()
    module = "org.apache.kafka.common.security.scram.ScramLoginModule"
    if shaded:
        module = SHADED_PREFIX + module
    return (
        f"{module} required "
        f'username="{username}" password="{password}";'
    )


def kafka_client_properties(shaded=False):
    """Kafka client properties, for KafkaSource/KafkaSink.set_property().

    Pass shaded=True only when the job loads flink-sql-connector-kafka-*.jar."""
    return {
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanism": SASL_MECHANISM,
        "sasl.jaas.config": jaas_config(shaded),
    }


def sql_options(shaded=False):
    """Extra WITH (...) lines for a Flink SQL 'kafka' or 'upsert-kafka' table.

    Usage inside an f-string, after the bootstrap servers line:

        'properties.bootstrap.servers' = '...',
        {sql_options()}
        'properties.group.id' = '...',
    """
    lines = []
    for key, value in kafka_client_properties(shaded).items():
        escaped = value.replace("'", "''")
        lines.append(f"'properties.{key}' = '{escaped}',")
    return "\n            ".join(lines)


def kafka_python_properties():
    """Keyword arguments for kafka-python KafkaProducer/KafkaConsumer."""
    username, password = credentials()
    return {
        "security_protocol": SECURITY_PROTOCOL,
        "sasl_mechanism": SASL_MECHANISM,
        "sasl_plain_username": username,
        "sasl_plain_password": password,
    }


def confluent_kafka_properties():
    """Configuration dict for confluent_kafka Producer/Consumer (librdkafka)."""
    username, password = credentials()
    return {
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanism": SASL_MECHANISM,
        "sasl.username": username,
        "sasl.password": password,
    }
