"""Confluent Cloud connection settings shared by the PyFlink examples in this folder.

The settings are read by /home/aio/training/confluent-python/confluent-cloud/client_config.py,
from the environment or from /home/aio/training/confluent-python/confluent-cloud/.env:

    BOOTSTRAP_SERVERS, KEY, SECRET                          Kafka cluster and its API key
    SCHEMA_REGISTRY_ENDPOINT, SCHEMA_REGISTRY_KEY,
    SCHEMA_REGISTRY_SECRET                                  Schema Registry and its API key

(prepare_common_lab.sh in that folder creates the keys and writes the .env file.)
Confluent Cloud uses SASL_SSL with the PLAIN mechanism, where the API key is the
user name and the API secret is the password.

Use the unshaded Kafka connector JARs that setup_pyflink.sh installs, so the login
module name is the plain Kafka one.
"""

import importlib.util
import os
from pathlib import Path

CLIENT_CONFIG = Path(
    os.environ.get(
        "CONFLUENT_CLOUD_CLIENT_CONFIG",
        Path(__file__).resolve().parents[2] / "confluent-cloud" / "client_config.py",
    )
)
LOGIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"


def _client_config():
    if not CLIENT_CONFIG.is_file():
        raise RuntimeError(
            f"{CLIENT_CONFIG} not found. Set CONFLUENT_CLOUD_CLIENT_CONFIG to the "
            "client_config.py of the confluent-cloud folder."
        )
    spec = importlib.util.spec_from_file_location("cloud_client_config", CLIENT_CONFIG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def bootstrap_servers():
    return _client_config().default_bootstrap_servers()


def kafka_client_properties():
    """Kafka client properties for KafkaSource/KafkaSink.set_property()."""
    config = _client_config().kafka_client_config()
    return {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": (
            f'{LOGIN_MODULE} required username="{config["sasl.username"]}" '
            f'password="{config["sasl.password"]}";'
        ),
    }


def sql_options():
    """Extra WITH (...) lines for a Flink SQL 'kafka' or 'upsert-kafka' table."""
    lines = []
    for key, value in kafka_client_properties().items():
        lines.append(f"'properties.{key}' = '{value}',")
    return "\n            ".join(lines)


def confluent_kafka_properties():
    """Configuration dict for confluent_kafka Producer/AdminClient (includes bootstrap.servers)."""
    return _client_config().kafka_client_config()


def schema_registry_config():
    """Configuration dict for confluent_kafka SchemaRegistryClient (url, basic.auth.user.info)."""
    return _client_config().schema_registry_config()


def schema_registry_sql_options(prefix="avro-confluent"):
    """WITH lines that let the Flink avro-confluent format log in to Schema Registry."""
    config = schema_registry_config()
    return "\n            ".join(
        [
            f"'{prefix}.url' = '{config['url']}',",
            f"'{prefix}.basic-auth.credentials-source' = 'USER_INFO',",
            f"'{prefix}.basic-auth.user-info' = '{config['basic.auth.user.info']}',",
        ]
    )


def ensure_topics(*names):
    """Create the topics that do not exist yet (1 partition; Confluent Cloud uses replication 3).

    Confluent Cloud does not create topics when a producer writes to them."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(confluent_kafka_properties())
    existing = set(admin.list_topics(timeout=30).topics)
    missing = [NewTopic(name, num_partitions=1) for name in names if name not in existing]
    if missing:
        for name, future in admin.create_topics(missing).items():
            future.result()
            print(f"Created topic {name} (1 partition)", flush=True)
