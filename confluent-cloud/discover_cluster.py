#!/usr/bin/env python3
"""Discover the Kafka brokers exposed by a Confluent Cloud cluster."""

from __future__ import annotations

import argparse
import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from client_config import default_bootstrap_servers, kafka_client_config


def parse_args() -> argparse.Namespace:
    """Parse an optional Kafka bootstrap-server override."""
    parser = argparse.ArgumentParser(
        description="Discover brokers in a Confluent Cloud Kafka cluster."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap_servers(),
        help="Kafka bootstrap server (default: value from .env).",
    )
    return parser.parse_args()


def main() -> int:
    """Connect to the cluster and print its broker metadata."""
    args = parse_args()
    try:
        admin = AdminClient(kafka_client_config(args.bootstrap_servers))
        description = admin.describe_cluster(request_timeout=20).result(
            timeout=25
        )
    except (KafkaException, RuntimeError, TimeoutError) as error:
        print(
            f"Could not discover Confluent Cloud brokers: {error}",
            file=sys.stderr,
        )
        return 1

    print(f"Confluent Cloud cluster: {description.cluster_id or 'available'}")
    print("Discovered brokers:")
    print(f"{'NODE ID':>7}  {'ENDPOINT':<50}  ROLE")
    print(f"{'-' * 7}  {'-' * 50}  {'-' * 20}")
    for node in sorted(description.nodes, key=lambda item: item.id):
        print(f"{node.id:>7}  {node.host + ':' + str(node.port):<50}  broker")
    print(
        "\nController nodes and controller roles are managed by "
        "Confluent Cloud and are not exposed through the Kafka broker API."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
