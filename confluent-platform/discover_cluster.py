#!/usr/bin/env python3
"""Discover Kafka brokers and KRaft controller roles in a Confluent cluster.

This tutorial combines the Confluent Kafka Python AdminClient API with the
Kafka metadata quorum command. The AdminClient discovers broker IDs and
advertised endpoints; the quorum command identifies controller voters and
the current controller.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from client_config import kafka_client_config, kafka_command_config


def parse_quorum_ids(output: str, section_name: str) -> set[int]:
    """Parse voter or observer node IDs from kafka-metadata-quorum output."""
    section_match = re.search(
        rf"^\s*{re.escape(section_name)}:\s*(.*?)(?=^\s*Current\w+:|\Z)",
        output,
        flags=re.MULTILINE | re.DOTALL,
    )
    if section_match is None:
        return set()

    section = section_match.group(1)
    # Newer Kafka versions print voter objects with an explicit "id" field;
    # older versions print a simple list such as [1, 2, 3].
    object_ids = re.findall(r"[\"']?id[\"']?\s*:\s*(\d+)", section)
    if object_ids:
        return {int(node_id) for node_id in object_ids}

    return {int(node_id) for node_id in re.findall(r"\d+", section)}


def parse_quorum_leader(output: str) -> int | None:
    """Return the KRaft leader ID, or None if the quorum has no leader."""
    match = re.search(r"^\s*LeaderId:\s*(-?\d+)", output, re.MULTILINE)
    if match is None:
        return None

    leader_id = int(match.group(1))
    return leader_id if leader_id >= 0 else None


def read_quorum_status(kafka_bin: Path, bootstrap_servers: str) -> str:
    """Ask Kafka for the live KRaft quorum status."""
    command = [
        str(kafka_bin / "kafka-metadata-quorum"),
        "--command-config",
        kafka_command_config(),
        "--bootstrap-server",
        bootstrap_servers,
        "describe",
        "--status",
    ]
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    return result.stdout


def discover_cluster(bootstrap_servers: str, kafka_bin: Path) -> int:
    """Discover and print cluster nodes and their current Kafka roles."""
    # Step 1: connect to one or more bootstrap brokers. The broker list in the
    # response is discovered from Kafka metadata, not copied from inventory.
    admin = AdminClient(kafka_client_config(bootstrap_servers))
    description = admin.describe_cluster(request_timeout=15).result(timeout=20)
    brokers = {node.id: node for node in description.nodes}

    # Step 2: Kafka metadata identifies the active controller and KRaft voters.
    # This command fills in controller roles that the client broker list alone
    # does not expose.
    quorum_status = read_quorum_status(kafka_bin, bootstrap_servers)
    controller_id = parse_quorum_leader(quorum_status)
    voter_ids = parse_quorum_ids(quorum_status, "CurrentVoters")
    observer_ids = parse_quorum_ids(quorum_status, "CurrentObservers")

    if not voter_ids:
        raise RuntimeError(
            "No KRaft controller voters found in quorum status."
        )

    print(f"Cluster ID: {description.cluster_id or 'unavailable'}")
    if controller_id is None:
        print("Active KRaft controller: unavailable")
    else:
        active_controller = brokers.get(controller_id)
        if active_controller is None:
            print(
                f"Active KRaft controller: node {controller_id} "
                "(not a broker)"
            )
        else:
            print(
                "Active KRaft controller: "
                f"node {controller_id} at {active_controller.host}:"
                f"{active_controller.port}"
            )

    print("\nDiscovered nodes:")
    print(f"{'NODE ID':>7}  {'ENDPOINT':<35}  ROLES")
    print(f"{'-' * 7}  {'-' * 35}  {'-' * 30}")

    all_node_ids = set(brokers) | voter_ids | observer_ids
    for node_id in sorted(all_node_ids):
        broker = brokers.get(node_id)
        endpoint = (
            f"{broker.host}:{broker.port}"
            if broker
            else "not in broker metadata"
        )
        roles = []
        if broker:
            roles.append("broker")
        if node_id in voter_ids:
            roles.append("KRaft controller voter")
        if node_id in observer_ids:
            roles.append("KRaft controller observer")
        if node_id == controller_id:
            roles.append("active controller")
        print(f"{node_id:>7}  {endpoint:<35}  {', '.join(roles)}")

    if not set(brokers).intersection(voter_ids):
        print(
            "\nNote: this cluster has separate controller nodes; "
            "the broker API "
            "does not provide their hostnames."
        )
    return 0


def parse_args() -> argparse.Namespace:
    """Read connection settings from CLI options or environment."""
    parser = argparse.ArgumentParser(
        description="Discover Kafka brokers and KRaft controller roles."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "CONFLUENT_BOOTSTRAP_SERVERS", "sa01:9092,sa02:9092,sa03:9092"
        ),
        help="Kafka bootstrap endpoints (default: lab or environment).",
    )
    parser.add_argument(
        "--kafka-bin",
        type=Path,
        default=Path(
            os.environ.get("CONFLUENT_BIN_DIR", "/app/confluent/bin")
        ),
        help="Directory containing kafka-metadata-quorum.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the tutorial and convert connection errors to a useful message."""
    args = parse_args()
    try:
        return discover_cluster(args.bootstrap_servers, args.kafka_bin)
    except (
        KafkaException,
        OSError,
        subprocess.SubprocessError,
        RuntimeError,
    ) as error:
        print(
            f"Could not discover Confluent cluster: {error}", file=sys.stderr
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
