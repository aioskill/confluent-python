#!/usr/bin/env python3
"""Tutorial for listing and managing topics in the Confluent Kafka cluster.

Most operations use confluent-kafka-python's AdminClient. Kafka has no direct
"set replication factor" topic operation, so that command builds and executes
a partition reassignment with the Kafka CLI, then waits for replicas to join
the in-sync replica set.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ConfigSource,
    NewPartitions,
    NewTopic,
    ResourceType,
)

from client_config import kafka_client_config, kafka_command_config


def wait_for_future(future: Any, timeout: float) -> Any:
    """Wait for one AdminClient operation and return its result."""
    return future.result(timeout=timeout)


def topic_metadata(admin: AdminClient, topic: str, timeout: float) -> Any:
    """Fetch a topic and fail clearly if it does not exist."""
    # Avoid auto-creating a missing topic if the broker enables that setting.
    metadata = admin.list_topics(timeout=timeout)
    result = metadata.topics.get(topic)
    if result is None:
        raise RuntimeError(f"Topic {topic!r} was not found.")
    if result.error is not None:
        raise RuntimeError(f"Could not read topic {topic!r}: {result.error}")
    return result


def list_topics(admin: AdminClient, timeout: float) -> None:
    """List topics discovered from Kafka metadata."""
    metadata = admin.list_topics(timeout=timeout)
    print("Topics:")
    for name in sorted(metadata.topics):
        print(f"  {name}")


def describe_topic(admin: AdminClient, topic: str, timeout: float) -> None:
    """Show partition leaders, replicas, ISR, and topic properties."""
    metadata = topic_metadata(admin, topic, timeout)
    print(f"Topic: {topic}")
    print("Partitions:")
    for partition_id, partition in sorted(metadata.partitions.items()):
        print(
            f"  {partition_id}: leader={partition.leader}, "
            f"replicas={partition.replicas}, isr={partition.isrs}"
        )

    resource = ConfigResource(ResourceType.TOPIC, topic)
    configs = wait_for_future(
        admin.describe_configs([resource])[resource], timeout
    )
    print("Properties:")
    for name, entry in sorted(configs.items()):
        value = "<sensitive>" if entry.is_sensitive else entry.value
        source = ConfigSource(entry.source).name
        print(f"  {name}={value} (source={source})")


def create_topic(
    admin: AdminClient,
    topic: str,
    partitions: int,
    replication_factor: int,
    timeout: float,
) -> None:
    """Create a topic with the requested partition and replica counts."""
    new_topic = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=replication_factor,
    )
    wait_for_future(
        admin.create_topics(
            [new_topic], request_timeout=timeout
        )[topic],
        timeout,
    )
    print(
        f"Created topic {topic!r} with {partitions} partition(s) and "
        f"replication factor {replication_factor}."
    )


def parse_config_values(values: list[str]) -> dict[str, str]:
    """Parse repeated --set key=value options."""
    parsed = {}
    for item in values:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise ValueError(f"Expected key=value, got {item!r}.")
        parsed[key] = value
    return parsed


def alter_topic_config(
    admin: AdminClient,
    topic: str,
    set_values: list[str],
    delete_values: list[str],
    timeout: float,
) -> None:
    """Set or reset topic properties without replacing unrelated settings."""
    set_configs = parse_config_values(set_values)
    if set(set_configs).intersection(delete_values):
        raise ValueError(
            "A property cannot be set and deleted in one request."
        )
    if not set_configs and not delete_values:
        raise ValueError("Supply at least one --set or --delete property.")

    resource = ConfigResource(ResourceType.TOPIC, topic)
    for name, value in set_configs.items():
        resource.add_incremental_config(
            ConfigEntry(
                name,
                value,
                incremental_operation=AlterConfigOpType.SET,
            )
        )
    for name in delete_values:
        if not name:
            raise ValueError(
                "Property names passed to --delete cannot be empty."
            )
        resource.add_incremental_config(
            ConfigEntry(
                name,
                None,
                incremental_operation=AlterConfigOpType.DELETE,
            )
        )

    future = admin.incremental_alter_configs(
        [resource], request_timeout=timeout
    )[resource]
    wait_for_future(future, timeout)
    print(f"Updated properties for topic {topic!r}.")


def change_partition_count(
    admin: AdminClient,
    topic: str,
    new_count: int,
    timeout: float,
) -> None:
    """Increase a topic's partition count; Kafka cannot decrease it."""
    metadata = topic_metadata(admin, topic, timeout)
    current_count = len(metadata.partitions)
    if new_count < current_count:
        raise ValueError(
            f"Partition count cannot be decreased from {current_count} "
            f"to {new_count}."
        )
    if new_count == current_count:
        print(f"Topic {topic!r} already has {current_count} partitions.")
        return

    new_partitions = NewPartitions(topic, new_count)
    wait_for_future(
        admin.create_partitions(
            [new_partitions], request_timeout=timeout
        )[topic],
        timeout,
    )
    print(f"Increased topic {topic!r} to {new_count} partitions.")


def build_replica_assignment(
    admin: AdminClient,
    topic: str,
    replication_factor: int,
    timeout: float,
) -> list[dict[str, Any]]:
    """Build a reassignment that preserves existing replicas where possible."""
    description = admin.describe_cluster(request_timeout=timeout).result(
        timeout=timeout
    )
    broker_ids = sorted(node.id for node in description.nodes)
    if replication_factor > len(broker_ids):
        raise ValueError(
            f"Replication factor {replication_factor} exceeds the "
            f"{len(broker_ids)} available brokers."
        )

    metadata = topic_metadata(admin, topic, timeout)
    partitions = sorted(metadata.partitions.items())
    replica_load = {broker_id: 0 for broker_id in broker_ids}
    for _, partition in partitions:
        for broker_id in partition.replicas:
            if broker_id in replica_load:
                replica_load[broker_id] += 1

    assignment = []
    for partition_id, partition in partitions:
        current = list(partition.replicas)
        if not current:
            raise RuntimeError(
                f"Partition {partition_id} has no current replicas."
            )

        target = current[:replication_factor]
        for broker_id in current[replication_factor:]:
            if broker_id in replica_load:
                replica_load[broker_id] -= 1

        while len(target) < replication_factor:
            candidates = [
                broker_id
                for broker_id in broker_ids
                if broker_id not in target
            ]
            if not candidates:
                raise RuntimeError(
                    "Could not find enough brokers for partition "
                    f"{partition_id}."
                )
            selected = min(
                candidates, key=lambda item: (replica_load[item], item)
            )
            target.append(selected)
            replica_load[selected] += 1

        if target != current:
            assignment.append(
                {
                    "topic": topic,
                    "partition": partition_id,
                    "replicas": target,
                }
            )
    return assignment


def replicas_are_ready(
    admin: AdminClient,
    topic: str,
    assignment: list[dict[str, Any]],
    timeout: float,
) -> bool:
    """Return true when every reassigned replica is in-sync."""
    metadata = topic_metadata(admin, topic, timeout)
    for item in assignment:
        partition = metadata.partitions[item["partition"]]
        expected = set(item["replicas"])
        if (
            set(partition.replicas) != expected
            or set(partition.isrs) != expected
        ):
            return False
    return True


def change_replication_factor(
    admin: AdminClient,
    topic: str,
    replication_factor: int,
    bootstrap_servers: str,
    kafka_bin: Path,
    timeout: float,
) -> None:
    """Change replication using Kafka's partition reassignment tool."""
    if replication_factor < 1:
        raise ValueError("Replication factor must be at least 1.")

    assignment = build_replica_assignment(
        admin, topic, replication_factor, timeout
    )
    if not assignment:
        print(
            f"Topic {topic!r} already has replication factor "
            f"{replication_factor}."
        )
        return

    plan = {"version": 1, "partitions": assignment}
    with tempfile.TemporaryDirectory(
        prefix="kafka-reassignment-"
    ) as directory:
        plan_path = Path(directory) / "reassignment.json"
        plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")
        command = [
            str(kafka_bin / "kafka-reassign-partitions"),
            "--command-config",
            kafka_command_config(),
            "--bootstrap-server",
            bootstrap_servers,
            "--execute",
            "--reassignment-json-file",
            str(plan_path),
        ]
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.stdout:
            print(result.stdout.rstrip())
        if result.stderr:
            print(result.stderr.rstrip(), file=sys.stderr)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if replicas_are_ready(admin, topic, assignment, timeout):
                print(
                    f"Replication factor {replication_factor} is active "
                    f"for topic {topic!r}."
                )
                return
            print("Waiting for reassigned replicas to become in-sync...")
            time.sleep(min(5, max(0, deadline - time.monotonic())))

    raise TimeoutError(
        f"Replication reassignment for {topic!r} did not finish within "
        f"{timeout:g} seconds. The Kafka reassignment may still be running."
    )


def delete_topic(admin: AdminClient, topic: str, timeout: float) -> None:
    """Delete a topic after the caller confirms the destructive operation."""
    wait_for_future(
        admin.delete_topics([topic], request_timeout=timeout)[topic], timeout
    )
    print(f"Deleted topic {topic!r}.")


def build_parser() -> argparse.ArgumentParser:
    """Build CLI commands that correspond to the tutorial operations."""
    parser = argparse.ArgumentParser(
        description="List and manage topics in a Confluent Kafka cluster."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "CONFLUENT_BOOTSTRAP_SERVERS", "sa01:9092,sa02:9092,sa03:9092"
        ),
    )
    parser.add_argument(
        "--kafka-bin",
        type=Path,
        default=Path(
            os.environ.get("CONFLUENT_BIN_DIR", "/app/confluent/bin")
        ),
    )
    parser.add_argument("--timeout", type=float, default=600)
    subparsers = parser.add_subparsers(dest="operation", required=True)

    subparsers.add_parser("list", help="List topics")

    create_parser = subparsers.add_parser("create", help="Create a topic")
    create_parser.add_argument("topic")
    create_parser.add_argument("--partitions", type=int, required=True)
    create_parser.add_argument("--replication-factor", type=int, required=True)

    describe_parser = subparsers.add_parser(
        "describe", help="Show topic partitions and properties"
    )
    describe_parser.add_argument("topic")

    config_parser = subparsers.add_parser(
        "config", help="Set or reset topic properties"
    )
    config_parser.add_argument("topic")
    config_parser.add_argument(
        "--set", action="append", default=[], metavar="KEY=VALUE"
    )
    config_parser.add_argument(
        "--delete", action="append", default=[], metavar="KEY"
    )

    partitions_parser = subparsers.add_parser(
        "partitions", help="Increase a topic's partition count"
    )
    partitions_parser.add_argument("topic")
    partitions_parser.add_argument("--count", type=int, required=True)

    replication_parser = subparsers.add_parser(
        "replication-factor", help="Change a topic's replica count"
    )
    replication_parser.add_argument("topic")
    replication_parser.add_argument("--count", type=int, required=True)

    delete_parser = subparsers.add_parser("delete", help="Delete a topic")
    delete_parser.add_argument("topic")
    delete_parser.add_argument(
        "--yes", action="store_true", help="Confirm permanent topic deletion"
    )
    return parser


def main() -> int:
    """Connect to Kafka and dispatch the requested topic operation."""
    parser = build_parser()
    args = parser.parse_args()
    admin = AdminClient(kafka_client_config(args.bootstrap_servers))

    try:
        if args.operation == "list":
            list_topics(admin, args.timeout)
        elif args.operation == "create":
            if args.partitions < 1 or args.replication_factor < 1:
                raise ValueError(
                    "Partitions and replication factor must be positive."
                )
            create_topic(
                admin,
                args.topic,
                args.partitions,
                args.replication_factor,
                args.timeout,
            )
        elif args.operation == "describe":
            describe_topic(admin, args.topic, args.timeout)
        elif args.operation == "config":
            alter_topic_config(
                admin,
                args.topic,
                args.set,
                args.delete,
                args.timeout,
            )
        elif args.operation == "partitions":
            if args.count < 1:
                raise ValueError("Partition count must be positive.")
            change_partition_count(admin, args.topic, args.count, args.timeout)
        elif args.operation == "replication-factor":
            change_replication_factor(
                admin,
                args.topic,
                args.count,
                args.bootstrap_servers,
                args.kafka_bin,
                args.timeout,
            )
        elif args.operation == "delete":
            if not args.yes:
                raise ValueError(
                    "Deleting a topic is permanent; "
                    "repeat the command with --yes."
                )
            delete_topic(admin, args.topic, args.timeout)
        return 0
    except (
        KafkaException,
        OSError,
        subprocess.SubprocessError,
        TimeoutError,
        ValueError,
        RuntimeError,
    ) as error:
        print(f"Topic operation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
