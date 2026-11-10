#!/usr/bin/env python3
"""Create and manage topics on Confluent Cloud using Kafka AdminClient."""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from confluent_kafka import KafkaError, KafkaException
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

from client_config import default_bootstrap_servers, kafka_client_config


def wait_for_future(future: Any, timeout: float) -> Any:
    """Wait for one AdminClient operation and return its result."""
    return future.result(timeout=timeout)


def topic_metadata(admin: AdminClient, topic: str, timeout: float) -> Any:
    """Fetch topic metadata, allowing Cloud time to publish new topics."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        metadata = admin.list_topics(
            topic=topic,
            timeout=min(10, max(1, deadline - time.monotonic())),
        )
        result = metadata.topics.get(topic)
        if result is not None:
            if result.error is not None:
                if result.error.code() in {
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                    KafkaError.LEADER_NOT_AVAILABLE,
                }:
                    time.sleep(1)
                    continue
                raise RuntimeError(
                    f"Could not read topic {topic!r}: {result.error}"
                )
            if result.partitions:
                return result
        time.sleep(1)
    raise RuntimeError(
        f"Topic {topic!r} was not visible after {timeout:g} seconds. "
        "Check topic DESCRIBE permissions and Cloud propagation."
    )


def list_topics(admin: AdminClient, timeout: float) -> None:
    """Print topic names from cluster metadata."""
    metadata = admin.list_topics(timeout=timeout)
    print("Topics:")
    for name in sorted(metadata.topics):
        print(f"  {name}")


def describe_topic(admin: AdminClient, topic: str, timeout: float) -> None:
    """Show partitions, replicas, ISR, and effective topic properties."""
    metadata = topic_metadata(admin, topic, timeout)
    print(f"Topic: {topic}")
    print("Partitions:")
    for partition_id, partition in sorted(metadata.partitions.items()):
        print(
            f"  {partition_id}: leader={partition.leader}, "
            f"replicas={partition.replicas}, isr={partition.isrs}"
        )

    resource = ConfigResource(ResourceType.TOPIC, topic)
    future = admin.describe_configs([resource])[resource]
    configs = wait_for_future(future, timeout)
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
    """Create a topic; Cloud manages replication at a fixed factor of three."""
    if replication_factor != 3:
        raise ValueError(
            "Confluent Cloud fixes topic replication factor at 3; "
            "the requested factor cannot be set."
        )
    new_topic = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=3,
    )
    future = admin.create_topics([new_topic], request_timeout=timeout)[topic]
    wait_for_future(future, timeout)
    topic_metadata(admin, topic, timeout)
    print(
        f"Created topic {topic!r} with {partitions} partition(s); "
        "Confluent Cloud replication factor is 3."
    )


def parse_config_values(values: list[str]) -> dict[str, str]:
    """Parse repeated KEY=VALUE options."""
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
    """Set or reset topic configs supported by the Cloud cluster type."""
    set_configs = parse_config_values(set_values)
    if set(set_configs).intersection(delete_values):
        raise ValueError("A property cannot be set and deleted together.")
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
    """Increase the partition count; Kafka does not support reducing it."""
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
    future = admin.create_partitions(
        [new_partitions], request_timeout=timeout
    )[topic]
    wait_for_future(future, timeout)
    print(f"Increased topic {topic!r} to {new_count} partitions.")


def check_replication_factor(
    admin: AdminClient,
    topic: str,
    replication_factor: int,
    timeout: float,
) -> None:
    """Report Cloud's fixed RF of three; do not attempt reassignment."""
    if replication_factor != 3:
        raise ValueError(
            "Confluent Cloud does not allow changing topic replication "
            "factor; it is fixed at 3."
        )
    metadata = topic_metadata(admin, topic, timeout)
    incorrect = [
        partition_id
        for partition_id, partition in metadata.partitions.items()
        if len(partition.replicas) != 3
    ]
    if incorrect:
        raise RuntimeError(
            "Cloud metadata did not report replication factor 3 for "
            f"partition(s): {incorrect}."
        )
    print(f"Topic {topic!r} uses Confluent Cloud replication factor 3.")


def delete_topic(admin: AdminClient, topic: str, timeout: float) -> None:
    """Delete a topic after explicit confirmation from the caller."""
    future = admin.delete_topics([topic], request_timeout=timeout)[topic]
    wait_for_future(future, timeout)
    print(f"Deleted topic {topic!r}.")


def build_parser() -> argparse.ArgumentParser:
    """Build the Cloud topic management command line."""
    parser = argparse.ArgumentParser(
        description="Manage topics in a Confluent Cloud Kafka cluster."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap_servers(),
    )
    parser.add_argument("--timeout", type=float, default=60)
    operations = parser.add_subparsers(dest="operation", required=True)
    operations.add_parser("list", help="List topics")

    create = operations.add_parser("create", help="Create a topic")
    create.add_argument("topic")
    create.add_argument("--partitions", type=int, required=True)
    create.add_argument("--replication-factor", type=int, default=3)

    describe = operations.add_parser("describe", help="Describe a topic")
    describe.add_argument("topic")

    config = operations.add_parser("config", help="Change topic properties")
    config.add_argument("topic")
    config.add_argument("--set", action="append", default=[])
    config.add_argument("--delete", action="append", default=[])

    partitions = operations.add_parser(
        "partitions", help="Increase a topic's partition count"
    )
    partitions.add_argument("topic")
    partitions.add_argument("--count", type=int, required=True)

    replication = operations.add_parser(
        "replication-factor",
        help="Check Cloud's fixed replication factor of three",
    )
    replication.add_argument("topic")
    replication.add_argument("--count", type=int, default=3)

    delete = operations.add_parser("delete", help="Delete a topic")
    delete.add_argument("topic")
    delete.add_argument("--yes", action="store_true")
    return parser


def main() -> int:
    """Connect to Cloud and run the selected topic operation."""
    args = build_parser().parse_args()
    try:
        admin = AdminClient(kafka_client_config(args.bootstrap_servers))
        if args.operation == "list":
            list_topics(admin, args.timeout)
        elif args.operation == "create":
            if args.partitions < 1:
                raise ValueError("Partition count must be positive.")
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
            check_replication_factor(
                admin, args.topic, args.count, args.timeout
            )
        elif args.operation == "delete":
            if not args.yes:
                raise ValueError(
                    "Topic deletion is permanent; repeat with --yes."
                )
            delete_topic(admin, args.topic, args.timeout)
        return 0
    except (
        KafkaException,
        OSError,
        TimeoutError,
        ValueError,
        RuntimeError,
    ) as error:
        print(f"Topic operation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
