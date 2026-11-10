#!/usr/bin/env python3
"""Produce keyed updates and tombstones to a compacted Kafka topic.

The topic must already exist with ``cleanup.policy=compact``. Compaction keeps
the latest value for each key, so updates for one key should use the same key
each time. A record with a key and a ``None`` value is a tombstone: after
compaction, Kafka removes earlier values for that key as well. Compaction runs
asynchronously, so producing a tombstone does not immediately erase old log
records.

With no ``--record`` or ``--delete-key`` options, this script sends two values
for ``account-1`` and then tombstones ``account-2``. The companion consumer
folds those records to show the current logical key/value state.
"""

from __future__ import annotations

import argparse
import sys

from confluent_kafka import KafkaException, Producer

from client_config import default_bootstrap_servers, kafka_client_config


def parse_record(record: str) -> tuple[str, str]:
    """Parse a KEY=VALUE record while allowing '=' in the value."""
    key, separator, value = record.partition("=")
    if not separator or not key:
        raise ValueError(f"Expected KEY=VALUE, got {record!r}.")
    return key, value


def parse_args() -> argparse.Namespace:
    """Read the target topic and keyed records from the command line."""
    parser = argparse.ArgumentParser(
        description="Produce updates and tombstones to a compacted topic."
    )
    parser.add_argument("topic", help="Existing compacted topic")
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap_servers(),
    )
    parser.add_argument(
        "--record",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Keyed update; repeat to send multiple values for a key.",
    )
    parser.add_argument(
        "--delete-key",
        action="append",
        default=[],
        metavar="KEY",
        help="Produce a tombstone for this key; may be repeated.",
    )
    return parser.parse_args()


def main() -> int:
    """Send sample updates and optional tombstones, then await delivery."""
    args = parse_args()
    try:
        records = [parse_record(record) for record in args.record]
        if not records and not args.delete_key:
            # Repeated keys demonstrate replacement; the tombstone removes the
            # current value for account-2 when consumers fold the topic log.
            records = [
                ("account-1", "created"),
                ("account-2", "active"),
                ("account-1", "updated"),
            ]
            args.delete_key = ["account-2"]
        if any(not key for key in args.delete_key):
            raise ValueError("Tombstone keys cannot be empty.")
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2

    producer = Producer(
        {
            **kafka_client_config(args.bootstrap_servers),
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    delivered = 0
    delivery_errors = []

    def on_delivery(error, message) -> None:
        nonlocal delivered
        if error is not None:
            delivery_errors.append(str(error))
            print(f"Delivery failed: {error}", file=sys.stderr)
            return
        delivered += 1
        print(
            f"Delivered key={message.key().decode('utf-8')!r} "
            f"partition={message.partition()} offset={message.offset()}"
        )

    try:
        for key, value in records:
            producer.produce(
                args.topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=on_delivery,
            )
            producer.poll(0)

        for key in args.delete_key:
            # Kafka interprets a null value on a keyed record as a tombstone.
            producer.produce(
                args.topic,
                key=key.encode("utf-8"),
                value=None,
                on_delivery=on_delivery,
            )
            producer.poll(0)

        pending = producer.flush(30)
        if pending:
            print(
                f"Timed out with {pending} message(s) not delivered.",
                file=sys.stderr,
            )
            return 1
    except (KafkaException, BufferError, RuntimeError) as error:
        producer.flush(5)
        print(
            f"Could not produce compacted-topic records: {error}",
            file=sys.stderr,
        )
        return 1

    if delivery_errors:
        print(
            f"Delivered {delivered} record(s) with "
            f"{len(delivery_errors)} delivery error(s).",
            file=sys.stderr,
        )
        return 1

    print(f"Delivered {delivered} keyed record(s) to {args.topic!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
