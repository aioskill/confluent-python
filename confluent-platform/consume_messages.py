#!/usr/bin/env python3
"""Consume messages from a Confluent Kafka topic."""

from __future__ import annotations

import argparse
import os
import sys
import time

from confluent_kafka import Consumer, KafkaException

from client_config import kafka_client_config


def parse_args() -> argparse.Namespace:
    """Read the topic, consumer group, and bounded polling settings."""
    parser = argparse.ArgumentParser(
        description="Consume messages from a Kafka topic."
    )
    parser.add_argument("topic", help="Topic from which to read messages")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "CONFLUENT_BOOTSTRAP_SERVERS", "sa01:9092,sa02:9092,sa03:9092"
        ),
        help="Kafka bootstrap endpoints.",
    )
    parser.add_argument(
        "--group",
        help="Consumer group ID (default: tutorial-<topic>).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Stop after this many messages (default: 10).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="Stop polling after this many seconds (default: 30).",
    )
    return parser.parse_args()


def main() -> int:
    """Subscribe, display messages, and commit offsets when finished."""
    args = parse_args()
    if args.count < 1:
        print("--count must be at least 1.", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("--timeout must be greater than zero.", file=sys.stderr)
        return 2

    group_id = args.group or f"tutorial-{args.topic}"
    consumer = Consumer(
        {
            **kafka_client_config(args.bootstrap_servers),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    received = 0
    deadline = time.monotonic() + args.timeout

    try:
        consumer.subscribe([args.topic])
        print(
            f"Waiting for up to {args.count} message(s) from {args.topic!r} "
            f"as group {group_id!r}."
        )

        while received < args.count and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            message = consumer.poll(min(1.0, max(0.0, remaining)))
            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())

            value = message.value().decode("utf-8", errors="replace")
            key = message.key()
            decoded_key = (
                key.decode("utf-8", errors="replace") if key else None
            )
            print(
                f"topic={message.topic()} partition={message.partition()} "
                f"offset={message.offset()} key={decoded_key!r} "
                f"value={value!r}"
            )
            received += 1

        if received:
            committed_offsets = consumer.commit(asynchronous=False)
            commit_errors = [
                partition.error
                for partition in committed_offsets or []
                if partition.error is not None
            ]
            if commit_errors:
                raise RuntimeError(
                    "Could not commit consumer offsets: "
                    + ", ".join(str(error) for error in commit_errors)
                )
    except (KafkaException, RuntimeError) as error:
        print(f"Consumer failed: {error}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    if received == 0:
        print(
            f"No messages arrived on {args.topic!r} within "
            f"{args.timeout:g} seconds.",
            file=sys.stderr,
        )
        return 1

    print(f"Consumed {received} message(s) from {args.topic!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
