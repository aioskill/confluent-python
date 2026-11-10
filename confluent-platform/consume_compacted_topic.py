#!/usr/bin/env python3
"""Rebuild a compacted topic's current key/value state by consuming its log."""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException

from client_config import kafka_client_config


def parse_args() -> argparse.Namespace:
    """Read the topic and consumer polling limits."""
    parser = argparse.ArgumentParser(
        description=(
            "Read a compacted topic and fold records into current state."
        )
    )
    parser.add_argument("topic", help="Existing compacted topic")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "CONFLUENT_BOOTSTRAP_SERVERS", "sa01:9092,sa02:9092,sa03:9092"
        ),
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=5,
        help="Stop after this many quiet seconds (default: 5).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60,
        help="Maximum total read time in seconds (default: 60).",
    )
    return parser.parse_args()


def main() -> int:
    """Consume from the earliest offset and apply updates and tombstones."""
    args = parse_args()
    if args.idle_timeout <= 0 or args.timeout <= 0:
        print("Timeout values must be greater than zero.", file=sys.stderr)
        return 2

    # A fresh group ensures this state rebuild starts at the beginning on each
    # run, regardless of offsets committed by earlier tutorial invocations.
    group_id = f"compacted-state-{uuid.uuid4().hex}"
    consumer = Consumer(
        {
            **kafka_client_config(args.bootstrap_servers),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    state = {}
    records_read = 0
    started = time.monotonic()
    deadline = started + args.timeout
    idle_deadline = None

    try:
        consumer.subscribe([args.topic])
        print(
            f"Reading compacted topic {args.topic!r} "
            "from its earliest offset."
        )
        while time.monotonic() < deadline:
            now = time.monotonic()
            if idle_deadline is not None and now >= idle_deadline:
                break

            remaining = deadline - now
            poll_time = min(1.0, remaining)
            if idle_deadline is not None:
                poll_time = min(poll_time, max(0.0, idle_deadline - now))
            message = consumer.poll(poll_time)

            if consumer.assignment() and idle_deadline is None:
                idle_deadline = time.monotonic() + args.idle_timeout
            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())

            key_bytes = message.key()
            if key_bytes is None:
                raise RuntimeError(
                    "Compacted-topic records must have a non-null key."
                )
            key = key_bytes.decode("utf-8", errors="replace")
            value_bytes = message.value()
            if value_bytes is None:
                # A null value is a tombstone: remove the key from state.
                state.pop(key, None)
                print(f"Tombstone key={key!r}")
            else:
                value = value_bytes.decode("utf-8", errors="replace")
                state[key] = value
                print(
                    f"Update key={key!r} value={value!r} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )
            records_read += 1
            idle_deadline = time.monotonic() + args.idle_timeout
    except (KafkaException, RuntimeError) as error:
        print(f"Could not consume compacted topic: {error}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    if time.monotonic() >= deadline:
        print(
            "Stopped at the total timeout; the topic may have more records.",
            file=sys.stderr,
        )

    print(f"\nRead {records_read} record(s). Current key/value state:")
    if not state:
        print("  <empty>")
    else:
        for key, value in sorted(state.items()):
            print(f"  {key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
