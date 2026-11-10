#!/usr/bin/env python3
"""Produce sample messages to an existing Confluent Kafka topic.

Create the topic first with manage_topics.py. The producer uses acknowledgments
from all in-sync replicas and idempotence to avoid duplicate writes on retry.
"""

from __future__ import annotations

import argparse
import os
import sys

from confluent_kafka import KafkaException, Producer

from client_config import kafka_client_config


def parse_args() -> argparse.Namespace:
    """Read the target topic and producer settings."""
    parser = argparse.ArgumentParser(
        description="Produce tutorial messages to a Kafka topic."
    )
    parser.add_argument("topic", help="Existing topic to receive messages")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get(
            "CONFLUENT_BOOTSTRAP_SERVERS", "sa01:9092,sa02:9092,sa03:9092"
        ),
        help="Kafka bootstrap endpoints.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of sample messages to produce (default: 10).",
    )
    parser.add_argument(
        "--message",
        action="append",
        dest="messages",
        help="Message value to send; may be supplied multiple times.",
    )
    parser.add_argument(
        "--key-prefix",
        help=(
            "Optional key prefix; messages receive keys "
            "prefix-1, prefix-2, ..."
        ),
    )
    return parser.parse_args()


def main() -> int:
    """Send messages and wait for broker delivery reports."""
    args = parse_args()
    if args.count < 1:
        print("--count must be at least 1.", file=sys.stderr)
        return 2

    messages = args.messages or [
        f"tutorial message {number}" for number in range(1, args.count + 1)
    ]
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
            f"Delivered topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )

    try:
        for number, value in enumerate(messages, start=1):
            key = (
                f"{args.key_prefix}-{number}".encode("utf-8")
                if args.key_prefix
                else None
            )
            producer.produce(
                args.topic,
                key=key,
                value=value.encode("utf-8"),
                on_delivery=on_delivery,
            )
            # Serve delivery callbacks while messages send asynchronously.
            producer.poll(0)

        remaining = producer.flush(30)
        if remaining:
            print(
                f"Timed out with {remaining} message(s) not delivered.",
                file=sys.stderr,
            )
            return 1
    except (KafkaException, BufferError, RuntimeError) as error:
        producer.flush(5)
        print(f"Could not produce messages: {error}", file=sys.stderr)
        return 1

    if delivery_errors:
        print(
            f"Delivered {delivered}/{len(messages)} message(s); "
            f"{len(delivery_errors)} delivery error(s).",
            file=sys.stderr,
        )
        return 1

    print(f"Successfully delivered {delivered} message(s) to {args.topic!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
