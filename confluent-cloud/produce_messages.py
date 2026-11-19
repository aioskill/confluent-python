#!/usr/bin/env python3
"""Produce sample messages to an existing Confluent Kafka topic.

Create the topic first with manage_topics.py. The producer uses acknowledgments
from all in-sync replicas and idempotence to avoid duplicate writes on retry.
"""

from __future__ import annotations

import argparse
import itertools
import signal
import sys
import threading

from confluent_kafka import KafkaException, Producer

from client_config import default_bootstrap_servers, kafka_client_config


def parse_args() -> argparse.Namespace:
    """Read the target topic and producer settings."""
    parser = argparse.ArgumentParser(
        description="Produce tutorial messages to a Kafka topic."
    )
    parser.add_argument("topic", help="Existing topic to receive messages")
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap_servers(),
        help="Kafka bootstrap endpoints.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of records to produce (default: 100; -1 = no limit, stop with Ctrl+C).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        help=(
            "Seconds to wait between records (default: 0, or 0.1 when --count is -1)."
        ),
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


def record_numbers(count: int, interval: float, stop: threading.Event):
    """Yield 1, 2, 3, ... up to count (-1 = no limit) or until Ctrl+C sets stop."""
    numbers = itertools.count(1) if count == -1 else range(1, count + 1)
    for number in numbers:
        if stop.is_set():
            return
        yield number
        if interval and stop.wait(interval):
            return


def stop_on_ctrl_c() -> threading.Event:
    """Return an event that is set when the user presses Ctrl+C."""
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    return stop


def main() -> int:
    """Send messages and wait for broker delivery reports."""
    args = parse_args()
    if args.count < 1 and args.count != -1:
        print("--count must be at least 1, or -1 for no limit.", file=sys.stderr)
        return 2
    if args.interval is None:
        args.interval = 0.1 if args.count == -1 else 0.0
    stop = stop_on_ctrl_c()

    if args.messages and args.count == -1:
        values = itertools.cycle(args.messages)  # repeat the given messages
    elif args.messages:
        values = iter(args.messages)  # --count does not apply to explicit messages
    else:
        values = (f"tutorial message {n}" for n in itertools.count(1))
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
        limit = len(args.messages) if args.messages and args.count != -1 else args.count
        number = 0
        for number in record_numbers(limit, args.interval, stop):
            value = next(values)
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
            f"Delivered {delivered}/{number} message(s); "
            f"{len(delivery_errors)} delivery error(s).",
            file=sys.stderr,
        )
        return 1

    print(f"Successfully delivered {delivered} message(s) to {args.topic!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
