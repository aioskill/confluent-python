#!/usr/bin/env python3
"""Produce Avro records using Confluent Schema Registry."""

from __future__ import annotations

import argparse
import itertools
import json
import signal
import sys
import threading
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from client_config import (
    default_bootstrap_servers,
    default_schema_registry_url,
    kafka_client_config,
    schema_registry_config,
)


DEFAULT_SCHEMA = Path(__file__).with_name("order.avsc")


def parse_args() -> argparse.Namespace:
    """Read Kafka, Schema Registry, and sample message settings."""
    parser = argparse.ArgumentParser(
        description="Produce Avro records to an existing Kafka topic."
    )
    parser.add_argument("topic", help="Existing topic for the Avro records")
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap_servers(),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=default_schema_registry_url(),
    )
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
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
    return parser.parse_args()


def make_order(number: int) -> dict[str, str | float]:
    """Build one record matching the tutorial's Avro Order schema."""
    return {
        "order_id": f"order-{number:04d}",
        "customer_id": f"customer-{number % 3 + 1}",
        "amount": round(number * 12.5, 2),
        "status": "CREATED",
    }


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
    """Register the Avro schema and produce sample orders."""
    args = parse_args()
    if args.count < 1 and args.count != -1:
        print("--count must be at least 1, or -1 for no limit.", file=sys.stderr)
        return 2
    if args.interval is None:
        args.interval = 0.1 if args.count == -1 else 0.0
    stop = stop_on_ctrl_c()

    try:
        schema_text = args.schema.read_text(encoding="utf-8")
        json.loads(schema_text)
        schema_registry = SchemaRegistryClient(
            schema_registry_config(args.schema_registry_url)
        )
        serializer = AvroSerializer(schema_registry, schema_text)
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
                f"Delivered key={message.key().decode('utf-8')} "
                f"partition={message.partition()} offset={message.offset()}"
            )

        number = 0
        for number in record_numbers(args.count, args.interval, stop):
            order = make_order(number)
            context = SerializationContext(args.topic, MessageField.VALUE)
            serialized_value = serializer(order, context)
            producer.produce(
                args.topic,
                key=order["order_id"].encode("utf-8"),
                value=serialized_value,
                on_delivery=on_delivery,
            )
            producer.poll(0)

        pending = producer.flush(30)
        if pending:
            print(
                f"Timed out with {pending} record(s) not delivered.",
                file=sys.stderr,
            )
            return 1
        if delivery_errors:
            print(
                f"Delivered {delivered}/{number} records; "
                f"{len(delivery_errors)} delivery error(s).",
                file=sys.stderr,
            )
            return 1
    except (KafkaException, OSError, ValueError, RuntimeError) as error:
        print(f"Could not produce Avro records: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        print(f"Avro serialization failed: {error}", file=sys.stderr)
        return 1

    print(
        f"Successfully delivered {delivered} Avro record(s) to {args.topic!r}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
