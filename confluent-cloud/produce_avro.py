#!/usr/bin/env python3
"""Produce Avro records using Confluent Schema Registry."""

from __future__ import annotations

import argparse
import json
import sys
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
    parser.add_argument("--count", type=int, default=5)
    return parser.parse_args()


def make_order(number: int) -> dict[str, str | float]:
    """Build one record matching the tutorial's Avro Order schema."""
    return {
        "order_id": f"order-{number:04d}",
        "customer_id": f"customer-{number % 3 + 1}",
        "amount": round(number * 12.5, 2),
        "status": "CREATED",
    }


def main() -> int:
    """Register the Avro schema and produce sample orders."""
    args = parse_args()
    if args.count < 1:
        print("--count must be at least 1.", file=sys.stderr)
        return 2

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

        for number in range(1, args.count + 1):
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
                f"Delivered {delivered}/{args.count} records; "
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
