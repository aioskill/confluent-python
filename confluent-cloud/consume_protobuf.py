#!/usr/bin/env python3
"""Consume Protocol Buffer records and resolve their schemas from Registry."""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from client_config import (
    default_bootstrap_servers,
    default_schema_registry_url,
    kafka_client_config,
    schema_registry_config,
)

import order_pb2


def parse_args() -> argparse.Namespace:
    """Read Kafka, Schema Registry, and bounded consumer settings."""
    parser = argparse.ArgumentParser(
        description="Consume Protobuf records from a Kafka topic."
    )
    parser.add_argument("topic", help="Topic containing Protobuf records")
    parser.add_argument(
        "--bootstrap-servers",
        default=default_bootstrap_servers(),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=default_schema_registry_url(),
    )
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=30)
    return parser.parse_args()


def main() -> int:
    """Read and deserialize records, then commit their consumed offsets."""
    args = parse_args()
    if args.count < 1 or args.timeout <= 0:
        print("--count and --timeout must be positive.", file=sys.stderr)
        return 2

    schema_registry = SchemaRegistryClient(
        schema_registry_config(args.schema_registry_url)
    )
    deserializer = ProtobufDeserializer(
        order_pb2.Order, schema_registry_client=schema_registry
    )
    consumer = Consumer(
        {
            **kafka_client_config(args.bootstrap_servers),
            "group.id": f"protobuf-tutorial-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    received = 0
    deadline = time.monotonic() + args.timeout

    try:
        consumer.subscribe([args.topic])
        while received < args.count and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            message = consumer.poll(min(1.0, max(0.0, remaining)))
            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())

            context = SerializationContext(
                message.topic(), MessageField.VALUE, message.headers()
            )
            record = deserializer(message.value(), context)
            key_bytes = message.key()
            key = key_bytes.decode("utf-8") if key_bytes else None
            print(
                f"key={key} "
                f"partition={message.partition()} offset={message.offset()} "
                f"value={json.dumps({
                    'order_id': record.order_id,
                    'customer_id': record.customer_id,
                    'amount': record.amount,
                    'status': record.status,
                }, sort_keys=True)}"
            )
            received += 1

        if received:
            consumer.commit(asynchronous=False)
    except Exception as error:
        print(f"Could not consume Protobuf records: {error}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    if received == 0:
        print(
            f"No records arrived on {args.topic!r} within "
            f"{args.timeout:g} seconds.",
            file=sys.stderr,
        )
        return 1
    print(f"Consumed {received} Protobuf record(s) from {args.topic!r}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
