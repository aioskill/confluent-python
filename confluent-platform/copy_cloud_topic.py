#!/usr/bin/env python3
"""Copy a Confluent Cloud topic to a Confluent Platform topic (text, Avro, Protobuf).

The script reads every record that is in the Cloud topic when it starts, and
writes it to the Platform topic. Keys, headers and timestamps are kept.

  --format text      keys and values are copied as they are, after checking that
                     they are valid UTF-8 (plain text, JSON, CSV, ...).
  --format avro      values are in the Confluent Avro wire format.
  --format protobuf  values are in the Confluent Protobuf wire format.

For Avro and Protobuf the two clusters have separate Schema Registries, so the
same schema has a different ID in each. For every schema ID found in the Cloud
topic, the script registers that schema (and the schemas it references) in the
Platform Schema Registry under "<platform-topic>-value", then replaces the 4-byte
schema ID at the start of the value. The serialized payload is copied unchanged,
so nothing is decoded and no generated classes are needed.

How Schema Registry is handled (Avro and Protobuf only; text does not use it)
-----------------------------------------------------------------------------
Two registries are involved:

  * source, Confluent Cloud: SCHEMA_REGISTRY_ENDPOINT, SCHEMA_REGISTRY_KEY and
    SCHEMA_REGISTRY_SECRET from the Cloud .env file. It is only read.
  * target, Confluent Platform: --target-schema-registry-url (default
    http://sa01:8081, no authentication). Schemas are registered here.

Why the schema ID must change: a value in the Confluent wire format starts with a
magic byte 0 and a 4-byte schema ID. The ID only has a meaning in the registry that
issued it, so the same schema can be ID 100001 in Cloud and ID 2 on Platform.
Copying the bytes unchanged would make Platform consumers fetch the wrong schema.

For every record:
  1. read the schema ID from bytes 1-4 of the value;
  2. if the ID is new, fetch the schema from the Cloud registry (with its type,
     AVRO or PROTOBUF, and its references);
  3. register it in the Platform registry under "<target-topic>-value". The registry
     returns the Platform ID, or the existing ID when the identical schema is already
     registered there;
  4. cache the Cloud-to-Platform ID mapping, so each schema is looked up only once;
  5. write the Platform ID into bytes 1-4. The rest of the value is copied unchanged
     (for Protobuf that includes the message-index list after the ID).

References: when a schema references another one (for example a Protobuf import),
the referenced schema is registered first under its own subject, and the main schema
points to the version number the Platform registry assigned to it.

Limits:
  * Subjects always use the topic name strategy ("<topic>-value"). Records written
    with another strategy (for example record name) land under the wrong subject.
  * Only values are remapped. Avro or Protobuf keys are copied as raw bytes, so they
    keep their Cloud schema IDs.
  * The Platform registry's compatibility setting applies. A schema that conflicts
    with one already under the subject makes the copy fail with an error.

Cloud settings come from ./confluent-cloud/.env
(BOOTSTRAP_SERVERS, KEY, SECRET, SCHEMA_REGISTRY_ENDPOINT, SCHEMA_REGISTRY_KEY,
SCHEMA_REGISTRY_SECRET). Platform settings come from CONFLUENT_SASL_PASSWORD or
/app/confluent/etc/kafka/client.properties, as in the other scripts here.

Run again and the records are copied again (the Platform topic then has duplicates);
delete and re-create the Platform topic to start over.

Create the Platform topic first (single partition)::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties \\
        --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \\
        --create --topic orders-copy --partitions 1 --replication-factor 3

Examples::

    python3 copy_cloud_topic.py --format text     --source-topic messages --target-topic messages-copy
    python3 copy_cloud_topic.py --format avro     --source-topic avro-orders --target-topic avro-orders-copy
    python3 copy_cloud_topic.py --format protobuf --source-topic protobuf-orders --target-topic protobuf-orders-copy

Check the result::

    python3 consume_avro.py avro-orders-copy --count 5
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING, Producer, TopicPartition
from confluent_kafka.schema_registry import Schema, SchemaReference, SchemaRegistryClient

import client_config as platform_config

CLOUD_CONFIG = Path(__file__).resolve().parent.parent / "confluent-cloud" / "client_config.py"
MAGIC_BYTE = 0


def load_cloud_config():
    """Load confluent-cloud/client_config.py (it has the same module name as ours)."""
    spec = importlib.util.spec_from_file_location("cloud_client_config", CLOUD_CONFIG)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy a Confluent Cloud topic to a Confluent Platform topic."
    )
    parser.add_argument("--format", choices=["text", "avro", "protobuf"], required=True)
    parser.add_argument("--source-topic", required=True, help="Topic in Confluent Cloud")
    parser.add_argument("--target-topic", required=True, help="Existing topic on Confluent Platform")
    parser.add_argument(
        "--target-bootstrap-servers",
        default=os.environ.get("CONFLUENT_BOOTSTRAP_SERVERS", "sa01:9092,sa02:9092,sa03:9092"),
    )
    parser.add_argument(
        "--target-schema-registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://sa01:8081"),
    )
    parser.add_argument("--timeout", type=float, default=30, help="seconds to wait for a record")
    return parser.parse_args()


class SchemaIdMapper:
    """Maps a Cloud schema ID to the ID of the same schema in the Platform registry."""

    def __init__(self, source, target, target_subject):
        self.source = source
        self.target = target
        self.target_subject = target_subject
        self.ids = {}

    def map(self, source_id: int) -> int:
        if source_id not in self.ids:
            schema = self.source.get_schema(source_id)
            self.ids[source_id] = self.target.register_schema(
                self.target_subject, self.copy_schema(schema)
            )
            print(f"Schema {schema.schema_type} id {source_id} (Cloud) -> id "
                  f"{self.ids[source_id]} (Platform, subject {self.target_subject})", flush=True)
        return self.ids[source_id]

    def copy_schema(self, schema: Schema) -> Schema:
        """Register the referenced schemas first, under their own subjects."""
        references = [
            SchemaReference(ref.name, ref.subject, self.register_reference(ref))
            for ref in schema.references or []
        ]
        return Schema(schema.schema_str, schema.schema_type, references)

    def register_reference(self, ref: SchemaReference) -> int:
        """Register one referenced schema in the Platform registry; return its version."""
        version = self.source.get_version(ref.subject, ref.version)
        copied = self.copy_schema(version.schema)
        self.target.register_schema(ref.subject, copied)
        return self.target.lookup_schema(ref.subject, copied).version


def rewrite_value(value: bytes, mapper: SchemaIdMapper) -> bytes:
    """Replace the schema ID in the Confluent wire format header."""
    if len(value) < 5 or value[0] != MAGIC_BYTE:
        raise ValueError("value is not in the Confluent wire format (magic byte 0 + schema ID)")
    new_id = mapper.map(int.from_bytes(value[1:5], "big"))
    return bytes([MAGIC_BYTE]) + new_id.to_bytes(4, "big") + value[5:]


def convert(args, mapper, record):
    """Return (key, value) bytes for the Platform topic."""
    key, value = record.key(), record.value()
    if value is None:  # tombstone
        return key, None
    if args.format == "text":
        value.decode("utf-8")
        if key is not None:
            key.decode("utf-8")
        return key, value
    return key, rewrite_value(value, mapper)


def main() -> int:
    args = parse_args()
    cloud = load_cloud_config()

    consumer = Consumer({
        **cloud.kafka_client_config(),
        "group.id": f"copy-{args.source_topic}-to-platform",  # offsets are not committed
        "enable.auto.commit": False,
    })
    producer = Producer({
        **platform_config.kafka_client_config(args.target_bootstrap_servers),
        "acks": "all",
        "enable.idempotence": True,
    })
    mapper = None
    if args.format != "text":
        mapper = SchemaIdMapper(
            SchemaRegistryClient(cloud.schema_registry_config()),
            SchemaRegistryClient({"url": args.target_schema_registry_url}),
            f"{args.target_topic}-value",
        )

    # Snapshot the end of every partition, then read from the start up to that point.
    metadata = consumer.list_topics(args.source_topic, timeout=30)
    if metadata.topics[args.source_topic].error is not None:
        print(f"Cloud topic {args.source_topic} not found: "
              f"{metadata.topics[args.source_topic].error}", file=sys.stderr)
        return 1
    ends = {}
    for partition in metadata.topics[args.source_topic].partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(args.source_topic, partition), timeout=30
        )
        if high > low:
            ends[partition] = high
    if not ends:
        print(f"{args.source_topic} is empty; nothing to copy.")
        return 0
    consumer.assign([TopicPartition(args.source_topic, p, OFFSET_BEGINNING) for p in ends])

    copied = 0
    errors = []

    def on_delivery(error, message) -> None:
        if error is not None:
            errors.append(str(error))

    deadline = time.monotonic() + args.timeout
    try:
        while ends:
            record = consumer.poll(1.0)
            if record is None:
                if time.monotonic() > deadline:
                    print("Timed out waiting for records.", file=sys.stderr)
                    return 1
                continue
            if record.error():
                if record.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {record.error()}", file=sys.stderr)
                return 1
            deadline = time.monotonic() + args.timeout
            if record.offset() < ends.get(record.partition(), 0):
                key, value = convert(args, mapper, record)
                producer.produce(
                    args.target_topic, key=key, value=value,
                    headers=record.headers() or None,
                    timestamp=record.timestamp()[1], on_delivery=on_delivery,
                )
                producer.poll(0)
                copied += 1
            if record.offset() + 1 >= ends.get(record.partition(), 0):
                ends.pop(record.partition(), None)
    except ValueError as error:
        print(f"Cannot copy as {args.format}: {error}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    producer.flush(60)
    if errors:
        print(f"{len(errors)} records were not delivered: {errors[0]}", file=sys.stderr)
        return 1
    print(f"Copied {copied} {args.format} records from Cloud {args.source_topic} "
          f"to Platform {args.target_topic}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
