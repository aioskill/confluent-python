#!/usr/bin/env python3
"""Proof of concept: Avro, Protobuf and JSON Schema records in ONE topic.

  python mixed_formats_poc.py produce <topic>   # 2 records of each format
  python mixed_formats_poc.py consume <topic>   # read them all back
  python mixed_formats_poc.py default <topic>   # show what the default naming strategy does

Each format uses a different record name (OrderAvro, Order, OrderJson), and the
serializers use the TopicRecordNameStrategy, so every record type registers its own
subject, "<topic>-<record name>", instead of sharing "<topic>-value".
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    topic_record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from client_config import kafka_client_config, schema_registry_config

import order_pb2

HERE = Path(__file__).parent
STRATEGY = {"subject.name.strategy": topic_record_subject_name_strategy}


def avro_schema() -> str:
    schema = json.loads((HERE / "order.avsc").read_text())
    schema["name"] = "OrderAvro"          # a distinct record name for this format
    return json.dumps(schema)


def json_schema() -> str:
    schema = json.loads((HERE / "order.schema.json").read_text())
    schema["title"] = "OrderJson"         # JSON Schema serializers use the title as record name
    return json.dumps(schema)


def order(n: int, status: str = "CREATED") -> dict:
    return {"order_id": f"order-{n}", "customer_id": f"customer-{n % 3 + 1}",
            "amount": n * 12.5, "status": status}


def produce(topic: str, default_strategy: bool = False) -> int:
    sr = SchemaRegistryClient(schema_registry_config())
    conf = {} if default_strategy else STRATEGY
    avro = AvroSerializer(sr, avro_schema(), conf=conf)
    proto = ProtobufSerializer(order_pb2.Order, sr, conf={"use.deprecated.format": False, **conf})
    jsons = JSONSerializer(json_schema(), sr, conf=conf)
    ctx = SerializationContext(topic, MessageField.VALUE)

    producer = Producer(kafka_client_config())
    errors = []
    def done(err, msg):
        if err:
            errors.append(err)

    for n in (1, 2):
        producer.produce(topic, key=f"avro-{n}", value=avro(order(n), ctx), on_delivery=done)
        pb = order_pb2.Order(**order(n + 10))
        producer.produce(topic, key=f"proto-{n}", value=proto(pb, ctx), on_delivery=done)
        producer.produce(topic, key=f"json-{n}", value=jsons(order(n + 20), ctx), on_delivery=done)
    producer.flush(30)
    print("delivery errors:", errors or "none")
    print("subjects:", sorted(s for s in sr.get_subjects() if s.startswith(topic)))
    return 1 if errors else 0


def consume(topic: str) -> int:
    sr = SchemaRegistryClient(schema_registry_config())
    avro = AvroDeserializer(sr)
    proto = ProtobufDeserializer(order_pb2.Order, {"use.deprecated.format": False})
    json_decoders = {}                     # schema id -> JSONDeserializer (needs the schema text)

    consumer = Consumer({**kafka_client_config(), "group.id": "mixed-formats-poc-reader",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    ctx = SerializationContext(topic, MessageField.VALUE)
    seen, idle = 0, 0
    try:
        while idle < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            idle = 0
            if msg.error():
                print("error:", msg.error())
                continue
            raw = msg.value()
            # Confluent wire format: magic byte 0, then a 4-byte schema ID.
            schema_id = int.from_bytes(raw[1:5], "big")
            kind = sr.get_schema(schema_id).schema_type or "AVRO"
            if kind == "AVRO":
                value = avro(raw, ctx)
            elif kind == "PROTOBUF":
                value = proto(raw, ctx)
            else:
                if schema_id not in json_decoders:
                    json_decoders[schema_id] = JSONDeserializer(sr.get_schema(schema_id).schema_str)
                value = json_decoders[schema_id](raw, ctx)
            seen += 1
            print(f"offset={msg.offset()} key={msg.key().decode():8} schema_id={schema_id} "
                  f"format={kind:8} value={value if isinstance(value, dict) else dict(order_fields(value))}")
    finally:
        consumer.close()
    print(f"read {seen} records")
    return 0 if seen else 1


def order_fields(pb):
    return [(f.name, getattr(pb, f.name)) for f in pb.DESCRIPTOR.fields]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("action", choices=["produce", "consume", "default"])
    parser.add_argument("topic")
    args = parser.parse_args()
    try:
        if args.action == "produce":
            return produce(args.topic)
        if args.action == "consume":
            return consume(args.topic)
        return produce(args.topic, default_strategy=True)
    except (KafkaException, RuntimeError, OSError) as error:
        print(f"Failed: {error}", file=sys.stderr)
        return 1
    except Exception as error:      # schema registry / serialization errors
        print(f"Failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
