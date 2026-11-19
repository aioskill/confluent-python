"""Read Protobuf records written with Confluent Schema Registry in Flink SQL.

Flink has an ``avro-confluent`` format (see ``ex16_avro_schema_registry.py``) but no
Schema Registry-aware Protobuf format. A Protobuf message in the Confluent wire
format is: a magic byte (0), the 4-byte schema ID, the message-index list, then the
Protobuf payload. So this example:

1. Produces a few orders with the Confluent ``ProtobufSerializer``. It registers the
   ``com.example.confluent.Order`` schema (the same message as ``order.proto`` in the
   Confluent Platform tutorials) under the subject ``<topic>-value``.
2. Reads the topic in Flink SQL with the ``raw`` format (one BYTES column).
3. Decodes each message in a Python scalar UDF with the Confluent
   ``ProtobufDeserializer``, which looks the schema up in Schema Registry by the ID
   in the message, and returns a ROW.

The Order message class is built at run time from a descriptor, so there is no
generated ``order_pb2.py`` to keep in step with the installed ``protobuf`` version.

Each run produces 5 more orders, so a rerun shows more rows; delete the topic to start over.

Run locally (embedded Flink mini cluster, from this script's directory)::

    export CONFLUENT_SASL_PASSWORD='<password from /app/confluent/etc/kafka/client.properties>'
    python ex17_protobuf_schema_registry.py

Submit to a Flink cluster (the TaskManagers need ``confluent-kafka[protobuf]``;
ship the helper module with ``--pyFiles ../kafka_auth.py``)::

    flink run -m sa03:8967 -py ex17_protobuf_schema_registry.py --pyFiles ../kafka_auth.py

Create the topic first (single partition)::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic orders_protobuf --partitions 1 --replication-factor 1

Clean up (topic and schema)::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --delete --topic orders_protobuf
    curl -X DELETE http://sa01:8081/subjects/orders_protobuf-value
    curl -X DELETE 'http://sa01:8081/subjects/orders_protobuf-value?permanent=true'
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import confluent_kafka_properties, sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import ScalarFunction, udf


def order_message_class():
    """Build the Order message class (the equivalent of order.proto) at run time."""
    proto = descriptor_pb2.FileDescriptorProto()
    proto.name = "order.proto"
    proto.package = "com.example.confluent"
    proto.syntax = "proto3"
    message = proto.message_type.add()
    message.name = "Order"
    for number, (name, field_type) in enumerate(
        [
            ("order_id", descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
            ("customer_id", descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
            ("amount", descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE),
            ("status", descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ],
        start=1,
    ):
        field = message.field.add()
        field.name = name
        field.number = number
        field.type = field_type
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    pool = descriptor_pool.DescriptorPool()
    pool.Add(proto)
    return message_factory.GetMessageClass(
        pool.FindMessageTypeByName("com.example.confluent.Order")
    )


def produce_orders(args):
    """Step 1: write Protobuf orders; the serializer registers the schema."""
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    Order = order_message_class()
    registry = SchemaRegistryClient({"url": args.schema_registry_url})
    serializer = ProtobufSerializer(
        Order, registry, {"use.deprecated.format": False}
    )
    producer = Producer(
        {**confluent_kafka_properties(), "bootstrap.servers": args.bootstrap_servers}
    )
    orders = [
        ("order-0001", "customer-1", 12.5, "CREATED"),
        ("order-0002", "customer-2", 25.0, "CREATED"),
        ("order-0003", "customer-3", 37.5, "PAID"),
        ("order-0004", "customer-1", 50.0, "SHIPPED"),
        ("order-0005", "customer-2", 62.5, "DELIVERED"),
    ]
    context = SerializationContext(args.topic, MessageField.VALUE)
    for order_id, customer_id, amount, status in orders:
        order = Order(
            order_id=order_id, customer_id=customer_id, amount=amount, status=status
        )
        producer.produce(args.topic, value=serializer(order, context))
    producer.flush(30)
    print(f"Produced {len(orders)} Protobuf orders to {args.topic}", flush=True)


class DecodeOrder(ScalarFunction):
    """Decode one Confluent-framed Protobuf message into a ROW."""

    def __init__(self, topic, schema_registry_url):
        self.topic = topic
        self.schema_registry_url = schema_registry_url

    def open(self, function_context):
        # Created on the TaskManager, once per parallel instance of the UDF.
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        self.context = SerializationContext(self.topic, MessageField.VALUE)
        self.deserializer = ProtobufDeserializer(
            order_message_class(),
            {"use.deprecated.format": False},
            SchemaRegistryClient({"url": self.schema_registry_url}),
        )

    def eval(self, payload):
        if payload is None:
            return None
        order = self.deserializer(payload, self.context)
        schema_id = int.from_bytes(payload[1:5], "big")  # bytes 1-4 of the framing
        return Row(order.order_id, order.customer_id, order.amount, order.status, schema_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--schema-registry-url", default="http://sa01:8081")
    parser.add_argument("--topic", default="orders_protobuf")
    parser.add_argument("--group-id", default="ex17-protobuf-schema-registry")
    args = parser.parse_args()

    produce_orders(args)

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")
    table_env.create_temporary_system_function(
        "decode_order",
        udf(
            DecodeOrder(args.topic, args.schema_registry_url),
            input_types=[DataTypes.BYTES()],
            result_type=DataTypes.ROW(
                [
                    DataTypes.FIELD("order_id", DataTypes.STRING()),
                    DataTypes.FIELD("customer_id", DataTypes.STRING()),
                    DataTypes.FIELD("amount", DataTypes.DOUBLE()),
                    DataTypes.FIELD("status", DataTypes.STRING()),
                    DataTypes.FIELD("schema_id", DataTypes.INT()),
                ]
            ),
        ),
    )
    table_env.execute_sql(f"""
        CREATE TABLE orders_raw (
            payload BYTES
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'format' = 'raw'
        )
    """)
    print("Decoding the Protobuf orders in Flink:", flush=True)
    table_env.execute_sql("""
        SELECT d.order_id, d.customer_id, d.amount, d.status, d.schema_id
        FROM (SELECT decode_order(payload) AS d FROM orders_raw)
    """).print()


if __name__ == "__main__":
    main()
