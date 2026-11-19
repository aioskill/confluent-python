"""Write and read Avro records with Confluent Schema Registry using Flink SQL.

The ``avro-confluent`` format stores each record in the Confluent wire format
(a magic byte, the 4-byte schema ID, then the Avro payload). The schema itself is
kept in Schema Registry, under the subject ``<topic>-value``.

What this example does:

1. Writes a few orders to a Kafka topic. Flink registers the Avro schema
   (``com.example.confluent.Order``, the same schema as ``order.avsc`` in the
   Confluent Platform tutorials) in Schema Registry the first time it writes.
2. Prints the registered schema, fetched from Schema Registry.
3. Reads the topic back. Flink downloads the schema by the ID found in each
   message, so the writer and reader never share a schema file.

Records written by ``produce_avro.py`` from the Confluent Platform tutorials use
the same subject naming and can be read by step 3 as well.

Each run writes 5 more orders, so a rerun shows more rows; delete the topic to start over.

Run locally (embedded Flink mini cluster, from this script's directory)::

    export CONFLUENT_SASL_PASSWORD='<password from /app/confluent/etc/kafka/client.properties>'
    python ex16_avro_schema_registry.py

Submit to a Flink cluster::

    flink run -m sa03:8967 -py ex16_avro_schema_registry.py

Needs the ``flink-sql-avro-confluent-registry`` JAR, installed by ``setup_pyflink.sh``.
Create the topic first (single partition)::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic orders_avro --partitions 1 --replication-factor 1

Clean up (topic and schema)::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --delete --topic orders_avro
    curl -X DELETE http://sa01:8081/subjects/orders_avro-value
    curl -X DELETE 'http://sa01:8081/subjects/orders_avro-value?permanent=true'
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse
import json
import urllib.request

from pyflink.table import EnvironmentSettings, TableEnvironment

# The writer schema. The record name and namespace matter: without an explicit
# schema Flink would generate a record called "record" from the table columns.
ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "com.example.confluent",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "status", "type": "string"},
    ],
}

COLUMNS = """
            order_id STRING NOT NULL,
            customer_id STRING NOT NULL,
            amount DOUBLE NOT NULL,
            status STRING NOT NULL
"""


def kafka_table(name, args, extra_options=""):
    return f"""
        CREATE TABLE {name} ({COLUMNS}) WITH (
            'connector' = 'kafka',
            'topic' = '{args.topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{args.schema_registry_url}',
            'avro-confluent.subject' = '{args.topic}-value',
            {extra_options}
            'scan.startup.mode' = 'earliest-offset'
        )
    """


def show_registered_schema(args):
    url = f"{args.schema_registry_url}/subjects/{args.topic}-value/versions/latest"
    with urllib.request.urlopen(url, timeout=10) as response:
        registered = json.load(response)
    print(
        f"Schema Registry: subject={registered['subject']} "
        f"version={registered['version']} id={registered['id']} "
        f"type={registered.get('schemaType', 'AVRO')}",
        flush=True,
    )
    print(json.dumps(json.loads(registered["schema"]), indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--schema-registry-url", default="http://sa01:8081")
    parser.add_argument("--topic", default="orders_avro")
    parser.add_argument("--group-id", default="ex16-avro-schema-registry")
    args = parser.parse_args()

    # 1. Write. A bounded VALUES query ends by itself, so the script moves on.
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    writer_schema = json.dumps(ORDER_SCHEMA).replace("'", "''")
    table_env.execute_sql(
        kafka_table("orders_sink", args, f"'avro-confluent.schema' = '{writer_schema}',")
    )
    print(f"Writing 5 Avro orders to {args.topic}", flush=True)
    table_env.execute_sql("""
        INSERT INTO orders_sink VALUES
            ('order-0001', 'customer-1', 12.50, 'CREATED'),
            ('order-0002', 'customer-2', 25.00, 'CREATED'),
            ('order-0003', 'customer-3', 37.50, 'PAID'),
            ('order-0004', 'customer-1', 50.00, 'SHIPPED'),
            ('order-0005', 'customer-2', 62.50, 'DELIVERED')
    """).wait()

    # 2. The schema is now in Schema Registry.
    show_registered_schema(args)

    # 3. Read back. The reader schema comes from Schema Registry by schema ID.
    #    "scan.bounded.mode" stops the query at the last offset it sees at start.
    reader_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    reader_env.execute_sql(
        kafka_table("orders", args, "'scan.bounded.mode' = 'latest-offset',")
    )
    print("Reading the orders back:", flush=True)
    reader_env.execute_sql("SELECT * FROM orders").print()


if __name__ == "__main__":
    main()
