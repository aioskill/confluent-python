"""Write and read Avro records with Confluent Cloud Schema Registry in Flink SQL.

The ``avro-confluent`` format stores each record as: a magic byte, a 4-byte schema ID and
the Avro payload. The schema is registered in the Cloud Schema Registry under the subject
``<topic>-value``. The registry is reached over HTTPS with its own API key (basic auth),
which cloud_auth.py reads from the .env file of the confluent-cloud folder.

The script writes 5 orders to <prefix>orders_avro (the topic is created if it is missing),
prints the registered schema, and reads the orders back (bounded read). Running it again
adds 5 more orders.

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex04_avro_schema_registry.py --prefix alice-

Submit to a Flink cluster (needs the avro-confluent JAR installed by setup_pyflink.sh)::

    flink run -py ex04_avro_schema_registry.py --pyFiles cloud_auth.py -- --prefix alice-

Delete the topic and the schema when you are done::

    python ./confluent-cloud/manage_topics.py delete alice-orders_avro --yes
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cloud_auth import (  # noqa: E402
    bootstrap_servers,
    ensure_topics,
    schema_registry_config,
    schema_registry_sql_options,
    sql_options,
)

from pyflink.table import EnvironmentSettings, TableEnvironment

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


def kafka_table(name, args, extra_options=""):
    return f"""
        CREATE TABLE {name} (
            order_id STRING NOT NULL,
            customer_id STRING NOT NULL,
            amount DOUBLE NOT NULL,
            status STRING NOT NULL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.prefix}orders_avro',
            'properties.bootstrap.servers' = '{bootstrap_servers()}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'format' = 'avro-confluent',
            {schema_registry_sql_options()}
            {extra_options}
            'avro-confluent.subject' = '{args.prefix}orders_avro-value'
        )
    """


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--prefix", default="pyflink-")
    parser.add_argument("--group-id", default="cloud-ex04-avro")
    args = parser.parse_args()

    ensure_topics(f"{args.prefix}orders_avro")
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")

    # The writer schema is given explicitly so that the registered schema has the
    # name com.example.confluent.Order (Flink would generate a record name otherwise).
    # The columns are NOT NULL because the Avro fields are not nullable unions.
    schema = json.dumps(ORDER_SCHEMA).replace("'", "''")
    stmnt = kafka_table("orders_sink", args, f"'avro-confluent.schema' = '{schema}',")
    print(f"Create orders_sink table with schema: {stmnt}", flush=True)
    table_env.execute_sql(stmnt)
    table_env.execute_sql("""
        INSERT INTO orders_sink VALUES
            ('order-0001', 'customer-1', 12.5, 'CREATED'),
            ('order-0002', 'customer-2', 25.0, 'CREATED'),
            ('order-0003', 'customer-3', 37.5, 'PAID'),
            ('order-0004', 'customer-1', 50.0, 'SHIPPED'),
            ('order-0005', 'customer-2', 62.5, 'DELIVERED')
    """).wait()

    from confluent_kafka.schema_registry import SchemaRegistryClient

    registered = SchemaRegistryClient(schema_registry_config()).get_latest_version(
        f"{args.prefix}orders_avro-value"
    )
    print(f"Schema Registry: subject {registered.subject}, version {registered.version}, "
          f"id {registered.schema_id}", flush=True)
    print(registered.schema.schema_str, flush=True)

    table_env.execute_sql(kafka_table(
        "orders_source", args,
        "'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset',"))
    table_env.execute_sql("SELECT * FROM orders_source").print()


if __name__ == "__main__":
    main()
