"""Enrich clicks from a Debezium user changelog held in Flink state.

Architecture::

    MySQL users -> Debezium -> Kafka users_cdc -> Flink state -> enriched clicks

Unlike a JDBC lookup join, this job does not query MySQL for every click.
Flink consumes the Debezium changelog and maintains the latest user row in its
managed state. Checkpoints persist that state, while RocksDB keeps large state
off the JVM heap.

Prerequisites:

* Create the Kafka topics (adjust the partition count as appropriate)::

      /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \
          --topic clicks --partitions 3 --replication-factor 1
      /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \
          --topic users_cdc --partitions 3 --replication-factor 1

* Configure a Debezium MySQL source connector to publish the ``users`` table
  to ``users_cdc``. The topic value must use Debezium JSON, for example an
  insert/update event has an ``after`` object containing ``user_id``,
  ``user_name``, ``country``, and ``membership``.
* Make sure the Kafka connector and Debezium JSON format JARs are available to
  the Flink client and TaskManagers.

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex14_enrich_clicks_user_cdc.py

Submit to a Flink cluster::

    flink run -py ex14_enrich_clicks_user_cdc.py

The job is unbounded and should be stopped with Ctrl-C. Start Debezium first,
then start this job, so the user changelog can be replayed from the earliest
offset before clicks are enriched.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--users-topic", default="users_cdc")
    parser.add_argument("--clicks-group-id", default="ex14-clicks-cdc")
    parser.add_argument("--users-group-id", default="ex14-users-cdc")
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/pyflink-training/ex14-checkpoints",
        help="Shared checkpoint directory",
    )
    parser.add_argument(
        "--rocksdb-dir",
        default="/tmp/pyflink-training/ex14-rocksdb",
        help="Local RocksDB directory on each TaskManager",
    )
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    args = parser.parse_args()

    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    configuration = table_env.get_config().get_configuration()
    configuration.set_string("parallelism.default", "1")
    configuration.set_string("state.backend.type", "rocksdb")
    configuration.set_string("state.backend.rocksdb.localdir", args.rocksdb_dir)
    configuration.set_string("state.checkpoints.dir", args.checkpoint_dir)
    configuration.set_string(
        "execution.checkpointing.interval",
        f"{args.checkpoint_interval_ms}ms",
    )
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")
    # An idle partition must not hold back the watermark of the temporal join.
    configuration.set_string("table.exec.source.idle-timeout", "5 s")

    table_env.execute_sql(f"""
        CREATE TABLE clicks (
            event_type STRING,
            click_id STRING,
            view_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            received_at BIGINT,
            event_time_iso STRING,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.clicks_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.clicks_group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'true',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # The primary key tells Flink that this is a changelog keyed by user_id.
    # INSERT, UPDATE, and DELETE events from Debezium update Flink-managed
    # state instead of causing a database lookup for every click.
    table_env.execute_sql(f"""
        CREATE TABLE users_cdc (
            user_id INT,
            user_name STRING,
            country STRING,
            membership STRING,
            -- time Debezium captured the change (ts_ms): the version time of each user row
            change_ts TIMESTAMP_LTZ(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
            WATERMARK FOR change_ts AS change_ts,
            PRIMARY KEY (user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.users_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.users_group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'true',
            'format' = 'debezium-json',
            'debezium-json.ignore-parse-errors' = 'true'
        )
    """)

    enriched_clicks = table_env.sql_query("""
        SELECT
            c.click_id,
            c.view_id,
            c.user_id,
            c.page_id,
            c.event_time,
            c.event_time_iso,
            u.user_name,
            u.country,
            u.membership
        FROM clicks AS c
        LEFT JOIN users_cdc FOR SYSTEM_TIME AS OF c.event_ts AS u
          ON c.user_id = u.user_id
    """)

    print(
        "Enriching clicks from the Debezium user changelog in Flink state:",
        flush=True,
    )
    enriched_clicks.execute().print()


if __name__ == "__main__":
    main()
