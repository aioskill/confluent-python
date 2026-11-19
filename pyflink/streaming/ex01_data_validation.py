"""Validate, enrich, and republish page-view events with Flink SQL.

The job reads JSON page views from Kafka, validates the required fields, adds
one enrichment field (``page_category``), and writes valid records to another
Kafka topic as JSON.

Create the topics before running when Kafka topic auto-creation is disabled::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic validated_page_views --partitions 3 --replication-factor 1

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex01_data_validation.py

Submit to a Flink cluster::

    flink run -m sa03:8967 \
        --detached  \
        -py ex01_data_validation.py \
        --checkpoint-dir s3://my-bucket/flink/checkpoints

Checkpoint defaults to local filesystem storage. For a cluster, use shared
storage such as S3 with ``--checkpoint-dir``.

Invalid records are filtered out by the validation predicate. For production,
route invalid records to a quarantine topic instead of silently dropping them.

Find consumer group offset
/app/confluent/bin/kafka-consumer-groups --command-config \
        /app/confluent/etc/kafka/client.properties \
        --bootstrap-server sa01:9092 \
        --describe \
        --group data-validation


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
    parser.add_argument("--source-topic", default="page_views")
    parser.add_argument("--sink-topic", default="validated_page_views")
    parser.add_argument("--group-id", default="data-validation")
    parser.add_argument("--rest-port", type=int, default=0)
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/pyflink-training/ex01-data-validation-checkpoints",
        help="Checkpoint directory; use shared S3 on a cluster",
    )
    # parser.add_argument(
    #     "--rocksdb-dir",
    #     default="/tmp/pyflink-training/ex01-data-validation-rocksdb",
    #     help="Local RocksDB state directory",
    # )
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    args = parser.parse_args()

    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")

    if args.rest_port != 0:
        if args.rest_port <= 0 or args.rest_port > 65535:
            raise ValueError("rest-port must be between 1 and 65535")

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )

    if args.rest_port != 0:
        table_env.get_config().set("rest.port", str(args.rest_port))
    configuration = table_env.get_config().get_configuration()
    configuration.set_string(
        "execution.checkpointing.interval",
        f"{args.checkpoint_interval_ms}ms",
    )
    # RocksDB stores working keyed state locally on the TaskManager. The
    # checkpoint directory above stores recoverable snapshots and can be
    # shared S3 storage in a cluster.
    configuration.set_string("state.backend.type", "rocksdb")
    # configuration.set_string("state.backend.rocksdb.localdir", args.rocksdb_dir)
    # Snapshot operator state and source offsets consistently so a recovery
    # does not re-apply successfully checkpointed Kafka records. This requires
    # checkpoint-aware source/sink behavior and costs more coordination than
    # AT_LEAST_ONCE mode.
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")

    configuration.set_string("table.exec.state.ttl", "3600000")  # State TTL: 1 hour

    configuration.set_string("rest.flamegraph.enabled", "true")

    # If you do not set checkpoint directory
    # Job manager will store checkpointing data (limited by
    configuration.set_string("state.checkpoints.dir", args.checkpoint_dir)

    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            event_type STRING,
            view_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            received_at BIGINT,
            event_time_iso STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.source_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            -- native Kafka Consumer configuration passed down through Flink. 
            -- any config prefixed with properties. bypasses Flink's parsing layer and 
            -- goes directly to the underlying Kafka client.
            'properties.auto.offset.reset' = 'earliest',
            -- Kafka offsets are committed with successful Flink checkpoints.
            'properties.enable.auto.commit' = 'true',
            'properties.commit.offsets.on.checkpoint' = 'true',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    table_env.execute_sql(f"""
        CREATE TABLE validated_page_views (
            event_type STRING,
            view_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            received_at BIGINT,
            event_time_iso STRING,
            page_category STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.sink_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'format' = 'json'
        )
    """)

    # Validation keeps only page-view events with the required identifiers and
    # positive timestamps. Enrichment derives a category from page_id.
    table_env.execute_sql("""
        INSERT INTO validated_page_views
        SELECT
            event_type,
            view_id,
            user_id,
            page_id,
            event_time,
            received_at,
            event_time_iso,
            CASE
                WHEN page_id IN ('page-1', 'page-2') THEN 'landing'
                WHEN page_id IN ('page-3', 'page-4') THEN 'product'
                ELSE 'other'
            END AS page_category
        FROM page_views
        WHERE event_type = 'page_view'
          AND view_id IS NOT NULL
          AND view_id <> ''
          AND user_id IS NOT NULL
          AND user_id > 0
          AND page_id IS NOT NULL
          AND page_id <> ''
          AND event_time IS NOT NULL
          AND event_time > 0
    """).wait()  # keep the local mini cluster alive while the job runs


if __name__ == "__main__":
    main()
