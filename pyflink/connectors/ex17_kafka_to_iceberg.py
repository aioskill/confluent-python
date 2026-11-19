"""Read page views and clicks from Kafka and write date-partitioned Iceberg tables.

The input schema matches ``streaming/ex00_produce_page_views_and_clicks.py``.
The two INSERT statements are submitted together as one streaming job. The
Iceberg tables are partitioned by the calendar day derived from ``event_time``.

Prerequisites:

1. Create the Kafka topics and run the synthetic producer if required:

   /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \\
       --topic page_views --partitions 3 --replication-factor 1
   /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \\
       --topic clicks --partitions 3 --replication-factor 1

2. Run ``flinklibs/configure`` so the Kafka and Iceberg connector JARs are
   available in the PyFlink/cluster ``lib`` directory.

Run locally (embedded Flink mini cluster, from this script's directory):

    python ex17_kafka_to_iceberg.py --warehouse file:///tmp/flink-iceberg-events

Submit to a running Flink cluster in detached mode (run from the directory
containing this script):

flink run -m sa03:8967 \
    --detached \
    -Dclassloader.check-leaked-classloader=false \
    -Dexecution.checkpointing.interval=30s \
    -Dexecution.checkpointing.storage=filesystem \
    -Dstate.checkpoints.dir=s3://my-bucket/flink/checkpoints \
    -py ex17_kafka_to_iceberg.py \
    --warehouse file:///tmp/flink-iceberg-events \
    --no-wait \
    --checkpoint-dir s3://my-bucket/flink/checkpoints

The warehouse must be a local path (on a cluster with several machines it must be
a directory shared by all of them). Checkpoints can go to S3. An ``s3://`` Iceberg
warehouse is not supported by the installed JARs.

The ``--checkpoint-dir`` argument is included because the script also applies
the setting through the Table API. ``state.checkpoints.dir`` is the Flink
configuration key for the checkpoint location; it is distinct from the
checkpoint interval and storage settings.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


CATALOG_NAME = "iceberg_catalog"
DATABASE_NAME = "events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write Kafka page views and clicks to Iceberg tables."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="sa01:9092",
        help="Comma-separated Kafka brokers (default: %(default)s)",
    )
    parser.add_argument(
        "--page-views-topic",
        default="page_views",
        help="Page-view Kafka topic (default: %(default)s)",
    )
    parser.add_argument(
        "--clicks-topic",
        default="clicks",
        help="Clicks Kafka topic (default: %(default)s)",
    )
    parser.add_argument(
        "--group-id",
        default="iceberg-events-writer",
        help="Kafka consumer group ID (default: %(default)s)",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Return after submitting the job (use with 'flink run --detached')",
    )
    parser.add_argument(
        "--warehouse",
        default="file:///tmp/flink-iceberg-events",
        help="Iceberg warehouse path (default: %(default)s)",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/flink-iceberg-events-checkpoints",
        help="Checkpoint storage path (default: %(default)s)",
    )
    parser.add_argument(
        "--checkpoint-interval-ms",
        type=int,
        default=10000,
        help="Checkpoint interval in milliseconds (default: %(default)s)",
    )
    return parser.parse_args()


def create_catalog_and_tables(
    table_env: TableEnvironment, args: argparse.Namespace
) -> None:
    table_env.execute_sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = '{args.warehouse}'
        )
        """
    )
    table_env.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}"
    )

    source_columns = """
        event_type STRING,
        user_id INT,
        page_id STRING,
        event_time BIGINT,
        received_at BIGINT,
        event_time_iso STRING,
        view_id STRING,
        click_id STRING
    """
    for table_name, topic, suffix in (
        ("page_views_source", args.page_views_topic, "page-views"),
        ("clicks_source", args.clicks_topic, "clicks"),
    ):
        table_env.execute_sql(
            f"""
            CREATE TABLE {table_name} (
                {source_columns}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{args.bootstrap_servers}',
                {sql_options()}
                'properties.group.id' = '{args.group_id}-{suffix}',
                'properties.enable.auto.commit' = 'false',
                'properties.commit.offsets.on.checkpoint' = 'true',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'false'
            )
            """
        )

    for table_name in ("page_views", "clicks"):
        table_env.execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS
                {CATALOG_NAME}.{DATABASE_NAME}.{table_name} (
                event_type STRING,
                user_id INT,
                page_id STRING,
                event_id STRING,
                event_time TIMESTAMP_LTZ(3),
                received_at TIMESTAMP_LTZ(3),
                event_time_iso STRING,
                `event_date` DATE
            -- Use the explicit DATE column because this Flink SQL runtime
            -- accepts identifier partitions but rejects transform syntax.
            ) PARTITIONED BY (event_date) WITH (
                'format-version' = '2',
                'write.upsert.enabled' = 'false'
            )
            """
        )


def add_inserts(table_env: TableEnvironment, wait: bool = True) -> None:
    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(
        f"""
        INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.page_views
        SELECT
            event_type,
            user_id,
            page_id,
            view_id AS event_id,
            TO_TIMESTAMP_LTZ(event_time, 3) AS event_time,
            TO_TIMESTAMP_LTZ(received_at, 3) AS received_at,
            event_time_iso,
            CAST(TO_TIMESTAMP_LTZ(event_time, 3) AS DATE) AS event_date
        FROM page_views_source
        WHERE event_type = 'page_view'
        """
    )
    statement_set.add_insert_sql(
        f"""
        INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.clicks
        SELECT
            event_type,
            user_id,
            page_id,
            click_id AS event_id,
            TO_TIMESTAMP_LTZ(event_time, 3) AS event_time,
            TO_TIMESTAMP_LTZ(received_at, 3) AS received_at,
            event_time_iso,
            CAST(TO_TIMESTAMP_LTZ(event_time, 3) AS DATE) AS event_date
        FROM clicks_source
        WHERE event_type = 'click'
        """
    )

    print("Submitting the page_views and clicks Iceberg sinks", flush=True)
    rs = statement_set.execute()

    # A local run must wait, otherwise the process ends and stops the job.
    # With "flink run --detached" pass --no-wait so the submission returns.
    if wait:
        rs.wait()



def main() -> None:
    args = parse_args()
    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set_local_timezone("UTC")
    configuration = table_env.get_config().get_configuration()
    # Iceberg's streaming sink commits data files when a checkpoint completes.
    # Without checkpoints, the job may run while the tables contain metadata
    # only, so no committed data files appear in the warehouse.
    configuration.set_string("state.checkpoints.dir", args.checkpoint_dir)
    configuration.set_string(
        "execution.checkpointing.interval",
        f"{args.checkpoint_interval_ms}ms",
    )
    configuration.set_string("execution.checkpointing.mode", "AT_LEAST_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")
    create_catalog_and_tables(table_env, args)
    add_inserts(table_env, wait=not args.no_wait)


if __name__ == "__main__":
    main()
