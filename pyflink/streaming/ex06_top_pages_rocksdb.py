"""Top-10 pages with RocksDB managed state and checkpointing.

This example is the SQL/upsert version of the top-pages job with explicit
state backend configuration. RocksDB stores working keyed state on local disk;
checkpoints persist recoverable snapshots to filesystem storage.

For a cluster, use shared storage for ``--checkpoint-dir`` (for example S3) and
keep ``--rocksdb-dir`` on local TaskManager disks::

    python ex06_top_pages_rocksdb.py \
        --checkpoint-dir file:///tmp/pyflink-training/ex06-checkpoints
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
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--sink-topic", default="top_pages_rocksdb")
    parser.add_argument("--group-id", default="ex06-top-pages-rocksdb")
    parser.add_argument(
        "--rocksdb-dir",
        default="/tmp/pyflink-training/ex06-rocksdb",
        help="Local RocksDB state directory",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/pyflink-training/ex06-checkpoints",
        help="Checkpoint storage directory; use shared S3 on a cluster",
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
    table_env.get_config().set_local_timezone("UTC")

    configuration = table_env.get_config().get_configuration()
    configuration.set_string("state.backend.type", "rocksdb")
    configuration.set_string("state.backend.rocksdb.localdir", args.rocksdb_dir)
    configuration.set_string("state.checkpoints.dir", args.checkpoint_dir)
    configuration.set_string(
        "execution.checkpointing.interval",
        f"{args.checkpoint_interval_ms}ms",
    )
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")

    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            page_id STRING,
            event_time BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.page_views_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'true',
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    table_env.execute_sql(f"""
        CREATE TABLE top_pages_sink (
            window_end TIMESTAMP_LTZ(3),
            page_id STRING,
            window_start TIMESTAMP_LTZ(3),
            page_views BIGINT,
            page_rank BIGINT,
            PRIMARY KEY (window_end, page_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{args.sink_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    table_env.execute_sql("""
        INSERT INTO top_pages_sink
        WITH page_counts AS (
            SELECT window_start, window_end, page_id, COUNT(*) AS page_views
            FROM TABLE(
                HOP(
                    TABLE page_views,
                    DESCRIPTOR(event_ts),
                    INTERVAL '5' SECOND,
                    INTERVAL '30' SECOND
                )
            )
            GROUP BY window_start, window_end, page_id
        ), ranked_pages AS (
            SELECT
                window_start,
                window_end,
                page_id,
                page_views,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end
                    ORDER BY page_views DESC, page_id
                ) AS page_rank
            FROM page_counts
        ), latest_window AS (
            SELECT MAX(window_end) AS window_end
            FROM ranked_pages
        )
        SELECT ranked.window_end,
               ranked.page_id,
               ranked.window_start,
               ranked.page_views,
               ranked.page_rank
        FROM ranked_pages AS ranked
        JOIN latest_window AS latest
          ON ranked.window_end = latest.window_end
        WHERE ranked.page_rank <= 10
        ON CONFLICT DO DEDUPLICATE
    """).wait()


if __name__ == "__main__":
    main()
