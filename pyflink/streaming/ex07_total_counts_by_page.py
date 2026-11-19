"""Count page views per page over the last 24 hours using Flink SQL.

The query uses a 24-hour event-time HOP window that advances every hour.
Consequently, each completed result contains the rolling total for the
previous 24 hours. Results are written to an upsert Kafka topic keyed by
``(window_end, page_id)``.

The output value has this shape::

    {
        "window_start": "...",
        "window_end": "...",
        "page_id": "page-1",
        "page_views": 123
    }

Create the required Kafka topics before starting the job::

/app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic page_view_totals_24h --partitions 3 \
        --replication-factor 1 --config cleanup.policy=compact

Use the appropriate replication factor for a multi-broker cluster. The
compacted sink topic retains the latest value for each ``(window_end,
page_id)`` key; consumers should process tombstones as deletions.

Kafka does not preserve display order. A consumer should materialize the
latest window and sort by ``page_views DESC, page_id ASC`` when displaying it.


/app/confluent/bin/kafka-console-consumer \
     --command-config /app/confluent/etc/kafka/client.properties \
     --bootstrap-server sa01:9092 \
     --topic page_view_totals_24h \
     --from-beginning \
     --property print.key=true \
     --property key.separator=" | "


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
    parser.add_argument("--sink-topic", default="page_view_totals_24h")
    parser.add_argument("--group-id", default="ex07-page-totals-24h")
    parser.add_argument(
        "--slide-minutes",
        type=int,
        default=60,
        help="How often the rolling 24-hour total is recalculated",
    )
    args = parser.parse_args()

    if args.slide_minutes <= 0 or args.slide_minutes > 24 * 60:
        raise ValueError("slide-minutes must be between 1 and 1440")

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    table_env.get_config().set_local_timezone("UTC")

    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            page_id STRING,
            event_time BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            -- Advance event-time processing while allowing events to arrive
            -- up to 10 seconds later than the current event-time watermark.
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
            'properties.commit.offsets.on.checkpoint' = 'false',
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    table_env.execute_sql(f"""
        CREATE TABLE page_totals_sink (
            window_end TIMESTAMP_LTZ(3),
            page_id STRING,
            window_start TIMESTAMP_LTZ(3),
            page_views BIGINT,
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

    slide_interval = f"INTERVAL '{args.slide_minutes}' MINUTE"
    table_env.execute_sql(f"""
        INSERT INTO page_totals_sink
        WITH page_counts AS (
            SELECT
                window_start,
                window_end,
                page_id,
                COUNT(*) AS page_views
            FROM TABLE(
                HOP(
                    TABLE page_views,
                    DESCRIPTOR(event_ts),
                    {slide_interval},
                    INTERVAL '24' HOUR
                )
            )
            GROUP BY window_start, window_end, page_id
        ), latest_window AS (
            SELECT MAX(window_end) AS window_end
            FROM page_counts
        )
        SELECT counts.window_end,
               counts.page_id,
               counts.window_start,
               counts.page_views
        FROM page_counts AS counts
        JOIN latest_window AS latest
          ON counts.window_end = latest.window_end
        ON CONFLICT DO DEDUPLICATE
    """).wait()


if __name__ == "__main__":
    main()
