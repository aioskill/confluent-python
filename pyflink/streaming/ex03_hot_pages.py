"""Find the most visited pages in a hopping event-time window using Flink SQL.

Source -> kafka Topic -> Flink

              interval   count
===================================
W1 10:00:00 - 10:00:30 : c1 (removed from state store)
W2 10:00:05 - 10:00:35 : c2
W3 10:00:10 - 10:00:40 : c3
W4 10:00:15 - 10:00:45 : c3
W5 10:00:20 - 10:00:50 : c3
W6 10:00:25 - 10:00:55 : c3
W7 10:00:30 - 10:00:60 : c3 <-watermark
...

W1 (batch)
10:00:00
10:00:04
10:00:20
10:00:16
09:50:16 (got stuck in traffic between source and kafka)

The test query uses a 30-second hopping window (sliding window) that advances every 5 seconds.
The producer's ``event_time`` is epoch milliseconds, and the watermark allows
up to 10 seconds of out-of-order data.
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
    parser.add_argument("--group-id", default="ex03-host-pages")
    args = parser.parse_args()

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
            -- Allow windows to close when a Kafka partition has no events.
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE top_pages_sink (
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            page_id STRING,
            page_views BIGINT,
            page_rank BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Count each page in the previous 30 seconds. HOP produces a new result
    # every 5 seconds and includes late events until the watermark closes a
    # window.
    table_env.execute_sql("""
        INSERT INTO top_pages_sink
        WITH page_counts AS (
            SELECT
                window_start,
                window_end,
                page_id,
                COUNT(*) AS page_views
            FROM TABLE(
                -- HOP creates overlapping event-time windows: each window is
                -- 30 seconds long and a new window starts every 5 seconds.
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
        )
        SELECT window_start, window_end, page_id, page_views, page_rank
        FROM ranked_pages
        WHERE page_rank = 1
    """).wait()


if __name__ == "__main__":
    main()
