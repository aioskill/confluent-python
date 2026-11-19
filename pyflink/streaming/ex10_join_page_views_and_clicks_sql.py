"""Join page views and clicks by user and event-time window using Flink SQL.

Both Kafka tables use event-time watermarks and are grouped into matching
30-second tumbling windows. Clicks are counted per user and window, then
left-joined to page views so a page view with no click still produces
``click_count = 0``.

Create the input topics before running::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic page_views --partitions 3 --replication-factor 1
    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic clicks --partitions 3 --replication-factor 1

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex10_join_page_views_and_clicks_sql.py

Submit to a Flink cluster::

    flink run -py ex10_join_page_views_and_clicks_sql.py

The print sink displays the SQL changelog. Late events may produce updates
for a previously emitted window.
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
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--group-id", default="ex10-join-page-views-clicks-sql")
    args = parser.parse_args()

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    table_env.get_config().set_local_timezone("UTC")

    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            user_id INT,
            page_id STRING,
            view_id STRING,
            event_time BIGINT,
            -- Convert epoch milliseconds (precision 3) to an event-time
            -- timestamp that Flink can use for windows and watermarks.
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            -- Allow events to arrive up to 10 seconds out of order.
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.page_views_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}-page-views',
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
        CREATE TABLE clicks (
            user_id INT,
            page_id STRING,
            click_id STRING,
            event_time BIGINT,
            -- The producer stores event_time as epoch milliseconds.
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.clicks_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}-clicks',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'false',
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE joined_sink (
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            user_id INT,
            page_id STRING,
            view_id STRING,
            click_count BIGINT
        ) WITH ('connector' = 'print')
    """)

    # Build one row per page view and one click-count row per user/window.
    # Joining on both window boundaries makes this an event-time window join.
    table_env.execute_sql("""
        INSERT INTO joined_sink
        WITH windowed_page_views AS (
            SELECT window_start, window_end, user_id, page_id, view_id
            FROM TABLE(
                TUMBLE(
                    TABLE page_views,
                    DESCRIPTOR(event_ts),
                    INTERVAL '30' SECOND
                )
            )
        ), windowed_click_counts AS (
            SELECT
                window_start,
                window_end,
                user_id,
                COUNT(*) AS click_count
            FROM TABLE(
                TUMBLE(
                    TABLE clicks,
                    DESCRIPTOR(event_ts),
                    INTERVAL '30' SECOND
                )
            )
            GROUP BY window_start, window_end, user_id
        )
        SELECT
            pv.window_start,
            pv.window_end,
            pv.user_id,
            pv.page_id,
            pv.view_id,
            COALESCE(clicks.click_count, CAST(0 AS BIGINT)) AS click_count
        FROM windowed_page_views AS pv
        LEFT JOIN windowed_click_counts AS clicks
          ON pv.window_start = clicks.window_start
         AND pv.window_end = clicks.window_end
         AND pv.user_id = clicks.user_id
        WHERE click_count >0
    """).wait()


if __name__ == "__main__":
    main()
