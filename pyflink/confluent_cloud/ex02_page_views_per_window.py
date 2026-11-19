"""Count page views per page in 30-second event-time windows and write the counts to Confluent Cloud.

Reads <prefix>page_views, counts the views per page in tumbling event-time windows
(watermark = event time - 5 s), and writes one JSON row per page and window to the
topic <prefix>page_view_counts. The read is bounded, so the last windows are closed
when the input ends and the job finishes. A second query then reads the result topic.

Create the data first (use your own prefix)::

    python ex00_produce_events.py --prefix alice-

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex02_page_views_per_window.py --prefix alice-

Submit to a Flink cluster::

    flink run -py ex02_page_views_per_window.py --pyFiles cloud_auth.py -- --prefix alice-

Running it again writes the counts again: delete <prefix>page_view_counts to start over.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cloud_auth import bootstrap_servers, ensure_topics, sql_options  # noqa: E402

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--prefix", default="pyflink-")
    parser.add_argument("--group-id", default="cloud-ex02-window-counts")
    args = parser.parse_args()

    ensure_topics(f"{args.prefix}page_view_counts")
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")
    table_env.get_config().set_local_timezone("UTC")
    kafka = f"""
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{bootstrap_servers()}',
            {sql_options()}
            'format' = 'json'"""

    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            view_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
            'topic' = '{args.prefix}page_views',
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            {kafka}
        )
    """)
    table_env.execute_sql(f"""
        CREATE TABLE page_view_counts (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            page_id STRING,
            view_count BIGINT
        ) WITH (
            'topic' = '{args.prefix}page_view_counts',
            {kafka}
        )
    """)

    print("Counting page views per page and 30-second window...", flush=True)
    table_env.execute_sql("""
        INSERT INTO page_view_counts
        SELECT window_start, window_end, page_id, COUNT(*) AS view_count
        FROM TABLE(TUMBLE(TABLE page_views, DESCRIPTOR(event_ts), INTERVAL '30' SECOND))
        GROUP BY window_start, window_end, page_id
    """).wait()

    print("Rows written to Confluent Cloud:", flush=True)
    table_env.execute_sql(f"""
        CREATE TABLE page_view_counts_read (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            page_id STRING,
            view_count BIGINT
        ) WITH (
            'topic' = '{args.prefix}page_view_counts',
            'properties.group.id' = '{args.group_id}-read',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            {kafka}
        )
    """)
    table_env.execute_sql("""
        SELECT window_start, page_id, view_count FROM page_view_counts_read
    """).print()


if __name__ == "__main__":
    main()
