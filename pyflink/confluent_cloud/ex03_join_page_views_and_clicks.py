"""Join page views with their clicks on Confluent Cloud (event-time interval join).

A page view is joined with the click that has the same view_id and happened at most
one minute after the view. A LEFT JOIN keeps the views without a click (click_id is
NULL). Both topics use event-time watermarks and are read up to their current end, so
the job finishes by itself.

Create the data first (use your own prefix)::

    python ex00_produce_events.py --prefix alice-

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex03_join_page_views_and_clicks.py --prefix alice-

Submit to a Flink cluster::

    flink run -py ex03_join_page_views_and_clicks.py --pyFiles cloud_auth.py -- --prefix alice-
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cloud_auth import bootstrap_servers, sql_options  # noqa: E402

from pyflink.table import EnvironmentSettings, TableEnvironment


def kafka_table(table_env, args, name, topic, columns):
    table_env.execute_sql(f"""
        CREATE TABLE {name} (
            {columns},
            event_time BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.prefix}{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers()}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}-{name}',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--prefix", default="pyflink-")
    parser.add_argument("--group-id", default="cloud-ex03-join")
    args = parser.parse_args()

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")
    table_env.get_config().set_local_timezone("UTC")
    kafka_table(table_env, args, "page_views", "page_views",
                "view_id STRING, user_id INT, page_id STRING")
    kafka_table(table_env, args, "clicks", "clicks",
                "click_id STRING, view_id STRING, user_id INT, page_id STRING")

    table_env.execute_sql("""
        SELECT v.view_id, v.user_id, v.page_id, c.click_id,
               TIMESTAMPDIFF(SECOND, v.event_ts, c.event_ts) AS seconds_to_click
        FROM page_views v
        LEFT JOIN clicks c
          ON v.view_id = c.view_id
         AND c.event_ts BETWEEN v.event_ts AND v.event_ts + INTERVAL '1' MINUTE
        WHERE v.user_id = 2
    """).print()


if __name__ == "__main__":
    main()
