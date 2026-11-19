"""Read a Confluent Cloud topic with Flink SQL and print the rows.

The Kafka table logs in to Confluent Cloud with SASL_SSL + PLAIN (an API key and
secret, see cloud_auth.py). The read is bounded: it stops at the offsets that the
topic has when the job starts, so the script ends by itself.

Create the data first (use your own prefix)::

    python ex00_produce_events.py --prefix alice-

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex01_read_topic_sql.py --prefix alice-

Submit to a Flink cluster (the cluster needs the Kafka connector JARs)::

    flink run -py ex01_read_topic_sql.py --pyFiles cloud_auth.py -- --prefix alice-
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cloud_auth import bootstrap_servers, sql_options  # noqa: E402

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--prefix", default="pyflink-")
    parser.add_argument("--group-id", default="cloud-ex01-read-topic")
    args = parser.parse_args()

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")
    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            view_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.prefix}page_views',
            'properties.bootstrap.servers' = '{bootstrap_servers()}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    table_env.execute_sql("""
        SELECT view_id, user_id, page_id, event_ts FROM page_views WHERE user_id <= 2
    """).print()


if __name__ == "__main__":
    main()
