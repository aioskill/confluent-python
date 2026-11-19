"""Read the page-view and click Kafka topics with Flink SQL.

The job is intentionally long-running. Stop it with Ctrl-C after observing
records from both topics. Create/populate the topics with
``ex00_produce_page_views_and_clicks.py`` first. Kafka offset commits are disabled
for this reader, so every new consumer group starts from the configured
earliest offset.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


EVENT_COLUMNS = """
    event_type STRING,
    view_id STRING,
    click_id STRING,
    user_id INT,
    page_id STRING,
    event_time BIGINT,
    received_at BIGINT,
    event_time_iso STRING
"""


def create_kafka_table(table_env, table_name, topic, bootstrap_servers, group_id):
    table_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            {EVENT_COLUMNS}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'false',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--group-id", default="ex02-read-topics-sql")
    args = parser.parse_args()
    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )

    create_kafka_table(
        table_env,
        "page_views",
        args.page_views_topic,
        args.bootstrap_servers,
        args.group_id,
    )
    create_kafka_table(
        table_env,
        "clicks",
        args.clicks_topic,
        args.bootstrap_servers,
        f"{args.group_id}-clicks",
    )

    table_env.execute_sql("""
        CREATE TABLE console_sink (
            stream_name STRING,
            event_type STRING,
            view_id STRING,
            click_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            received_at BIGINT,
            event_time_iso STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    table_env.execute_sql("""
        INSERT INTO console_sink
        SELECT 'page_views', event_type, view_id, click_id, user_id,
               page_id, event_time, received_at, event_time_iso
        FROM page_views
        UNION ALL
        SELECT 'clicks', event_type, view_id, click_id, user_id,
               page_id, event_time, received_at, event_time_iso
        FROM clicks
    """).wait()


if __name__ == "__main__":
    main()
