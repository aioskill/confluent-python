"""Write the latest top-10 pages to MySQL with a JDBC upsert sink.

This is the database-sink version of ``ex05_top_pages_snapshot.py``. It uses
the same 30-second HOP window, recalculated every 5 seconds, but writes only
to MySQL. The primary key ``(window_end, page_id)`` allows Flink JDBC to
upsert changing page counts and delete rows that leave the top 10.

Prepare MySQL::

    CREATE DATABASE IF NOT EXISTS training;
    CREATE USER IF NOT EXISTS 'training'@'%' IDENTIFIED BY 'training321';
    GRANT ALL PRIVILEGES ON training.* TO 'training'@'%';
    FLUSH PRIVILEGES;
    USE training;
    CREATE TABLE top_pages (
        window_end TIMESTAMP(3),
        page_id VARCHAR(255),
        window_start TIMESTAMP(3),
        page_views BIGINT,
        page_rank BIGINT,
        PRIMARY KEY (window_end, page_id)
    );

Prepare Kafka and dependencies::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \
        --topic page_views --partitions 3 --replication-factor 1
    bash flinklibs/configure
    python ex00_produce_page_views_and_clicks.py

The JDBC connector and MySQL Connector/J must be available in Flink's ``lib``
directory. The job is unbounded and should be stopped with Ctrl-C.
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
    parser.add_argument("--group-id", default="ex05-top-pages-jdbc")
    parser.add_argument("--mysql-host", default="sa01")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-database", default="training")
    parser.add_argument("--mysql-table", default="top_pages")
    parser.add_argument("--mysql-user", default="training")
    parser.add_argument("--mysql-password", default="training321")
    parser.add_argument("--mysql-sink-parallelism", type=int, default=2)
    args = parser.parse_args()

    if args.mysql_sink_parallelism <= 0:
        raise ValueError("mysql-sink-parallelism must be positive")

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    table_env.get_config().set_local_timezone("UTC")
    table_env.get_config().set("parallelism.default", "1")

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
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    mysql_url = (
        f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/"
        f"{args.mysql_database}?useSSL=false&allowPublicKeyRetrieval=true"
    )
    # JDBC upsert behavior is driven by the declared primary key. For each
    # changelog row, Flink inserts when (window_end, page_id) is new, updates
    # the existing MySQL row when that key already exists, and deletes the
    # keyed row when the query emits a DELETE change. Consequently, the MySQL
    # table must have the same primary key as the Flink sink definition.
    table_env.execute_sql(f"""
        CREATE TABLE top_pages_mysql_sink (
            window_end TIMESTAMP_LTZ(3),
            page_id STRING,
            window_start TIMESTAMP_LTZ(3),
            page_views BIGINT,
            page_rank BIGINT,
            PRIMARY KEY (window_end, page_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{mysql_url}',
            'table-name' = '{args.mysql_table}',
            'username' = '{args.mysql_user}',
            'password' = '{args.mysql_password}',
            'sink.parallelism' = '{args.mysql_sink_parallelism}'
        )
    """)

    query = """
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
    """

    print(
        f"Writing top pages to MySQL with sink parallelism "
        f"{args.mysql_sink_parallelism}:",
        flush=True,
    )
    table_env.execute_sql(f"""
        INSERT INTO top_pages_mysql_sink
        {query}
    """).wait()


if __name__ == "__main__":
    main()
