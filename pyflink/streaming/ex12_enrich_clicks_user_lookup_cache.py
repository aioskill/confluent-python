"""Enrich Kafka clicks with MySQL users using a JDBC lookup cache.

This example is a cached variant of ``ex11_enrich_clicks_user.py``. On a cache
miss Flink queries MySQL; subsequent clicks for the same ``user_id`` are served
from a local cache in the TaskManager. This reduces database load when users
are reused frequently, at the cost of potentially stale user attributes until
the cache entry expires.

Prerequisites:

* The ``clicks`` topic exists and contains events from
  ``ex00_produce_page_views_and_clicks.py``.
* MySQL contains the ``users`` table described in ``ex11``.
* The Flink JDBC connector and MySQL Connector/J are available in Flink's
  ``lib`` directory.

Example::

    python ex12_enrich_clicks_user_lookup_cache.py \
        --mysql-host sa01

The job is unbounded and should be stopped with Ctrl-C.
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
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--group-id", default="ex12-enrich-clicks-user-cache")
    parser.add_argument("--mysql-host", default="sa01")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-database", default="training")
    parser.add_argument("--mysql-table", default="users")
    parser.add_argument("--mysql-user", default="training")
    parser.add_argument("--mysql-password", default="training321")
    parser.add_argument("--cache-max-rows", type=int, default=10000)
    parser.add_argument(
        "--cache-expire-after-write",
        default="10 min",
        help="TTL after a user row is added to the cache",
    )
    parser.add_argument(
        "--cache-expire-after-access",
        default="5 min",
        help="TTL after the cached user row is read",
    )
    args = parser.parse_args()

    if args.cache_max_rows <= 0:
        raise ValueError("cache-max-rows must be positive")

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    table_env.get_config().set("parallelism.default", "1")

    table_env.execute_sql(f"""
        CREATE TABLE clicks (
            event_type STRING,
            click_id STRING,
            view_id STRING,
            user_id INT,
            page_id STRING,
            event_time BIGINT,
            received_at BIGINT,
            event_time_iso STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.clicks_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'false',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    mysql_url = (
        f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/"
        f"{args.mysql_database}?useSSL=false&allowPublicKeyRetrieval=true"
    )
    table_env.execute_sql(f"""
        CREATE TABLE users (
            user_id INT,
            user_name STRING,
            country STRING,
            membership STRING,
            PRIMARY KEY (user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{mysql_url}',
            'table-name' = '{args.mysql_table}',
            'username' = '{args.mysql_user}',
            'password' = '{args.mysql_password}',
            'lookup.cache' = 'PARTIAL',
            'lookup.partial-cache.max-rows' = '{args.cache_max_rows}',
            'lookup.partial-cache.expire-after-write' = '{args.cache_expire_after_write}',
            'lookup.partial-cache.expire-after-access' = '{args.cache_expire_after_access}',
            'lookup.partial-cache.cache-missing-key' = 'false',
            'lookup.max-retries' = '3'
        )
    """)

    enriched_clicks = table_env.sql_query("""
        SELECT
            c.click_id,
            c.view_id,
            c.user_id,
            c.page_id,
            c.event_time,
            c.event_time_iso,
            u.user_name,
            u.country,
            u.membership
        FROM clicks AS c
        LEFT JOIN users FOR SYSTEM_TIME AS OF c.proc_time AS u
          ON c.user_id = u.user_id
    """)

    print(
        "Enriching clicks with MySQL JDBC lookup cache "
        f"(max_rows={args.cache_max_rows}, "
        f"expire_after_write={args.cache_expire_after_write}, "
        f"expire_after_access={args.cache_expire_after_access}):",
        flush=True,
    )
    enriched_clicks.execute().print()


if __name__ == "__main__":
    main()
