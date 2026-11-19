"""Enrich the Kafka clicks stream with user data from MySQL using Flink SQL.

Prerequisites:

* The ``clicks`` Kafka topic exists and contains the JSON events produced by
  ``ex00_produce_page_views_and_clicks.py``.
* MySQL contains a ``users`` table keyed by ``user_id``. For example::

      CREATE TABLE users (
          user_id INT PRIMARY KEY,
          user_name VARCHAR(255),
          country VARCHAR(100),
          membership VARCHAR(50)
      );
      INSERT INTO users (user_id, user_name, country, membership) VALUES
          (1, 'Alice', 'India', 'premium'),
          (2, 'Bob', 'Germany', 'standard'),
          (3, 'Charlie', 'United States', 'premium'),
          (4, 'Diana', 'United Kingdom', 'standard'),
          (5, 'Eve', 'Australia', 'premium');

  The Flink JDBC connector and MySQL Connector/J must be available in Flink's
  ``lib`` directory. They are configured by ``flinklibs/configure`` in this
  repository.

This is a temporal lookup join. Each click looks up the current matching user
row in MySQL using the click's processing-time attribute. A missing user is
retained because the query uses ``LEFT JOIN``.

Example::

    python ex11_enrich_clicks_user.py \
        --mysql-host sa01

The job is unbounded and should be stopped with Ctrl-C after observing output.

The producer writes two representations of the user identifier: the JSON
payload contains ``"user_id": 1`` as an integer, while the Kafka record key is
the string ``"1"``. This SQL example reads the JSON payload, so ``user_id`` is
declared as ``INT`` and is joined to the integer MySQL primary key.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


CLICK_COLUMNS = """
    event_type STRING,
    click_id STRING,
    view_id STRING,
    user_id INT,
    page_id STRING,
    event_time BIGINT,
    received_at BIGINT,
    event_time_iso STRING,
    proc_time AS PROCTIME()
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--group-id", default="ex11-enrich-clicks-user")
    parser.add_argument("--mysql-host", default="sa01")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-database", default="training")
    parser.add_argument("--mysql-table", default="users")
    parser.add_argument("--mysql-user", default="training")
    parser.add_argument("--mysql-password", default="training321")
    args = parser.parse_args()

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    table_env.get_config().set("parallelism.default", "1")

    table_env.execute_sql(f"""
        CREATE TABLE clicks (
            {CLICK_COLUMNS}
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
            'password' = '{args.mysql_password}'
        )
    """)

    print("Enriching clicks with user data from MySQL:", flush=True)
    table_env.sql_query("""
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
        """).execute().print()

if __name__ == "__main__":
    main()
