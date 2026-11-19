import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--transactions-topic", default="transactions")
    parser.add_argument("--group-id", default="ex15-transactions-125")
    parser.add_argument("--output-path", default="/tmp/ex15-transactions-json/")
    args = parser.parse_args()

    output_path = args.output_path

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    config = table_env.get_config()
    config.set_local_timezone("UTC")
    config.set("table.exec.source.idle-timeout", "10second")
    config.set("execution.checkpointing.interval", "10 s")
    config.set("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set("execution.checkpointing.storage", "filesystem")
    config.set(
        "state.checkpoints.dir",
        "file:///tmp/ex15-transaction-checkpoints",
    )

    table_env.execute_sql(f"""
            CREATE TABLE transactions (
                id STRING,
                merchant_id STRING,
                customer_id STRING,
                amount float,
                category STRING,
                `timestamp` BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{args.transactions_topic}',
                'properties.bootstrap.servers' = '{args.bootstrap_servers}',
                {sql_options()}
                'properties.group.id' = '{args.group_id}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.ignore-parse-errors' = 'true',
                'properties.enable.auto.commit' = 'false',
                'scan.watermark.idle-timeout' = '30 s'
            )
        """)

    table_env.execute_sql(f"""
    CREATE TABLE json_sink (
        id STRING,
        merchant_id STRING,
        customer_id STRING,
        amount float,
        category STRING,
        `timestamp` BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.map-null-key.mode' = 'DROP',
        'json.encode.decimal-as-plain-number' = 'true',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '1 min',
        'sink.partition-commit.policy.kind' = 'success-file'
    )
    """)

    table_env.execute_sql("""
        INSERT INTO json_sink SELECT * FROM transactions
    """).wait()



if __name__ == "__main__":
    main()
