"""
transaction_producer.py script generates synthetic transaction data and send to kafka topic.
    - update the script to generate timestamp over last 60 days (required for long range testing)

Using flink SQL write a job that will
1. Find count by each transaction category in last 24 hours
2. Find total number of transactions and total amount in rolling 24 hours period for each customer id
3. Find avg daily amount by each customer id based on last 30 days
4. If transaction amount over rolling 24hour exceeds the avg (from step 3) x 1.5 (tolerance threshold),
   create an alert record. (final output). The avg calculation for calculating the dynamic threshold
   must be using lagged data so that calculation does not threshold calculation does not include current period

Write the final output to kafka topic.

Use the following variables:
- window length
- sliding interval
- watermark
- tolerance (threshold)

Future enhancements:
- currency normalization
- threshold by transaction category
- customer age
- credit history
- geography
- cross border transaction flags
- PEP (Politically Exposed Person)



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
    parser.add_argument("--transactions-topic", default="transactions")
    parser.add_argument("--group-id", default="ex15-transactions-124")
    args = parser.parse_args()

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    config = table_env.get_config()
    config.set_local_timezone("UTC")
    config.set("table.exec.source.idle-timeout", "10second")

    tol = 2.0

    table_env.execute_sql(f"""
            CREATE TABLE transactions (
                id STRING,
                merchant_id STRING,
                customer_id STRING,
                amount float,
                category STRING,
                `timestamp` BIGINT,
                -- Convert epoch milliseconds (precision 3) to an event-time
                -- timestamp that Flink can use for windows and watermarks.
                timestamp_ts AS TO_TIMESTAMP_LTZ(`timestamp` * 1000, 3),
                -- Allow events to arrive up to 10 seconds out of order.
                WATERMARK FOR timestamp_ts AS timestamp_ts - INTERVAL '10' SECOND
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

    query1 = f"""
        WITH current_hour AS (
            SELECT
                window_start,
                window_end,
                window_time,
                customer_id,
                COUNT(*) AS transaction_count_current_hour,
                SUM(amount) AS total_amount_current_hour
            FROM TABLE(
                TUMBLE(
                    TABLE transactions,
                    DESCRIPTOR(timestamp_ts),
                    INTERVAL '1' HOUR
                )
            )
            GROUP BY window_start, window_end, window_time, customer_id
        ),
        customer_30d AS (
            SELECT
                window_end,
                window_time,
                customer_id,
                -- 30 days contain 720 hours. The current hour is excluded
                -- by joining this window to current_hour.window_start.
                SUM(amount) / 720.0 AS average_hourly_amount_30d
            FROM TABLE(
                HOP(
                    TABLE transactions,
                    DESCRIPTOR(timestamp_ts),
                    INTERVAL '1' HOUR,
                    INTERVAL '30' DAY
                )
            )
            GROUP BY window_end, window_time, customer_id
        )
        SELECT
            c.window_start,
            c.window_end,
            c.customer_id,
            c.transaction_count_current_hour,
            c.total_amount_current_hour,
            d.average_hourly_amount_30d,
            c.total_amount_current_hour > d.average_hourly_amount_30d * {tol}
                AS is_anomaly
        FROM current_hour AS c
        JOIN customer_30d AS d
          ON c.customer_id = d.customer_id
         AND c.window_start = d.window_end
        WHERE c.total_amount_current_hour > d.average_hourly_amount_30d * {tol}
        """

    table_env.execute_sql(query1).print()



if __name__ == "__main__":
    main()
