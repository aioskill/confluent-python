import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

from pyflink.table import EnvironmentSettings, TableEnvironment

"""

Make sure that the Kafka cluster is started and the topic is
created before executing this job.

/app/confluent/bin/kafka-topics  --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic transactions --partitions 3 --replication-factor 1 --create

Download the required jar files

flink-sql kafka connector
https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka/3.4.0-1.20

flink-json data handling
https://mvnrepository.com/artifact/org.apache.flink/flink-json/1.20.1

"""


kafka_hosts = "sa01:9092"
topic = "transactions"
group_id = "test_group_4"


def main():
    # Setup environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Define Kafka source
    table_env.execute_sql(f"""
        CREATE TABLE transactions (
            id STRING,
            merchant_id STRING,
            customer_id STRING,
            amount DOUBLE,
            category STRING,
            `timestamp` BIGINT,
            -- epoch seconds written by transaction_producer.py -> event time
            ts AS TO_TIMESTAMP_LTZ(`timestamp` * 1000, 3),
            WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{kafka_hosts}',
            {sql_options()}
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
        """)

    # # Define sink (could be another Kafka topic, database, etc.)
    # table_env.execute_sql("""
    #     CREATE TABLE event_summary_sink (
    #         transaction_id STRING,
    #         ts TIMESTAMP(3),
    #         sender_account_id STRING,
    #         receiver_account_id STRING,
    #         transaction_type STRING,
    #         amount DOUBLE,
    #         currency STRING,
    #         sender_bank STRING,
    #         receiver_bank STRING,
    #         location STRING,
    #         transaction_purpose STRING,
    #         is_suspicious BOOLEAN
    #     ) WITH (
    #         'connector' = 'print'
    #     )
    #     """)
    #
    # # Execute pipeline
    # table_env.execute_sql("""
    #    INSERT INTO event_summary_sink
    #    SELECT
    #          *
    #    FROM transactions
    #    """).wait()

    table_env.execute_sql("""
        select
            customer_id,
            category,
            amount,
            ts,
            CURRENT_WATERMARK(ts)
        from transactions
            where category LIKE 'pos'
    """).print()

    print("Job is complete")

if __name__ == '__main__':
    main()