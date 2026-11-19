import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment

"""

Make sure that the Kafka cluster is started and the topic is
created before executing this job.


/app/confluent/bin/kafka-topics  --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic transactions --delete
/app/confluent/bin/kafka-topics  --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic transactions --partitions 1 --replication-factor 1 --create


https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/kafka/
https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-agg/

"""


kafka_hosts = "sa01:9092"
topic = "transactions"
group_id = "test_group_13"


def main():

    # https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/table/table_environment/
    config = Configuration()
    config.set_string('execution.buffer-timeout', '10sec')
    config.set_string("execution.checkpointing.interval", "3second")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set_string("table.exec.source.idle-timeout", "10second")

    env_settings = (EnvironmentSettings
                    .new_instance()
                    .in_streaming_mode()
                    .build()

                    )
    table_env = TableEnvironment.create(env_settings)
    # table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "3second")

    # # set the restart strategy to "fixed-delay"
    # table_env.get_config().set("restart-strategy.type", "fixed-delay")
    # table_env.get_config().set("restart-strategy.fixed-delay.attempts", "3")
    # table_env.get_config().set("restart-strategy.fixed-delay.delay", "30s")
    #
    # # set the checkpoint mode to EXACTLY_ONCE
    # table_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
    # table_env.get_config().set("execution.checkpointing.interval", "3min")

    # set the statebackend type to "rocksdb", other available options are "filesystem" and "jobmanager"
    # you can also set the full qualified Java class name of the StateBackendFactory to this option
    # e.g. org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory
    table_env.get_config().set("state.backend.type", "rocksdb")

    # set the checkpoint directory, which is required by the RocksDB state backend
    table_env.get_config().set("execution.checkpointing.dir", "file:///tmp/checkpoints/")
    table_env.get_config().set("table.exec.source.idle-timeout", "5 s")
    # Set the local time zone for TIMESTAMP_LTZ if you plan to use it
    table_env.get_config().set_local_timezone("Asia/Kolkata")


    # table_env.get_config().set("table.exec.emit.early-fire.enabled", "true")
    # table_env.get_config().set("table.exec.emit.early-fire.delay", "1 s")
    # Prevents idle partitions from holding back watermarks
    # table_env.get_config().set("table.exec.source.idle-timeout", "30000")  # 30 seconds

    # Define Kafka source
    table_env.execute_sql(f"""
        CREATE TABLE transactions (
            transaction_id STRING,
            event_time TIMESTAMP(3) METADATA FROM 'timestamp',
            ts TIMESTAMP(6),
            sender_account_id STRING,
            receiver_account_id STRING,
            transaction_type STRING,
            amount DOUBLE,
            currency STRING,
            sender_bank STRING,
            receiver_bank STRING,
            location STRING,
            transaction_purpose STRING,
            is_suspicious BOOLEAN,
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{kafka_hosts}',
            {sql_options()}
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'properties.auto.offset.reset' = 'earliest',
            'json.ignore-parse-errors' = 'true'
        )
        """)
    # table_env.execute_sql("SHOW CREATE TABLE transactions").print()


    # Define sink (could be another Kafka topic, database, etc.)
    # table_env.execute_sql("""
    #     CREATE TABLE event_summary_sink (
    #         `amount` DOUBLE,
    #          window_start TIMESTAMP(3),
    #          window_end TIMESTAMP(3)
    #     ) WITH (
    #         'connector' = 'print'
    #     )
    #     """)

    # Execute pipeline
    # table_env.execute_sql("""
    #    INSERT INTO event_summary_sink
    #    SELECT
    #          transaction_type,
    #          count(1) `count`,
    #          sum(amount) amount,
    #          TUMBLE_START(`timestamp`, INTERVAL '5' MINUTES) AS window_start
    #    FROM transactions
    #    GROUP BY
    #             transaction_type,
    #             TUMBLE(`timestamp`, INTERVAL '5' MINUTES)
    #    """).wait()

    # Add watermark debug output
    # table_env.execute_sql("""
    # SELECT
    #     transaction_type,
    #     sum(amount),
    #     count(1)
    # FROM transactions
    # GROUP BY
    #     transaction_type
    # """).print()


    # table_env.execute_sql("""
    # SELECT
    #     transaction_type,
    #     amount,
    #     event_time,
    #     CURRENT_WATERMARK(event_time) AS current_wm
    # FROM transactions
    # WHERE
    #     CURRENT_WATERMARK(event_time) IS NOT NULL
    # """).print()


    # Processing time window
    table_env.execute_sql("""
        SELECT
             transaction_type,
             count(1) `count`,
             sum(amount) amount,
             TUMBLE_START(PROCTIME(), INTERVAL '10' SECOND) AS window_start,
             TUMBLE_END(PROCTIME(), INTERVAL '10' SECOND) AS window_start
        FROM transactions
        GROUP BY
            transaction_type,
            TUMBLE(PROCTIME(), INTERVAL '10' SECOND)
    """).print()


    """
    Window TVFs (Table-Valued Functions):
    
    This is a more modern and often more flexible way to define windows in Flink SQL, 
    particularly for more complex scenarios or when you want to directly 
    access window start/end times as columns.
    
    TUMBLE(time_attr, interval) - Tumbling Windows
    e.g. TUMBLE(event_time, INTERVAL '1' HOUR)
    
    TUMBLE_START(time_attr, interval): Returns the inclusive start timestamp of the tumbling window.
    TUMBLE_END(time_attr, interval): Returns the exclusive end timestamp of the tumbling window.
    
    HOP(time_attr, slide, size) - Hopping Windows:
    e.g. HOP(event_time, INTERVAL '30' MINUTE, INTERVAL '1' HOUR) for 1-hour windows that hop every 30 minutes.
    HOP_START(time_attr, slide, size): Returns the inclusive start timestamp.
    HOP_END(time_attr, slide, size): Returns the exclusive end timestamp.
    
    
    SESSION(time_attr, interval) - Session Windows:
    Dynamic windows defined by a period of inactivity (gap). 
    A session closes when no new events arrive for the specified interval.
    e.g. SESSION(event_time, INTERVAL '10' MINUTE) for sessions with a 10-minute inactivity gap.

    SESSION_START(time_attr, interval): Returns the inclusive start timestamp.
    SESSION_END(time_attr, interval): Returns the exclusive end timestamp.


    """

    # table_env.execute_sql("""
    #    SELECT
    #          transaction_type,
    #          count(1),
    #          sum(amount) as amount,
    #          TUMBLE_START(event_time, INTERVAL '3' SECOND) AS window_start,
    #          TUMBLE_END(event_time, INTERVAL '3' SECOND) AS window_start
    #    FROM
    #          transactions
    #    GROUP BY
    #          transaction_type,
    #          TUMBLE(event_time, INTERVAL '3' SECOND)
    #    """).print()

    print("Job is complete")

if __name__ == '__main__':
    main()