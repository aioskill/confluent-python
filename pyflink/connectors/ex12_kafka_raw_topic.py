"""Read raw Kafka records"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

from pyflink.table import EnvironmentSettings, TableEnvironment

table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())


create_table_statement = f"""
CREATE TABLE kafka_text_table (
    kafka_key STRING,
    kafka_value STRING,
    `topic` STRING METADATA FROM 'topic' VIRTUAL,
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    `timestamp-type` STRING METADATA FROM 'timestamp-type' VIRTUAL,
    `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    `headers` MAP<STRING, BYTES> METADATA FROM 'headers',
     WATERMARK FOR `kafka_timestamp` AS `kafka_timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'sa01:9092',
    {sql_options()}
    'properties.group.id' = 'text-consumer-group',
    'properties.enable.auto.commit' = 'false',
    
    -- Define Key Format as Plain Text
    'key.format' = 'raw',
    'key.fields' = 'kafka_key',

    -- Define Value Format as Plain Text
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY',
    
    'scan.startup.mode' = 'earliest-offset',
    'properties.enable.auto.commit' = 'false',
    'scan.watermark.idle-timeout' = '30 s'
)
"""

table_env.execute_sql(create_table_statement)

table_env.execute_sql(
    f"""
        SELECT *
        FROM kafka_text_table
    """
).print()
