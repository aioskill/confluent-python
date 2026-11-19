"""Read raw Kafka records for a bounded time range and then stop.

The start and end values are lookback durations relative to script launch.
By default, the script reads the previous ten minutes and ends at launch time.
The Kafka source uses timestamp-based startup and bounded consumption, so the
SQL query finishes after the requested range has been processed.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse
import re
from datetime import datetime, timedelta, timezone

from pyflink.table import EnvironmentSettings, TableEnvironment


def duration_to_timedelta(value: str) -> timedelta:
    """Parse a duration such as 30s, 10m, 2h, or 1d."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", value.strip().lower())
    if not match:
        raise ValueError("duration must look like 30s, 10m, 2h, or 1d")
    amount = float(match.group(1))
    unit = match.group(2)
    seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return timedelta(seconds=amount * seconds_per_unit[unit])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Kafka raw messages within a bounded timestamp range."
    )
    parser.add_argument(
        "--start",
        default="10m",
        help=(
            "Look back duration from launch for the start time "
            "(default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--end",
        default="0s",
        help=(
            "Look back duration from launch for the end time; 0s means launch "
            "time (default: %(default)s)"
        ),
    )
    args = parser.parse_args()
    try:
        start_delta = duration_to_timedelta(args.start)
        end_delta = duration_to_timedelta(args.end)
    except ValueError as error:
        parser.error(str(error))

    if start_delta <= end_delta:
        parser.error("--start must be a larger lookback duration than --end")

    launch_time = datetime.now(timezone.utc)
    start_time = launch_time - start_delta
    end_time = launch_time - end_delta
    args.start_millis = int(start_time.timestamp() * 1000)
    args.end_millis = int(end_time.timestamp() * 1000)
    return args


args = parse_args()


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
    -- Start at the first Kafka record in the requested launch-relative range.
    'scan.startup.mode' = 'timestamp',
    'scan.startup.timestamp-millis' = '{args.start_millis}',
    -- Stop when the source reaches the Kafka offsets at the end time.
    'scan.bounded.mode' = 'timestamp',
    'scan.bounded.timestamp-millis' = '{args.end_millis}',

    -- Define Key Format as Plain Text
    'key.format' = 'raw',
    'key.fields' = 'kafka_key',

    -- Define Value Format as Plain Text
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY'
)
"""

table_env.execute_sql(create_table_statement)

table_env.execute_sql(
    f"""
    SELECT *
    FROM kafka_text_table
    WHERE kafka_timestamp >= TO_TIMESTAMP_LTZ({args.start_millis}, 3)
      AND kafka_timestamp < TO_TIMESTAMP_LTZ({args.end_millis}, 3)
    """
).print()
