"""Generate synthetic data and discard it with Flink SQL's blackhole sink.

The blackhole sink intentionally drops every record. It is useful when the
purpose of an example is to exercise source generation and job throughput
without producing console output or writing an output file.

Run locally:

    python ex11_datagen_blackhole.py

Run with the Flink CLI:

    flink run -py ex11_datagen_blackhole.py
"""

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--number-of-rows",
        type=int,
        default=1000,
        help="Number of rows to generate before the bounded job finishes",
    )
    parser.add_argument(
        "--rows-per-second",
        type=int,
        default=100,
        help="Maximum DataGen generation rate",
    )
    args = parser.parse_args()

    if args.number_of_rows <= 0:
        raise ValueError("number-of-rows must be positive")
    if args.rows_per_second <= 0:
        raise ValueError("rows-per-second must be positive")

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")

    table_env.execute_sql(f"""
        CREATE TABLE generated_events (
            event_id BIGINT,
            user_id INT,
            event_type STRING,
            event_ts TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '{args.number_of_rows}',
            'rows-per-second' = '{args.rows_per_second}',
            'fields.event_id.kind' = 'sequence',
            'fields.event_id.start' = '1',
            'fields.event_id.end' = '{args.number_of_rows}',
            'fields.user_id.min' = '1',
            'fields.user_id.max' = '100',
            'fields.event_type.kind' = 'random',
            'fields.event_type.length' = '8',
            'fields.event_ts.max-past' = '1 h'
        )
    """)

    # The blackhole sink consumes records without displaying or persisting
    # them. This makes it suitable for source and pipeline performance tests.
    table_env.execute_sql("""
        CREATE TABLE discarded_events (
            event_id BIGINT,
            user_id INT,
            event_type STRING,
            event_ts TIMESTAMP_LTZ(3)
        ) WITH ('connector' = 'blackhole')
    """)

    print(
        f"Generating {args.number_of_rows} rows and sending them to the "
        "blackhole sink...",
        flush=True,
    )
    table_env.execute_sql("""
        INSERT INTO discarded_events
        SELECT event_id, user_id, event_type, event_ts
        FROM generated_events
    """).wait()
    print("DataGen blackhole SQL example is complete", flush=True)


if __name__ == "__main__":
    main()
