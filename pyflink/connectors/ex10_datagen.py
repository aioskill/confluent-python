"""Generate synthetic rows with Flink SQL's built-in DataGen connector.

The DataGen connector is useful for demos and local testing because it does
not need an external source. This example creates 20 rows, prints them, and
then exits. Remove ``number-of-rows`` from the source table if an unbounded
synthetic stream is required.

Run locally:

    python ex10_datagen.py

Run with the Flink CLI:

    flink run -py ex10_datagen.py

The DataGen connector is included with Flink's Table API distribution; no
additional connector JAR is required for this example.
"""

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--number-of-rows",
        type=int,
        default=20,
        help="Number of rows to generate before the bounded job finishes",
    )
    parser.add_argument(
        "--rows-per-second",
        type=int,
        default=5,
        help="Maximum generation rate",
    )
    args = parser.parse_args()

    if args.number_of_rows <= 0:
        raise ValueError("number-of-rows must be positive")
    if args.rows_per_second <= 0:
        raise ValueError("rows-per-second must be positive")

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    table_env.get_config().set("parallelism.default", "1")

    # order_id is deterministic because it uses a sequence generator. The
    # remaining numeric fields are generated randomly within their ranges.
    table_env.execute_sql(f"""
        CREATE TABLE generated_orders (
            order_id BIGINT,
            user_id INT,
            product_id INT,
            amount DOUBLE,
            event_ts TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '{args.number_of_rows}',
            'rows-per-second' = '{args.rows_per_second}',
            'fields.order_id.kind' = 'sequence',
            'fields.order_id.start' = '1',
            'fields.order_id.end' = '{args.number_of_rows}',
            'fields.user_id.min' = '1',
            'fields.user_id.max' = '5',
            'fields.product_id.min' = '1',
            'fields.product_id.max' = '10',
            'fields.amount.min' = '10.0',
            'fields.amount.max' = '500.0',
            'fields.event_ts.max-past' = '1 h'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE console_sink (
            order_id BIGINT,
            user_id INT,
            product_id INT,
            amount DOUBLE,
            event_ts TIMESTAMP_LTZ(3)
        ) WITH ('connector' = 'print')
    """)

    print(
        f"Generating {args.number_of_rows} rows at up to "
        f"{args.rows_per_second} rows/second:",
        flush=True,
    )
    table_env.execute_sql("""
        INSERT INTO console_sink
        SELECT order_id, user_id, product_id, amount, event_ts
        FROM generated_orders
    """).wait()
    print("DataGen SQL example is complete", flush=True)


if __name__ == "__main__":
    main()
