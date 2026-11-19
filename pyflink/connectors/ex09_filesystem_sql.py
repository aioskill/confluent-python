"""Write synthetic data to files and read it back using Flink SQL.

The path can be local (``file:///tmp/pyflink-sql-synthetic``) or S3
(``s3://my-bucket/pyflink-sql-synthetic``). On a cluster with more than one
machine, use S3 so that every TaskManager sees the same files. S3 needs the
``flink-s3-fs-hadoop`` plugin and S3 credentials in the Flink configuration.

Example::

    python ex09_filesystem_sql.py --path file:///tmp/pyflink-sql-synthetic

The output is a directory containing CSV part files. Remove the old output
directory before rerunning, or provide a new path.

Checkpoint storage is configured at ``--checkpoint-dir``. Flink creates the
job-specific subdirectories below that directory automatically.
"""

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


SYNTHETIC_DATA = [
    (1, "laptop", "electronics", 899.99, 2),
    (2, "headphones", "electronics", 79.50, 8),
    (3, "coffee-maker", "kitchen", 49.95, 4),
    (4, "desk-lamp", "home", 29.99, 6),
    (5, "notebook", "stationery", 6.75, 15),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        default="file:///tmp/pyflink-sql-synthetic",
        help="Directory (file:// or s3://) used for the CSV files",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/pyflink-training/ex09-checkpoints",
        help="Checkpoint directory (file:// or s3://); Flink adds the job ID below it",
    )
    parser.add_argument(
        "--rocksdb-dir",
        default="/tmp/pyflink-training/ex09-rocksdb",
        help="Local RocksDB state directory",
    )
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    args = parser.parse_args()

    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")

    # The write phase is a streaming job so checkpointing is active. RocksDB
    # stores working state locally; checkpoints are persisted to --checkpoint-dir.
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    configuration = table_env.get_config().get_configuration()
    configuration.set_string("parallelism.default", "1")
    configuration.set_string("state.backend.type", "rocksdb")
    configuration.set_string("state.backend.rocksdb.localdir", args.rocksdb_dir)
    configuration.set_string("state.checkpoints.dir", args.checkpoint_dir)
    configuration.set_string(
        "execution.checkpointing.interval",
        f"{args.checkpoint_interval_ms}ms",
    )
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")

    # Generate a small bounded table without an external data source.
    synthetic_table = table_env.from_elements(
        SYNTHETIC_DATA,
        ["product_id", "product_name", "category", "unit_price", "quantity"],
    )
    table_env.create_temporary_view("synthetic_products", synthetic_table)

    # Write the synthetic table to files using the filesystem SQL connector.
    table_env.execute_sql(f"""
        CREATE TABLE products_sink (
            product_id BIGINT,
            product_name STRING,
            category STRING,
            unit_price DOUBLE,
            quantity BIGINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{args.path}',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '30 min'
        )
    """)

    print(f"Writing synthetic products to: {args.path}", flush=True)
    table_env.execute_sql("""
        INSERT INTO products_sink
        SELECT product_id, product_name, category, unit_price, quantity
        FROM synthetic_products
    """).wait()

    # Read the completed files in a separate bounded environment. A streaming
    # filesystem source would keep monitoring the directory for newly discovered files.
    read_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    read_env.get_config().set("parallelism.default", "1")
    read_env.execute_sql(f"""
        CREATE TABLE products (
            product_id BIGINT,
            product_name STRING,
            category STRING,
            unit_price DOUBLE,
            quantity BIGINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{args.path}',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'csv.ignore-parse-errors' = 'false'
        )
    """)
    read_env.execute_sql("""
        CREATE TABLE console_sink (
            product_id BIGINT,
            product_name STRING,
            category STRING,
            unit_price DOUBLE,
            quantity BIGINT
        ) WITH ('connector' = 'print')
    """)

    print("Reading products back from the files:", flush=True)
    read_env.execute_sql("""
        INSERT INTO console_sink
        SELECT product_id, product_name, category, unit_price, quantity
        FROM products
    """).wait()

    print("Filesystem SQL read/write job is complete", flush=True)


if __name__ == "__main__":
    main()
