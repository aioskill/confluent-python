"""Write synthetic rows to an Apache Iceberg table with PyFlink SQL.

Install a Flink-compatible ``iceberg-flink-runtime`` JAR in the cluster
``lib`` directory with ``flinklibs/configure``. The warehouse must be writable
by the process running the job.
"""

import argparse
from datetime import datetime

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment


CATALOG_NAME = "iceberg_catalog"
DATABASE_NAME = "db"
TABLE_NAME = "my_table"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write synthetic data to an Apache Iceberg table."
    )
    parser.add_argument(
        "--warehouse",
        default="file:///tmp/flink-iceberg",
        help="Iceberg warehouse path (default: %(default)s)",
    )
    parser.add_argument(
        "--keep-table",
        action="store_true",
        help="Keep the existing table instead of dropping and recreating it.",
    )
    return parser.parse_args()


def create_iceberg_objects(
    table_env: TableEnvironment, warehouse: str, keep_table: bool
) -> None:
    table_env.execute_sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} WITH (
          'type' = 'iceberg',
          'catalog-type' = 'hadoop',
          'warehouse' = '{warehouse}'
        )
        """
    )
    table_env.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}"
    )
    if not keep_table:
        table_env.execute_sql(
            f"DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"
        )
    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME} (
            user_id STRING,
            event_time TIMESTAMP(3),
            attributes STRING
        ) WITH (
          'format-version' = '2',
          'write.upsert.enabled' = 'false'
        )
        """
    )


def main() -> None:
    args = parse_args()
    table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    create_iceberg_objects(table_env, args.warehouse, args.keep_table)

    rows = [
        ("user1", datetime(2023, 1, 1, 10, 0, 0), '{"action":"login"}'),
        ("user2", datetime(2023, 1, 2, 11, 0, 0), '{"action":"purchase"}'),
        ("user3", datetime(2023, 1, 3, 12, 30, 0), '{"item":"book","price":25.5}'),
        ("user1", datetime(2023, 1, 1, 10, 15, 0), '{"action":"logout"}'),
        ("user4", datetime(2023, 1, 4, 9, 0, 0), '{"source":"web","device":"mobile"}'),
    ]
    row_type = DataTypes.ROW([
        DataTypes.FIELD("user_id", DataTypes.STRING()),
        DataTypes.FIELD("event_time", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("attributes", DataTypes.STRING()),
    ])

    table_env.create_temporary_view(
        "source_view", table_env.from_elements(rows, row_type)
    )
    table_env.execute_sql(
        f"""
        INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}
        SELECT user_id, event_time, attributes FROM source_view
        """
    ).wait()
    print(
        f"Wrote {len(rows)} rows to "
        f"{CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"
    )

    # Read the committed Iceberg table back using SQL.
    print("Rows currently stored in Iceberg:")
    table_env.execute_sql(
        f"""
        SELECT user_id, event_time, attributes
        FROM {CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}
        ORDER BY event_time, user_id
        """
    ).print()


if __name__ == "__main__":
    main()
