"""Read the page_views and clicks Iceberg tables with Flink SQL.

The tables are created by ``ex17_kafka_to_iceberg.py`` in the ``events``
database. This is a bounded batch query: it reads the current committed
Iceberg snapshots and prints each table separately.

Local example::

    python ex18_query_iceberg_tables.py \
        --warehouse file:///tmp/flink-iceberg-events

The Iceberg warehouse must be a local path (``file:///...``). An ``s3://`` warehouse
is not supported by these JARs: Iceberg's file-based catalog loads its own Hadoop
``s3`` filesystem, which is not installed.
"""

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


CATALOG_NAME = "iceberg_catalog"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse",
        default="file:///tmp/flink-iceberg-events",
        help="Iceberg warehouse path (default: %(default)s)",
    )
    parser.add_argument(
        "--database",
        default="events",
        help="Iceberg database containing the tables (default: %(default)s)",
    )
    parser.add_argument(
        "--page-views-table",
        default="page_views",
        help="Page-views table name (default: %(default)s)",
    )
    parser.add_argument(
        "--clicks-table",
        default="clicks",
        help="Clicks table name (default: %(default)s)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows per table; 0 reads all rows (default: %(default)s)",
    )
    return parser.parse_args()


def create_catalog(table_env: TableEnvironment, warehouse: str) -> None:
    table_env.execute_sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = '{warehouse}'
        )
        """
    )
    table_env.execute_sql(f"USE CATALOG {CATALOG_NAME}")


def print_table(
    table_env: TableEnvironment,
    database: str,
    table_name: str,
    label: str,
    limit: int,
) -> None:
    limit_clause = f"LIMIT {limit}" if limit else ""
    print(f"\n{label} ({database}.{table_name}):", flush=True)
    table_env.sql_query(
        f"""
        SELECT
            event_type,
            user_id,
            page_id,
            event_id,
            event_time,
            received_at,
            event_time_iso,
            event_date
        FROM `{database}`.`{table_name}`
        ORDER BY event_time, event_id
        {limit_clause}
        """
    ).execute().print()


def main() -> None:
    args = parse_args()
    if args.limit < 0:
        raise ValueError("limit must be zero or positive")

    table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    table_env.get_config().set_local_timezone("UTC")
    create_catalog(table_env, args.warehouse)

    print_table(
        table_env,
        args.database,
        args.page_views_table,
        "Page views",
        args.limit,
    )
    print_table(
        table_env,
        args.database,
        args.clicks_table,
        "Clicks",
        args.limit,
    )


if __name__ == "__main__":
    main()
