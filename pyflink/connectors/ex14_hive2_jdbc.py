"""Write to and read from Hive using Flink SQL and HiveCatalog.

HiveServer2 at ``sa01:10000`` is a JDBC endpoint for clients such as Beeline.
Flink's JDBC connector does not provide a Hive dialect, so a
``jdbc:hive2://...`` URL cannot be used as a Flink SQL JDBC table. Flink
connects to Hive through the Hive Metastore instead, using ``hive-site.xml``
and ``HiveCatalog``.

Install a Flink Hive connector compatible with Flink 2.3 and Hive 2.3 on the
local PyFlink installation and every cluster node. The Flink 2.3 Hive
connector is not available from Maven Central, so obtain/build the connector
and place it, together with the matching Hive runtime dependencies, in
``flinklibs/lib`` (or the Flink ``lib`` directory). Run the normal dependency
setup for the other connectors with::

    bash flinklibs/configure

Do not add the HiveServer2 JDBC driver as a replacement: Flink's JDBC
connector does not have a Hive dialect.

The Hive version in ``flinklibs/pom.xml`` should match the Hive installation.
The ``--hive-conf-dir`` directory must contain ``hive-site.xml`` with the Hive
Metastore URI, commonly ``thrift://sa01:9083``. HiveServer2 port ``10000`` is
not used by this Flink SQL example.

Run locally::

    python ex14_hive2_jdbc.py \\
        --hive-conf-dir /etc/hive/conf
"""

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


ROWS = [
    (1, "Ada", 98.5),
    (2, "Grace", 91.0),
    (3, "Linus", 87.5),
]


def create_hive_catalog(table_env: TableEnvironment, hive_conf_dir: str) -> None:
    table_env.execute_sql(
        f"""
        CREATE CATALOG hive_catalog WITH (
            'type' = 'hive',
            'default-database' = 'default',
            'hive-conf-dir' = '{hive_conf_dir}'
        )
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hive-conf-dir",
        default="/etc/hive/conf",
        help="Directory containing hive-site.xml (default: %(default)s)",
    )
    parser.add_argument("--database", default="training")
    parser.add_argument("--table-name", default="flink_sql_users")
    args = parser.parse_args()

    table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    create_hive_catalog(table_env, args.hive_conf_dir)
    table_env.execute_sql("USE CATALOG hive_catalog")
    table_env.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS `{args.database}`"
    )
    table_env.execute_sql(f"USE `{args.database}`")
    table_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS `{args.table_name}` (
            user_id INT,
            user_name STRING,
            score DOUBLE
        ) WITH ('connector' = 'hive')
    """)

    # Write a bounded synthetic table through a Flink SQL INSERT.
    synthetic_users = table_env.from_elements(
        ROWS, ["user_id", "user_name", "score"]
    )
    table_env.create_temporary_view("synthetic_users", synthetic_users)

    remote_table = f"{args.database}.{args.table_name}"
    print(f"Writing {len(ROWS)} rows to Hive table {remote_table}", flush=True)
    table_env.execute_sql(f"""
        INSERT INTO `{args.table_name}`
        SELECT user_id, user_name, score
        FROM synthetic_users
    """).wait()

    # Read the rows back from the HiveCatalog table.
    print(f"Reading rows from Hive table {remote_table}:", flush=True)
    table_env.sql_query(f"""
        SELECT user_id, user_name, score
        FROM `{args.table_name}`
        ORDER BY user_id
    """).execute().print()


if __name__ == "__main__":
    main()
