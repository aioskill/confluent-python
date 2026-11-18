#!/usr/bin/env python3
"""Read a Tableflow table through Confluent Cloud's Iceberg REST catalog.

Tableflow turns a Kafka topic into an Apache Iceberg table. This script is a
small Iceberg client (PyIceberg): it connects to the catalog, prints the table
schema, and prints the rows. It never touches Kafka.

    pip install "pyiceberg[pyarrow]"
    python3 query_tableflow.py --api-key KEY --api-secret SECRET \
        --organization ORG_ID --environment env-xxxxxx \
        --cluster lkc-xxxxxx --topic tableflow-orders-s07
"""
import argparse

from pyiceberg.catalog import load_catalog


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--api-key", required=True, help="Tableflow API key")
    p.add_argument("--api-secret", required=True, help="Tableflow API secret")
    p.add_argument("--organization", required=True, help="Confluent Cloud organization ID")
    p.add_argument("--environment", required=True, help="environment ID (env-...)")
    p.add_argument("--cluster", required=True, help="Kafka cluster ID (lkc-...); the Iceberg namespace")
    p.add_argument("--topic", required=True, help="topic name; the Iceberg table name")
    p.add_argument("--region", default="ap-east-1", help="cloud region of the cluster")
    p.add_argument("--where", help='row filter, for example "amount >= 20"')
    p.add_argument("--all-columns", action="store_true",
                   help="also show Tableflow's metadata columns ($$offset, $$partition, ...)")
    args = p.parse_args()

    # Confluent Cloud serves one Iceberg REST catalog per environment. The
    # namespace is the Kafka cluster ID and each table is a topic.
    uri = (f"https://tableflow.{args.region}.aws.confluent.cloud/iceberg/catalog"
           f"/organizations/{args.organization}/environments/{args.environment}")
    catalog = load_catalog("tableflow", **{
        "type": "rest",
        "uri": uri,
        "credential": f"{args.api_key}:{args.api_secret}",
        # The storage bucket belongs to Confluent: the catalog hands out
        # short-lived credentials so this client can read the data files.
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    })

    table = catalog.load_table(f"{args.cluster}.{args.topic}")
    print(f"table {args.cluster}.{args.topic}, {len(table.snapshots())} snapshot(s)")
    print("schema:")
    for field in table.schema().fields:
        print(f"  {field.name}: {field.field_type}")

    scan = table.scan(row_filter=args.where) if args.where else table.scan()
    rows = scan.to_arrow().to_pylist()
    if not args.all_columns:
        rows = [{k: v for k, v in r.items() if not k.startswith("$$")} for r in rows]
    print(f"{len(rows)} row(s):")
    for row in sorted(rows, key=lambda r: str(r.get("order_id", ""))):
        print(" ", row)


if __name__ == "__main__":
    main()
