# Lab 8: Tableflow - Kafka Topics as Apache Iceberg Tables

Tableflow turns a Kafka topic into an Apache Iceberg table that analytics tools
read directly, with no consumer, no connector, and no ETL job. In this lab you
produce Avro orders to a topic, enable Tableflow on it, and read the table with
an Iceberg client (PyIceberg) through Confluent Cloud's Iceberg REST catalog.
Then you evolve the schema and watch a new column appear in the table.

Use a sandbox organization. All students share one Kafka cluster, one Schema
Registry and one Iceberg catalog. Replace `<STUDENT_ID>` with the short ID
assigned by the instructor (lowercase letters and digits, for example `s07`) in
every name below. The topic name ends with `-<STUDENT_ID>`, and the instructor
gave your service account permissions only on that exact name. Tableflow
materialization and Confluent-managed storage are billed while a topic is
enabled, so finish the Cleanup section.

## Learning objectives

- Explain what Tableflow does and how a topic's schema becomes a table schema.
- Enable Tableflow on a topic with the CLI and read the materialization status.
- Read the resulting Iceberg table with a client that never talks to Kafka.
- Explain the extra columns Tableflow adds, and what happens when the schema
  evolves.
- Explain the difference between Confluent-managed storage and bring-your-own
  storage.

## How it works

```
producer --> topic (Avro) --+--> Schema Registry (schema)
                            |
                            v
                       Tableflow  --> Parquet data files + Iceberg metadata
                                       in object storage (S3)
                                              ^
                       Iceberg REST catalog --+-- read by PyIceberg, Spark,
                       (Tableflow API key)        Trino, DuckDB, Snowflake...
```

- Tableflow needs a **schema** for the topic, from Schema Registry (Avro, JSON
  Schema or Protobuf). The schema becomes the table's columns.
- Records are written as **Parquet** files, and Iceberg **metadata** (snapshots)
  tracks them. Each commit is a new snapshot, so a reader always sees a
  consistent table.
- With **Confluent-managed storage**, which this lab uses, Confluent owns the
  bucket and the catalog hands a reader short-lived credentials for it. With
  **bring-your-own storage** the files go to your own S3 bucket through an IAM
  role (a *provider integration*), and the catalog can also be synced to AWS
  Glue or Snowflake. This lab does not use those; see the references.
- Tableflow adds **metadata columns** to every table, named with a `$$` prefix,
  such as `$$topic`, `$$partition`, `$$offset` and `$$timestamp`. They let you
  trace any row back to its Kafka record.

## Prerequisites

- Confluent CLI installed and logged in (`confluent login`).
- A shared Kafka cluster and Schema Registry in the class environment, the
  environment ID, the cluster ID, and the class Kafka credentials file from the
  instructor (see "Instructor setup" below). The cluster's region must support
  Tableflow.
- A Confluent Cloud login that may enable Tableflow topics and create a
  Tableflow API key in the environment. If a command below is refused, ask the
  instructor.
- Python 3.9 or newer, with the `confluent-cloud` directory of this repository
  as your working directory (it contains `order.avsc`, `order_v2.avsc` is
  created below, and `query_tableflow.py`).

### Instructor setup (once)

Lab 8 uses the class service account and Kafka key from
[Lab 6A](LAB-6A-MANAGED-CONNECTORS.md#instructor-setup-kafka-access-once). Add one
ACL, once, for the topics of this lab, which are all named
`tableflow-orders-<STUDENT_ID>`:

```bash
confluent kafka acl create --allow --service-account <SERVICE_ACCOUNT_ID> \
  --environment <ENVIRONMENT_ID> --cluster <KAFKA_CLUSTER_ID> \
  --operations read,write,create,describe,describe-configs,alter-configs,delete \
  --topic "tableflow-orders-" --prefix
```

### Student setup

The class Kafka credentials file contains `kafka_api_key` and `kafka_api_secret`;
they work on topics named `tableflow-orders-<anything>`, so use only
`tableflow-orders-<STUDENT_ID>`. Select the environment and cluster, and
get the Schema Registry details:

```bash
confluent environment use <ENVIRONMENT_ID>
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent schema-registry cluster describe
```

Create your own Schema Registry key (the secret is shown only once; save it as
`<SCHEMA_REGISTRY_API_KEY>` and `<SCHEMA_REGISTRY_API_SECRET>`), and note your
organization ID for step 3:

```bash
confluent api-key create --resource <SCHEMA_REGISTRY_CLUSTER_ID>
confluent organization describe
```

Install the Iceberg client in a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/pip install "pyiceberg[pyarrow]"
```

## 1. Create the topic and produce Avro orders

Create the topic, and register the schema that becomes the table's columns.
The subject for the topic's values is `tableflow-orders-<STUDENT_ID>-value`:

```bash
confluent kafka topic create tableflow-orders-<STUDENT_ID> --partitions 1

confluent schema-registry schema create \
  --subject tableflow-orders-<STUDENT_ID>-value \
  --schema order.avsc \
  --type avro
```

Produce three orders. Enter one JSON record per line, then press `Ctrl-D`:

```bash
confluent kafka topic produce tableflow-orders-<STUDENT_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --value-format avro \
  --schema order.avsc \
  --schema-registry-endpoint <SCHEMA_REGISTRY_ENDPOINT> \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

```json
{"order_id":"order-1","customer_id":"customer-1","amount":12.5,"status":"NEW"}
{"order_id":"order-2","customer_id":"customer-2","amount":30.0,"status":"PAID"}
{"order_id":"order-3","customer_id":"customer-1","amount":7.25,"status":"NEW"}
```

## 2. Enable Tableflow on the topic

```bash
confluent tableflow topic enable tableflow-orders-<STUDENT_ID> \
  --storage-type MANAGED \
  --table-formats ICEBERG
```

The output shows the settings Tableflow chose. Read them again, and watch the
phase:

```bash
confluent tableflow topic describe tableflow-orders-<STUDENT_ID>
```

`Phase` starts as `PENDING` and becomes `RUNNING` within a minute or two. Note
the values that matter:

- `Storage Type MANAGED`: Confluent owns the bucket in `Table Path`.
- `Table Formats ICEBERG`: the table format (Delta Lake is the alternative).
- `Error Handling SUSPEND`: a record that does not match the schema stops the
  materialization (`SKIP` drops it, `LOG` sends it to a topic you name).
- `Retention Ms`: how long old snapshots are kept (7 days by default).

## 3. Read the table with an Iceberg client

The catalog is a different service from Kafka, so it has its own credentials.
Create a Tableflow API key (it is tied to your login, not to a cluster) and
save the key and secret as `<TABLEFLOW_API_KEY>` and `<TABLEFLOW_API_SECRET>`:

```bash
confluent api-key create --resource tableflow
```

Read the table. `query_tableflow.py` is a short PyIceberg program (read its
comments): it connects to the environment's Iceberg REST catalog, loads the
table named after the topic, and prints the schema and rows. The first rows can
take a few minutes to appear after the phase turns `RUNNING`; repeat the command
until it shows three rows:

```bash
.venv/bin/python query_tableflow.py \
  --api-key <TABLEFLOW_API_KEY> \
  --api-secret <TABLEFLOW_API_SECRET> \
  --organization <ORGANIZATION_ID> \
  --environment <ENVIRONMENT_ID> \
  --cluster <KAFKA_CLUSTER_ID> \
  --topic tableflow-orders-<STUDENT_ID>
```

Expected: the table has the columns of `order.avsc` (`order_id`, `customer_id`,
`amount`, `status`, plus a `key` column) followed by the `$$` metadata columns,
and three rows. Nothing in this program used Kafka, a consumer group or your
Kafka key: the data came from Parquet files through the catalog.

Show the metadata columns, which tie each row to its record, and filter with a
predicate that the Iceberg client pushes down to the files:

```bash
.venv/bin/python query_tableflow.py \
  --api-key <TABLEFLOW_API_KEY> \
  --api-secret <TABLEFLOW_API_SECRET> \
  --organization <ORGANIZATION_ID> \
  --environment <ENVIRONMENT_ID> \
  --cluster <KAFKA_CLUSTER_ID> \
  --topic tableflow-orders-<STUDENT_ID> \
  --where "amount >= 20" \
  --all-columns
```

Expected: only `order-2`, with `$$topic`, `$$partition` 0, and `$$offset` 1.
(The catalog lists every student's table, because the Tableflow key is not
limited to your topic.)

## 4. Evolve the schema

Tableflow follows the topic's schema. Add an optional field with a default
(a backward-compatible change, see [Lab 4](LAB-4-CONFLUENT_CLI.md)). Create
`order_v2.avsc`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.confluent",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": "string"},
    {"name": "coupon", "type": ["null", "string"], "default": null}
  ]
}
```

Produce a record that uses the new field. The producer registers version 2 of
the schema by itself. In an Avro union, a non-null value is written as
`{"string": "..."}`:

```bash
confluent kafka topic produce tableflow-orders-<STUDENT_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --value-format avro \
  --schema order_v2.avsc \
  --schema-registry-endpoint <SCHEMA_REGISTRY_ENDPOINT> \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

```json
{"order_id":"order-4","customer_id":"customer-3","amount":99.0,"status":"NEW","coupon":{"string":"SAVE10"}}
```

Run the step 3 read command again after a few minutes. Expected: the table
gains a `coupon` column, `order-4` has `SAVE10`, and the three older rows have
`None` for `coupon`. The table also has more snapshots: every batch of
records Tableflow commits is one.

Questions to answer from what you saw:

- Why did the old rows not need rewriting when the column was added?
- What would a breaking schema change (for example `amount` as a string) do to
  the materialization? (See `Error Handling` in step 2.)
- Which credentials did each command use: Kafka key, Schema Registry key,
  Tableflow key, and your login?

## Cleanup

Disable Tableflow on the topic first, then delete the topic (answer `y` at each
confirmation prompt, or add `--force`). Disabling stops
the materialization and removes the table from the catalog:

```bash
confluent tableflow topic disable tableflow-orders-<STUDENT_ID>
confluent kafka topic delete tableflow-orders-<STUDENT_ID> --force
```

Delete the schema subject, first softly and then permanently (permanent
deletion cannot be undone), your local files, and the keys created for this lab:

```bash
confluent schema-registry schema delete \
  --subject tableflow-orders-<STUDENT_ID>-value --version all
confluent schema-registry schema delete \
  --subject tableflow-orders-<STUDENT_ID>-value --version all --permanent
rm -rf .venv order_v2.avsc

confluent api-key list
confluent api-key delete <SCHEMA_REGISTRY_API_KEY>
confluent api-key delete <TABLEFLOW_API_KEY>
```

## Review questions

1. What does Tableflow need from Schema Registry, and what happens to the table
   when the topic's schema evolves?
2. Why can the Iceberg client read the table without a Kafka consumer, and who
   pays for the storage in each of the two storage modes?
3. What do the `$$` metadata columns let you do?
4. When would you choose bring-your-own storage and a catalog integration over
   Confluent-managed storage?

## References

- [Tableflow in Confluent Cloud](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html)
- [Storage with Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-storage.html)
- [Integrate catalogs with Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/catalog-integration/overview.html)
- [Confluent CLI `tableflow` commands](https://docs.confluent.io/confluent-cli/current/command-reference/tableflow/index.html)
- [PyIceberg](https://py.iceberg.apache.org/)
