# Lab 6B: PostgreSQL Sink Connector on Your Own VM

In this lab the Kafka cluster is in Confluent Cloud, but the database and the
connector run on your own VM `sa01`. You take the `datagen-orders-<STUDENT_ID>`
topic that the Datagen connector fills in [Lab 6A](LAB-6A-MANAGED-CONNECTORS.md),
run a Kafka Connect worker on `sa01` with the JDBC sink connector, and write the
orders into a PostgreSQL table. Nothing connects *into* your VM: the worker makes
outbound connections to Confluent Cloud, which is why this pattern works for
databases that Cloud cannot reach (compare Lab 7, which uses managed connectors and
PrivateLink instead).

```
 Confluent Cloud                       Your VM sa01
+--------------------------+  outbound  +---------------------------------+
| datagen-orders-<ID>      | <--------- | Connect worker + JDBC sink      |
| Schema Registry          | <--------- |        |                        |
+--------------------------+            |        v  JDBC, localhost:5432  |
                                        | PostgreSQL, database lab6b      |
                                        +---------------------------------+
```

Replace `<STUDENT_ID>` with the short ID assigned by the instructor, as in Lab 6A. This
lab creates no topics: it only reads your Lab 6A topic. It uses the same class service
account and Kafka key as Lab 6A, whose permissions already cover reading
`datagen-orders-<STUDENT_ID>` and the consumer group `connect-pg-sink-<STUDENT_ID>`.

## Learning objectives

- Explain when to run a sink connector yourself instead of using a managed one.
- Configure a Connect worker on `sa01` to use Confluent Cloud and Cloud Schema Registry.
- Run the JDBC sink connector so that Avro records become rows in PostgreSQL, and see
  why a nested record needs the `Flatten` transform.

## Prerequisites

- Lab 6A completed up to the Datagen connector: `datagen-orders-<STUDENT_ID>` exists
  and has records (the connector may still be running; it keeps adding records).
- The class Kafka key and secret (`<KAFKA_API_KEY>`, `<KAFKA_API_SECRET>`) from the
  instructor, the same as in Lab 6A.
- A Schema Registry key and secret (`<SCHEMA_REGISTRY_API_KEY>`,
  `<SCHEMA_REGISTRY_API_SECRET>`). If you do not have one from Lab 6A, create one
  (the secret is shown only once):

  ```bash
  confluent api-key create --resource <SCHEMA_REGISTRY_CLUSTER_ID>
  ```

- The Cloud connection details, from the CLI on your workstation. Take the bootstrap
  endpoint without its `SASL_SSL://` prefix (`<CLOUD_BOOTSTRAP>`), and the Schema
  Registry endpoint URL (`<SCHEMA_REGISTRY_URL>`) and cluster ID:

  ```bash
  confluent environment use <ENVIRONMENT_ID>
  confluent kafka cluster describe <KAFKA_CLUSTER_ID>
  confluent schema-registry cluster describe
  ```

- A terminal on `sa01` (`ssh ubuntu@sa01`). Run all commands in the steps below there.

## 1. Install PostgreSQL and the JDBC connector

Install PostgreSQL, which also installs the `psql` client. It listens on `localhost`
only, which is enough because the connector runs on the same VM. Then create the
database and the login the connector will use:

```bash
sudo apt-get update
sudo apt-get install -y postgresql

sudo -u postgres psql \
  -c "CREATE USER lab6b WITH PASSWORD 'lab6b-pass'" \
  -c "CREATE DATABASE lab6b OWNER lab6b"

psql "host=localhost dbname=lab6b user=lab6b password=lab6b-pass" -c "select 1"
```

Expected: the last command prints one row with the value `1`.

Install the Confluent JDBC connector plugin into the directory the worker will load
plugins from:

```bash
sudo mkdir -p /app/confluent/share/confluent-hub-components
sudo /app/confluent/bin/confluent-hub install --no-prompt \
  --component-dir /app/confluent/share/confluent-hub-components \
  --worker-configs /dev/null \
  confluentinc/kafka-connect-jdbc:latest
```

Expected: the directory `confluentinc-kafka-connect-jdbc` appears in
`/app/confluent/share/confluent-hub-components`.

## 2. Configure the Connect worker

Create the worker configuration. It contains the Cloud keys, so restrict its
permissions first:

```bash
install -m 600 /dev/null ~/sink-worker.properties
```

Edit it to contain the following. A *standalone* worker is one process that runs the
connectors named on its command line; it keeps its offsets in a local file, so unlike
the distributed worker of Lab 6A it needs no internal topics in the cluster. The
worker and its consumer and admin clients each need the Cloud connection settings:

```properties
bootstrap.servers=<CLOUD_BOOTSTRAP>
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";
admin.security.protocol=SASL_SSL
admin.sasl.mechanism=PLAIN
admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=<SCHEMA_REGISTRY_URL>
value.converter.basic.auth.credentials.source=USER_INFO
value.converter.basic.auth.user.info=<SCHEMA_REGISTRY_API_KEY>:<SCHEMA_REGISTRY_API_SECRET>

offset.storage.file.filename=/home/ubuntu/sink.offsets
plugin.path=/app/confluent/share/confluent-hub-components
plugin.discovery=service_load
listeners=http://localhost:8083
```

`value.converter` turns the Avro bytes in the topic back into records with a schema,
using Cloud Schema Registry (the schema ID is in each message; Datagen registered the
schema in Lab 6A). `plugin.discovery=service_load` makes the worker find plugins from
their declared service files instead of scanning every class in every jar. The scan
needs much more memory than the 300 MB heap used below (it fails with
`OutOfMemoryError`), because the JDBC plugin also bundles drivers for other databases.

## 3. Configure the connector

Create `pg-sink.properties` (it holds the database password, so restrict it too):

```bash
install -m 600 /dev/null ~/pg-sink.properties
```

```properties
name=pg-sink-<STUDENT_ID>
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=datagen-orders-<STUDENT_ID>
connection.url=jdbc:postgresql://localhost:5432/lab6b
connection.user=lab6b
connection.password=lab6b-pass
table.name.format=orders
insert.mode=upsert
pk.mode=record_value
pk.fields=orderid
auto.create=true
transforms=flatten
transforms.flatten.type=org.apache.kafka.connect.transforms.Flatten$Value
transforms.flatten.delimiter=_
```

- `auto.create=true` creates the table `orders` from the Avro schema.
- `insert.mode=upsert` with `pk.mode=record_value` and `pk.fields=orderid` means a record
  with an existing `orderid` updates the row instead of adding one.
- The Datagen `address` field is a nested record, but a table row is flat. The `Flatten`
  transform turns `address.city` into the column `address_city`, and likewise for
  `address_state` and `address_zipcode`.
- The consumer group is `connect-pg-sink-<STUDENT_ID>`, which the class service account
  may use because it starts with `connect-`.

**How the table is created.** You do not create the table `orders`; the connector does,
because of `auto.create=true`. When the first batch of records arrives, the connector reads
the record's Avro schema (through the converter and Schema Registry), maps each field to a
PostgreSQL type (`long` to `bigint`, `int` to `integer`, `string` to `text`, `double` to
`double precision`), and runs `CREATE TABLE orders (...)`. The table name comes from
`table.name.format`, and the primary key from `pk.mode` and `pk.fields`. Three details:

- The table appears only after the worker has started and the task has its first records,
  so for the first minute `select ... from orders` can answer that the relation does not
  exist.
- The login `lab6b` can create it because it owns the database `lab6b`
  (`CREATE DATABASE lab6b OWNER lab6b` in step 1).
- `auto.create` only creates a *missing* table. If a table `orders` already exists with
  different columns, the connector does not change it and the writes can fail. This lab
  does not set `auto.evolve`, so fields added to the topic's schema later are not added as
  columns.

## 4. Run the worker and check the table

Start the worker in the foreground with a small heap. It prints a lot while it starts
and needs about a minute before it connects (it logs warnings about a broker-detection
topic first; they are harmless):

```bash
KAFKA_HEAP_OPTS="-Xms64m -Xmx300m" /app/confluent/bin/connect-standalone \
  ~/sink-worker.properties ~/pg-sink.properties
```

In a second terminal on `sa01`, check the connector through the worker's REST API and
read the table:

```bash
curl -s http://localhost:8083/connectors/pg-sink-<STUDENT_ID>/status

psql "host=localhost dbname=lab6b user=lab6b password=lab6b-pass" \
  -c '\d orders' \
  -c 'select * from orders order by orderid limit 3' \
  -c 'select count(*) from orders'
```

Expected: the connector and its task are `RUNNING`; the table has the columns
`ordertime`, `orderid`, `itemid`, `orderunits`, `address_city`, `address_state` and
`address_zipcode`, with `orderid` as the primary key; and the three rows are the
first orders (`orderid` 0, 1 and 2). Run the `count` again after a few seconds: while
the Datagen connector of Lab 6A is running, the number keeps growing.

## 5. Stop and restart the worker

Stop the worker with `Ctrl-C`. Note the row count, wait a minute, then start the worker
again with the same command. Check the count again.

The worker resumes from the offsets that the consumer group
`connect-pg-sink-<STUDENT_ID>` committed, so it neither loses records nor writes
them twice. (And if it did replay some, `upsert` would overwrite the same rows.)

Questions to answer from what you saw:

- What happened to the database while the worker was stopped, and what happened to the
  records produced meanwhile?
- Why can the database stay private while the topic is in the cloud?
- What would you change to run several tasks, or to survive the loss of the VM?

## Cleanup

Stop the worker with `Ctrl-C`, then on `sa01` remove the files that contain keys and
the database:

```bash
rm -f ~/sink-worker.properties ~/pg-sink.properties ~/sink.offsets
sudo -u postgres psql -c "DROP DATABASE lab6b" -c "DROP USER lab6b"
```

Do not delete the topic or the Datagen connector here; they belong to Lab 6A and are
removed in its cleanup. If you created a Schema Registry key only for this lab, delete
it with `confluent api-key delete <SCHEMA_REGISTRY_API_KEY>`.

## Instructor notes

- Nothing needs to be set up for this lab on the Cloud side. The permissions from Lab 6A
  (read on the prefix `datagen-orders-`, the group prefix `connect-`) cover it, and the
  students install everything else on their own VMs.
- To check a student's sink from the CLI, read the lag of its group:
  `confluent kafka consumer group lag summarize connect-pg-sink-<STUDENT_ID> --cluster <KAFKA_CLUSTER_ID>`
- If many students need a new Kafka key, rotate the shared one:
  `confluent api-key create --resource <KAFKA_CLUSTER_ID> --service-account <SERVICE_ACCOUNT_ID>`,
  hand it out, then `confluent api-key delete <OLD_KEY>`.
- Common student problems: no records in the table after two minutes (check the topic has
  records, and the worker log for `authorization` or `401` errors, which mean a wrong
  Kafka or Schema Registry key); `Connection refused` from `psql` (PostgreSQL is not
  running: `sudo systemctl status postgresql`); `OutOfMemoryError` (the line
  `plugin.discovery=service_load` is missing).

## Review questions

1. Why does this connector need no inbound network access to your VM?
2. What do `insert.mode=upsert`, `pk.mode`, and `pk.fields` do together?
3. Why does the nested `address` field need the `Flatten` transform?
4. When would you choose this over a fully managed connector with PrivateLink (Lab 7)?

## References

- [JDBC sink connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
- [Kafka Connect standalone mode](https://docs.confluent.io/platform/current/connect/userguide.html)
- [Flatten transform](https://docs.confluent.io/platform/current/connect/transforms/flatten.html)
