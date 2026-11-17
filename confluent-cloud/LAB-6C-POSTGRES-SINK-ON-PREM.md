# Lab 6C: PostgreSQL Sink Connector in the Data Center

In this lab the Kafka cluster is in Confluent Cloud, but the database and the
connector are in your own data center (the lab hosts `sa01` to `sa03`). You
produce Avro records to a Cloud topic, run a Kafka Connect worker on a lab host
that reads the topic, and the JDBC sink connector writes the records to a
PostgreSQL database on `sa01`. Nothing connects *into* the data center: the
worker makes outbound connections to Confluent Cloud, which is why this pattern
works for databases that Cloud cannot reach (compare Lab 7, which uses managed
connectors and PrivateLink instead).

```
 Confluent Cloud                              Lab hosts (data center)
+---------------------------+   outbound   +-----------------------------------+
| Kafka topic               | <----------- | Connect worker  (sa01, sa02 or    |
|   <STUDENT_ID>-pg-orders  |   TLS 9092   |   sa03: JDBC sink connector)      |
| Schema Registry           | <----------- |        |                          |
+---------------------------+   HTTPS 443  |        v  JDBC, port 5432         |
                                           | PostgreSQL on sa01, database      |
                                           |   lab6c, schema <STUDENT_ID>      |
                                           +-----------------------------------+
```

Replace `<STUDENT_ID>` with the short ID assigned by the instructor (lowercase
letters and digits, for example `s07`) in every name below. This lab uses the
same service account, Kafka key, and shared cluster as
[Lab 6B](LAB-6B-MANAGED-CONNECTORS.md), but you can do it on its own.

## Learning objectives

- Explain when to run a sink connector yourself instead of using a managed one.
- Configure a Connect worker on a lab host to use Confluent Cloud and Cloud
  Schema Registry.
- Run the JDBC sink connector so that Avro records become rows in PostgreSQL,
  and see how `upsert` and `auto.evolve` behave.

## Prerequisites

- Confluent CLI installed and logged in (`confluent login`).
- The shared environment and cluster, and your private credentials file from the
  instructor (`kafka_api_key`, `kafka_api_secret`), exactly as in Lab 6B.
- Your PostgreSQL settings file from the instructor (`db_host`, `db_name`,
  `db_user`, `db_password`).
- SSH access to the lab host assigned to you (`<LAB_HOST>`: `sa01`, `sa02`, or
  `sa03`) and a worker port (`<WORKER_PORT>`, for example 8091 plus your student
  number) so that workers on a shared host do not collide.
- The `confluent-cloud` directory of this repository as your working directory
  (it contains `order.avsc`).

### Instructor setup

Once, from a workstation with passwordless `ssh ubuntu@sa0N` and `sudo`, run:

```bash
./lab6c_setup.sh init
```

It installs PostgreSQL on `sa01` (listening on the lab network only, for the
database `lab6c`), and installs the `psql` client and the Confluent JDBC
connector plugin (`confluent-hub install confluentinc/kafka-connect-jdbc`) under
`/app/confluent/share/confluent-hub-components` on `sa01`, `sa02`, and `sa03`.
Then, for each student:

```bash
./lab6c_setup.sh create <STUDENT_ID>
```

This creates the database login `<STUDENT_ID>` and a schema of the same name that
the login owns, and writes the settings to `lab6c-students/<STUDENT_ID>.txt`
(private, git-ignored). The Kafka service account also needs the consumer-group
permission that `lab6_setup_student.sh` grants for this lab (prefix
`connect-<STUDENT_ID>-`); re-run its `create` for students set up before that was
added. After class, run `./lab6c_setup.sh delete <STUDENT_ID>`. Each worker is a
Java process of roughly 300 MB, so spread students over the three hosts.

### Student setup

Select the environment and cluster, and collect the connection details:

```bash
confluent environment use <ENVIRONMENT_ID>
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent kafka cluster describe
confluent schema-registry cluster describe
```

From the first, note the bootstrap endpoint without its `SASL_SSL://` prefix
(`<CLOUD_BOOTSTRAP>`). From the second, note the Schema Registry endpoint
(`<SCHEMA_REGISTRY_URL>`) and cluster ID. If you do not already have a Schema
Registry key from Lab 6B, create one (the secret is shown only once):

```bash
confluent api-key create --resource <SCHEMA_REGISTRY_CLUSTER_ID>
```

## 1. Produce Avro records to a Cloud topic

Create the topic and send three orders. `order.avsc` defines the fields
`order_id`, `customer_id`, `amount`, and `status`. The producer registers it in
Schema Registry under the subject `<STUDENT_ID>-pg-orders-value`:

```bash
confluent kafka topic create <STUDENT_ID>-pg-orders --partitions 1

printf '%s\n' \
  '{"order_id":"o-1","customer_id":"c-1","amount":12.5,"status":"NEW"}' \
  '{"order_id":"o-2","customer_id":"c-2","amount":99.99,"status":"NEW"}' \
  '{"order_id":"o-3","customer_id":"c-1","amount":5.0,"status":"NEW"}' |
confluent kafka topic produce <STUDENT_ID>-pg-orders \
  --api-key <KAFKA_API_KEY> --api-secret <KAFKA_API_SECRET> \
  --value-format avro --schema order.avsc \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

## 2. Configure the Connect worker

Log in to your lab host (`ssh ubuntu@<LAB_HOST>`) and create the worker
configuration. It contains the Cloud keys, so restrict its permissions first:

```bash
install -m 600 /dev/null ~/<STUDENT_ID>-sink-worker.properties
```

Edit it to contain the following. A *standalone* worker is one process that runs
the connectors named on its command line; it keeps its offsets in a local file,
so unlike the distributed worker of Lab 6B it needs no internal topics in the
cluster. The worker, and its consumer and admin clients, each need the Cloud
connection settings:

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

offset.storage.file.filename=/home/ubuntu/<STUDENT_ID>-sink.offsets
plugin.path=/app/confluent/share/confluent-hub-components
plugin.discovery=service_load
listeners=http://localhost:<WORKER_PORT>
```

`value.converter` is what turns the Avro bytes in the topic back into records
with a schema, using Cloud Schema Registry (the schema ID is in each message).
`plugin.path` is where the instructor installed the JDBC connector.
`plugin.discovery=service_load` makes the worker find plugins from their
declared service files instead of scanning every class in every jar. The scan
needs much more memory than the 300 MB heap used below (it fails with
`OutOfMemoryError`), because the JDBC plugin also bundles drivers for other
databases.

Now the connector. Create `<STUDENT_ID>-pg-sink.properties` (it holds your
database password, so restrict it too):

```bash
install -m 600 /dev/null ~/<STUDENT_ID>-pg-sink.properties
```

```properties
name=<STUDENT_ID>-pg-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=<STUDENT_ID>-pg-orders
connection.url=jdbc:postgresql://<DB_HOST>:5432/<DB_NAME>
connection.user=<DB_USER>
connection.password=<DB_PASSWORD>
table.name.format=orders
insert.mode=upsert
pk.mode=record_value
pk.fields=order_id
auto.create=true
auto.evolve=true
```

- `table.name.format=orders` has no schema in it: your login's default search
  path starts with a schema named like the login, so the table is created in your
  schema `<STUDENT_ID>`.
- `insert.mode=upsert` with `pk.fields=order_id` means a record with an existing
  `order_id` updates the row instead of adding one.
- `auto.create` creates the table from the Avro schema; `auto.evolve` adds
  columns when the schema gains fields.
- The consumer group is `connect-<STUDENT_ID>-pg-sink`, which your service
  account may use because it starts with `connect-<STUDENT_ID>-`.

## 3. Run the worker and check the table

Start the worker in the foreground with a small heap (the lab hosts are shared).
It prints a lot while it starts; wait about 30 seconds until it stops scrolling:

```bash
KAFKA_HEAP_OPTS="-Xms64m -Xmx300m" /app/confluent/bin/connect-standalone \
  ~/<STUDENT_ID>-sink-worker.properties ~/<STUDENT_ID>-pg-sink.properties
```

In a second terminal on the same host, check the connector through the worker's
REST API and read your table with `psql`:

```bash
curl -s http://localhost:<WORKER_PORT>/connectors/<STUDENT_ID>-pg-sink/status

psql "host=<DB_HOST> dbname=<DB_NAME> user=<DB_USER> password=<DB_PASSWORD>" \
  -c '\dt' -c 'select * from orders order by order_id'
```

Expected: the connector and task are `RUNNING`, the table `orders` exists in
schema `<STUDENT_ID>`, and it has the three rows. The column types come from the
Avro schema: `amount` is `double precision`, the others `text`.

## 4. Upserts and schema evolution

Change one order and add a new one, from your workstation. Order `o-2` keeps
its ID but its status changes:

```bash
printf '%s\n' \
  '{"order_id":"o-2","customer_id":"c-2","amount":99.99,"status":"SHIPPED"}' \
  '{"order_id":"o-4","customer_id":"c-3","amount":42.0,"status":"NEW"}' |
confluent kafka topic produce <STUDENT_ID>-pg-orders \
  --api-key <KAFKA_API_KEY> --api-secret <KAFKA_API_SECRET> \
  --value-format avro --schema order.avsc \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

Run the `select` again on the lab host. Expected: four rows, `o-2` is now
`SHIPPED` (updated, not duplicated), and `o-4` is new.

Now evolve the schema by adding an optional field with a default (a backward
compatible change, as in Lab 6A). Create `order_coupon.avsc` in the
`confluent-cloud` directory on your workstation:

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

```bash
printf '%s\n' \
  '{"order_id":"o-5","customer_id":"c-4","amount":7.0,"status":"NEW","coupon":{"string":"SPRING"}}' |
confluent kafka topic produce <STUDENT_ID>-pg-orders \
  --api-key <KAFKA_API_KEY> --api-secret <KAFKA_API_SECRET> \
  --value-format avro --schema order_coupon.avsc \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

On the lab host run `select * from orders order by order_id` once more.
Expected: a new nullable column `coupon`, empty for `o-1` to `o-4` and `SPRING`
for `o-5`. The `ALTER TABLE` was issued by the connector because of
`auto.evolve=true`; without it the record would fail and the task would stop.
Check the schema versions with
`confluent schema-registry subject describe <STUDENT_ID>-pg-orders-value`.

Questions to answer from what you saw:

- What happens to the database if the worker is stopped while you produce more
  records, and then started again? (Look at the committed offsets and the
  consumer group `connect-<STUDENT_ID>-pg-sink`.)
- Why can the database stay private while the topic is in the cloud?
- What would you change to run several tasks, or to survive the loss of the lab
  host?

## Cleanup

Stop the worker with `Ctrl-C`, then on the lab host remove the files that
contain keys and the table:

```bash
psql "host=<DB_HOST> dbname=<DB_NAME> user=<DB_USER> password=<DB_PASSWORD>" -c 'drop table orders'
rm -f ~/<STUDENT_ID>-sink-worker.properties ~/<STUDENT_ID>-pg-sink.properties ~/<STUDENT_ID>-sink.offsets
```

From your workstation, delete the topic and the schema subject (first softly and
then permanently; permanent deletion cannot be undone), and the local file:

```bash
confluent kafka topic delete <STUDENT_ID>-pg-orders --force
confluent schema-registry schema delete \
  --subject <STUDENT_ID>-pg-orders-value --version all
confluent schema-registry schema delete \
  --subject <STUDENT_ID>-pg-orders-value --version all --permanent
rm -f order_coupon.avsc
```

If you created a Schema Registry key only for this lab, delete it with
`confluent api-key delete <SCHEMA_REGISTRY_API_KEY>`.

## Review questions

1. Why does this connector need no inbound network access to the data center?
2. What do `insert.mode=upsert`, `pk.mode`, and `pk.fields` do together?
3. What does `auto.evolve` change, and why can it be risky in production?
4. When would you choose this over a fully managed connector with PrivateLink
   (Lab 7)?

## References

- [JDBC Sink connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)
- [Connect self-managed Connect to Confluent Cloud](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html)
- [Kafka Connect standalone mode](https://docs.confluent.io/platform/current/connect/userguide.html#standalone-mode)
