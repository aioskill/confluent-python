# Lab 6B: PostgreSQL Sink Connector in the Data Center

In this lab the Kafka cluster is in Confluent Cloud, but the database and the
connector are in your own data center (the lab host `sa01`). You
produce Avro records to a Cloud topic, run a Kafka Connect worker on `sa01`
that reads the topic, and the JDBC sink connector writes the records to a
PostgreSQL database on `sa01`. Nothing connects *into* the data center: the
worker makes outbound connections to Confluent Cloud, which is why this pattern
works for databases that Cloud cannot reach (compare Lab 7, which uses managed
connectors and PrivateLink instead).

```
 Confluent Cloud                              Lab hosts (data center)
+---------------------------+   outbound   +-----------------------------------+
| Kafka topic               | <----------- | Connect worker on sa01            |
|   pg-orders-<STUDENT_ID>  |   TLS 9092   |   (JDBC sink connector)           |
| Schema Registry           | <----------- |        |                          |
+---------------------------+   HTTPS 443  |        v  JDBC, port 5432         |
                                           | PostgreSQL on sa01, database      |
                                           |   lab6b                           |
                                           +-----------------------------------+
```

Replace `<STUDENT_ID>` with the short ID assigned by the instructor (lowercase
letters and digits, for example `s07`) in every name below. Topic, connector and
file names end with `-<STUDENT_ID>`. This lab uses the same class service account,
Kafka key, and shared cluster as [Lab 6A](LAB-6A-MANAGED-CONNECTORS.md), but you
can do it on its own.

## Learning objectives

- Explain when to run a sink connector yourself instead of using a managed one.
- Configure a Connect worker on `sa01` to use Confluent Cloud and Cloud
  Schema Registry.
- Run the JDBC sink connector so that Avro records become rows in PostgreSQL,
  and see how `upsert` and `auto.evolve` behave.

## Prerequisites

- Confluent CLI installed and logged in (`confluent login`).
- The shared environment and cluster, and the class Kafka credentials file from the
  instructor (`kafka_api_key`, `kafka_api_secret`), the same one as in Lab 6A.
- SSH access to your own `sa01` (`ssh ubuntu@sa01`). Each student has a separate set of
  lab hosts, so nothing on `sa01` is shared with other students.
- The `confluent-cloud` directory of this repository as your working directory
  (it contains `order.avsc`).

### Instructor setup

Everything for this lab is on `sa01`: PostgreSQL, the `psql` client and the JDBC connector
plugin (installed only there). The setup script runs **on `sa01`** and does not use `ssh`.
Copy it over and run it:

```bash
scp lab6b_setup.sh ubuntu@sa01:
ssh ubuntu@sa01 'bash lab6b_setup.sh init'
```

`init` installs PostgreSQL (bound to its lab-network address, `192.168.120.11`, and to
localhost, not to every interface), creates the database `lab6b` with the login `lab6b`
(password `lab6b-pass`) as its owner, and installs the `psql` client and the Confluent JDBC
connector plugin (`confluent-hub install confluentinc/kafka-connect-jdbc`) under
`/app/confluent/share/confluent-hub-components`. It is safe to run again. All students use this
one database login, so there is nothing to create per student. After class, run
`bash lab6b_setup.sh delete` on `sa01` to drop the database and the login.

Each student has their own `sa01`, so every student runs this setup once on their own
host (or the instructor does it on each). Nothing is shared between students on the lab
hosts; only the Confluent Cloud cluster, the service account and the Kafka key are shared.

The Kafka side uses the class service account from
[Lab 6A](LAB-6A-MANAGED-CONNECTORS.md#instructor-setup-kafka-access-once), which
already covers the consumer group of the sink (`connect-pg-sink-<STUDENT_ID>`, under
the prefix `connect-`) and the cluster-level permissions. Add one ACL for this
lab's topics, once, which are all named `pg-orders-<STUDENT_ID>`:

```bash
confluent kafka acl create --allow --service-account <SERVICE_ACCOUNT_ID> \
  --environment <ENVIRONMENT_ID> --cluster <KAFKA_CLUSTER_ID> \
  --operations read,write,create,describe,describe-configs,alter-configs,delete \
  --topic "pg-orders-" --prefix
```

### Instructor: common tasks

**On the student's `sa01`** (`ssh ubuntu@sa01`):

| Task | Command |
|---|---|
| Prepare PostgreSQL and the JDBC plugin (safe to repeat) | `bash lab6b_setup.sh init` (see above) |
| Check the database is reachable | `psql "host=sa01 dbname=lab6b user=lab6b password=lab6b-pass" -c "select 1"` |
| Check the JDBC plugin is installed | `ls /app/confluent/share/confluent-hub-components` (expect `confluentinc-kafka-connect-jdbc`) |
| See which tables exist | `psql "host=sa01 dbname=lab6b user=lab6b password=lab6b-pass" -c "\dt"` |
| See the running worker | `pgrep -af connect-standalone` |
| Stop a stuck worker | `pkill -f connect-standalone` |
| Reset the table | Stop the worker, then `psql "host=sa01 dbname=lab6b user=lab6b password=lab6b-pass" -c "drop table orders"` and `rm -f ~/sink.offsets` (so the topic is replayed); start the worker again and it re-creates the table |
| After class: remove the database | `bash lab6b_setup.sh delete` |

**On the instructor workstation** (Confluent CLI logged in as the administrator; `sa01` does not
have the CLI):

| Task | Command |
|---|---|
| Read a student's consumer-group lag | `confluent kafka consumer group lag summarize connect-pg-sink-<STUDENT_ID> --cluster <KAFKA_CLUSTER_ID>` |
| Rotate the shared Kafka key | `confluent api-key create --resource <KAFKA_CLUSTER_ID> --service-account <SERVICE_ACCOUNT_ID>`, hand it out, then `confluent api-key delete <OLD_KEY>` |
| After class: remove student topics and subjects | For each student: `confluent kafka topic delete pg-orders-<STUDENT_ID> --force`, then `confluent schema-registry schema delete --subject pg-orders-<STUDENT_ID>-value --version all --force`, then the same with `--permanent` |

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
Registry key from Lab 6A, create one (the secret is shown only once):

```bash
confluent api-key create --resource <SCHEMA_REGISTRY_CLUSTER_ID>
```

Finally, check from `sa01` that the database is reachable (expect a
single row with the value `1`). If this fails, tell the instructor before you
continue:

```bash
ssh ubuntu@sa01 'psql "host=sa01 dbname=lab6b user=lab6b password=lab6b-pass" -c "select 1"'
```

## 1. Produce Avro records to a Cloud topic

Create the topic and send three orders. `order.avsc` defines the fields
`order_id`, `customer_id`, `amount`, and `status`. The producer registers it in
Schema Registry under the subject `pg-orders-<STUDENT_ID>-value`:

```bash
confluent kafka topic create pg-orders-<STUDENT_ID> --partitions 1

printf '%s\n' \
  '{"order_id":"o-1","customer_id":"c-1","amount":12.5,"status":"NEW"}' \
  '{"order_id":"o-2","customer_id":"c-2","amount":99.99,"status":"NEW"}' \
  '{"order_id":"o-3","customer_id":"c-1","amount":5.0,"status":"NEW"}' |
confluent kafka topic produce pg-orders-<STUDENT_ID> \
  --api-key <KAFKA_API_KEY> --api-secret <KAFKA_API_SECRET> \
  --value-format avro --schema order.avsc \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

## 2. Configure the Connect worker

Log in to `sa01` (`ssh ubuntu@sa01`) and create the worker
configuration. It contains the Cloud keys, so restrict its permissions first:

```bash
install -m 600 /dev/null ~/sink-worker.properties
```

Edit it to contain the following. A *standalone* worker is one process that runs
the connectors named on its command line; it keeps its offsets in a local file,
so unlike the distributed worker of Lab 6A it needs no internal topics in the
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

offset.storage.file.filename=/home/ubuntu/sink.offsets
plugin.path=/app/confluent/share/confluent-hub-components
plugin.discovery=service_load
listeners=http://localhost:8083
```

`value.converter` is what turns the Avro bytes in the topic back into records
with a schema, using Cloud Schema Registry (the schema ID is in each message).
`plugin.path` is where the instructor installed the JDBC connector.
`plugin.discovery=service_load` makes the worker find plugins from their
declared service files instead of scanning every class in every jar. The scan
needs much more memory than the 300 MB heap used below (it fails with
`OutOfMemoryError`), because the JDBC plugin also bundles drivers for other
databases.

Now the connector. Create `pg-sink.properties` (it holds the
database password, so restrict it too):

```bash
install -m 600 /dev/null ~/pg-sink.properties
```

```properties
name=pg-sink-<STUDENT_ID>
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
tasks.max=1
topics=pg-orders-<STUDENT_ID>
connection.url=jdbc:postgresql://sa01:5432/lab6b
connection.user=lab6b
connection.password=lab6b-pass
table.name.format=orders
insert.mode=upsert
pk.mode=record_value
pk.fields=order_id
auto.create=true
auto.evolve=true
```

- `table.name.format=orders` is the table the connector creates in the `public`
  schema of the database `lab6b` on your `sa01`.
- `insert.mode=upsert` with `pk.fields=order_id` means a record with an existing
  `order_id` updates the row instead of adding one.
- `auto.create` creates the table from the Avro schema; `auto.evolve` adds
  columns when the schema gains fields.
- The consumer group is `connect-pg-sink-<STUDENT_ID>`, which your service
  account may use because it starts with `connect-`.

## 3. Run the worker and check the table

Start the worker in the foreground with a small heap (`sa01` is shared).
It prints a lot while it starts; wait about 30 seconds until it stops scrolling:

```bash
KAFKA_HEAP_OPTS="-Xms64m -Xmx300m" /app/confluent/bin/connect-standalone \
  ~/sink-worker.properties ~/pg-sink.properties
```

In a second terminal on `sa01`, check the connector through the worker's
REST API and read your table with `psql`:

```bash
curl -s http://localhost:8083/connectors/pg-sink-<STUDENT_ID>/status

psql "host=sa01 dbname=lab6b user=lab6b password=lab6b-pass" \
  -c '\dt' -c 'select * from orders order by order_id'
```

Expected: the connector and task are `RUNNING`, the table `orders` exists, and it has the three rows. The column types come from the
Avro schema: `amount` is `double precision`, the others `text`.

## 4. Upserts and schema evolution

Change one order and add a new one, from your workstation. Order `o-2` keeps
its ID but its status changes:

```bash
printf '%s\n' \
  '{"order_id":"o-2","customer_id":"c-2","amount":99.99,"status":"SHIPPED"}' \
  '{"order_id":"o-4","customer_id":"c-3","amount":42.0,"status":"NEW"}' |
confluent kafka topic produce pg-orders-<STUDENT_ID> \
  --api-key <KAFKA_API_KEY> --api-secret <KAFKA_API_SECRET> \
  --value-format avro --schema order.avsc \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

Run the `select` again on `sa01`. Expected: four rows, `o-2` is now
`SHIPPED` (updated, not duplicated), and `o-4` is new.

Now evolve the schema by adding an optional field with a default (a backward
compatible change, as in [Lab 4](LAB-4-CONFLUENT_CLI.md)). Create `order_coupon.avsc` in the
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
confluent kafka topic produce pg-orders-<STUDENT_ID> \
  --api-key <KAFKA_API_KEY> --api-secret <KAFKA_API_SECRET> \
  --value-format avro --schema order_coupon.avsc \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

On `sa01` run `select * from orders order by order_id` once more.
Expected: a new nullable column `coupon`, empty for `o-1` to `o-4` and `SPRING`
for `o-5`. The `ALTER TABLE` was issued by the connector because of
`auto.evolve=true`; without it the record would fail and the task would stop.
Check the schema versions with
`confluent schema-registry subject describe pg-orders-<STUDENT_ID>-value`.

Questions to answer from what you saw:

- What happens to the database if the worker is stopped while you produce more
  records, and then started again? (Look at the committed offsets and the
  consumer group `connect-pg-sink-<STUDENT_ID>`.)
- Why can the database stay private while the topic is in the cloud?
- What would you change to run several tasks, or to survive the loss of the lab
  host?

## Cleanup

Stop the worker with `Ctrl-C`, then on `sa01` remove the files that
contain keys and the table:

```bash
psql "host=sa01 dbname=lab6b user=lab6b password=lab6b-pass" -c 'drop table orders'
rm -f ~/sink-worker.properties ~/pg-sink.properties ~/sink.offsets
```

From your workstation, delete the topic and the schema subject (first softly and
then permanently; permanent deletion cannot be undone), and the local file:

```bash
confluent kafka topic delete pg-orders-<STUDENT_ID> --force
confluent schema-registry schema delete \
  --subject pg-orders-<STUDENT_ID>-value --version all
confluent schema-registry schema delete \
  --subject pg-orders-<STUDENT_ID>-value --version all --permanent
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
