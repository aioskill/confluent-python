# Lab 6B: Managed and Self-Managed Connectors

In this lab you run a fully managed Datagen source connector, write its topic to
Amazon S3 with a fully managed S3 sink connector, monitor both in the Confluent
Cloud console, and finally run a connector on a self-managed Kafka Connect
worker that you host yourself. It follows
[Lab 6A](LAB-6A-SCHEMA-REGISTRY.md), which covers Schema Registry compatibility;
you can do either lab on its own.

Use a sandbox organization. All students share one Kafka cluster and one
Schema Registry. Replace `<STUDENT_ID>` with the short ID assigned by the
instructor (lowercase letters and digits, for example `s07`) in every name
below. Every topic, consumer group, connector, and schema subject you create
**starts with** `<STUDENT_ID>-`: the instructor gave your service account
permissions only on names with that prefix, so a name without it is rejected.
Managed connectors and the Kafka cluster incur charges while they run, so
finish the Cleanup section.

## Learning objectives

- Deploy a fully managed Datagen source connector with the CLI.
- Deploy a fully managed Amazon S3 sink connector with limited AWS credentials
  and check the files it writes.
- Check connector and task status, and throughput, in the Cloud console.
- Explain how fully managed, custom, and self-managed (externally hosted)
  connectors differ, and run a connector on a worker you operate.

## Prerequisites

- Confluent CLI installed and logged in (`confluent login`).
- A shared Kafka cluster and Schema Registry in the class environment, the
  environment ID and cluster ID, and your private credentials file from the
  instructor. The instructor setup (`lab6_setup_student.sh`, once per student) is
  described in [Lab 6A](LAB-6A-SCHEMA-REGISTRY.md#instructor-setup-once-per-student).
- For step 2 only: the AWS CLI and your private S3 credentials file from the
  instructor (see "Instructor setup: S3 access" below).
- For step 4 only: SSH access to a lab host (for example `ubuntu@sa01`), where
  the Confluent Platform Kafka Connect tools are installed, and a worker port
  assigned by the instructor (`<WORKER_PORT>`).

### Instructor setup: S3 access

Step 2 writes to one shared S3 bucket. Before class, an AWS administrator
creates the bucket and one IAM user per student, limited to the prefix
`<STUDENT_ID>/` of the bucket, with the Terraform in `lab6b-aws/` (read its
`README.md` first):

```bash
cd lab6b-aws
terraform init
terraform apply -var 'students=["s01","s02","s03","s04","s05","s06","s07","s08","s09","s10","s11","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","s22","s23","s24","s25","s26"]'
```

It writes `lab6b-aws/student-credentials/<STUDENT_ID>.txt` (private, git-ignored)
for each student. Give each student only their own file. Run `terraform destroy`
with the same list after class. The S3 sink also needs the consumer-group
permission that `lab6_setup_student.sh` grants (the prefix `connect-lcc-`): re-run
the script's `create` for students set up before that was added, or add it by
hand.

### Student setup

Your credentials file contains `kafka_api_key` and `kafka_api_secret`. Use
them wherever this lab says `<KAFKA_API_KEY>` and `<KAFKA_API_SECRET>`; they
only work on names starting with `<STUDENT_ID>-`. Select the environment and
cluster, and get the Schema Registry details:

```bash
confluent environment use <ENVIRONMENT_ID>
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent schema-registry cluster describe
```

Create your own Schema Registry key for the consume command in step 1:

```bash
confluent api-key create --resource <SCHEMA_REGISTRY_CLUSTER_ID>
```

Save the key and secret (`<SCHEMA_REGISTRY_API_KEY>`,
`<SCHEMA_REGISTRY_API_SECRET>`); the secret is shown only once. The Schema
Registry subject commands in this lab use your CLI login.

For step 2, your S3 credentials file contains `s3_bucket`, `aws_region`,
`aws_access_key_id` and `aws_secret_access_key`. Store them in a named AWS CLI
profile, so that they do not replace your other AWS settings:

```bash
aws configure set aws_access_key_id <AWS_ACCESS_KEY_ID> --profile lab6b
aws configure set aws_secret_access_key <AWS_SECRET_ACCESS_KEY> --profile lab6b
aws configure set region <AWS_REGION> --profile lab6b
aws sts get-caller-identity --profile lab6b
```

The last command must show the user `lab6b-<STUDENT_ID>`. (If the AWS CLI is not
installed, see "Install and configure the AWS CLI" in
[Lab 7](lab7/LAB-7-JDBC-CONNECTOR-PRIVATE-LINK.md#install-and-configure-the-aws-cli).)

## 1. Deploy a fully managed source connector (Datagen)

A fully managed connector runs in Confluent Cloud, and you pay for it only
while it runs. Datagen generates sample records, so it needs no external
system. Create the target topic first; a managed connector does not create it:

```bash
confluent kafka topic create <STUDENT_ID>-datagen-orders --partitions 1
```

List the available managed connector plugins and the settings Datagen needs:

```bash
confluent connect plugin list
confluent connect plugin describe DatagenSource
```

Create `datagen-orders.json`. The file contains the Kafka API key and secret,
so keep it private and delete it during Cleanup:

```json
{
  "name": "<STUDENT_ID>-datagen-orders",
  "config": {
    "connector.class": "DatagenSource",
    "name": "<STUDENT_ID>-datagen-orders",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "kafka.topic": "<STUDENT_ID>-datagen-orders",
    "output.data.format": "AVRO",
    "quickstart": "ORDERS",
    "max.interval": "1000",
    "tasks.max": "1"
  }
}
```

Deploy and wait for the connector to reach `RUNNING`:

```bash
confluent connect cluster create --config-file datagen-orders.json
confluent connect cluster list
confluent connect cluster describe <CONNECTOR_ID>
```

Provisioning takes a few minutes. When it is running, read some records. The
consumer needs both key pairs because the values are Avro:

```bash
confluent kafka topic consume <STUDENT_ID>-datagen-orders \
  --group <STUDENT_ID>-datagen-reader \
  --from-beginning \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --value-format avro \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

Press `Ctrl-C` to stop. The connector registered its own schema, under the
same subject naming rule as in Lab 6A. Inspect it and apply the same protection:

```bash
confluent schema-registry subject list --prefix <STUDENT_ID>-datagen-orders
confluent schema-registry schema describe \
  --subject <STUDENT_ID>-datagen-orders-value --version latest
confluent schema-registry subject update <STUDENT_ID>-datagen-orders-value \
  --compatibility backward
```

## 2. Write the topic to Amazon S3 (fully managed sink)

A *sink* connector moves data out of Kafka. Here the managed Amazon S3 Sink reads
`<STUDENT_ID>-datagen-orders` and writes it as JSON files to the shared bucket,
under your own prefix. The connector runs in Confluent Cloud, outside your AWS
account, so it signs in to AWS with the access key from your S3 credentials file.
That IAM user may only read, write and delete objects under `<STUDENT_ID>/`.

List the settings the sink supports (long) and compare with the file below:

```bash
confluent connect plugin describe S3_SINK
```

Create `s3-sink.json`. It contains two secrets (the Kafka key and the AWS key),
so keep it private and delete it during Cleanup:

```json
{
  "name": "<STUDENT_ID>-s3-sink",
  "config": {
    "connector.class": "S3_SINK",
    "name": "<STUDENT_ID>-s3-sink",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<KAFKA_API_KEY>",
    "kafka.api.secret": "<KAFKA_API_SECRET>",
    "topics": "<STUDENT_ID>-datagen-orders",
    "input.data.format": "AVRO",
    "output.data.format": "JSON",
    "s3.bucket.name": "<S3_BUCKET>",
    "s3.region": "<AWS_REGION>",
    "topics.dir": "<STUDENT_ID>",
    "aws.access.key.id": "<AWS_ACCESS_KEY_ID>",
    "aws.secret.access.key": "<AWS_SECRET_ACCESS_KEY>",
    "flush.size": "1000",
    "rotate.schedule.interval.ms": "60000",
    "timezone": "UTC",
    "errors.deadletterqueue.topic.name": "<STUDENT_ID>-s3-sink-dlq",
    "tasks.max": "1"
  }
}
```

What the less obvious settings do:

- `input.data.format` is `AVRO`: the connector reads the schema that the Datagen
  connector registered, so it needs Schema Registry (it is available in this
  environment). `output.data.format` is the file format written to S3.
- `topics.dir` is the first part of every object name, so your files land under
  `<STUDENT_ID>/` (the only prefix your IAM user may write).
- `flush.size` is the number of records per file; Confluent Cloud requires at
  least 1000. At about one record per second that would take many minutes, so
  `rotate.schedule.interval.ms` also closes a file every 60 seconds.
- `s3.region` is the bucket's region. Confluent recommends a bucket in the same
  region as the Kafka cluster; naming the region explicitly lets this lab's
  bucket live in a different one.
- A sink connector needs a *dead letter queue* topic, where records it cannot
  write are sent. Its name must start with `<STUDENT_ID>-`, or your service
  account is not allowed to create it.

Deploy it and wait for `RUNNING`:

```bash
confluent connect cluster create --config-file s3-sink.json
confluent connect cluster list
confluent connect cluster describe <S3_CONNECTOR_ID>
```

If it ends in `FAILED`, read the `Trace` line of `describe`. It names the cause
(invalid AWS credentials, a bucket you cannot write, or a missing Kafka
permission) and the connector is not charged while failed. Delete it, fix
`s3-sink.json`, and create it again.

After about a minute the first file appears. List and read what the connector
wrote, using your `lab6b` profile:

```bash
aws s3 ls s3://<S3_BUCKET>/<STUDENT_ID>/ --recursive --profile lab6b
aws s3 cp s3://<S3_BUCKET>/<STUDENT_ID>/<STUDENT_ID>-datagen-orders/<FILE_KEY> - --profile lab6b | head -3
```

Use a key from the first command for `<FILE_KEY>` (the part after
`<STUDENT_ID>/<STUDENT_ID>-datagen-orders/`). Each line is one order as JSON.
The path contains the partition folders `year=/month=/day=/hour=` and a file
name `<topic>+<partition>+<first offset>.json`.

Your IAM user is limited to your prefix. Writing anywhere else is refused:

```bash
echo test | aws s3 cp - s3://<S3_BUCKET>/not-my-prefix/test.txt --profile lab6b
```

Expected: `AccessDenied`. (The user can *list* the bucket, because the connector's
bucket check requires it, so object names of other students are visible, but
their contents are not readable.)

## 3. Monitor connector tasks and throughput in the console

1. Sign in to the Cloud Console and open your environment and cluster.
2. Choose **Connectors** in the left menu, then open
   `<STUDENT_ID>-datagen-orders`. (Open `<STUDENT_ID>-s3-sink` the same way to
   compare a source with a sink.)
3. On the **Overview** tab, record the connector status (`Running`) and the
   status of each task. The throughput graphs show messages per second and
   bytes per second produced by the connector.
4. Open the **Settings** tab to see the configuration you submitted (secrets
   are masked).
5. Open **Topics**, choose `<STUDENT_ID>-datagen-orders`, then **Messages**, to
   see the records arrive. The topic's **Overview** tab also shows production
   throughput.

Now change the load and watch the graphs react. Pause the connector, wait a
minute, check that task status and throughput fall to zero, then resume it:

```bash
confluent connect cluster pause <CONNECTOR_ID>
confluent connect cluster describe <CONNECTOR_ID>
```

The status can lag behind the command by up to a minute, so repeat `describe`
until the connector shows `PAUSED` and the task shows `STOPPED`.

```bash
confluent connect cluster resume <CONNECTOR_ID>
confluent connect cluster describe <CONNECTOR_ID>
```

Optionally speed it up by editing `max.interval` to `100` in the connector
configuration and applying it with
`confluent connect cluster update <CONNECTOR_ID> --config-file datagen-orders.json`;
compare the messages-per-second graph before and after. The CLI shows status
and task state; throughput graphs are in the console.

## 4. Externally hosted connectors

Kafka Connect connectors can run in three places:

| Option | Who runs the workers | Typical use |
| --- | --- | --- |
| Fully managed connector | Confluent Cloud | Supported sources and sinks (Datagen, S3, and others); you only supply configuration. |
| Custom connector | Confluent Cloud runs a plugin that you upload | A connector or SMT that is not in the managed catalog; you still do not operate the workers. |
| Self-managed connector (externally hosted) | You, on your own VMs, containers, or Kubernetes | Systems that Cloud cannot reach (private networks, on-premises), plugins you cannot upload, or a connector that is not offered as managed. |

A self-managed worker is an ordinary Kafka Connect process that uses
Confluent Cloud as its Kafka cluster. Its internal topics (connector configs,
offsets, status) live in the Cloud cluster, and you provide Java, plugins,
scaling, monitoring, and upgrades.

In this exercise you run a worker on a lab host and use the Kafka
`MirrorSourceConnector` to replicate a topic from the on-premises lab cluster
into Confluent Cloud. That is a common hybrid pattern that Cloud cannot do
without network access to your data center. Ask the instructor for a SCRAM
username and password for the lab cluster that can read your source topic and
write the offset-sync topic.

### 4.1 Create a source topic on the lab cluster

Log in to the lab host (`ssh ubuntu@sa01`) and create and fill a topic using
the lab client config:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --create --topic <STUDENT_ID>-onprem-orders --partitions 1 --replication-factor 3

printf 'order-1\norder-2\norder-3\n' | /app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --topic <STUDENT_ID>-onprem-orders
```

### 4.2 Configure the worker

On the same host, create `<STUDENT_ID>-connect-cloud.properties` and restrict
its permissions, because it contains the Cloud key:

```bash
install -m 600 /dev/null ~/<STUDENT_ID>-connect-cloud.properties
```

Edit it to contain the following. The `<CLOUD_BOOTSTRAP>` value is the cluster's
bootstrap host and port from `confluent kafka cluster describe`, without the
`SASL_SSL://` prefix. A Connect worker needs the client settings repeated for
its producer, consumer, and admin clients:

```properties
bootstrap.servers=<CLOUD_BOOTSTRAP>
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";

producer.security.protocol=SASL_SSL
producer.sasl.mechanism=PLAIN
producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";
admin.security.protocol=SASL_SSL
admin.sasl.mechanism=PLAIN
admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";

group.id=<STUDENT_ID>-connect
config.storage.topic=<STUDENT_ID>-connect-configs
offset.storage.topic=<STUDENT_ID>-connect-offsets
status.storage.topic=<STUDENT_ID>-connect-status
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3
offset.storage.partitions=1
status.storage.partitions=1

key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
plugin.path=/home/ubuntu/<STUDENT_ID>-plugins
listeners=http://localhost:<WORKER_PORT>

confluent.topic.replication.factor=3
confluent.topic.bootstrap.servers=<CLOUD_BOOTSTRAP>
confluent.topic.security.protocol=SASL_SSL
confluent.topic.sasl.mechanism=PLAIN
confluent.topic.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<KAFKA_API_KEY>" password="<KAFKA_API_SECRET>";
```

The last five lines are for the Confluent Platform worker's license check,
which uses its own client and needs the same Cloud connection settings.

Do not point `plugin.path` at the whole Kafka `share/java` directory: the worker
scans every jar there, which takes many minutes. Create a directory that
contains only the mirror connector jar instead:

```bash
mkdir -p ~/<STUDENT_ID>-plugins
ln -sf /app/confluent/share/java/kafka/connect_mirror_mirror-project.jar \
  ~/<STUDENT_ID>-plugins/
```

Start the worker in the foreground in this terminal (or run it with `nohup`).
It takes a minute to start; wait for the log to stop scrolling:

```bash
/app/confluent/bin/connect-distributed ~/<STUDENT_ID>-connect-cloud.properties
```

In a second terminal on the same host, confirm the plugin is loaded:

```bash
curl -s http://localhost:<WORKER_PORT>/connector-plugins | grep -i MirrorSource
```

Use the worker port the instructor assigned (`<WORKER_PORT>`, for example
8083 plus your student number) so that workers on a shared host do not collide.

### 4.3 Deploy the mirror connector

Create `<STUDENT_ID>-mirror.json`. The `source.cluster.*` settings read from
the lab cluster (SCRAM over plaintext), the `target.cluster.*` settings write
to Confluent Cloud:

```json
{
  "name": "<STUDENT_ID>-mirror",
  "config": {
    "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "tasks.max": "1",
    "source.cluster.alias": "onprem",
    "target.cluster.alias": "<STUDENT_ID>",
    "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
    "source.cluster.bootstrap.servers": "sa01:9092,sa02:9092,sa03:9092",
    "source.cluster.security.protocol": "SASL_PLAINTEXT",
    "source.cluster.sasl.mechanism": "SCRAM-SHA-512",
    "source.cluster.sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"<LAB_USER>\" password=\"<LAB_PASSWORD>\";",
    "target.cluster.bootstrap.servers": "<CLOUD_BOOTSTRAP>",
    "target.cluster.security.protocol": "SASL_SSL",
    "target.cluster.sasl.mechanism": "PLAIN",
    "target.cluster.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<KAFKA_API_KEY>\" password=\"<KAFKA_API_SECRET>\";",
    "topics": "<STUDENT_ID>-onprem-orders",
    "replication.factor": "3",
    "offset-syncs.topic.replication.factor": "3",
    "refresh.topics.interval.seconds": "10",
    "sync.topic.acls.enabled": "false",
    "sync.topic.configs.enabled": "false",
    "emit.checkpoints.enabled": "false",
    "emit.heartbeats.enabled": "false",
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
  }
}
```

Submit it to the worker's REST API and check its status:

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @<STUDENT_ID>-mirror.json http://localhost:<WORKER_PORT>/connectors

curl -s http://localhost:<WORKER_PORT>/connectors/<STUDENT_ID>-mirror/status
```

Expected: the connector and its task report `RUNNING`. The connector creates
a topic with the same name, `<STUDENT_ID>-onprem-orders`, in Confluent Cloud
(`IdentityReplicationPolicy` keeps the name; the default policy would prepend
the source alias, which would break the `<STUDENT_ID>-` prefix permissions).
The `target.cluster.alias` is your student ID so that each student's offset-sync
topic on the lab cluster, `mm2-offset-syncs.<STUDENT_ID>.internal`, is distinct.

Read the topic from the Cloud side with your Cloud
key:

```bash
confluent kafka topic consume <STUDENT_ID>-onprem-orders \
  --group <STUDENT_ID>-mirror-reader \
  --from-beginning \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET>
```

You should see `order-1`, `order-2`, and `order-3`. Produce one more line to
the lab topic and watch it arrive in Cloud.

Compare the two connector types you ran:

- Where do you see the self-managed connector's status? (Its REST API and the
  worker's log, not the Cloud console's **Connectors** page.)
- Who is responsible for upgrading, scaling, and restarting each connector?
- What network access did each connector need?

## Cleanup

Stop and delete the connectors and their topics. The Datagen connector keeps
producing and billing until you delete it:

```bash
confluent connect cluster delete <CONNECTOR_ID> --force
confluent connect cluster delete <S3_CONNECTOR_ID> --force
rm -f datagen-orders.json s3-sink.json
```

Remove your files from S3 (they also expire automatically after a few days) and
your AWS profile:

```bash
aws s3 rm s3://<S3_BUCKET>/<STUDENT_ID>/ --recursive --profile lab6b
aws configure set aws_access_key_id "" --profile lab6b
aws configure set aws_secret_access_key "" --profile lab6b
```

For the self-managed worker, delete the connector, stop the worker with
`Ctrl-C`, and remove its config (it contains the Cloud key):

```bash
curl -s -X DELETE http://localhost:<WORKER_PORT>/connectors/<STUDENT_ID>-mirror
rm -f ~/<STUDENT_ID>-connect-cloud.properties ~/<STUDENT_ID>-mirror.json
```

Delete the topics. The `confluent` commands remove the Cloud topics (the
Datagen topic, the mirrored topic, and the worker's internal topics). The last
command, run on the lab host, removes the source topic.
The mirror connector also created `mm2-offset-syncs.<STUDENT_ID>.internal` on the lab
cluster; ask the instructor to remove it if it is no longer needed:

```bash
confluent kafka topic delete <STUDENT_ID>-datagen-orders --force
confluent kafka topic delete <STUDENT_ID>-s3-sink-dlq --force
confluent kafka topic delete <STUDENT_ID>-onprem-orders --force
confluent kafka topic delete <STUDENT_ID>-connect-configs --force
confluent kafka topic delete <STUDENT_ID>-connect-offsets --force
confluent kafka topic delete <STUDENT_ID>-connect-status --force

/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --delete --topic <STUDENT_ID>-onprem-orders
```

Delete the schema subject the Datagen connector registered, first softly and
then permanently (permanent deletion cannot be undone):

```bash
confluent schema-registry schema delete \
  --subject <STUDENT_ID>-datagen-orders-value --version all
confluent schema-registry schema delete \
  --subject <STUDENT_ID>-datagen-orders-value --version all --permanent
```

Finally, list and delete the API keys created for this lab:

```bash
confluent api-key list
confluent api-key delete <KAFKA_API_KEY>
confluent api-key delete <SCHEMA_REGISTRY_API_KEY>
```

## Review questions

1. Why does the S3 sink need AWS credentials, and what is the least an IAM
   user needs to be allowed to do for it to work?
2. Which pieces does Confluent run for a fully managed connector, and which do
   you run for a self-managed one?
3. Where do you look to see task status and throughput for each kind of
   connector?
4. When would you choose a self-managed or custom connector over a fully
   managed one?

## References

- [Datagen Source connector for Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html)
- [Amazon S3 Sink connector for Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-s3-sink/cc-s3-sink.html)
- [Manage connectors with the Confluent CLI](https://docs.confluent.io/cloud/current/connectors/cc-connect-cli.html)
- [Connect self-managed Connect to Confluent Cloud](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html)
- [Custom connectors](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/overview-byoc.html)
