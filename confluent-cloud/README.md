# Confluent Cloud Python tutorials

For the separate Confluent CLI walkthrough, see
[Lab 4: Confluent CLI](LAB-4-CONFLUENT_CLI.md).

For a console-style Confluent Cloud produce/consume exercise using this
directory's `.env` credentials, see
[console-produce-consume.md](console-produce-consume.md).

For CLI and Terraform provisioning, service-account API keys, and endpoint
checks, see [Lab 3: Provisioning via CLI & Terraform](LAB-3-PROVISIONING-VIA-CLI-TERRAFORM.md).

To invite human users from an email list, see [`invite_users.sh`](invite_users.sh).
[`assign_roles.sh`](assign_roles.sh) dynamically finds current users and grants
`OrganizationAdmin` to everyone except `training@avalanchio.com`; this grants
full access across the organization, so use it only in a dedicated training
organization. See [Lab 4](LAB-4-CONFLUENT_CLI.md) for usage details.

For a hands-on service-account RBAC exercise covering prefixed topic access,
environment-wide administration, authorization denial, and audit logs, see
the [Lab 5: RBAC And Audit Logs](LAB-5-RBAC-PREFIX-AND-AUDIT-LAB.md).

Schema Registry compatibility is covered in Lab 4. For a fully managed
Datagen connector and a self-managed Connect worker, see
[Lab 6A: Managed and Self-Managed Connectors](LAB-6A-MANAGED-CONNECTORS.md)
(it includes a managed Amazon S3 sink). For a self-managed PostgreSQL sink
connector that runs on your own VM, see
[Lab 6B: PostgreSQL Sink Connector on Your Own VM](LAB-6B-POSTGRES-SINK-ON-PREM.md).
For Tableflow, which turns a Kafka topic into an Iceberg table that you read without
Kafka, see [Lab 8: Tableflow](LAB-8-TABLEFLOW.md).
For stream processing with SQL on a managed ksqlDB cluster, see
[Lab 10: ksqlDB](LAB-10-KSQLDB.md).
For stream processing with Flink SQL on Confluent Cloud, see
[Lab 11: Flink SQL](LAB-11-FLINK-SQL.md).
For monitoring Confluent Cloud with the Metrics API and Prometheus, see
[Lab 13: Monitoring](lab13/LAB-13-MONITORING.md).

For a managed JDBC (PostgreSQL) source connector that reaches a private AWS RDS
database over PrivateLink, see
[Lab 7: JDBC Connector with PrivateLink](lab7/LAB-7-JDBC-CONNECTOR-PRIVATE-LINK.md).

On Ubuntu 22.04 or newer, install the CLI with
[`install_confluent_cli_ubuntu.sh`](install_confluent_cli_ubuntu.sh).
Install Terraform with
[`install_terraform_ubuntu.sh`](install_terraform_ubuntu.sh).

These scripts use Confluent Cloud's native Kafka endpoint and Schema Registry.
They load the adjacent `.env` file automatically. Credential values are never
printed.

The `.env` file should contain these settings:

```dotenv
BOOTSTRAP_SERVRS=lkc-...confluent.cloud:9092
REST_ENDPOINT=https://pkc-...confluent.cloud:443
KEY=Kafka-cluster-API-key
SECRET=Kafka-cluster-API-secret
SCHEMA_REGISTRY_ENDPOINT=https://psrc-...confluent.cloud
SCHEMA_REGISTRY_KEY=Schema-Registry-API-key
SCHEMA_REGISTRY_SECRET=Schema-Registry-API-secret
```

The loader accepts the existing `BOOTSTRAP_SERVRS` spelling and the corrected
`BOOTSTRAP_SERVERS` or `CONFLUENT_BOOTSTRAP_SERVERS` names. You can override
settings with process environment variables. Kafka clients use `SASL_SSL` and
`PLAIN`; Schema Registry clients use HTTP Basic Auth.

Activate the Python environment that has the dependencies (or create a
virtualenv; Ubuntu 24 blocks system-wide `pip install`), then run commands from
this directory:

```bash
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt
python3 discover_cluster.py
python3 verify_confluent_cloud.py
```

## Topic management

In a class, all learners share one Kafka cluster. Replace `<STUDENT_ID>` with
your instructor-assigned ID (for example `s07`) in every topic name below so
topics and Schema Registry subjects (`<topic>-value`) do not collide. Examples
that use a plain `demo` topic are for single-user practice.

```bash
python3 manage_topics.py list
python3 manage_topics.py create demo --partitions 1
python3 manage_topics.py describe demo
python3 manage_topics.py config demo --set retention.ms=604800000
python3 manage_topics.py config demo --delete retention.ms
python3 manage_topics.py partitions demo --count 6
python3 manage_topics.py replication-factor demo --count 3
python3 manage_topics.py delete demo --yes
```

Confluent Cloud manages replication and fixes topic replication factor at
three.
The matching `replication-factor` command verifies that policy; it cannot
reassign replicas or change the factor. Partition counts can be increased, not
decreased. Topic config edits remain subject to the Cloud cluster type's
supported configuration list and the API key's permissions.

## Produce and consume plain messages

Create the topic before producing. The consumer uses a fresh group by default,
so it reads existing messages from the beginning:

```bash
python3 manage_topics.py create demo-messages-<STUDENT_ID> --partitions 1
python3 produce_messages.py demo-messages-<STUDENT_ID> --count 10
python3 consume_messages.py demo-messages-<STUDENT_ID> --count 10
```

## Compacted topics

```bash
python3 manage_topics.py create compact-demo-<STUDENT_ID> --partitions 1
python3 manage_topics.py config compact-demo-<STUDENT_ID> --set cleanup.policy=compact
python3 produce_compacted_messages.py compact-demo-<STUDENT_ID>
python3 consume_compacted_topic.py compact-demo-<STUDENT_ID>
```

Compaction runs asynchronously. The consumer folds updates and tombstones into
the current logical key/value state.

## Schema Registry formats

Each producer registers its schema under the topic's value subject. The
principal associated with `SCHEMA_REGISTRY_KEY` needs schema subject read and
write access (for example, `DeveloperRead` and `DeveloperWrite` on the
subjects used by the tutorials). The Kafka key needs topic produce/consume
permissions.
The verifier also creates, configures, and deletes a temporary topic. Its Kafka
principal needs those topic permissions and permission to use a consumer group;
a cluster-scoped `CloudClusterAdmin` role is sufficient for the demo workflow.

```bash
python3 manage_topics.py create json-orders-<STUDENT_ID> --partitions 1
python3 produce_json_schema.py json-orders-<STUDENT_ID> --count 5
python3 consume_json_schema.py json-orders-<STUDENT_ID> --count 5

python3 manage_topics.py create avro-orders-<STUDENT_ID> --partitions 1
python3 produce_avro.py avro-orders-<STUDENT_ID> --count 5
python3 consume_avro.py avro-orders-<STUDENT_ID> --count 5

python3 manage_topics.py create protobuf-orders-<STUDENT_ID> --partitions 1
python3 produce_protobuf.py protobuf-orders-<STUDENT_ID> --count 5
python3 consume_protobuf.py protobuf-orders-<STUDENT_ID> --count 5
```

`order_pb2.py` is generated from `order.proto` and is included so no Protobuf
compiler is needed at runtime.

## Cloud-specific behavior

`discover_cluster.py` lists broker endpoints returned by Kafka metadata.
Confluent Cloud manages controller nodes and does not expose their roles
through this client API. `verify_confluent_cloud.py` checks Kafka SASL
authentication, the Kafka REST API, Schema Registry authentication, and a
temporary create/configure/produce/consume/delete workflow.
