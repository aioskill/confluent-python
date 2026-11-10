# Confluent Cloud Python tutorials

For the separate Confluent CLI walkthrough, see
[CONFLUENT_CLI.md](CONFLUENT_CLI.md).

To invite human users from an email list and assign Cloud roles, see
[`invite_users_and_assign_roles.sh`](invite_users_and_assign_roles.sh) and its
usage notes in the CLI walkthrough.

On Ubuntu 24.x, install the CLI with
[`install_confluent_cli_ubuntu24.sh`](install_confluent_cli_ubuntu24.sh).

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

Install the dependencies, then run commands from this directory:

```bash
python3 -m pip install -r requirements.txt
python3 discover_cluster.py
python3 verify_confluent_cloud.py
```

## Topic management

```bash
python3 manage_topics.py list
python3 manage_topics.py create demo --partitions 3
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
python3 manage_topics.py create demo-messages --partitions 3
python3 produce_messages.py demo-messages --count 10
python3 consume_messages.py demo-messages --count 10
```

## Compacted topics

```bash
python3 manage_topics.py create compact-demo --partitions 3
python3 manage_topics.py config compact-demo --set cleanup.policy=compact
python3 produce_compacted_messages.py compact-demo
python3 consume_compacted_topic.py compact-demo
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
python3 manage_topics.py create json-orders --partitions 3
python3 produce_json_schema.py json-orders --count 5
python3 consume_json_schema.py json-orders --count 5

python3 manage_topics.py create avro-orders --partitions 3
python3 produce_avro.py avro-orders --count 5
python3 consume_avro.py avro-orders --count 5

python3 manage_topics.py create protobuf-orders --partitions 3
python3 produce_protobuf.py protobuf-orders --count 5
python3 consume_protobuf.py protobuf-orders --count 5
```

`order_pb2.py` is generated from `order.proto` and is included so no Protobuf
compiler is needed at runtime.

## Cloud-specific behavior

`discover_cluster.py` lists broker endpoints returned by Kafka metadata.
Confluent Cloud manages controller nodes and does not expose their roles
through this client API. `verify_confluent_cloud.py` checks Kafka SASL
authentication, the Kafka REST API, Schema Registry authentication, and a
temporary create/configure/produce/consume/delete workflow.
