# Python cluster discovery tutorial

`discover_cluster.py` demonstrates how to discover broker nodes through the
Confluent Kafka Python `AdminClient`, then combine that metadata with KRaft
quorum status to identify controller voters and the active controller.

The Confluent Ansible role installs the Python client and Schema Registry
serializer dependencies in `/app/confluent/python-venv` on each broker. On a
broker, activate that environment before running the examples:

```bash
source /app/confluent/python-venv/bin/activate
```

For a separate machine, create a local environment and install the pinned
project dependencies instead:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
source .venv/bin/activate
```

Kafka uses SASL/SCRAM-SHA-512. When run on a broker, the tutorials read the
credentials from `/app/confluent/etc/kafka/client.properties`. For a client
running elsewhere, set `CONFLUENT_SASL_USERNAME` and `CONFLUENT_SASL_PASSWORD`
to the values in the private `clusters/lab/confluent_sasl_password` file on
the Ansible controller.

Run it from a broker host with the role's default lab addresses:

```bash
python3 discover_cluster.py
```

Or specify different brokers and the location of the Kafka CLI tools:

```bash
python3 discover_cluster.py \
  --bootstrap-servers broker-a:9092,broker-b:9092,broker-c:9092 \
  --kafka-bin /app/confluent/bin
```

The script uses the Kafka metadata API for brokers and
`kafka-metadata-quorum describe --status` for KRaft roles. With this role's
combined broker/controller topology, each node should appear as both a broker
and controller voter; one voter is also shown as the active controller.

## Topic management tutorial

`manage_topics.py` demonstrates topic operations with the Python AdminClient.
It uses the same default bootstrap addresses and Python package as the cluster
discovery tutorial:

```bash
python3 manage_topics.py list
python3 manage_topics.py create demo --partitions 3 --replication-factor 3
python3 manage_topics.py describe demo
python3 manage_topics.py config demo --set retention.ms=604800000
python3 manage_topics.py config demo --delete retention.ms
python3 manage_topics.py partitions demo --count 6
python3 manage_topics.py replication-factor demo --count 2
python3 manage_topics.py delete demo --yes
```

Topic partition counts can only be increased. Changing replication factor
performs a real partition reassignment and waits for the new replicas to become
in-sync; it moves data across brokers. Topic deletion is permanent and
requires `--yes`. Put global options such as `--bootstrap-servers` before the
subcommand, for example:

```bash
python3 manage_topics.py --bootstrap-servers broker-a:9092,broker-b:9092 \
  list
```

## Produce messages

Create the topic before producing to it, then consume the messages. This
example creates three partitions with a replica on each of the three lab
brokers, sends ten sample messages, and consumes them:

```bash
python3 manage_topics.py create demo-messages \
  --partitions 3 --replication-factor 3
python3 produce_messages.py demo-messages --count 10
python3 consume_messages.py demo-messages --count 10
```

To send explicit values instead, repeat `--message`. The script waits for
delivery reports and uses `acks=all` with producer idempotence enabled:

```bash
python3 produce_messages.py demo-messages \
  --message 'first event' --message 'second event' \
  --key-prefix tutorial
```

The topic must already exist. To choose another cluster, pass
`--bootstrap-servers` to each command or set `CONFLUENT_BOOTSTRAP_SERVERS`.

The consumer uses `auto.offset.reset=earliest` for a new group and commits its
offsets after reading. The default group is `tutorial-<topic>`; to read a topic
again from its beginning, use a new group ID:

```bash
python3 consume_messages.py demo-messages --count 10 --group tutorial-replay
```

## JSON Schema with Schema Registry

Create a topic, produce JSON Schema validated orders, and consume them through
Schema Registry. The producer registers `order.schema.json` under the
`<topic>-value` subject when it first serializes a record.

```bash
python3 manage_topics.py create json-schema-orders \
  --partitions 3 --replication-factor 3
python3 produce_json_schema.py json-schema-orders --count 5
python3 consume_json_schema.py json-schema-orders --count 5
```

The scripts use `http://sa01:8081` by default. Override it with
`--schema-registry-url` or set `SCHEMA_REGISTRY_URL`. The Kafka bootstrap
addresses can be changed with `--bootstrap-servers` or
`CONFLUENT_BOOTSTRAP_SERVERS`.

## Avro with Schema Registry

Create a topic, produce Avro orders, and consume them through Schema Registry.
The producer registers `order.avsc` under the `<topic>-value` subject when it
first serializes a record. The consumer reads the schema ID from each record
and fetches the writer schema from Schema Registry.

```bash
python3 manage_topics.py create avro-orders \
  --partitions 3 --replication-factor 3
python3 produce_avro.py avro-orders --count 5
python3 consume_avro.py avro-orders --count 5
```

The scripts use the same Kafka and Schema Registry defaults as the JSON Schema
examples. Override them with `--bootstrap-servers`, `--schema-registry-url`,
`CONFLUENT_BOOTSTRAP_SERVERS`, or `SCHEMA_REGISTRY_URL`.

## Protocol Buffers with Schema Registry

Create a topic, produce Protocol Buffer orders defined by `order.proto`, and
consume them through Schema Registry. `order_pb2.py` is generated from the
`.proto` file and included so the scripts can run without a compiler installed.

```bash
python3 manage_topics.py create protobuf-orders \
  --partitions 3 --replication-factor 3
python3 produce_protobuf.py protobuf-orders --count 5
python3 consume_protobuf.py protobuf-orders --count 5
```

If you edit `order.proto`, regenerate `order_pb2.py` with `grpcio-tools`:

```bash
python3 -m pip install grpcio-tools
python3 -m grpc_tools.protoc -I . --python_out=. order.proto
```

These scripts use the same Kafka and Schema Registry endpoint options as the
Avro examples.

## Compacted topics

Create a topic, enable compaction, then produce keyed updates and a tombstone:

```bash
python3 manage_topics.py create compact-demo \
  --partitions 3 --replication-factor 3
python3 manage_topics.py config compact-demo --set cleanup.policy=compact
python3 produce_compacted_messages.py compact-demo
python3 consume_compacted_topic.py compact-demo
```

Supply explicit records or tombstones when needed:

```bash
python3 produce_compacted_messages.py compact-demo \
  --record account-1=active \
  --record account-1=closed \
  --delete-key account-2
```
