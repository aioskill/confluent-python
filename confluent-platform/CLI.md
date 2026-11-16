# Confluent and Kafka CLI commands

The role installs Confluent Platform under `/app/confluent`. Its Kafka tools
are in `/app/confluent/bin`, and the cluster listens for clients on port 9092.
Kafka clients authenticate with SASL/SCRAM-SHA-512; use the installed client
properties file for each Kafka CLI command. This lab currently uses
`SASL_PLAINTEXT`, which authenticates clients but does not encrypt traffic.
The examples below use the lab brokers; replace these names when using another
inventory.

## Confluent CLI

```bash
/app/confluent/bin/confluent version
/app/confluent/bin/confluent --help
/app/confluent/bin/confluent kafka --help
```

Use `confluent --help` to see the commands supported by the installed CLI.
Kafka cluster administration below uses the Kafka command line tools shipped
with this installation.



## Cluster and broker health

Run the role-installed verification script from any broker to check all
configured Kafka broker endpoints, the KRaft leader, and all Schema Registry
and Kafka REST Proxy
endpoints. It also creates a temporary three-replica topic, changes and checks
its retention setting, produces and consumes a test message, then deletes the
topic.

```bash
/app/confluent/bin/verify-confluent-cluster
```

Override the client config file with `CONFLUENT_CLIENT_CONFIG`. Override the probe endpoints when needed with `CONFLUENT_BOOTSTRAP_SERVERS`,
`CONFLUENT_SCHEMA_REGISTRY_URLS`, `CONFLUENT_REST_PROXY_URLS`, or
`CONFLUENT_BIN_DIR`.

```bash
/app/confluent/bin/kafka-broker-api-versions \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092

/app/confluent/bin/kafka-metadata-quorum \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 describe --status

sudo systemctl status confluent-kafka.service
```

The metadata quorum status shows the current KRaft leader and voter set.
Run the systemd status command on a broker host.

For guided status, add/remove, and replacement exercises, see the
[KRaft administration exercise book](KRAFT-ADMINISTRATION-EXERCISE-BOOK.md).
For a lab on topic placement, configuration, and partition reassignment, see the
[cluster setup and partition reassignment lab](CLUSTER-SETUP-PARTITION-REASSIGNMENT-LAB.md).
For a focused failover exercise on `min.insync.replicas`, see the
[broker failover and minimum ISR lab](FAILOVER-MIN-INSYNC-REPLICAS-LAB.md).
For application principals and Kafka ACL commands, see the
[SCRAM and ACL security lab](SECURING-KAFKA-ACL-EXERCISES.md).

## Topics

```bash
# List topics
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --list

# Create a topic with three partitions and replication across all brokers
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --create --topic demo --partitions 3 --replication-factor 3

# Inspect a topic and its replica placement
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --describe --topic demo

# Delete a topic
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --delete --topic demo
```

## Produce and consume messages

In one terminal, start the producer and enter one message per line:

```bash
/app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --topic demo
```

To send a text file, redirect it to the console producer. Each line in the
file is sent as a separate message:

```bash
/app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --topic demo < /etc/passwd
```

In another terminal, read from the beginning:

```bash
/app/confluent/bin/kafka-console-consumer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --topic demo --from-beginning
```

If you want to see all fields for messages:

```bash
/app/confluent/bin/kafka-console-consumer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --topic demo --from-beginning \
  --property print.timestamp=true \
  --property print.partition=true \
  --property print.offset=true \
  --property print.headers=true \
  --property print.key=true \
  --property print.value=true
```

Send the key in the producer messages
```bash
/app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --topic demo \
  --property parse.key=true \
  --property key.separator=":"
```

## Consumer groups

```bash
# List groups
/app/confluent/bin/kafka-consumer-groups \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --list

# Inspect offsets and lag
/app/confluent/bin/kafka-consumer-groups \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --describe --group my-group
```

Increase the number of paritions

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --alter \
  --topic demo --partitions 2 

```

## Schema Registry

Schema Registry runs on each broker and uses Kafka's replicated `_schemas`
topic. Connect to any instance on port 8081 (or put the three endpoints behind
your own load balancer):

```bash
curl http://sa01:8081/
curl http://sa01:8081/subjects
curl http://sa01:8081/subjects/demo-value/versions/latest
sudo systemctl status confluent-schema-registry.service
```

## Kafka REST Proxy

The role runs Kafka REST Proxy on every broker on port 8082. Point clients at
any instance or place the three endpoints behind a load balancer. The v3 API
provides Kafka cluster, topic, and record operations:

```bash
curl http://sa01:8082/v3/clusters
sudo systemctl status confluent-rest-proxy.service
```

Register an Avro schema using the Schema Registry API:

```bash
curl -X POST http://sa01:8081/subjects/demo-value/versions \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"Demo\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}"}'
```

## Topic configuration

```bash
/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --entity-type topics --entity-name demo --describe

/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --entity-type topics --entity-name demo \
  --alter --add-config retention.ms=604800000
```

## Service operations

Run these commands on each broker host as needed:

```bash
sudo systemctl start confluent-kafka.service
sudo systemctl stop confluent-kafka.service
sudo systemctl restart confluent-kafka.service
sudo journalctl -u confluent-kafka.service -f
sudo systemctl status confluent-rest-proxy.service
sudo journalctl -u confluent-rest-proxy.service -f
```
