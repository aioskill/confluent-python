# Confluent and Kafka CLI commands

The role installs Confluent Platform under `/app/confluent`. Its Kafka tools
are in `/app/confluent/bin`, and the cluster listens for clients on port 9092.
Kafka clients authenticate with SASL/SCRAM-SHA-512; use the installed client
properties file for each Kafka CLI command. This lab currently uses
`SASL_PLAINTEXT`, which authenticates clients but does not encrypt traffic.
The examples below use the lab brokers; replace these names when using another
inventory.

```bash
export CONFLUENT_HOME=/app/confluent
export BOOTSTRAP_SERVERS=sa01:9092,sa02:9092,sa03:9092
export CONFLUENT_CLIENT_CONFIG=/app/confluent/etc/kafka/client.properties
```

## Confluent CLI

```bash
/app/confluent/bin/confluent version
/app/confluent/bin/confluent --help
/app/confluent/bin/confluent kafka --help
```

Use `confluent --help` to see the commands supported by the installed CLI.
Kafka cluster administration below uses the Kafka command line tools shipped
with this installation.

## Executables in `/app/confluent/bin`

The role configures Kafka brokers and KRaft controllers, Schema Registry, and
Kafka REST Proxy. The distribution also includes commands for optional
Confluent services; their presence does not mean those services are configured
or running.

| Executable(s) | Use when you need to... |
| --- | --- |
| `verify-confluent-cluster` | Check each configured broker, the KRaft leader, and all Schema Registry and REST Proxy HTTP endpoints. Installed by this role. |
| `confluent` | Use the top-level Confluent CLI; start with `--help` to see supported commands. |
| `confluent-hub` | Search for, install, or manage Kafka Connect plugins from Confluent Hub. |
| `kafka-server-start`, `kafka-server-stop` | Start or stop a Kafka broker manually. Prefer `systemctl` for this role’s managed service. |
| `kafka-storage` | Format or inspect broker storage during KRaft setup or recovery. Do not reformat an active broker’s data directory. |
| `kafka-metadata-quorum` | Inspect the KRaft controller quorum and its leader, voters, and replication status. |
| `kafka-metadata-shell`, `kafka-metadata-recovery` | Inspect metadata log contents or perform specialist metadata recovery. Use recovery only as part of a planned incident procedure. |
| `kafka-topics` | List, create, describe, alter, or delete topics. |
| `kafka-configs` | View or change dynamic broker, topic, client, or user configuration. |
| `kafka-acls` | Inspect or manage Kafka access control lists when authorization is configured. |
| `kafka-console-producer`, `kafka-console-consumer` | Send or read plain-text messages for a quick check. |
| `kafka-avro-console-producer`, `kafka-avro-console-consumer` | Produce or consume Avro messages with Schema Registry. |
| `kafka-json-schema-console-producer`, `kafka-json-schema-console-consumer` | Produce or consume JSON Schema messages with Schema Registry. |
| `kafka-protobuf-console-producer`, `kafka-protobuf-console-consumer` | Produce or consume Protobuf messages with Schema Registry. |
| `kafka-consumer-groups`, `kafka-groups`, `kafka-share-groups` | Inspect consumer group membership, offsets, and lag; `kafka-share-groups` is for share groups. |
| `kafka-get-offsets` | Query offsets for a topic’s partitions. |
| `kafka-delete-records` | Delete records before specified offsets. This is destructive to retained topic data. |
| `kafka-broker-api-versions`, `kafka-cluster` | Check broker API support or inspect cluster identity and metadata. |
| `kafka-features` | Inspect or manage supported Kafka feature levels. |
| `kafka-leader-election`, `kafka-reassign-partitions`, `kafka-replica-exclusions` | Perform partition leadership or replica placement operations. Plan and monitor these cluster changes. |
| `kafka-add-brokers`, `kafka-remove-brokers`, `kafka-alter-broker-health`, `kafka-replica-status`, `kafka-replica-verification`, `kafka-log-dirs` | Manage or diagnose broker membership, health, replicas, and log directories. |
| `kafka-cluster-links`, `kafka-mirrors` | Manage or inspect cluster links and mirroring workflows. |
| `kafka-delegation-tokens` | Manage delegation tokens when Kafka token authentication is configured. |
| `kafka-transactions` | Inspect transactional producer state. |
| `kafka-producer-perf-test`, `kafka-consumer-perf-test`, `kafka-producer-perf-connection-test`, `kafka-e2e-latency` | Run throughput, connection, or end-to-end latency measurements. Load tests can affect a live cluster. |
| `kafka-verifiable-producer`, `kafka-verifiable-consumer`, `kafka-verifiable-share-consumer` | Generate verifiable test traffic for reliability or integration testing. |
| `kafka-console-share-consumer`, `kafka-share-consumer-perf-test` | Consume share records or measure share-consumer performance. |
| `kafka-streams-groups`, `kafka-streams-application-reset` | Inspect Kafka Streams groups or reset an application’s offsets/state. Resetting changes application progress. |
| `kafka-dump-log`, `kafka-restore` | Inspect Kafka log segment files or use Kafka’s restore tooling. These are specialist offline/data recovery tools. |
| `kafka-jmx`, `kafka-client-metrics`, `kafka-mt-info`, `kafka-oom-event`, `kafka-redact-oom-heap-dumps` | Inspect or support JMX, client metrics, multithreaded consumer, or out-of-memory diagnostics. |
| `kafka-bootstrap` | Run Kafka bootstrap tooling; consult `--help` for the bootstrap operation supported by this version. |
| `kafka-cells-admin` | Administer Kafka cells in deployments that use the cells feature. |
| `kafka-rebalance-cluster` | Rebalance cluster load using the specialized Kafka rebalancing tool. |
| `connect-distributed`, `connect-standalone`, `connect-mirror-maker`, `connect-plugin-path` | Run Kafka Connect workers or MirrorMaker 2, or inspect the plugin path. This role does not configure Connect. |
| `schema-registry-start`, `schema-registry-stop`, `schema-registry-stop-service`, `schema-registry-run-class`, `schema-exporter`, `schema-compatibility-check` | Run Schema Registry, export schemas, or check schema compatibility. This role runs Schema Registry on all three brokers. |
| `sr-acl-cli` | Manage Schema Registry ACLs when Schema Registry authorization is enabled. |
| `ksql-server-start`, `ksql-server-stop`, `ksql-stop`, `ksql`, `ksql-datagen`, `ksql-migrations`, `ksql-print-metrics`, `ksql-restore-command-topic`, `ksql-test-runner`, `ksql-run-class`, `run-ksql-test` | Run or administer ksqlDB, generate sample data, manage migrations, or run ksqlDB tests. ksqlDB is not configured by this role. |
| `confluent-rebalancer` | Run Confluent Self-Balancing Cluster operations when that feature is configured and licensed. |
| `replicator`, `replicator-verifier` | Run or verify Confluent Replicator data replication. Replicator is not configured by this role. |
| `register-deks`, `rewrap-deks` | Manage data encryption keys for Confluent’s encryption-at-rest workflows. |
| `security-plugins-run-class` | Launch internal Confluent security plugin utilities; use only with the relevant Confluent security feature instructions. |
| `kafka-rest-start`, `kafka-rest-stop`, `kafka-rest-stop-service`, `kafka-rest-run-class` | Run or stop Kafka REST Proxy. This role runs one REST Proxy instance per broker on port 8082. |
| `trogdor` | Run Kafka’s distributed workload and fault-testing framework. Use in a test environment. |
| `cluster-information-migration-script` | Migrate cluster information as part of a supported Confluent upgrade procedure. |
| `kafka-run-class`, `ksql-run-class`, `schema-registry-run-class` | Internal Java launch helpers used by the product scripts; generally call the specific service or utility command instead. |

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
$CONFLUENT_HOME/bin/kafka-broker-api-versions \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS"

$CONFLUENT_HOME/bin/kafka-metadata-quorum \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" describe --status

sudo systemctl status confluent-kafka.service
```

The metadata quorum status shows the current KRaft leader and voter set.
Run the systemd status command on a broker host.

## Topics

```bash
# List topics
$CONFLUENT_HOME/bin/kafka-topics \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --list

# Create a topic with three partitions and replication across all brokers
$CONFLUENT_HOME/bin/kafka-topics \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create --topic demo --partitions 3 --replication-factor 3

# Inspect a topic and its replica placement
$CONFLUENT_HOME/bin/kafka-topics \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic demo

# Delete a topic
$CONFLUENT_HOME/bin/kafka-topics \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --delete --topic demo
```

## Produce and consume messages

In one terminal, start the producer and enter one message per line:

```bash
$CONFLUENT_HOME/bin/kafka-console-producer \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --topic demo
```

In another terminal, read from the beginning:

```bash
$CONFLUENT_HOME/bin/kafka-console-consumer \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --topic demo --from-beginning
```

## Consumer groups

```bash
# List groups
$CONFLUENT_HOME/bin/kafka-consumer-groups \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --list

# Inspect offsets and lag
$CONFLUENT_HOME/bin/kafka-consumer-groups \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --group my-group
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
$CONFLUENT_HOME/bin/kafka-configs \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --entity-type topics --entity-name demo --describe

$CONFLUENT_HOME/bin/kafka-configs \
  --command-config "$CONFLUENT_CLIENT_CONFIG" \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
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
