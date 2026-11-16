# Executables in `/app/confluent/bin`

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