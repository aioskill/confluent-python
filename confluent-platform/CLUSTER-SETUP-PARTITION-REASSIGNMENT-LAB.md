# Lab 1: Cluster setup and partition reassignment

This lab practices topic placement, topic configuration, and reassignment on
the existing three-broker KRaft cluster.

> **Cluster deployment is skipped.** These exercises use the cluster already
> deployed by the Confluent Ansible playbook.

## Learning objectives

By the end of the lab, you should be able to:

- inspect partition leaders, replica assignments, and ISR membership;
- update and verify topic-level `retention.ms` and `min.insync.replicas`;
- plan, execute, and verify a partition reassignment.

## Prepare the CLI

Run Kafka CLI commands from a broker host. The configured client file contains
the SCRAM credentials; do not copy its contents into commands or notes.

Confirm all three brokers are reachable before starting:

```bash
/app/confluent/bin/kafka-broker-api-versions \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092
```

## Exercise 1: create a topic and inspect placement

This exercise uses the fixed topic `lab-reassignment`. Check whether it
already exists before creating it; delete it only if it is leftover from an
earlier run and its data is no longer needed.

Create six partitions with a replica on every broker:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --create --topic lab-reassignment --partitions 6 --replication-factor 3
```

Describe the topic:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --describe --topic lab-reassignment
```

For each partition, record its `Leader`, `Replicas`, and `Isr`. Confirm that
each partition has three replicas and that all three are in the ISR while the
cluster is healthy. Count how many partitions each broker leads.

## Exercise 2: change topic configuration

Set a short retention period for this disposable topic and require at least
two in-sync replicas for writes using `acks=all`:

```bash
/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --entity-type topics --entity-name lab-reassignment \
  --alter --add-config retention.ms=120000,min.insync.replicas=2
```

Inspect the effective topic overrides:

```bash
/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --entity-type topics --entity-name lab-reassignment --describe
```

`min.insync.replicas=2` affects writes that use `acks=all`: the broker rejects
such writes if fewer than two replicas are in sync. `retention.ms=120000`
sets the age after which records become eligible for deletion. Retention
cleanup is asynchronous and depends on segment rolling and the broker cleanup
cycle, so do not expect records to disappear exactly at two minutes.

## Exercise 3: rebalance preferred leaders with reassignment

With replication factor three on exactly three brokers, every partition has a
replica on every broker. Reassignment cannot change which brokers store each
partition; it can change replica order, which changes the preferred leader.
Use this exercise to spread preferred leaders evenly across broker IDs.

First save and review the current assignment:

```bash
/app/confluent/bin/kafka-reassign-partitions \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties --list

/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --describe --topic lab-reassignment
```

Use the broker IDs shown in `Replicas` to create a target plan. This example
assumes IDs `1`, `2`, and `3`; replace them if your cluster uses different
IDs. Each broker becomes the first (preferred) replica for two partitions:

```bash
cat > /tmp/lab-reassignment.json <<'JSON'
{
  "version": 1,
  "partitions": [
    {"topic": "lab-reassignment", "partition": 0, "replicas": [1, 2, 3]},
    {"topic": "lab-reassignment", "partition": 1, "replicas": [2, 3, 1]},
    {"topic": "lab-reassignment", "partition": 2, "replicas": [3, 1, 2]},
    {"topic": "lab-reassignment", "partition": 3, "replicas": [1, 2, 3]},
    {"topic": "lab-reassignment", "partition": 4, "replicas": [2, 3, 1]},
    {"topic": "lab-reassignment", "partition": 5, "replicas": [3, 1, 2]}
  ]
}
JSON
```

Review the topic and broker IDs in the plan before executing it. Then execute
and verify the reassignment:

```bash
/app/confluent/bin/kafka-reassign-partitions \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --reassignment-json-file /tmp/lab-reassignment.json --execute

/app/confluent/bin/kafka-reassign-partitions \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --reassignment-json-file /tmp/lab-reassignment.json --verify
```

After verification, ask Kafka to elect each partition's preferred replica as
leader. This is scoped to the lab topic:

```bash
for partition in 0 1 2 3 4 5; do
  /app/confluent/bin/kafka-leader-election \
    --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
    --command-config /app/confluent/etc/kafka/client.properties \
    --election-type preferred --topic lab-reassignment --partition "$partition"
done
```

Describe the topic again. Confirm all partitions retain three replicas and
three ISR members, and the preferred replica distribution is two partitions
per broker. After the preferred elections, the leader should match the first
broker in each partition's replica list. If the topic was already in the
target order, reassignment may report no movement to perform.

## Cleanup

Delete the disposable topic and assignment plan when the exercise is complete:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --delete --topic lab-reassignment
rm -f /tmp/lab-reassignment.json
```

## Review questions

1. What is the difference between the replica list and the ISR?
2. Why does replication factor three on three brokers mean reassignment cannot
   move replicas to a different broker set?
3. How does a topic-level configuration override a broker default?
4. Why is retention cleanup not an exact timer for record deletion?

For a broker-failure exercise focused on `min.insync.replicas`, see the
[broker failover and minimum ISR lab](FAILOVER-MIN-INSYNC-REPLICAS-LAB.md).

## References

- [Apache Kafka: Basic operations and partition reassignment](https://kafka.apache.org/42/operations/basic-kafka-operations/)
- [Confluent: Broker configuration and `min.insync.replicas`](https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html)
- [Confluent: Topic configuration reference](https://docs.confluent.io/platform/7.8/installation/configuration/topic-configs.html)
