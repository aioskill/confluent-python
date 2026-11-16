# Lab: broker failover and minimum ISR

This exercise demonstrates how a partition can remain readable after a broker
failure while `acks=all` writes are rejected by `min.insync.replicas`. It uses
the existing three-broker lab cluster. Do not run the failure portion against
a production cluster.

## Learning objectives

- Read a partition's leader, replica list, and in-sync replica (ISR) list.
- Observe leader election and ISR shrinkage after one broker stops.
- Prove that `min.insync.replicas=2` rejects `acks=all` writes to an RF=2
  partition when only one replica remains in sync.
- Confirm that consumers can still read from the elected leader.
- Restore the broker and verify writes recover.

## Safety and preparation

The lab brokers are combined KRaft controllers and brokers. Stop only one
broker at a time: the two remaining voters maintain the required majority of
two. Stop and start the service over SSH, and restore the broker promptly
after the failure observation.

The Kafka CLI tools exist only on the broker hosts, so log in to one first
(`ssh ubuntu@sa01`) and run every Kafka command below there. The `ssh`
stop/start commands are run from your workstation (or from another broker).
When a broker is stopped, run the Kafka commands from a surviving broker.

This exercise uses the fixed topic name `failover-min-isr-v2`. Confirm the
topic does not already exist (`kafka-topics --list`) before starting; if it
does, delete it only if it is leftover from an earlier run.

Confirm all three brokers respond before proceeding:

```bash
/app/confluent/bin/kafka-broker-api-versions \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092
```

If a broker is already down, stop here and restore it before starting the lab.

## 1. Create and inspect the test topic

Create a single-partition topic with two replicas:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --create --topic failover-min-isr-v2 --partitions 1 --replication-factor 2
```

Check its effective topic configuration and placement:

```bash
/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --entity-type topics --entity-name failover-min-isr-v2 --describe

/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --describe --topic failover-min-isr-v2
```

The lab server configuration sets `min.insync.replicas=2`; a topic without an
override inherits it. Confirm the partition has two replicas and both are in
the ISR. Record the leader broker ID; it differs from run to run. Broker IDs map to
hosts as `1=sa01`, `2=sa02`, and `3=sa03` (the `kafka-broker-api-versions`
output above shows `sa0N:9092 (id: N ...)`). Call the leader's host
`LEADER_HOST` below, and pick one of the other two hosts as `SURVIVOR_HOST`.

## 2. Establish the healthy baseline

Produce a record with `acks=all`:

```bash
printf 'before-failure\n' | /app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-property acks=all --topic failover-min-isr-v2
```

The command should complete without a `NotEnoughReplicasException`. The ISR contains two replicas, satisfying the
minimum of two.

## 3. Stop the partition leader

From a machine with SSH access, replace `LEADER_HOST` with the host recorded
in step 1:

```bash
ssh ubuntu@LEADER_HOST 'sudo systemctl stop confluent-kafka.service'
```

Wait about 15 seconds, then log in to `SURVIVOR_HOST` and describe the topic
and the KRaft quorum. From here on, list only the two surviving brokers in
`--bootstrap-server` (shown below as `sa01,sa02`; substitute the two hosts
that are still running):

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 \
  --describe --topic failover-min-isr-v2

/app/confluent/bin/kafka-metadata-quorum \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 describe --replication
```

The remaining replica becomes leader and the ISR contains only that one
replica (the stopped broker appears under `Offline`). In the quorum output the
stopped voter's lag grows and it stops fetching. The cluster still has two of its
three KRaft voters, so the quorum can make progress.

## 4. Prove the write and read behavior

Try a write with `acks=all`:

```bash
printf 'during-failure-acks-all\n' | /app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 \
  --command-property acks=all --topic failover-min-isr-v2
```

Expected: after a few seconds of `NOT_ENOUGH_REPLICAS` retry warnings, the
record is rejected with `NotEnoughReplicasException`, because the ISR has one member and the minimum is two.
This is the intended protection against acknowledging an `acks=all` write
without the configured number of in-sync copies.

Now read from the beginning using a new group:

```bash
/app/confluent/bin/kafka-console-consumer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 \
  --topic failover-min-isr-v2 --group failover-min-isr-v2-read --from-beginning \
  --timeout-ms 10000
```

Expected: `before-failure` is printed. The `TimeoutException` error and
`Processed a total of 1 messages` line at the end are normal for this
one-shot command. The consumer can read because the surviving broker
is the partition leader; `min.insync.replicas` is a write constraint and does
not block reads.

## 4b. Optional control: demonstrate the minimum is the cause

While the broker is still stopped (one ISR member), lower this disposable
topic's minimum to one and repeat the write:

```bash
/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 \
  --entity-type topics --entity-name failover-min-isr-v2 \
  --alter --add-config min.insync.replicas=1

printf 'during-failure-min-isr-1\n' | /app/confluent/bin/kafka-console-producer \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 \
  --command-property acks=all --topic failover-min-isr-v2
```

Expected: the write now succeeds, because a topic override takes precedence
over the broker default. With RF=2, a minimum of one trades durability for
availability: that acknowledgement exists on only one broker. Remove the
override before continuing:

```bash
/app/confluent/bin/kafka-configs \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092 \
  --entity-type topics --entity-name failover-min-isr-v2 \
  --alter --delete-config min.insync.replicas
```

If you skip this control, ignore `during-failure-min-isr-1` below.

## 5. Restore and verify

From a machine with SSH access, start the same host that was stopped:

```bash
ssh ubuntu@LEADER_HOST 'sudo systemctl start confluent-kafka.service'
```

Wait until the broker is reachable and the topic ISR again contains both
replicas (`Isr: 1,2` in any order). Produce another `acks=all` record using the
command from step 2 (change the text to `after-recovery`), then consume from
the beginning again with a new group name such as `failover-min-isr-v2-read2`.
`before-failure` and `after-recovery` should be visible, plus
`during-failure-min-isr-1` if you ran the control.

## Cleanup

Confirm the broker is active and then delete the test topic:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --delete --topic failover-min-isr-v2
```

## Review questions

1. Why did the surviving replica become leader, but not accept `acks=all`
   writes?
2. Why did the consumer still read the record written before the failure?
3. How do replication factor, ISR size, `acks`, and `min.insync.replicas`
   work together?
4. What availability and durability tradeoff does lowering the minimum make?
5. Why must this three-voter KRaft lab stop only one broker at a time?
