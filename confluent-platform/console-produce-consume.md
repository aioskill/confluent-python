# Console producer and consumer with Confluent Platform

This tutorial sends a message to Kafka with the Confluent command-line producer, then reads it with the command-line consumer. It assumes the lab Confluent cluster is already installed and uses SASL/SCRAM-SHA-512.

## Cluster details

The lab playbook configures:

- Kafka client listener: `SASL_PLAINTEXT` on port `9092`
- Authentication: `SCRAM-SHA-512`
- Username: `confluent`
- Broker addresses: `sa01:9092,sa02:9092,sa03:9092`
- CLI authentication file on each broker: `/app/confluent/etc/kafka/client.properties` (mode `0600`)

`SASL_PLAINTEXT` authenticates clients but does not encrypt traffic. Use this listener only on a trusted lab network. The password is stored in the Ansible inventory secret file and in the protected client properties file; do not paste it into commands or share either file.

## 1. Connect to a broker

From a machine that can resolve and reach the lab broker hostnames:

```bash
ssh ubuntu@sa01
```

Run the examples on `sa01`, where the Kafka CLI and client properties file are installed. If running from another host, install/use compatible Kafka CLI tools and provide a protected client properties file with the same settings.

## 2. Create a topic

This walkthrough uses the fixed topic `console-tutorial`. If it already
exists and contains data you need, stop and choose another topic name. Delete
it before continuing only if it is leftover from an earlier run and you no
longer need its messages.

Create a topic with three partitions and replication factor three:

```bash
/app/confluent/bin/kafka-topics \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --create \
  --topic console-tutorial \
  --partitions 3 \
  --replication-factor 3
```

Inspect the topic placement with:

```bash
/app/confluent/bin/kafka-topics \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --describe \
  --topic console-tutorial
```

The `--command-config` option supplies SASL/SCRAM settings for each CLI command.

## 3. Start the console consumer

In a second terminal on `sa01`, start the consumer first. `--from-beginning`
reads existing messages as well as new ones. This walkthrough uses a fixed
consumer group; choose a different group name to replay the topic after the
group has committed offsets:

```bash
/app/confluent/bin/kafka-console-consumer \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --topic console-tutorial \
  --group console-tutorial-v2 \
  --from-beginning
```

Leave it running. The consumer prints each message value on its own line. Stop it with `Ctrl-C` when finished.

## 4. Produce messages

In the first terminal, run the console producer:

```bash
/app/confluent/bin/kafka-console-producer \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --topic console-tutorial
```

Type a message and press Enter, for example:

```text
Hello from the Confluent console producer
```

The consumer terminal should display that text. Enter additional lines to send more records. Stop the producer with `Ctrl-C`.

## 5. Read messages from the beginning (optional)

To read the topic in a separate one-shot command, use a new consumer group and a timeout:

```bash
/app/confluent/bin/kafka-console-consumer \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --topic console-tutorial \
  --group console-read-v2 \
  --from-beginning \
  --timeout-ms 10000
```

The timeout makes the command stop waiting after no messages arrive within
the configured interval. Depending on the Kafka CLI version, that timeout can
also be logged as a `TimeoutException` after the records have been printed.
On the first run, the group reads from the beginning. To replay the topic
after that group has committed offsets, choose a different group name. The
`--command-config` option is used for producer and consumer clients as well as
topic administration; the older `--producer.config` and `--consumer.config`
options are deprecated in the installed CLI.

## Troubleshooting

- **Authentication failure:** Check that the configured client file is readable by the current user. On `sa01`, the file is `/app/confluent/etc/kafka/client.properties` and should remain mode `0600`.
- **Could not connect / unknown host:** Confirm the broker names resolve and port `9092` is reachable from the client. Use the exact advertised broker hostnames configured for the cluster if these names differ in your environment.
- **Topic already exists:** If it contains data you need, stop and choose another topic name consistently in each command. Otherwise, delete the leftover topic before creating it.
- **No output:** Start the consumer before producing, check the topic name, and use `--from-beginning` with a new consumer group to inspect earlier records.
- **Broker addresses:** The lab inventory's `kafka_brokers` group is the source of truth. This tutorial uses `sa01`, `sa02`, and `sa03`, as named for the lab machines.

## Cleanup (optional)

Delete the tutorial topic only if you no longer need its messages:

```bash
/app/confluent/bin/kafka-topics \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties \
  --delete \
  --topic console-tutorial
```
