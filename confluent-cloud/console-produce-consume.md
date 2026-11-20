# Console produce and consume with Confluent Cloud

This exercise creates a Kafka topic in Confluent Cloud, writes text records with the Confluent CLI, then reads them back. It uses the Kafka API credentials and bootstrap endpoint in this directory's `.env` file. The client uses `SASL_SSL` with `PLAIN`; TLS encrypts the connection.

## Prerequisites

- Run these commands from `./confluent-cloud`.
- Python dependencies are installed: `python3 -m pip install -r requirements.txt`.
- The Confluent CLI is installed and on `PATH`: `confluent version`.
- The Kafka API key in `.env` has permission to create the topic, produce to it, consume from it, and use a consumer group. If topic creation is not granted, create the topic through an administrator and continue at step 3.

The `.env` file is already ignored by Git. Do not print, paste, or commit its values.

## 1. Create a protected CLI client config

The Confluent CLI accepts a JSON client config. Generate it from the project's existing credential loader, which reads `.env` without echoing the values. The file is created with owner-only permissions and is Git-ignored.

```bash
cd ./confluent-cloud
python3 - <<'PY'
import json
import os
from pathlib import Path

from client_config import kafka_client_config

path = Path(".kafka-cli-client.json")
config = kafka_client_config()
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.fchmod(fd, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as client_file:
    json.dump(config, client_file)
    client_file.write("\n")
print(f"Created {path} with permissions 0600")
PY

```

The generated config contains the Kafka API key and secret. Keep it local and remove it when finished. It is separate from the Schema Registry credentials in `.env`.

## 2. Create a topic

In a class, all learners share one cluster. Replace `<STUDENT_ID>` with your
instructor-assigned ID (for example `s07`) in every topic name below.

Create a single-partition topic. Confluent Cloud manages replication, so the command does not set a replication factor:

```bash
python3 manage_topics.py create cloud-console-demo-<STUDENT_ID> --partitions 1
python3 manage_topics.py describe cloud-console-demo-<STUDENT_ID>
```

If the topic already exists, keep using it or use another topic name consistently and repeat these commands.

## 3. Start the consumer

In one terminal, from this directory, start a new consumer group:

```bash
confluent kafka topic consume cloud-console-demo-<STUDENT_ID> \
  --config-file .kafka-cli-client.json \
  --from-beginning
```

Leave the consumer running. The CLI creates a random consumer group by default, so it reads the topic from the beginning. Stop it with `Ctrl-C` after producing records.

## 4. Produce messages

In a second terminal, open the same project directory:

```bash
cd ./confluent-cloud
```

Start the producer:

```bash
confluent kafka topic produce cloud-console-demo-<STUDENT_ID> \
  --config-file .kafka-cli-client.json
```

Type a message and press Enter, for example:

```text
Hello from the Confluent Cloud console producer
```

The consumer terminal should print the message. Enter more lines for more records, then stop the producer with `Ctrl-D` or `Ctrl-C`.

## 5. Read the topic again (optional)

Start a separate consumer with a new group to read the records from the beginning:

```bash
confluent kafka topic consume cloud-console-demo-<STUDENT_ID> \
  --config-file .kafka-cli-client.json \
  --from-beginning
```

Stop it with `Ctrl-C`.

## Troubleshooting

- **Authentication failure:** Recreate `.kafka-cli-client.json` from `.env` using step 1. The Kafka username and password are the Kafka API key and secret, not the Schema Registry pair.
- **Authorization failure:** Check that the Kafka API key is scoped to the correct cluster and has the required topic and consumer-group permissions. Topic creation requires the corresponding administrative permission.
- **Could not connect:** Check that `BOOTSTRAP_SERVRS` (the existing spelling in this project's `.env`) contains the Cloud Kafka bootstrap hostname and port. The existing `client_config.py` also accepts `BOOTSTRAP_SERVERS` and `CONFLUENT_BOOTSTRAP_SERVERS`.
- **No output:** Confirm the producer and consumer use the same topic. Keep `--from-beginning` and use a fresh consumer group to read earlier records.

## Cleanup

Delete the tutorial topic if you no longer need its messages, then remove the local credential config:

```bash
python3 manage_topics.py delete cloud-console-demo-<STUDENT_ID> --yes
rm -f .kafka-cli-client.json
```

Deleting the topic removes its records. Leave it in place if you want to repeat the produce/consume steps later.
