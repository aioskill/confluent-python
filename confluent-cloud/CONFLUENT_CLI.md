# Confluent CLI with Confluent Cloud

This walkthrough uses the Confluent CLI to inspect a Cloud account, select a
Kafka cluster, manage a topic, and optionally register a schema. It is separate
from the Python tutorials in this directory: the CLI uses your Confluent Cloud
user login for Cloud management commands, while Kafka data commands can use
Kafka and Schema Registry API keys.

## 1. Install and sign in

Install the CLI for your operating system using the
[official installation guide](https://docs.confluent.io/confluent-cli/current/install.html).
For Ubuntu 24.x, this project includes
[`install_confluent_cli_ubuntu24.sh`](install_confluent_cli_ubuntu24.sh); run it
with `./install_confluent_cli_ubuntu24.sh` (it uses `sudo` when needed).
Check that it is available:

```bash
confluent version
```

Sign in with your Confluent Cloud user account. If this machine has no browser,
use the browser flow on another machine with `--no-browser` and complete the
displayed URL/code steps:

```bash
confluent login
# or, on a headless host:
confluent login --no-browser
```

This is a user login, not the Kafka API key or Schema Registry API key in the
Python project's `.env` file. CLI user permissions determine which Cloud
management actions are allowed. For a service account or automation, use the
appropriate API credentials with the data-plane commands and follow your
organization's credential storage policy.

## 2. Select the Cloud environment and Kafka cluster

List environments and Kafka clusters visible to the logged-in user:

```bash
confluent environment list
confluent kafka cluster list
```

Set the active environment and Kafka cluster using their IDs from those lists
(environment IDs start with `env-`; Kafka cluster IDs start with `lkc-`):

```bash
confluent environment use <ENVIRONMENT_ID>
confluent kafka cluster use <KAFKA_CLUSTER_ID>
```

Confirm the selected cluster and list its topics:

```bash
confluent kafka cluster describe
confluent kafka topic list
```

You can also pass `--environment <ENVIRONMENT_ID>` and
`--cluster <KAFKA_CLUSTER_ID>` directly to commands that support those flags.
The CLI stores its own context locally; it does not read this project's `.env`.

## Invite users and assign roles in bulk

`invite_users_and_assign_roles.sh` invites human users from a text file and
assigns `CloudClusterAdmin` on one Kafka cluster plus `ResourceOwner` on all
Schema Registry subjects in the selected environment. The latter grants
management access to every subject in that registry, so use the requested
scope intentionally.

Create a file with one email address per line. Blank lines and comments are
allowed:

```text
# Kafka and schema administrators
alice@example.com
bob@example.com
```

After `confluent login`, run the script with the environment, Kafka cluster,
and Schema Registry cluster IDs:

```bash
./invite_users_and_assign_roles.sh users.txt <ENVIRONMENT_ID> \
  <KAFKA_CLUSTER_ID> <SCHEMA_REGISTRY_CLUSTER_ID>
```

The CLI account running the script needs permission to invite users and create
both role bindings. The invitations are sent before role bindings are created;
if a later role assignment fails, the invitation and any earlier assignments
remain in place. Review the CLI output and rerun the missing binding manually
if needed. The invitee must accept the invitation to activate the account.

## 3. Create and inspect a topic

Create a disposable tutorial topic. Cloud manages replication (the replication
factor is three), so choose the partition count but don't supply a replication
factor:

```bash
confluent kafka topic create cli-demo --partitions 3
confluent kafka topic list
confluent kafka topic describe cli-demo
confluent kafka topic configuration list cli-demo
```

Change an editable configuration and inspect the result:

```bash
confluent kafka topic update cli-demo --config retention.ms=86400000
confluent kafka topic configuration list cli-demo
```

Topic configurations supported for editing depend on the Cloud cluster type.

## 4. Produce and consume string messages

The CLI's Cloud `produce` and `consume` commands connect to the Kafka data
plane. Supply a Kafka cluster API key/secret (not the Cloud user password).
The examples below use placeholders: replace them locally, and avoid saving
real secrets in shell history, scripts, or shared files. The API key must have
access to the selected cluster and topic.

Produce a few records. Enter one message per line and press `Ctrl-D` when done:

```bash
confluent kafka topic produce cli-demo \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET>
```

Consume from the beginning; press `Ctrl-C` to stop:

```bash
confluent kafka topic consume cli-demo \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --from-beginning
```

The CLI creates a temporary consumer group by default. For a repeatable group,
add `--group <GROUP_ID>`.

## 5. Register and use a Schema Registry schema (optional)

List Schema Registry endpoints in the active environment, then view the
registry details, including its cluster ID and endpoint:

```bash
confluent schema-registry endpoint list
confluent schema-registry cluster describe
```

To query a different environment without changing the active CLI context, pass
its ID to both commands:

```bash
confluent schema-registry endpoint list --environment <ENVIRONMENT_ID>
confluent schema-registry cluster describe --environment <ENVIRONMENT_ID>
```

In Confluent Cloud, `schema-registry cluster describe` views the registry for
the selected environment. `schema-registry cluster list` is a Confluent
Platform command; use `schema-registry endpoint list` to list Cloud endpoints.

Register this project's sample Avro schema. Run the command from this
directory, and choose a subject name that is unused in your environment:

```bash
confluent schema-registry schema create \
  --subject cli-demo-value \
  --schema order.avsc \
  --type avro
```

The logged-in user needs the appropriate Schema Registry permissions. To
produce or consume Avro records with the CLI, use the command's
`--value-format avro`, `--schema` or `--schema-registry-endpoint` options, and
provide the Kafka and Schema Registry API key credentials with the respective
`--api-*` and `--schema-registry-api-*` flags. For example, the shape of a
produce command is:

```bash
confluent kafka topic produce cli-demo \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --value-format avro \
  --schema order.avsc \
  --schema-registry-endpoint <SCHEMA_REGISTRY_ENDPOINT> \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

The corresponding consume command uses `--value-format avro`,
`--schema-registry-endpoint`, and the Schema Registry API key flags, along with
the Kafka cluster credentials and `--from-beginning`. See the official
[produce](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_produce.html),
[consume](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_consume.html),
and [schema create](https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/schema/confluent_schema-registry_schema_create.html)
references for all flags and formats (Avro, JSON Schema, and Protobuf).

## 6. Clean up

Delete only the tutorial topic and subject if you no longer need them. These
commands permanently remove data/schema versions, so verify the names first:

```bash
confluent kafka topic describe cli-demo
confluent kafka topic delete cli-demo
confluent schema-registry subject list
```

Delete a subject only after confirming it is the tutorial subject and you want
to remove its schema history:

```bash
confluent schema-registry subject delete cli-demo-value
```

## References

- [Confluent CLI getting started](https://docs.confluent.io/confluent-cli/current/)
- [Connect the CLI to Confluent Cloud](https://docs.confluent.io/confluent-cli/current/connect.html)
- [Kafka topic commands](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/index.html)
- [Schema Registry commands](https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/index.html)
- [Invite a user](https://docs.confluent.io/confluent-cli/current/command-reference/iam/user/invitation/confluent_iam_user_invitation_create.html)
- [Create a role binding](https://docs.confluent.io/confluent-cli/current/command-reference/iam/rbac/role-binding/confluent_iam_rbac_role-binding_create.html)
