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

For an end-to-end CLI and Terraform provisioning exercise with API key and
endpoint checks, see
[Lab 3: Provisioning via CLI & Terraform](LAB-3-PROVISIONING-VIA-CLI-TERRAFORM.md).

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

## Optional: invite users and assign roles in bulk

For a guided service-account exercise comparing prefixed `DeveloperRead` with
`EnvironmentAdmin`, testing an authorization denial, and finding the audit
events, see [Lab 5: RBAC And Audit Logs](LAB-5-RBAC-PREFIX-AND-AUDIT-LAB.md).

`invite_users.sh` sends invitations from a text file. After invitees accept
and exist as Confluent Cloud users, `assign_roles.sh` fetches the current user
list and assigns `OrganizationAdmin` to each user except
`training@avalanchio.com`. This role allows users to create, manage, and delete
environments, clusters, Schema Registry resources, and other resources across
the organization. Use this script only in a dedicated training organization;
it grants broad organization-wide administration rights. Existing matching
bindings are skipped, so rerunning the script is idempotent. The script asks
for confirmation before it creates missing role bindings.

Create a file with one email address per line. Blank lines and comments are
allowed:

```text
# Kafka and schema administrators
alice@example.com
bob@example.com
```

After `confluent login`, send invitations:

```bash
./invite_users.sh users.txt
```

The CLI account needs permission to invite users. Each invitee must accept the
invitation and exist as a Confluent Cloud user before role assignment; the
assignment script cannot bind roles to a pending invitation.

Then assign the role to all current users except the excluded super admin:

```bash
./assign_roles.sh
```

The CLI account running this script needs permission to create the
`OrganizationAdmin` role binding. Pending invitations are not included by the
user-list command; rerun the script after invitees accept and appear in the
organization. Use `./assign_roles.sh --yes` to skip the confirmation prompt
when running in a controlled automation context.

## 3. Create and inspect a topic

In a class, all learners share one Kafka cluster, so every topic, consumer
group, and schema subject name below carries your instructor-assigned
`<STUDENT_ID>` (for example `s07`); replace it everywhere.

Create a disposable tutorial topic. Cloud manages replication (the replication
factor is three), so choose the partition count but don't supply a replication
factor:

```bash
confluent kafka topic create cli-demo-<STUDENT_ID> --partitions 1
confluent kafka topic list
confluent kafka topic describe cli-demo-<STUDENT_ID>
confluent kafka topic configuration list cli-demo-<STUDENT_ID>
```

Change an editable configuration and inspect the result:

```bash
confluent kafka topic update cli-demo-<STUDENT_ID> --config retention.ms=86400000
confluent kafka topic configuration list cli-demo-<STUDENT_ID>
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
confluent kafka topic produce cli-demo-<STUDENT_ID> \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET>
```

Consume from the beginning; press `Ctrl-C` to stop:

```bash
confluent kafka topic consume cli-demo-<STUDENT_ID> \
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

The `Used Schemas` count in the registry details can be nonzero even when
`subject list` returns no subjects. The default listing omits soft-deleted
subjects and schemas; include them with `--all`:

```bash
confluent schema-registry subject list --all
confluent schema-registry schema list \
  --all \
  --subject-prefix <SUBJECT_PREFIX>
```

Soft-deleted schemas can appear in these listings but may not be retrievable
with `schema describe --version latest` because the subject is deleted.

### Recover a soft-deleted schema

A soft-deleted schema can be recovered by fetching its definition and
registering it again under the same subject. This does not restore the old
version entry; registration creates a new version. Hard-deleted schemas cannot
be recovered this way. Use the Schema Registry API key for this environment;
when only the key is supplied, `curl` prompts for the secret.

Fetch a specific deleted version, replacing the endpoint, subject, and version
with the values from your registry:

```bash
mkdir -p /tmp/confluent-schema-recovery

curl --user <SCHEMA_REGISTRY_API_KEY> \
  "<SCHEMA_REGISTRY_ENDPOINT>/subjects/<SUBJECT>/versions/<VERSION>?deleted=true" \
  --output /tmp/confluent-schema-recovery/deleted-schema.json
```

Create a registration payload from that response, then post it to the same
subject:

```bash
jq '{schema: .schema, schemaType: (.schemaType // "AVRO")}' \
  /tmp/confluent-schema-recovery/deleted-schema.json \
  > /tmp/confluent-schema-recovery/schema-registration.json

curl --user <SCHEMA_REGISTRY_API_KEY> \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data-binary @/tmp/confluent-schema-recovery/schema-registration.json \
  "<SCHEMA_REGISTRY_ENDPOINT>/subjects/<SUBJECT>/versions"
```

Repeat for each soft-deleted version you need to recover. The API supports
retrieving deleted subjects and re-registering their schema definitions; see
[Confluent's recovery instructions](https://docs.confluent.io/cloud/current/sr/schemas-delete-cc.html).

Register this project's sample Avro schema. Run the command from this
directory, and the subject `cli-demo-avro-<STUDENT_ID>-value` is the one the Avro producer below uses for
topic `cli-demo-avro-<STUDENT_ID>` (`<topic>-value`). Choose a name that is unused in your
environment:

```bash
confluent schema-registry schema create \
  --subject cli-demo-avro-<STUDENT_ID>-value \
  --schema order.avsc \
  --type avro
```

List registered subjects, then list schemas matching the tutorial subject
prefix. Inspect the latest version registered under that subject:

```bash
confluent schema-registry subject list
confluent schema-registry schema list --subject-prefix cli-demo-avro-<STUDENT_ID>-value
confluent schema-registry schema describe \
  --subject cli-demo-avro-<STUDENT_ID>-value \
  --version latest
```

The logged-in user needs the appropriate Schema Registry permissions. To
produce or consume Avro records with the CLI, use the command's
`--value-format avro`, `--schema` or `--schema-registry-endpoint` options, and
provide the Kafka and Schema Registry API key credentials with the respective
`--api-*` and `--schema-registry-api-*` flags. Create a separate topic for
Avro records so the earlier plain-text messages in `cli-demo-<STUDENT_ID>` are not mixed
with Avro data:

```bash
confluent kafka topic create cli-demo-avro-<STUDENT_ID> --partitions 1
```

Start the Avro producer:

```bash
confluent kafka topic produce cli-demo-avro-<STUDENT_ID> \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --value-format avro \
  --schema order.avsc \
  --schema-registry-endpoint <SCHEMA_REGISTRY_ENDPOINT> \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET>
```

At the producer prompt, enter a JSON record that matches `order.avsc`:

```json
{"order_id":"order-1","customer_id":"customer-1","amount":12.5,"status":"NEW"}
```

Enter each record on one line. Plain text such as `hello` fails because the
Avro serializer expects a JSON record beginning with `{` and containing the
schema's fields with the correct types.

Consume and decode the Avro records from the beginning:

```bash
confluent kafka topic consume cli-demo-avro-<STUDENT_ID> \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <KAFKA_API_KEY> \
  --api-secret <KAFKA_API_SECRET> \
  --value-format avro \
  --schema-registry-endpoint <SCHEMA_REGISTRY_ENDPOINT> \
  --schema-registry-api-key <SCHEMA_REGISTRY_API_KEY> \
  --schema-registry-api-secret <SCHEMA_REGISTRY_API_SECRET> \
  --from-beginning
```

Kafka topics can contain Avro records that use different Schema Registry
schema IDs; each Avro record carries its own schema ID. The Avro consumer
still expects every record it reads to use the Avro wire format. It cannot
decode plain-text records. If it prints an Avro record and then reports
`unknown magic byte 104`, it encountered a plain-text record such as `hello`
in the same topic. Confirm that both producer and consumer use
`cli-demo-avro-<STUDENT_ID>`, not the earlier string topic `cli-demo-<STUDENT_ID>`. If
`cli-demo-avro-<STUDENT_ID>` also contains mixed data, create a clean topic and use that
same topic name for both commands:

```bash
confluent kafka topic create cli-demo-avro-clean-<STUDENT_ID> --partitions 1
```

See the official
[produce](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_produce.html),
[consume](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_consume.html),
and [Schema Registry schema commands](https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/schema/index.html)
references for all flags and formats (Avro, JSON Schema, and Protobuf).

## 6. Clean up

Delete only the tutorial topics and schema subject if you no longer need
them. Topic deletion removes the topic's data; schema deletion is soft by
default. Verify the names before proceeding:

```bash
confluent kafka topic describe cli-demo-<STUDENT_ID>
confluent kafka topic delete cli-demo-<STUDENT_ID>
confluent kafka topic delete cli-demo-avro-<STUDENT_ID>
confluent schema-registry subject list --all
```

If you created `cli-demo-avro-clean-<STUDENT_ID>`, delete it too
(`confluent kafka topic delete cli-demo-avro-clean-<STUDENT_ID>`); its producer registers
`cli-demo-avro-clean-<STUDENT_ID>-value`, which needs the same schema cleanup below.

Soft-delete every version of the tutorial subject with the schema delete
command. Confirm the subject name before running it:

```bash
confluent schema-registry schema delete \
  --subject cli-demo-avro-<STUDENT_ID>-value \
  --version all
```

To permanently remove the already soft-deleted versions, run the command
again with `--permanent`. This removes schema metadata and can affect consumers
of records that reference these schema IDs:

```bash
confluent schema-registry schema delete \
  --subject cli-demo-avro-<STUDENT_ID>-value \
  --version all \
  --permanent
```

Hard deletion is irreversible and removes schema metadata, including schema
IDs. Verify that no stored Kafka records or applications depend on these
schemas before running the commands. In a shared environment, first inspect
`confluent schema-registry subject list --all` and delete only your own
subjects.

## References

- [Confluent CLI getting started](https://docs.confluent.io/confluent-cli/current/)
- [Connect the CLI to Confluent Cloud](https://docs.confluent.io/confluent-cli/current/connect.html)
- [Kafka topic commands](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/index.html)
- [Schema Registry commands](https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/index.html)
- [Invite a user](https://docs.confluent.io/confluent-cli/current/command-reference/iam/user/invitation/confluent_iam_user_invitation_create.html)
- [Create a role binding](https://docs.confluent.io/confluent-cli/current/command-reference/iam/rbac/role-binding/confluent_iam_rbac_role-binding_create.html)
