# Lab 5: RBAC And Audit Logs

This lab compares a service account with `DeveloperRead` access on a Kafka
topic-name prefix to a second service account with `EnvironmentAdmin` on one
environment. Learners verify an allowed read, an out-of-prefix denial, and the
corresponding role-binding and authorization events in the audit log.

Use a private sandbox environment. `EnvironmentAdmin` can manage all resources in its
environment, including clusters, topics, service accounts, and access grants.
Do not assign it in a production environment for this exercise.

## Learning objectives

- Create service accounts and bind Cloud RBAC roles at different scopes.
- Inspect the resulting role bindings and compare their effective scope.
- Verify that a restricted account can read a topic in its prefix but cannot
  read a topic outside it.
- Find role-binding and denied-authorization events in the Cloud audit log.

## Prerequisites

- Confluent CLI installed and logged in with a user authorized to create
  service accounts, create these role bindings, create test topics, and create
  Kafka API keys for the service accounts.
- The logged-in user can grant both requested roles. In many organizations,
  `OrganizationAdmin` is required for these role bindings; follow your
  organization's delegated role-assignment policy.
- A sandbox Confluent Cloud environment and Kafka cluster. RBAC on Kafka
  topics and consumer groups requires a Standard, Enterprise, Dedicated, or
  Freight cluster; Basic clusters do not support these granular bindings.
- Organization audit logs enabled. Viewing them in the Cloud Console requires
  `OrganizationAdmin`, and the organization must have at least one Standard,
  Enterprise, Dedicated, or Freight Kafka cluster.

## 0. Create your own short-lived environment and Standard cluster

Each learner creates a private environment and Standard cluster for this lab
and deletes both at the end (see Cleanup). The topic and consumer-group role
bindings require granular RBAC, which Basic clusters do not support, and
`EnvironmentAdmin` grants access to everything in its environment, so a private
environment keeps learners from affecting each other. Because every learner has
their own cluster, topics, groups, and prefixes below need no student suffix;
only the environment, cluster, and service-account names (which must be unique
in the organization) carry `<STUDENT_ID>`.

Instructor prerequisites: the organization needs room for one environment and
one Standard cluster per learner at the same time (the default quota is 25
environments per organization, shared with every other lab and existing
environment), so raise the quota or stagger the class. Learners must finish
Lab 3 cleanup first. Cluster creation incurs hourly charges until deleted.

Sign in, then create and select the environment and cluster. Replace
`<STUDENT_ID>` with your assigned ID:

```bash
confluent version
confluent login

confluent environment create rbac-lab-<STUDENT_ID> \
  --governance-package essentials
confluent environment list
confluent environment use <ENVIRONMENT_ID>

confluent kafka cluster create rbac-lab-<STUDENT_ID> \
  --cloud aws \
  --region ap-east-1 \
  --type standard \
  --environment <ENVIRONMENT_ID>
```

Wait for the cluster to reach `UP`, then record its ID and select it:

```bash
confluent kafka cluster list
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent kafka cluster describe
confluent schema-registry cluster describe --environment <ENVIRONMENT_ID>
```

If your organization cannot create a Standard cluster in `ap-east-1`, use
another supported region. Do not use a Basic cluster for this exercise. If the
instructor instead provides an existing Standard (or higher) cluster, use the
provided IDs and skip the creation commands, but add your `<STUDENT_ID>` suffix
to every topic, group, and prefix name below.

Keep the environment ID, Kafka cluster ID, and Schema Registry cluster ID
handy. For role bindings on a Kafka resource, pass the Kafka cluster ID as
both `--cloud-cluster` and `--kafka-cluster`.

## 1. Create an administrator Kafka API key

The topic setup and seed commands below use an API key owned by the
administrator who is logged in to the Confluent CLI. Select the lab cluster
and create a key scoped to that cluster:

```bash
confluent environment use <ENVIRONMENT_ID>
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent api-key create --resource <KAFKA_CLUSTER_ID>
```

Save the displayed key and secret securely; the secret is shown only once.
Use them in the commands below as `<ADMIN_KAFKA_API_KEY>` and
`<ADMIN_KAFKA_API_SECRET>`. The key is scoped to the Kafka cluster. Its
permissions come from the logged-in administrator identity associated with
the key; creating a key does not independently grant permissions. The
administrator must already be allowed to create topics and produce messages
in this cluster.

## 2. Prepare two test topics

The restricted account will receive a `DeveloperRead` binding on the prefix
`rbac-lab-in-scope-`. Use one topic inside that prefix and one
outside it:

- In scope: `rbac-lab-in-scope-orders`
- Out of scope: `rbac-lab-outside-orders`

Create both topics as an administrator in the selected environment. If either
topic already exists and its data is not disposable lab data, stop and choose
different topic names consistently throughout the exercise.

```bash
confluent kafka topic create rbac-lab-in-scope-orders \
  --cluster <KAFKA_CLUSTER_ID> --environment <ENVIRONMENT_ID> --partitions 1

confluent kafka topic create rbac-lab-outside-orders \
  --cluster <KAFKA_CLUSTER_ID> --environment <ENVIRONMENT_ID> --partitions 1
```

Seed each topic with a test message using an administrator's Kafka API key.
The CLI prompts for input; enter a line and press `Ctrl-D`:

```bash
confluent kafka topic produce rbac-lab-in-scope-orders \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <ADMIN_KAFKA_API_KEY> \
  --api-secret <ADMIN_KAFKA_API_SECRET>

confluent kafka topic produce rbac-lab-outside-orders \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <ADMIN_KAFKA_API_KEY> \
  --api-secret <ADMIN_KAFKA_API_SECRET>
```

Use a secure method to supply credentials. Do not commit API secrets, put
them in shared notes, or paste them into the audit log.

## 3. Create the service accounts

Create two service accounts with stable lab names. If either already exists,
verify it is from an earlier run of this lab before reusing it:

```bash
confluent iam service-account create rbac-lab-topic-reader-<STUDENT_ID> \
  --description "RBAC lab: read only from a Kafka topic prefix"

confluent iam service-account create rbac-lab-environment-admin-<STUDENT_ID> \
  --description "RBAC lab: environment-scoped administrator"

confluent iam service-account list
```

Record their IDs from the output (they look like `sa-...`). In the remaining
commands, replace `<READER_SERVICE_ACCOUNT_ID>` and
`<ADMIN_SERVICE_ACCOUNT_ID>` with those IDs, without the angle brackets.
Confluent Cloud represents a service account as a `User:<service-account-id>`
principal in role-binding commands.

## 4. Bind `DeveloperRead` to the topic prefix

Grant the reader service account read-only access to topics whose names start
with `rbac-lab-in-scope-`:

```bash
confluent iam rbac role-binding create \
  --principal User:<READER_SERVICE_ACCOUNT_ID> \
  --role DeveloperRead \
  --environment <ENVIRONMENT_ID> \
  --cloud-cluster <KAFKA_CLUSTER_ID> \
  --kafka-cluster <KAFKA_CLUSTER_ID> \
  --resource Topic:rbac-lab-in-scope- \
  --prefix
```

To verify a normal consumer read, grant `DeveloperRead` on the one consumer
group used in this lab. Topic access alone does not authorize a consumer group
subscription:

```bash
confluent iam rbac role-binding create \
  --principal User:<READER_SERVICE_ACCOUNT_ID> \
  --role DeveloperRead \
  --environment <ENVIRONMENT_ID> \
  --cloud-cluster <KAFKA_CLUSTER_ID> \
  --kafka-cluster <KAFKA_CLUSTER_ID> \
  --resource Group:rbac-lab-reader-v1
```

## 5. Bind `EnvironmentAdmin` to the second service account

Grant the second service account full administrative access within the selected
environment:

```bash
confluent iam rbac role-binding create \
  --principal User:<ADMIN_SERVICE_ACCOUNT_ID> \
  --role EnvironmentAdmin \
  --environment <ENVIRONMENT_ID>
```

List each account's bindings, including nested scopes:

```bash
confluent iam rbac role-binding list \
  --principal User:<READER_SERVICE_ACCOUNT_ID> --inclusive

confluent iam rbac role-binding list \
  --principal User:<ADMIN_SERVICE_ACCOUNT_ID> --inclusive
```

For the prefixed topic binding, confirm the output shows `DeveloperRead`,
resource type `Topic`, name `rbac-lab-in-scope-`, and pattern
type `PREFIXED`. The reader's group binding should be `Group`,
`rbac-lab-reader-v1`, and
`LITERAL`. The second account's `EnvironmentAdmin` binding should show the
selected environment and no topic-specific resource.

Compare their effective scope:

| Account | Binding scope | Effective access |
| --- | --- | --- |
| Topic reader | `DeveloperRead` on topics prefixed `rbac-lab-in-scope-`, plus read on `rbac-lab-reader-v1` | Read and describe matching topics and the named group. No access is granted to `rbac-lab-outside-orders` by these bindings. |
| Environment administrator | `EnvironmentAdmin` on `<ENVIRONMENT_ID>` | Manage resources throughout that environment, including Kafka topics and access grants. It does not grant access to unrelated environments. |

The role-binding list shows the assigned scope. `DeveloperRead` is read-only
on its resource; `EnvironmentAdmin` manages resources throughout its
environment. See the predefined role documentation for the complete permission
sets and current limitations.

## 6. Create Kafka API keys for the service accounts

Kafka API keys let the test clients authenticate as the service accounts. The
CLI user creating a key for another service account must have `OrganizationAdmin`
or `ResourceOwner` on that service account. Create a key for each account,
scoped to the selected Kafka cluster:

```bash
confluent api-key create \
  --resource <KAFKA_CLUSTER_ID> \
  --service-account <READER_SERVICE_ACCOUNT_ID>

confluent api-key create \
  --resource <KAFKA_CLUSTER_ID> \
  --service-account <ADMIN_SERVICE_ACCOUNT_ID>
```

Save both key pairs securely; the secret is shown only when created. Below,
`<READER_KAFKA_API_KEY>`/`<READER_KAFKA_API_SECRET>` are the first pair and
`<ENVIRONMENT_ADMIN_KAFKA_API_KEY>`/`<ENVIRONMENT_ADMIN_KAFKA_API_SECRET>` are
the second (the environment-admin service account). These are different from
the human administrator's `<ADMIN_KAFKA_API_KEY>` created in step 1.

The environment-admin account will also be used for the Python Avro test. If
Schema Registry is available, record its ID from the setup section and create
a Schema Registry key owned by that service account:

```bash
confluent api-key create \
  --resource <SCHEMA_REGISTRY_CLUSTER_ID> \
  --service-account <ADMIN_SERVICE_ACCOUNT_ID>
```

Save this API key and secret securely. Kafka and Schema Registry API keys are
separate credentials for separate services.

## 7. Test the Python Avro producer and consumer

Use the `EnvironmentAdmin` service account for this test. The topic-reader
account remains read-only for the RBAC comparison. Create a one-partition
topic for Python Avro records:

```bash
confluent kafka topic create rbac-lab-python \
  --cluster <KAFKA_CLUSTER_ID> \
  --environment <ENVIRONMENT_ID> \
  --partitions 1
```

From the repository's `confluent-cloud` directory, run the Python examples with
the environment-admin key pairs. The scripts prefer process environment
variables over `.env`, so override the credentials and endpoints for this lab's
cluster on the command line and leave `.env` untouched. Replace the bootstrap
and Schema Registry endpoint with this lab cluster's values from
`confluent kafka cluster describe` (host and port, no `SASL_SSL://` prefix) and
`confluent schema-registry cluster describe`:

```bash
source .venv/bin/activate   # or the environment that has requirements.txt installed
export CONFLUENT_BOOTSTRAP_SERVERS='<KAFKA_BOOTSTRAP_HOST>:9092'
export SCHEMA_REGISTRY_ENDPOINT='<SCHEMA_REGISTRY_ENDPOINT>'
export KEY='<ENVIRONMENT_ADMIN_KAFKA_API_KEY>' SECRET='<ENVIRONMENT_ADMIN_KAFKA_API_SECRET>'
export SCHEMA_REGISTRY_KEY='<ENVIRONMENT_ADMIN_SCHEMA_REGISTRY_API_KEY>'
export SCHEMA_REGISTRY_SECRET='<ENVIRONMENT_ADMIN_SCHEMA_REGISTRY_API_SECRET>'

python produce_avro.py rbac-lab-python --count 5
python consume_avro.py rbac-lab-python --count 5
```

These exports exist only in this shell. After the test, run
`unset CONFLUENT_BOOTSTRAP_SERVERS SCHEMA_REGISTRY_ENDPOINT KEY SECRET SCHEMA_REGISTRY_KEY SCHEMA_REGISTRY_SECRET`
or close the shell.

Expected: the producer reports five delivered records and the consumer prints
five decoded `Order` records. The producer registers the topic's value schema;
the consumer fetches the writer schema from Schema Registry. This verifies
Kafka produce/consume and Schema Registry schema access for the
environment-admin service account.

## 8. Verify the allowed read and the out-of-scope denial

Consume from the in-scope topic using the reader service account and the group
authorized in step 4. Replace the placeholders with that account's key and
secret:

```bash
confluent kafka topic consume rbac-lab-in-scope-orders \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <READER_KAFKA_API_KEY> \
  --api-secret <READER_KAFKA_API_SECRET> \
  --group rbac-lab-reader-v1 \
  --from-beginning
```

The seeded message should be readable. Stop the consumer with `Ctrl-C`.

Now attempt the same operation on the topic outside the prefix:

```bash
confluent kafka topic consume rbac-lab-outside-orders \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <READER_KAFKA_API_KEY> \
  --api-secret <READER_KAFKA_API_SECRET> \
  --group rbac-lab-reader-v1 \
  --from-beginning
```

Expected: the request fails with an authorization error for the out-of-scope
topic. The reader binding includes the consumer group, so the failure should
be on the topic resource, not on group access. An authentication error does
not prove the RBAC denial; check the API key owner and secret if authentication
fails.

Compare the result with the environment-admin service account's credentials from step 6. The
administrator should be able to read the same out-of-prefix topic because its
role applies to all Kafka resources in the selected environment:

```bash
confluent kafka topic consume rbac-lab-outside-orders \
  --cluster <KAFKA_CLUSTER_ID> \
  --api-key <ENVIRONMENT_ADMIN_KAFKA_API_KEY> \
  --api-secret <ENVIRONMENT_ADMIN_KAFKA_API_SECRET> \
  --group rbac-lab-environment-admin-v1 \
  --from-beginning
```

The seeded message should be readable. This positive control makes the
permission difference observable: the reader is denied by the topic prefix
boundary, while the environment administrator can read across that
environment.

## 9. Locate the audit events

In the Confluent Cloud Console, open **Administration → Audit logs**. Filter
the time range to include this lab. Use separate searches for the binding
changes and the denied Kafka request because they have different actors:

- For the three role-binding changes, filter by the logged-in administrator
  who created them and the Cloud request method `CreateRoleBinding` or
  `BindRoleForPrincipal`. Open each event and inspect its request/resource
  details to identify the target service account, role, and scope.
- For the out-of-prefix read, filter by the reader service account principal
  and authorization result **Deny**. Find the Kafka authorization event with
  event type
  `io.confluent.kafka.server/authorization`, the reader principal, the
  out-of-scope topic name, and `data.authorizationInfo.granted` set to
  `false`. The operation may be a topic `Read` or `Describe` check depending
  on which request was rejected first.

Open the event details and inspect the principal, resource, operation, and
authorization result. The role-binding events prove who made the bindings;
the Kafka authorization event proves the reader's request was denied.

The Cloud Console audit-log view requires `OrganizationAdmin`. Audit logs are
available only when the organization has at least one Standard, Enterprise,
Dedicated, or Freight Kafka cluster. If the events do not appear immediately,
refresh and widen the time range; authorization and organization events are
recorded as separate event types.

## Cleanup

Remove the role bindings after recording the results. Prefix bindings require
`--prefix` when deleted:

```bash
confluent iam rbac role-binding delete \
  --principal User:<READER_SERVICE_ACCOUNT_ID> \
  --role DeveloperRead \
  --environment <ENVIRONMENT_ID> \
  --cloud-cluster <KAFKA_CLUSTER_ID> \
  --kafka-cluster <KAFKA_CLUSTER_ID> \
  --resource Topic:rbac-lab-in-scope- \
  --prefix

confluent iam rbac role-binding delete \
  --principal User:<READER_SERVICE_ACCOUNT_ID> \
  --role DeveloperRead \
  --environment <ENVIRONMENT_ID> \
  --cloud-cluster <KAFKA_CLUSTER_ID> \
  --kafka-cluster <KAFKA_CLUSTER_ID> \
  --resource Group:rbac-lab-reader-v1

confluent iam rbac role-binding delete \
  --principal User:<ADMIN_SERVICE_ACCOUNT_ID> \
  --role EnvironmentAdmin \
  --environment <ENVIRONMENT_ID>
```

If the service accounts were created only for this lab, list their Kafka API
keys, then delete the keys by ID. Use
`<ADMIN_KAFKA_API_KEY_ID>` for the logged-in administrator's key created in
Step 1, `<READER_KAFKA_API_KEY_ID>` for the reader service account's key,
`<ENVIRONMENT_ADMIN_KAFKA_API_KEY_ID>` for the environment-admin service
account's Kafka key, and `<ENVIRONMENT_ADMIN_SCHEMA_REGISTRY_API_KEY_ID>` for
its Schema Registry key. The service-account key IDs come from their scoped listings;
the administrator's key ID is in the cluster-scoped listing.

```bash
confluent api-key list --resource <KAFKA_CLUSTER_ID> \
  --service-account <READER_SERVICE_ACCOUNT_ID>
confluent api-key list --resource <KAFKA_CLUSTER_ID> \
  --service-account <ADMIN_SERVICE_ACCOUNT_ID>
confluent api-key list --resource <SCHEMA_REGISTRY_CLUSTER_ID> \
  --service-account <ADMIN_SERVICE_ACCOUNT_ID>

confluent api-key list --resource <KAFKA_CLUSTER_ID>
confluent api-key delete <ADMIN_KAFKA_API_KEY_ID>
confluent api-key delete <READER_KAFKA_API_KEY_ID>
confluent api-key delete <ENVIRONMENT_ADMIN_KAFKA_API_KEY_ID>
confluent api-key delete <ENVIRONMENT_ADMIN_SCHEMA_REGISTRY_API_KEY_ID>
confluent iam service-account delete <READER_SERVICE_ACCOUNT_ID>
confluent iam service-account delete <ADMIN_SERVICE_ACCOUNT_ID>
```

Delete the test topics only if they contain no data you need:

```bash
confluent kafka topic delete rbac-lab-in-scope-orders \
  --cluster <KAFKA_CLUSTER_ID>
confluent kafka topic delete rbac-lab-outside-orders \
  --cluster <KAFKA_CLUSTER_ID>
confluent kafka topic delete rbac-lab-python \
  --cluster <KAFKA_CLUSTER_ID>
```

If you created the environment and cluster only for this lab, delete the
cluster first, then the environment (skip this if the instructor provided a shared cluster):

```bash
confluent kafka cluster delete <KAFKA_CLUSTER_ID>
confluent environment delete <ENVIRONMENT_ID>
```

## Review questions

1. What does `PREFIXED` mean in the topic role-binding output?
2. Why did the reader need a separate consumer-group binding?
3. Which binding grants broader access, and what is its environment boundary?
4. Which audit-log fields prove that the out-of-scope operation was denied?

## References

- [Manage RBAC role bindings](https://docs.confluent.io/cloud/current/security/access-control/rbac/manage-role-bindings.html)
- [Predefined RBAC roles](https://docs.confluent.io/cloud/current/security/access-control/rbac/predefined-rbac-roles.html)
- [Access and consume audit logs](https://docs.confluent.io/cloud/current/monitoring/audit-logging/configure.html)
- [Audit log event records](https://docs.confluent.io/cloud/current/monitoring/audit-logging/audit-log-records.html)
- [RBAC audit event methods](https://docs.confluent.io/cloud/current/monitoring/audit-logging/event-methods/rbac.html)
