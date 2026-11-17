# Lab 6A: Schema Registry

In this lab you register an Avro schema under backward compatibility, evolve it
safely, and then attempt a breaking change and watch the compatibility check
reject it. [Lab 6B](LAB-6B-MANAGED-CONNECTORS.md) continues with managed and
self-managed connectors.

Use a sandbox organization. All students share one Kafka cluster and one
Schema Registry. Replace `<STUDENT_ID>` with the short ID assigned by the
instructor (lowercase letters and digits, for example `s07`) in every name
below. Every schema subject you create **starts with** `<STUDENT_ID>-`: the
instructor gave your service account permissions only on names with that
prefix, so a name without it is rejected.

## Learning objectives

- Register an Avro schema and set a subject's compatibility mode to `BACKWARD`.
- Evolve a schema safely, then trigger and read a compatibility failure.
- Explain why adding a field with a default is compatible and adding a required
  field is not.

## Prerequisites

- Confluent CLI installed and logged in (`confluent login`).
- A shared Kafka cluster and Schema Registry in the class environment. Schema
  Registry exists once the environment has the Stream Governance package (the
  labs create environments with `--governance-package essentials`). The
  instructor gives you the environment ID and cluster ID.
- The `confluent-cloud` directory of this repository as your working directory
  (it contains `order.avsc`).

### Instructor setup (once per student)

Before class, log in with the Confluent CLI as an administrator and run, for
each student:

```bash
./lab6_setup_student.sh create <STUDENT_ID> <ENVIRONMENT_ID> <KAFKA_CLUSTER_ID>
```

This creates the service account `lab6-<STUDENT_ID>`, gives it ACLs on topics
and consumer groups whose names start with `<STUDENT_ID>-` (plus the cluster
`idempotent-write` permission that Connect producers need, and access to the
shared `_confluent-command` topic where a Confluent Platform Connect worker
stores its license), creates a Kafka API
key for it, and writes the key to `lab6-students/<STUDENT_ID>.txt` (private,
git-ignored). Give each student only their own file. After class, run the same
command with `delete` instead of `create`. The cluster must allow one service
account and one key per student (Basic clusters allow 50 API keys).

### Student setup

Select the environment and cluster, and look at the Schema Registry cluster:

```bash
confluent environment use <ENVIRONMENT_ID>
confluent kafka cluster use <KAFKA_CLUSTER_ID>
confluent schema-registry cluster describe
```

The subject commands in this lab use your CLI login, so you do not need a
Schema Registry API key here.

## 1. Register an Avro schema with backward compatibility

The subject for a topic's record values is `<topic>-value`. Use the topic
`<STUDENT_ID>-lab6-orders`, so the subject is `<STUDENT_ID>-lab6-orders-value`.

Set the subject's compatibility mode before registering anything:

```bash
confluent schema-registry subject update <STUDENT_ID>-lab6-orders-value \
  --compatibility backward

confluent schema-registry configuration describe \
  --subject <STUDENT_ID>-lab6-orders-value
```

`BACKWARD` means a consumer using the new schema can read data written with the
previous schema. This is the Schema Registry default, but setting it explicitly
on the subject makes the rule visible and independent of the global setting.

Register the repository's `order.avsc` as version 1:

```bash
confluent schema-registry schema create \
  --subject <STUDENT_ID>-lab6-orders-value \
  --schema order.avsc \
  --type avro

confluent schema-registry schema describe \
  --subject <STUDENT_ID>-lab6-orders-value \
  --version latest
```

Make a compatible change: add an optional field with a default. Create
`order_v2.avsc`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.confluent",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": "string"},
    {"name": "coupon", "type": ["null", "string"], "default": null}
  ]
}
```

Check, register, and list the versions:

```bash
confluent schema-registry schema compatibility validate order_v2.avsc \
  --type avro \
  --subject <STUDENT_ID>-lab6-orders-value \
  --version latest

confluent schema-registry schema create \
  --subject <STUDENT_ID>-lab6-orders-value \
  --schema order_v2.avsc \
  --type avro

confluent schema-registry subject describe <STUDENT_ID>-lab6-orders-value
```

Expected: the validation reports the schema is compatible, and the subject
now has versions 1 and 2. Old records (without `coupon`) can still be read with
version 2 because the missing field takes its default.

## 2. Attempt a breaking schema change

Create `order_breaking.avsc`. It adds a required field with no default, and
changes the type of `amount`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.confluent",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "amount", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "coupon", "type": ["null", "string"], "default": null},
    {"name": "region", "type": "string"}
  ]
}
```

Validate it, then try to register it:

```bash
confluent schema-registry schema compatibility validate order_breaking.avsc \
  --type avro \
  --subject <STUDENT_ID>-lab6-orders-value \
  --version latest

confluent schema-registry schema create \
  --subject <STUDENT_ID>-lab6-orders-value \
  --schema order_breaking.avsc \
  --type avro
```

Expected: validation reports the schema is not compatible, and `schema create`
fails with an HTTP 409 error, "Schema being registered is incompatible with an
earlier schema". The subject still has only versions 1 and 2:

```bash
confluent schema-registry subject describe <STUDENT_ID>-lab6-orders-value
```

Fix the schema one change at a time and re-run the validation after each:

1. Change `amount` back to `double`. It is still incompatible, because a
   consumer on the new schema cannot read old records that lack `region`.
2. Give `region` a default, for example `"default": "unknown"`. The schema is
   now backward compatible and can be registered as version 3.

Questions to answer from what you saw:

- Which two edits were breaking, and why does `BACKWARD` reject each?
- Would removing the `coupon` field have been allowed? (Removing a field is
  backward compatible. It is not forward compatible.)
- What stops a producer from publishing records with the rejected schema?

## Cleanup

Delete your schema subject, first softly and then permanently (permanent
deletion cannot be undone), and the local schema files:

```bash
confluent schema-registry schema delete \
  --subject <STUDENT_ID>-lab6-orders-value --version all
confluent schema-registry schema delete \
  --subject <STUDENT_ID>-lab6-orders-value --version all --permanent
rm -f order_v2.avsc order_breaking.avsc
```

If you continue with Lab 6B, its Cleanup section removes the rest.

## Review questions

1. What does `BACKWARD` compatibility guarantee, and which consumers or
   producers must be upgraded first?
2. Why is adding a field with a default compatible, but adding a required field
   without one not?
3. What stops a producer from publishing records with a schema that Schema
   Registry rejected?

## References

- [Schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Schema Registry on Confluent Cloud](https://docs.confluent.io/cloud/current/sr/index.html)
