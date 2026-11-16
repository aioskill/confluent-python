# Lab 2: Securing the cluster with SCRAM and ACLs

This lab creates application-specific Kafka identities, grants narrowly
scoped topic and consumer-group access, and verifies that an authenticated
principal without ACLs is denied.

## Skip: enable SASL/SCRAM on broker listeners

The Confluent playbook already configures SASL/SCRAM-SHA-512 on the Kafka
client and inter-broker listeners. Use the existing admin client file for
administrative commands:

This exercise uses topic `acl-lab-orders` and consumer group
`acl-lab-order-consumers`. Confirm those names are not in use before starting;
remove leftovers only if they belong to an earlier run of this lab.

This deployment uses `SASL_PLAINTEXT`, which authenticates clients but does not
encrypt traffic. These exercises are for the isolated lab network.

## Authorization prerequisite

SASL authenticates a client; it does not restrict what an authenticated user
can do. The current Confluent role does not configure a Kafka authorizer, so
adding ACLs alone will not enforce access restrictions.

Before doing the ACL exercises, update the Ansible role so every broker and
controller uses KRaft's `StandardAuthorizer` and an explicit `super.users`
list. Include the existing administrative principal `User:confluent` and the
authenticated internal broker/controller principals required by the
deployment. Deploy this configuration through Ansible and verify that all
brokers return healthy. Do not edit individual `server.properties` files.

The role currently binds its `CONTROLLER` listener to all interfaces with
`PLAINTEXT`. Secure or network-restrict that listener as part of the
playbook-managed authorizer change. Do not add `User:ANONYMOUS` as a global
superuser on a reachable listener. Leave
`allow.everyone.if.no.acl.found` unset or false so principals with no matching
ACL are denied.

Verify the deployed configuration on each node before continuing:

```bash
grep -E '^(authorizer.class.name|super.users|allow.everyone.if.no.acl.found)=' \
  /app/confluent/etc/kafka/server.properties
```

Expect `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`,
the intended administrator and internal principals in `super.users`, and no
`allow.everyone.if.no.acl.found=true` setting. If the authorizer is absent,
stop here: ACL commands cannot demonstrate denial on this cluster yet.

## Exercise 1: create dedicated SCRAM principals

Create one principal for the producer, one for the consumer, and one
authenticated principal for the denial test. Generate separate random
passwords and store the short-lived files with restrictive permissions. Run
this loop on a broker host with the existing admin client file available:

```bash
umask 077
for principal in lab-producer lab-consumer lab-unassigned; do
  password=$(openssl rand -hex 24)
  scram_file="/tmp/${principal}.scram.properties"
  client_file="/tmp/${principal}.properties"

  printf 'SCRAM-SHA-512=[iterations=8192,password=%s]\n' "$password" \
    > "$scram_file"
  /app/confluent/bin/kafka-configs \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
    --alter --entity-type users --entity-name "$principal" \
    --add-config-file "$scram_file"

  cat > "$client_file" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$principal" password="$password";
EOF
  chmod 600 "$scram_file" "$client_file"
  unset password
done
```

The generated application files are `/tmp/lab-producer.properties`,
`/tmp/lab-consumer.properties`, and `/tmp/lab-unassigned.properties`. Do not
copy them into the public repository or paste their contents into a ticket.

Create a topic with the administrator identity. The application identities
will not receive topic-creation privileges:

```bash
/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --create --topic acl-lab-orders --partitions 3 --replication-factor 3
```

## Exercise 2: grant only the required ACLs

Grant the producer `DESCRIBE` and `WRITE` on this topic. These permissions are
enough to produce to the topic that the administrator created:

```bash
/app/confluent/bin/kafka-acls \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --add --allow-principal User:lab-producer \
  --operation Describe --operation Write --topic acl-lab-orders
```

Grant the consumer `DESCRIBE` and `READ` on the topic and `READ` on only its
consumer group:

```bash
/app/confluent/bin/kafka-acls \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --add --allow-principal User:lab-consumer \
  --operation Describe --operation Read --topic acl-lab-orders

/app/confluent/bin/kafka-acls \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --add --allow-principal User:lab-consumer \
  --operation Read --group acl-lab-order-consumers
```

List the ACLs and check that neither application has access to unrelated
topics or groups:

```bash
/app/confluent/bin/kafka-acls \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --list --topic acl-lab-orders

/app/confluent/bin/kafka-acls \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --list --group acl-lab-order-consumers
```

## Exercise 3: verify the producer and consumer

Produce a few records using the producer principal. Disable idempotence for
this simple console exercise so it needs only the topic ACLs above:

```bash
/app/confluent/bin/kafka-console-producer \
  --command-config /tmp/lab-producer.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --producer-property enable.idempotence=false \
  --topic acl-lab-orders
```

Enter several lines and press `Ctrl-D`. Consume them using the consumer
principal and the authorized group:

```bash
/app/confluent/bin/kafka-console-consumer \
  --command-config /tmp/lab-consumer.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --topic acl-lab-orders --group acl-lab-order-consumers --from-beginning --max-messages 3
```

Confirm the consumer receives the messages. Try changing `--group` to a
different group ID. The consumer should be denied because it has no `READ`
ACL on that group.

## Exercise 4: verify an unauthorized principal is denied

`lab-unassigned` has valid SCRAM credentials but no ACLs. Attempt to consume
from the protected topic:

```bash
/app/confluent/bin/kafka-console-consumer \
  --command-config /tmp/lab-unassigned.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --topic acl-lab-orders --group lab-unassigned-group --from-beginning
```

The command should fail with a topic or group authorization error. A SCRAM
authentication error means the credentials are wrong and does not prove ACL
enforcement. If the unauthorized user can read the topic, stop and inspect the
authorizer configuration, superuser list, and any wildcard ACLs.

## Cleanup

Remove the lab ACLs, topic, and SCRAM credentials using the administrative
identity:

```bash
for principal in lab-producer lab-consumer lab-unassigned; do
  /app/confluent/bin/kafka-acls \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
    --remove --allow-principal "User:$principal" --topic acl-lab-orders --force
done

/app/confluent/bin/kafka-acls \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --remove --allow-principal User:lab-consumer --group acl-lab-order-consumers --force

/app/confluent/bin/kafka-topics \
  --command-config /app/confluent/etc/kafka/client.properties \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 --delete --topic acl-lab-orders

for principal in lab-producer lab-consumer lab-unassigned; do
  /app/confluent/bin/kafka-configs \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
    --alter --entity-type users --entity-name "$principal" \
    --delete-config SCRAM-SHA-512
done

rm -f /tmp/lab-producer.scram.properties /tmp/lab-producer.properties \
  /tmp/lab-consumer.scram.properties /tmp/lab-consumer.properties \
  /tmp/lab-unassigned.scram.properties /tmp/lab-unassigned.properties
```

## Review questions

1. How is authentication different from authorization?
2. Which two ACLs does the consumer need, and what resource does each protect?
3. Why should an application principal not be a superuser?
4. What error proves authorization denied the request rather than SCRAM
   authentication rejecting the credentials?
5. What does `SASL_PLAINTEXT` protect, and what does it leave unencrypted?

## References

- [Confluent: ACL authorization in KRaft](https://docs.confluent.io/platform/current/security/authorization/acls/overview.html)
- [Apache Kafka: Authorization and ACLs](https://kafka.apache.org/42/security/authorization-and-acls/)
- [Confluent: Configure SCRAM credentials](https://docs.confluent.io/platform/8.4/security/authentication/sasl/scram/overview.html)
