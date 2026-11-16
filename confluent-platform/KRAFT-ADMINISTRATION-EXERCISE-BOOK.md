# KRaft administration exercise book

This workbook covers checking KRaft health and planning controller changes.
It uses the Confluent Platform CLI tools installed under
`/app/confluent/bin` and the lab's SCRAM client configuration.

## Read this before changing membership

The current lab is a three-node **static** KRaft quorum. Each node runs as both
a broker and a controller, and the role writes `controller.quorum.voters` to
every broker configuration. The cluster currently reports `kraft.version=0`.
The commands in the status exercises work on this cluster. The live
`add-controller` and `remove-controller` exercises below require a **dynamic**
quorum; do not run them against the current lab configuration.

Static membership changes require coordinated configuration updates and
restarts. For dynamic membership, first migrate the cluster to the dynamic
quorum format and update the Ansible role to provision controller nodes with
`controller.quorum.bootstrap.servers`. Confluent Platform 8.0 and later
supports migration; follow the version-specific migration procedure and
perform it through the deployment playbook. The current lab role does not yet
automate this migration or provision dedicated controller-only nodes.

The membership exercises assume an isolated dynamic-quorum practice cluster
with a spare controller host and a playbook that provisions and manages that
host. A controller membership change does not move Kafka topic replicas. The
lab's nodes are combined broker/controllers, so removing one also removes a
broker; move its partition replicas first and verify the result.

## Prepare the CLI

Run the commands below from a broker host. Keep the existing client
properties private; they contain the SCRAM credential.

## Exercise 1: inspect quorum status

Ask Kafka for the leader, voter set, and metadata log position:

```bash
/app/confluent/bin/kafka-metadata-quorum \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties describe --status
```

Then inspect replication state:

```bash
/app/confluent/bin/kafka-metadata-quorum \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties describe --replication
```

**Check your result:** there should be an elected leader, three current
voters, and no persistent replication lag. Record the leader ID, voter IDs,
high watermark, and each voter's log end offset. Repeat the command to see
whether these values remain healthy as metadata changes.

## Exercise 2: identify static or dynamic membership

Read the finalized KRaft feature version:

```bash
/app/confluent/bin/kafka-features \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config /app/confluent/etc/kafka/client.properties describe
```

Interpret `kraft.version` as follows:

- `FinalizedVersionLevel: 0` (or no finalized value): static quorum. Use the
  status exercises only; do not use the dynamic membership commands.
- `FinalizedVersionLevel: 1` or higher: dynamic quorum. Continue with the
  membership exercises only if the node configuration and playbook also use
  dynamic quorum settings.

For this lab, expect level `0`. Also inspect the managed configuration:

```bash
grep -E '^(process.roles|node.id|controller.quorum.voters|controller.quorum.bootstrap.servers)=' \
  /app/confluent/etc/kafka/server.properties
```

The current role should show `process.roles=broker,controller` and
`controller.quorum.voters=...`.

## Exercise 3: add a controller to a dynamic quorum

Use a disposable practice cluster, not the current static lab. Have the
playbook provision a new controller with a unique node ID, a new empty data
directory, the same cluster ID, controller listener/security settings, and
`controller.quorum.bootstrap.servers`. Do not set
`controller.quorum.voters` on dynamic-quorum nodes. For Confluent Platform
8.2 and later, enable `controller.quorum.auto.join.enable=true` on controllers.

1. Confirm all existing voters are online and caught up using
   `kafka-metadata-quorum describe --status` and
   `kafka-metadata-quorum describe --replication`.
2. Use Ansible to format the new controller storage for the existing cluster
   with `kafka-storage format --cluster-id <cluster-id>
   --no-initial-controllers --config <new-controller-config>`.
3. Start the new controller using the Ansible `systemd` module.
4. Recheck replication. Wait until the new node is caught up and appears in
   the voter set before calling the exercise complete.

If automatic join is disabled, use the installed tool to request the add
operation after the new controller is running and caught up:

```bash
/app/confluent/bin/kafka-metadata-quorum \
  --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
  --command-config <controller-client-config> add-controller
```

The configuration passed to this command must include the required Admin
Client authentication and controller settings. Check the installed tool's
`add-controller --help` and the matching Confluent documentation before using
this manual path.

**Check your result:** the voter set includes the new controller, the leader
is elected, and replication remains caught up. Adding a controller does not
change broker count or topic replication.

## Exercise 4: remove a controller from a dynamic quorum

Only remove a controller when the remaining nodes are healthy. Remove one
controller at a time. For a three-voter quorum, two healthy voters are the
minimum needed to retain a majority; removing a voter while another is down
can stop metadata progress.

1. Run `describe --status` and `describe --replication` and identify the
   controller ID and directory ID of the target. Confirm the target is not the
   only healthy voter and the remaining quorum is caught up.
2. If the target also brokers data, reassign its topic replicas to other
   brokers and verify the reassignment is complete.
3. Remove the target from the quorum **before** stopping its service:

   ```bash
   /app/confluent/bin/kafka-metadata-quorum \
     --bootstrap-server sa01:9092,sa02:9092,sa03:9092 \
     --command-config /app/confluent/etc/kafka/client.properties remove-controller \
     --controller-id <controller-id> \
     --controller-directory-id <controller-directory-id>
   ```

4. Verify the voter set and leader again. Then use Ansible's `systemd` module
   to stop and disable the retired controller service.

**Check your result:** the retired ID is no longer a voter, a leader is
available, and the remaining voters are caught up. For tool versions that
support controller unregistering, use the documented unregister procedure as
well; this lab's Confluent 8.3.2 CLI does not expose `--unregister` on
`remove-controller`.

## Exercise 5: replace a failed controller

Use this sequence in a dynamic-quorum practice cluster when the old controller
cannot be recovered:

1. Capture the old controller's ID and directory ID from quorum status.
2. Remove the old voter using Exercise 4. If the old process is still alive,
   stop it promptly after the removal so it cannot rejoin.
3. Have Ansible provision a replacement with a **new node ID**, a fresh
   storage directory, the existing cluster ID, and dynamic quorum settings.
   Do not copy the failed controller's metadata directory or format unrelated
   existing data directories.
4. Start the replacement and wait for it to catch up and join the voter set.
5. Recheck status and replication. If this is a combined broker/controller,
   separately restore the desired Kafka partition replica placement.

**Check your result:** the replacement appears as a healthy voter, the quorum
has a leader, and broker partition replicas meet the intended replication
factor.

## Review questions

1. What is the difference between a broker and a KRaft controller voter?
2. Why should a controller be caught up before it is added to a quorum?
3. Why can removing one of three voters be unsafe when another voter is down?
4. Why does removing a combined broker/controller require a separate partition
   reassignment plan?
5. Which output proves the current lab is static, and which playbook changes
   would be required before practicing dynamic membership here?

## References

- [Confluent: Configure and monitor KRaft](https://docs.confluent.io/platform/current/kafka-metadata/config-kraft.html)
- [Confluent: KRaft overview and controller membership](https://docs.confluent.io/platform/current/kafka-metadata/kraft.html)
- [Confluent: KRaft version compatibility](https://docs.confluent.io/platform/current/installation/versions-interoperability.html)
