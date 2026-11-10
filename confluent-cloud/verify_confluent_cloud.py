#!/usr/bin/env python3
"""Verify Kafka, REST API, and Schema Registry access in Confluent Cloud."""

from __future__ import annotations

import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)
from confluent_kafka.schema_registry import SchemaRegistryClient

from client_config import (
    cloud_cluster_id,
    cloud_rest_request,
    default_bootstrap_servers,
    kafka_client_config,
    schema_registry_config,
)


def verify_cloud_endpoints() -> tuple[AdminClient, bool]:
    """Check Kafka metadata, the Cloud Kafka REST API, and Schema Registry."""
    admin = AdminClient(kafka_client_config())
    checks_passed = True

    try:
        metadata = admin.list_topics(timeout=20)
        print(
            "PASS Kafka SASL_SSL/PLAIN: "
            f"connected to {len(metadata.brokers)} broker(s)"
        )
    except (KafkaException, RuntimeError, TimeoutError) as error:
        print(f"FAIL Kafka SASL_SSL/PLAIN: {error}", file=sys.stderr)
        checks_passed = False

    try:
        cloud_rest_request(
            f"/kafka/v3/clusters/{cloud_cluster_id()}"
        )
        print("PASS Confluent Cloud Kafka REST API authentication")
    except (RuntimeError, ValueError) as error:
        print(f"FAIL Confluent Cloud Kafka REST API: {error}", file=sys.stderr)
        checks_passed = False

    try:
        registry = SchemaRegistryClient(schema_registry_config())
        subjects = registry.get_subjects()
        print(
            "PASS Schema Registry Basic Auth/read access: "
            f"connected ({len(subjects)} subject(s))"
        )
    except Exception as error:
        print(f"FAIL Schema Registry authentication: {error}", file=sys.stderr)
        checks_passed = False

    return admin, checks_passed


def verify_topic_round_trip(admin: AdminClient) -> bool:
    """Create a temporary RF=3 topic, configure it, and round-trip a record."""
    topic = f"cloud_verify_{uuid.uuid4().hex[:12]}"
    group_id = f"cloud-verify-{uuid.uuid4().hex}"
    consumer = None
    topic_created = False
    success = False

    try:
        create = admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=1,
                    replication_factor=3,
                )
            ],
            request_timeout=30,
        )[topic]
        create.result(timeout=30)
        topic_created = True
        print("PASS Created a temporary topic with Cloud RF=3")
        retryable_errors = {
            KafkaError.UNKNOWN_TOPIC_OR_PART,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError.NOT_LEADER_FOR_PARTITION,
        }

        metadata_deadline = time.monotonic() + 30
        topic_metadata = None
        while time.monotonic() < metadata_deadline:
            metadata = admin.list_topics(topic=topic, timeout=10)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata and topic_metadata.partitions:
                break
            time.sleep(1)
        if topic_metadata is None or not topic_metadata.partitions:
            raise RuntimeError(
                "Topic creation succeeded, but partition metadata is not "
                "visible. The API key may need DESCRIBE access on the topic."
            )
        if len(topic_metadata.partitions[0].replicas) != 3:
            raise RuntimeError("Cloud did not report replication factor 3.")
        print("PASS Topic reports replication factor 3")

        resource = ConfigResource(ResourceType.TOPIC, topic)
        resource.add_incremental_config(
            ConfigEntry(
                "retention.ms",
                "120000",
                incremental_operation=AlterConfigOpType.SET,
            )
        )
        config_deadline = time.monotonic() + 45
        while True:
            try:
                admin.incremental_alter_configs(
                    [resource], request_timeout=10
                )[resource].result(timeout=10)
                break
            except KafkaException as error:
                kafka_error = error.args[0] if error.args else None
                if (
                    not isinstance(kafka_error, KafkaError)
                    or kafka_error.code() not in retryable_errors
                    or time.monotonic() >= config_deadline
                ):
                    raise
                time.sleep(2)

        config_visible = False
        while time.monotonic() < config_deadline:
            try:
                configs = admin.describe_configs([resource])[resource].result(
                    timeout=10
                )
            except KafkaException as error:
                kafka_error = error.args[0] if error.args else None
                if (
                    not isinstance(kafka_error, KafkaError)
                    or kafka_error.code() not in retryable_errors
                    or time.monotonic() >= config_deadline
                ):
                    raise
            else:
                if str(configs["retention.ms"].value) == "120000":
                    config_visible = True
                    break
            time.sleep(2)
        if not config_visible:
            raise RuntimeError("Topic retention.ms did not update.")
        print("PASS Changed topic retention.ms")

        payload = f"cloud-verification-{uuid.uuid4().hex}"
        producer = Producer(kafka_client_config())
        delivery_deadline = time.monotonic() + 45
        delivered = False
        while time.monotonic() < delivery_deadline and not delivered:
            delivery_errors = []

            def on_delivery(error, _message) -> None:
                if error is not None:
                    delivery_errors.append(error)
                else:
                    nonlocal delivered
                    delivered = True

            try:
                producer.produce(
                    topic,
                    value=payload.encode("utf-8"),
                    on_delivery=on_delivery,
                )
            except KafkaException as error:
                kafka_error = error.args[0] if error.args else None
                if (
                    isinstance(kafka_error, KafkaError)
                    and kafka_error.code() in retryable_errors
                ):
                    time.sleep(2)
                    continue
                raise
            remaining = producer.flush(
                min(5, max(0, delivery_deadline - time.monotonic()))
            )
            if remaining:
                raise TimeoutError("Producer did not deliver before timeout.")
            if delivery_errors:
                delivery_error = delivery_errors[-1]
                if delivery_error.code() not in retryable_errors:
                    raise KafkaException(delivery_error)
                time.sleep(2)
        if not delivered:
            raise TimeoutError(
                "Producer could not reach the new topic before timeout."
            )
        print("PASS Produced a verification record")

        consumer = Consumer(
            {
                **kafka_client_config(),
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            message = consumer.poll(min(1.0, deadline - time.monotonic()))
            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())
            if message.value().decode("utf-8") == payload:
                success = True
                print("PASS Consumed the verification record")
                break
        if not success:
            raise TimeoutError("Verification record was not consumed.")
    except (
        KafkaException,
        OSError,
        RuntimeError,
        TimeoutError,
        ValueError,
    ) as error:
        error_code = None
        if isinstance(error, KafkaException) and error.args:
            kafka_error = error.args[0]
            if isinstance(kafka_error, KafkaError):
                error_code = kafka_error.code()
        if error_code == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            print(
                "FAIL Kafka topic round-trip: this API key can authenticate "
                "but lacks a required topic permission (CREATE, DESCRIBE, "
                "ALTER_CONFIGS, WRITE, READ, DELETE) or consumer-group READ. "
                "Grant the missing permissions and rerun verification.",
                file=sys.stderr,
            )
        else:
            print(f"FAIL Kafka topic round-trip: {error}", file=sys.stderr)
        success = False
    finally:
        if consumer is not None:
            consumer.close()
        if topic_created:
            try:
                admin.delete_topics([topic], request_timeout=30)[topic].result(
                    timeout=30
                )
                print("PASS Deleted the temporary topic")
            except (KafkaException, RuntimeError, TimeoutError) as error:
                print(
                    "FAIL Could not delete temporary topic "
                    f"{topic!r}: {error}",
                    file=sys.stderr,
                )
                success = False
    return success


def main() -> int:
    """Run endpoint and temporary-topic checks."""
    try:
        default_bootstrap_servers()
        admin, endpoints_ok = verify_cloud_endpoints()
        if not endpoints_ok:
            return 1
        return 0 if verify_topic_round_trip(admin) else 1
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Cloud verification could not start: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
