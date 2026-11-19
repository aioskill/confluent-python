"""

Produce synthetic page-view and click events to two Kafka topics.

Create the topics before running this producer when Kafka topic auto-creation is
disabled : (run the these commands from sa01)

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \
        --topic page_views --partitions 3 --replication-factor 1
    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --create \
        --topic clicks --partitions 3 --replication-factor 1

The two topics are written by separate threads at different rates. Events carry
an ``event_time`` that can be older than their Kafka arrival time, which makes
the topics useful for the later Flink event-time/joining exercise.


/app/confluent/bin/kafka-console-consumer --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic page_views --from-beginning
/app/confluent/bin/kafka-console-consumer --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic clicks --from-beginning
"""

import argparse
import json
import logging
import random
import threading
import time
from datetime import datetime, timezone

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import kafka_python_properties  # noqa: E402  (SASL/SCRAM settings)

from kafka import KafkaProducer


LOG = logging.getLogger(__name__)


def epoch_millis():
    return int(time.time() * 1000)


def event_timestamp(jitter_max_ms, random_source):
    """Return an event timestamp that may lag wall-clock time."""
    return epoch_millis() - random_source.randint(0, jitter_max_ms)


def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
        **kafka_python_properties(),  # SASL/SCRAM login
    )


def produce_page_views(args, stop_event, errors):
    random_source = random.Random(args.seed)
    producer = create_producer(args.bootstrap_servers)

    try:
        for sequence in range(args.page_views_count):
            if stop_event.is_set():
                break

            user_id = (sequence % args.users) + 1
            page_id = f"page-{(sequence % args.pages) + 1}"
            received_at = epoch_millis()
            event_time = event_timestamp(args.jitter_ms, random_source)
            event = {
                "event_type": "page_view",
                "view_id": f"view-{sequence + 1}",
                "user_id": user_id,
                "page_id": page_id,
                "event_time": event_time,
                "received_at": received_at,
                "event_time_iso": datetime.fromtimestamp(
                    event_time / 1000,
                    tz=timezone.utc,
                ).isoformat(),
            }
            producer.send(args.page_views_topic, key=str(user_id), value=event)
            LOG.info("page_views: %s", event)
            stop_event.wait(args.page_view_interval)
    except Exception as exc:  # Propagate thread failures to the main thread.
        errors.append(("page_views", exc))
        stop_event.set()
    finally:
        producer.flush()
        producer.close()


def produce_clicks(args, stop_event, errors):
    random_source = random.Random(args.seed + 1)
    producer = create_producer(args.bootstrap_servers)

    try:
        for sequence in range(args.clicks_count):
            if stop_event.is_set():
                break

            user_id = (sequence % args.users) + 1
            page_id = f"page-{(sequence % args.pages) + 1}"
            received_at = epoch_millis()
            event_time = event_timestamp(args.jitter_ms, random_source)
            event = {
                "event_type": "click",
                "click_id": f"click-{sequence + 1}",
                "view_id": f"view-{sequence + 1}",
                "user_id": user_id,
                "page_id": page_id,
                "event_time": event_time,
                "received_at": received_at,
                "event_time_iso": datetime.fromtimestamp(
                    event_time / 1000,
                    tz=timezone.utc,
                ).isoformat(),
            }
            producer.send(args.clicks_topic, key=str(user_id), value=event)
            LOG.info("clicks: %s", event)
            stop_event.wait(args.click_interval)
    except Exception as exc:  # Propagate thread failures to the main thread.
        errors.append(("clicks", exc))
        stop_event.set()
    finally:
        producer.flush()
        producer.close()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default="sa01:9092",
        help="Comma-separated Kafka brokers",
    )
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--page-views-count", type=int, default=30)
    parser.add_argument("--clicks-count", type=int, default=15)
    parser.add_argument("--users", type=int, default=5)
    parser.add_argument("--pages", type=int, default=4)
    parser.add_argument("--page-view-interval", type=float, default=0.5)
    parser.add_argument("--click-interval", type=float, default=1.5)
    parser.add_argument(
        "--jitter-ms",
        type=int,
        default=5000,
        help="Maximum event-time lag relative to message production time",
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main():
    args = parse_args()
    if (
        args.page_views_count < 1
        or args.clicks_count < 1
        or args.users < 1
        or args.pages < 1
        or args.jitter_ms < 0
    ):
        raise ValueError(
            "page-views-count, clicks-count, users, and pages must be positive; "
            "jitter-ms must be non-negative"
        )

    kafka_python_properties()  # fail early if the SASL/SCRAM credentials are missing
    args.bootstrap_servers = [
        server.strip()
        for server in args.bootstrap_servers.split(",")
        if server.strip()
    ]
    if not args.bootstrap_servers:
        raise ValueError("At least one Kafka bootstrap server is required")

    stop_event = threading.Event()
    errors = []
    threads = [
        threading.Thread(
            target=produce_page_views,
            args=(args, stop_event, errors),
            name="page-views-producer",
        ),
        threading.Thread(
            target=produce_clicks,
            args=(args, stop_event, errors),
            name="clicks-producer",
        ),
    ]

    LOG.info(
        "Producing %d page views and %d clicks to %s",
        args.page_views_count,
        args.clicks_count,
        ",".join(args.bootstrap_servers),
    )
    for thread in threads:
        thread.start()

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        LOG.info("Stopping producers")
        stop_event.set()
        for thread in threads:
            thread.join()

    if errors:
        topic, error = errors[0]
        raise RuntimeError(f"{topic} producer failed") from error

    LOG.info("Finished producing events")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(threadName)s %(levelname)s %(message)s",
    )
    main()
