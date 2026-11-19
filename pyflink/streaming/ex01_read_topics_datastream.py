"""Read the synthetic page-view and click Kafka topics with DataStream API.

The producer for these topics is:

    ex00_produce_page_views_and_clicks.py

This job is intentionally long-running. Stop it with Ctrl-C after observing
records from both streams. Start from the earliest offsets by default so local
runs can consume records produced before the Flink job started.

Kafka auto-commit is disabled. Flink commits source offsets to Kafka only
after successful checkpoints, keeping committed offsets aligned with the
checkpointed processing state.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import kafka_client_properties  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.common import WatermarkStrategy, Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


def kafka_source(bootstrap_servers, topic, group_id):
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("enable.auto.commit", "false")
        .set_property("commit.offsets.on.checkpoint", "true")
    )
    # The cluster requires SASL/SCRAM authentication.
    for key, value in kafka_client_properties().items():
        builder.set_property(key, value)
    return builder.build()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--group-id", default="ex01-read-topics")
    parser.add_argument(
        "--checkpoint-interval-ms",
        type=int,
        default=10000,
        help="Flink checkpoint interval used for Kafka offset commits",
    )
    args = parser.parse_args()

    config = Configuration()
    config.set_string("rest.port", "60000")
    config.set_string("rest.bind-address", "127.0.0.1")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")
    env.enable_checkpointing(args.checkpoint_interval_ms)
    page_views = env.from_source(
        kafka_source(args.bootstrap_servers, args.page_views_topic, args.group_id),
        WatermarkStrategy.no_watermarks(),
        "page views source",
    )
    clicks = env.from_source(
        kafka_source(args.bootstrap_servers, args.clicks_topic, f"{args.group_id}-clicks"),
        WatermarkStrategy.no_watermarks(),
        "clicks source",
    )

    # Keep the streams separate so the next exercise can add parsing,
    # timestamps, watermarks, and a keyed stream join.
    page_views.map(lambda value: f"page_views: {value}").print()
    clicks.map(lambda value: f"clicks: {value}").print()

    env.execute("read page_views and clicks topics")


if __name__ == "__main__":
    main()
