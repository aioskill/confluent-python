"""Demonstrate Kafka, watermarks, late events, windows, TTL, and RocksDB.

The job reads page views from Kafka and uses a keyed ``ProcessFunction`` to
implement 30-second tumbling event-time windows. A watermark allows ten
seconds of out-of-order data. Events that arrive after the window's allowed
lateness are sent to a late-events side output.

Window counts are kept in TTL-enabled managed keyed state. RocksDB stores the
working state locally, and checkpoints provide recovery. Create the source
topic before running::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \\
        --create --topic page_views --partitions 3 --replication-factor 1

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex08_datastream_watermark_late_events.py

Submit to a Flink cluster::

    flink run -py ex08_datastream_watermark_late_events.py

The main output contains one final count per page and window. Late events are
printed separately with the ``late-events`` prefix.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import kafka_client_properties  # noqa: E402  (SASL/SCRAM settings)

import argparse
import json

from pyflink.common import Configuration, Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig


WINDOW_MILLIS = 30_000
OUT_OF_ORDERNESS_MILLIS = 10_000
ALLOWED_LATENESS_MILLIS = 10_000
STATE_TTL_HOURS = 1

PAGE_VIEW_TYPE = Types.TUPLE([Types.STRING(), Types.LONG(), Types.STRING()])


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_timestamp):
        return value[1]


class CountPageViews(KeyedProcessFunction):
    """Count one page's events per event-time window in managed state."""

    def __init__(self, late_events):
        self.late_events = late_events

    def open(self, runtime_context):
        ttl_config = (
            StateTtlConfig.new_builder(Time.hours(STATE_TTL_HOURS))
            .use_processing_time()
            .update_ttl_on_create_and_write()
            .never_return_expired()
            .cleanup_in_rocksdb_compact_filter(1000, Time.hours(STATE_TTL_HOURS))
            .build()
        )
        descriptor = MapStateDescriptor(
            "page-window-counts",
            Types.LONG(),
            Types.LONG(),
        )
        descriptor.enable_time_to_live(ttl_config)
        self.window_counts = runtime_context.get_map_state(descriptor)

    def process_element(self, value, context):
        page_id, event_time, view_id = value
        watermark = context.timer_service().current_watermark()
        window_end = ((event_time // WINDOW_MILLIS) + 1) * WINDOW_MILLIS

        # A record is too late once its allowed-lateness boundary has passed.
        if watermark >= event_time + ALLOWED_LATENESS_MILLIS:
            context.output(self.late_events, json.dumps({
                "reason": "beyond_allowed_lateness",
                "page_id": page_id,
                "view_id": view_id,
                "event_time": event_time,
                "current_watermark": watermark,
            }, sort_keys=True))
            return

        old_count = self.window_counts.get(window_end) or 0
        self.window_counts.put(window_end, old_count + 1)
        context.timer_service().register_event_time_timer(
            window_end + ALLOWED_LATENESS_MILLIS
        )

    def on_timer(self, timestamp, context):
        window_end = timestamp - ALLOWED_LATENESS_MILLIS
        count = self.window_counts.get(window_end)
        if count is None:
            return

        yield json.dumps({
            "page_id": context.get_current_key(),
            "window_start": window_end - WINDOW_MILLIS,
            "window_end": window_end,
            "page_views": count,
            "watermark": timestamp,
        }, sort_keys=True)
        self.window_counts.remove(window_end)


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


def parse_page_view(raw_value):
    event = json.loads(raw_value)
    return (
        event["page_id"],
        int(event["event_time"]),
        event.get("view_id", "unknown"),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--group-id", default="ex08-watermark-late-events")
    parser.add_argument(
        "--rocksdb-dir",
        default="/tmp/pyflink-training/ex08-rocksdb",
        help="Local RocksDB state directory",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/pyflink-training/ex08-checkpoints",
        help="Checkpoint directory; use shared S3 on a cluster",
    )
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    args = parser.parse_args()

    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")

    configuration = Configuration()
    configuration.set_string("state.backend.type", "rocksdb")
    configuration.set_string("state.backend.rocksdb.localdir", args.rocksdb_dir)
    configuration.set_string("state.checkpoints.dir", args.checkpoint_dir)
    configuration.set_string(
        "execution.checkpointing.interval",
        f"{args.checkpoint_interval_ms}ms",
    )
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")

    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.enable_checkpointing(args.checkpoint_interval_ms)

    raw_page_views = env.from_source(
        kafka_source(args.bootstrap_servers, args.page_views_topic, args.group_id),
        WatermarkStrategy.no_watermarks(),
        "page views Kafka source",
    )
    page_views = raw_page_views.map(parse_page_view, output_type=PAGE_VIEW_TYPE)
    page_views = page_views.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(OUT_OF_ORDERNESS_MILLIS // 1000)
        )
        .with_timestamp_assigner(EventTimeAssigner())
        .with_idleness(Duration.of_seconds(5))
    )

    late_events = OutputTag("late-events", Types.STRING())
    results = (
        page_views
        .key_by(lambda event: event[0], key_type=Types.STRING())
        .process(
            CountPageViews(late_events),
            output_type=Types.STRING(),
        )
    )
    results.print("window-results")
    results.get_side_output(late_events).print("late-events")

    env.execute("DataStream watermarks late events and RocksDB state")


if __name__ == "__main__":
    main()
