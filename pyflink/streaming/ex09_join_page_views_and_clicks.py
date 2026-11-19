"""Join page views and clicks by user and event-time window.

Both Kafka streams are keyed by ``user_id`` and joined in a 30-second tumbling
event-time window. The output contains the page from the page-view event and
the number of clicks made by the same user during that window. If a click
arrives after the page view but before the allowed-lateness boundary, an
updated joined result is emitted.

The example uses RocksDB-backed managed state with a one-hour TTL. Events that
arrive beyond the ten-second allowed-lateness boundary are sent to a side
output. Create the input topics before running::

    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \\
        --create --topic page_views --partitions 3 --replication-factor 1
    /app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \\
        --create --topic clicks --partitions 3 --replication-factor 1

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex09_join_page_views_and_clicks.py

Submit to a Flink cluster::

    flink run -py ex09_join_page_views_and_clicks.py
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
from pyflink.datastream.functions import KeyedCoProcessFunction
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig


WINDOW_MILLIS = 30_000
OUT_OF_ORDERNESS_MILLIS = 10_000
ALLOWED_LATENESS_MILLIS = 10_000
STATE_TTL_HOURS = 1

PAGE_VIEW_TYPE = Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()])
CLICK_TYPE = Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING(), Types.LONG()])
WINDOW_EVENT_KEY_TYPE = Types.TUPLE([Types.LONG(), Types.STRING()])


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, _record_timestamp):
        return value[3]


class JoinPageViewsAndClicks(KeyedCoProcessFunction):
    """Join events for each user and event-time window in managed state."""

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
        self.page_views = runtime_context.get_map_state(
            self._state_descriptor("page-views", ttl_config)
        )
        self.clicks = runtime_context.get_map_state(
            self._state_descriptor("clicks", ttl_config)
        )

    @staticmethod
    def _state_descriptor(name, ttl_config):
        descriptor = MapStateDescriptor(
            name,
            WINDOW_EVENT_KEY_TYPE,
            Types.STRING(),
        )
        descriptor.enable_time_to_live(ttl_config)
        return descriptor

    @staticmethod
    def _window_end(event_time):
        return ((event_time // WINDOW_MILLIS) + 1) * WINDOW_MILLIS

    def _is_too_late(self, event_time, context):
        watermark = context.timer_service().current_watermark()
        return watermark >= event_time + ALLOWED_LATENESS_MILLIS

    def _emit_late(self, event, event_type, context):
        context.output(
            self.late_events,
            json.dumps({
                "event_type": event_type,
                "user_id": event[0],
                "page_id": event[1],
                "event_id": event[2],
                "event_time": event[3],
                "current_watermark": context.timer_service().current_watermark(),
                "reason": "beyond_allowed_lateness",
            }, sort_keys=True),
        )

    def _register_cleanup_timer(self, window_end, context):
        context.timer_service().register_event_time_timer(
            window_end + ALLOWED_LATENESS_MILLIS
        )

    def process_element1(self, event, context):
        user_id, page_id, view_id, event_time = event
        if self._is_too_late(event_time, context):
            self._emit_late(event, "page_view", context)
            return

        window_end = self._window_end(event_time)
        self.page_views.put((window_end, view_id), json.dumps({
            "page_id": page_id,
            "view_id": view_id,
            "event_time": event_time,
        }))
        self._register_cleanup_timer(window_end, context)

        click_count = sum(
            1
            for (click_window_end, _click_id), _click in self.clicks.items()
            if click_window_end == window_end
        )
        yield self._joined_result(user_id, page_id, view_id, window_end, click_count)

    def process_element2(self, event, context):
        user_id, page_id, click_id, event_time = event
        if self._is_too_late(event_time, context):
            self._emit_late(event, "click", context)
            return

        window_end = self._window_end(event_time)
        self.clicks.put((window_end, click_id), json.dumps({
            "page_id": page_id,
            "click_id": click_id,
            "event_time": event_time,
        }))
        self._register_cleanup_timer(window_end, context)

        # A newly arrived click changes every page-view result for this user
        # and window, so emit an updated result for each matching page view.
        click_count = sum(
            1
            for (click_window_end, _click_id), _click in self.clicks.items()
            if click_window_end == window_end
        )
        for (view_window_end, _view_id), raw_view in self.page_views.items():
            if view_window_end != window_end:
                continue
            view = json.loads(raw_view)
            yield self._joined_result(
                user_id,
                view["page_id"],
                view["view_id"],
                window_end,
                click_count,
            )

    @staticmethod
    def _joined_result(user_id, page_id, view_id, window_end, click_count):
        return json.dumps({
            "user_id": user_id,
            "page_id": page_id,
            "view_id": view_id,
            "window_start": window_end - WINDOW_MILLIS,
            "window_end": window_end,
            "click_count": click_count,
        }, sort_keys=True)

    def on_timer(self, timestamp, context):
        window_end = timestamp - ALLOWED_LATENESS_MILLIS
        for state, _value in list(self.page_views.items()):
            if state[0] == window_end:
                self.page_views.remove(state)
        for state, _value in list(self.clicks.items()):
            if state[0] == window_end:
                self.clicks.remove(state)


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
        int(event["user_id"]),
        event["page_id"],
        event.get("view_id", "unknown"),
        int(event["event_time"]),
    )


def parse_click(raw_value):
    event = json.loads(raw_value)
    return (
        int(event["user_id"]),
        event["page_id"],
        event.get("click_id", "unknown"),
        int(event["event_time"]),
    )


def event_time_watermarks():
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(OUT_OF_ORDERNESS_MILLIS // 1000)
        )
        .with_timestamp_assigner(EventTimeAssigner())
        .with_idleness(Duration.of_seconds(5))
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--clicks-topic", default="clicks")
    parser.add_argument("--group-id", default="ex09-join-page-views-clicks")
    parser.add_argument(
        "--rocksdb-dir",
        default="/tmp/pyflink-training/ex09-rocksdb",
        help="Local RocksDB state directory",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/pyflink-training/ex09-checkpoints",
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
    raw_clicks = env.from_source(
        kafka_source(args.bootstrap_servers, args.clicks_topic, f"{args.group_id}-clicks"),
        WatermarkStrategy.no_watermarks(),
        "clicks Kafka source",
    )

    page_views = raw_page_views.map(parse_page_view, output_type=PAGE_VIEW_TYPE)
    page_views = page_views.assign_timestamps_and_watermarks(event_time_watermarks())
    clicks = raw_clicks.map(parse_click, output_type=CLICK_TYPE)
    clicks = clicks.assign_timestamps_and_watermarks(event_time_watermarks())

    late_events = OutputTag("late-events", Types.STRING())
    joined = (
        page_views
        .key_by(lambda event: event[0], key_type=Types.INT())
        .connect(clicks.key_by(lambda event: event[0], key_type=Types.INT()))
        .process(
            JoinPageViewsAndClicks(late_events),
            output_type=Types.STRING(),
        )
    )
    joined.print("joined-page-views")
    joined.get_side_output(late_events).print("late-events")

    env.execute("join page views and clicks by user and time window")


if __name__ == "__main__":
    main()
