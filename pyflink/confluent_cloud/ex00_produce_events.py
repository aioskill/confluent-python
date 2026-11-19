"""Produce page views and clicks to Confluent Cloud (JSON), for the other examples here.

Creates three single-partition topics when they do not exist (Confluent Cloud uses
replication factor 3), then writes the events:

    <prefix>page_views   one event per page view
    <prefix>clicks       a click for every second page view, 1 to 5 seconds after the view

The events are spread evenly over the last --span-seconds (default 180) and are sent at
once, so the Flink examples see several 30-second windows without a long wait. Use your
own --prefix (for example your user name): the Confluent Cloud cluster is shared.

Run (from this script's directory)::

    python ex00_produce_events.py --prefix alice-

Check the topics with the Confluent Cloud console, or::

    python /home/aio/training/confluent-python/confluent-cloud/consume_messages.py alice-page_views --count 3

Delete the topics when you are done::

    python /home/aio/training/confluent-python/confluent-cloud/manage_topics.py delete alice-page_views --yes
"""

import argparse
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cloud_auth import confluent_kafka_properties, ensure_topics  # noqa: E402

from confluent_kafka import Producer


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--prefix", default="pyflink-", help="topic name prefix (use your own)")
    parser.add_argument("--page-views-count", type=int, default=40)
    parser.add_argument("--users", type=int, default=5)
    parser.add_argument("--pages", type=int, default=4)
    parser.add_argument("--span-seconds", type=int, default=180)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    views_topic = f"{args.prefix}page_views"
    clicks_topic = f"{args.prefix}clicks"
    config = confluent_kafka_properties()
    ensure_topics(views_topic, clicks_topic)

    rng = random.Random(args.seed)
    producer = Producer({**config, "acks": "all", "enable.idempotence": True})
    now = int(time.time() * 1000)
    step = args.span_seconds * 1000 // args.page_views_count
    clicks = 0
    for n in range(args.page_views_count):
        user_id = n % args.users + 1
        page_id = f"page-{n % args.pages + 1}"
        event_time = now - args.span_seconds * 1000 + n * step
        view = {"view_id": f"view-{n + 1}", "user_id": user_id, "page_id": page_id, "event_time": event_time}
        producer.produce(views_topic, key=str(user_id), value=json.dumps(view), timestamp=event_time)
        if n % 2 == 1:
            clicks += 1
            click = {
                "click_id": f"click-{clicks}", "view_id": view["view_id"], "user_id": user_id,
                "page_id": page_id, "event_time": event_time + rng.randint(1000, 5000),
            }
            producer.produce(clicks_topic, key=str(user_id), value=json.dumps(click), timestamp=click["event_time"])
    producer.flush(60)
    print(f"Produced {args.page_views_count} page views to {views_topic} and {clicks} clicks to {clicks_topic}")


if __name__ == "__main__":
    main()
