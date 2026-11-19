"""Write the latest top-10 pages to an upsert Kafka topic using Flink SQL.

The query uses a 30-second event-time window that advances every 5 seconds.
The result is written to an ``upsert-kafka`` sink keyed by
``(window_end, page_id)``. A downstream consumer or materialized-view service
can therefore maintain the current top-10 rows without exposing Flink's
internal ``+I``/``-U`` changelog records.

The output topic contains at most ten rows for the latest completed window.
Rows are ordered by ``page_views DESC`` and then ``page_id ASC`` when the
consumer displays the current state. Kafka does not preserve this ordering.

Run kafka-console-consumer and kafka-topics (in /app/confluent/bin) on sa01/sa02/sa03

Create the sink topic with log compaction before starting the job::

/app/confluent/bin/kafka-topics --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 \
        --create --topic top_pages --partitions 3 --replication-factor 1 \
        --config cleanup.policy=compact

Run a consumer against the output topic, for example::

/app/confluent/bin/kafka-console-consumer \
    --command-config /app/confluent/etc/kafka/client.properties \
    --bootstrap-server sa01:9092 \
    --topic top_pages \
    --from-beginning \
    --property print.key=true \
    --property key.separator=" | "

Open a new terminal and keep the page_view and clicks data generator running. Inside pycharm.

python3 ex00_produce_page_views_and_clicks.py --page-views-count  100000 --clicks-count 100000



Use the appropriate replication factor for a multi-broker cluster. Kafka does
not enforce uniqueness; ``(window_end, page_id)`` is the Kafka message key
that consumers use to maintain the latest row. Older physical records remain
until Kafka compaction runs.

Deleted rows are represented by Kafka tombstones. A consumer must remove the
corresponding key from its materialized state when it receives a tombstone.

Preparation sequence:

1. Run the Kafka topic creation command above. Also create ``page_views`` if
   it does not already exist.
2. Configure the Flink dependencies from the repository root::

       bash flinklibs/configure

3. Start the synthetic producer from the repository root::

       python ex00_produce_page_views_and_clicks.py

4. Start this job to write the latest top-10 changelog to Kafka.



Run locally (embedded Flink mini cluster, from this script's directory)
python ex05_top_pages_snapshot.py

Submit the job to flink cluster (copy this script to sa03 and run the following)
flink run -m sa03:8967 \
    --detached  \
    -Dexecution.checkpointing.interval=30s \
    -Dexecution.checkpointing.storage=filesystem \
    -Dexecution.checkpointing.dir=s3://my-bucket/flink/checkpoints \
    -py ex05_top_pages_snapshot.py


Find consumer group offset
/app/confluent/bin/kafka-consumer-groups --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --describe --group ex05-top-pages-upsert

Stop job and save state
/app/flink/bin/flink stop --savepointPath s3://my-bucket/flink/savepoints <job-id>

Checkpoint (automated failure recovery)

Restart from a save point (manual restart of job)
flink run -m sa03:8967 \
    --detached  \
    -Dexecution.checkpointing.interval=30s \
    -Dexecution.checkpointing.storage=filesystem \
    -Dexecution.checkpointing.dir=s3://my-bucket/flink/checkpoints \
    -s s3://my-bucket/flink/savepoints/<save point id> \
    -py ex05_top_pages_snapshot.py



Test cases:
1. (Checkpointing) Stop the task manager which is executing an operator. (simulate a fail over condition)
    Observation: the operator will automatically switch to another task manager
2. (Save point)
    A. Stop the job with save point
    B. Restart the job from the last save point
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import sql_options  # noqa: E402  (SASL/SCRAM settings)

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="sa01:9092")
    parser.add_argument("--page-views-topic", default="page_views")
    parser.add_argument("--sink-topic", default="top_pages")
    parser.add_argument("--group-id", default="ex05-top-pages-upsert")
    args = parser.parse_args()

    table_env = TableEnvironment.create(
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .build()
    )
    table_env.get_config().set_local_timezone("UTC")

    table_env.execute_sql(f"""
        CREATE TABLE page_views (
            page_id STRING,
            event_time BIGINT,
            event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.page_views_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'properties.group.id' = '{args.group_id}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'properties.enable.auto.commit' = 'true',
            'properties.commit.offsets.on.checkpoint' = 'true',
            -- Allow windows to close when a Kafka partition has no events.
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # The primary key makes this an upsert sink. Updates to a page replace its
    # current value, while rows removed from the top 10 become tombstones.
    # https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/table/upsert-kafka/
    table_env.execute_sql(f"""
        CREATE TABLE top_pages_sink (
            window_end TIMESTAMP_LTZ(3),
            page_id STRING,
            window_start TIMESTAMP_LTZ(3),
            page_views BIGINT,
            page_rank BIGINT,
            PRIMARY KEY (window_end, page_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{args.sink_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            {sql_options()}
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # HOP creates overlapping windows: each window covers 30 seconds and a
    # new window starts every 5 seconds. MAX(window_end) retains only the
    # latest completed window in the materialized upsert table.
    top_pages_query = """
        WITH page_counts AS (
            SELECT
                window_start,
                window_end,
                page_id,
                COUNT(*) AS page_views
            FROM TABLE(
                HOP(
                    TABLE page_views,
                    DESCRIPTOR(event_ts),
                    INTERVAL '5' SECOND,
                    INTERVAL '30' SECOND
                )
            )
            GROUP BY window_start, window_end, page_id
        ), ranked_pages AS (
            SELECT
                window_start,
                window_end,
                page_id,
                page_views,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end
                    ORDER BY page_views DESC, page_id
                ) AS page_rank
            FROM page_counts
        ), latest_window AS (
            SELECT MAX(window_end) AS window_end
            FROM ranked_pages
        )
        SELECT ranked.window_end,
               ranked.page_id,
               ranked.window_start,
               ranked.page_views,
               ranked.page_rank
        FROM ranked_pages AS ranked
        JOIN latest_window AS latest
          ON ranked.window_end = latest.window_end
        WHERE ranked.page_rank <= 10
        ON CONFLICT DO DEDUPLICATE
    """

    table_env.execute_sql(f"""
        INSERT INTO top_pages_sink
        {top_pages_query}
    """).wait()  # keep the local mini cluster alive while the job runs


if __name__ == "__main__":
    main()
