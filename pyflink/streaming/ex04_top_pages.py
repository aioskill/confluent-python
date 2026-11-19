"""Find the top 10 pages in each recent 30-second window using Flink SQL.

The query uses a 30-second hopping event-time window that advances every
5 seconds. Therefore, Flink recalculates the ranking every 5 seconds while
each result still represents the previous 30 seconds of page views.

Example output and interpretation

6> +I[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-1, 2, 3]
6> +I[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-3, 3, 2]
6> +I[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-2, 3, 1]
6> +I[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-4, 2, 4]
6> -U[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-1, 2, 3]
6> -U[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-3, 3, 2]
6> -U[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-2, 3, 1]
6> -U[2026-09-01T17:03:50Z, 2026-09-01T17:04:20Z, page-4, 2, 4]
15> +I[2026-09-01T17:51:25Z, 2026-09-01T17:51:55Z, page-3, 3, 1]
15> +I[2026-09-01T17:51:25Z, 2026-09-01T17:51:55Z, page-4, 3, 2]
15> +I[2026-09-01T17:51:25Z, 2026-09-01T17:51:55Z, page-1, 2, 3]
15> +I[2026-09-01T17:51:25Z, 2026-09-01T17:51:55Z, page-2, 2, 4]


  - 6> / 15>: sink subtask that produced the record.
  - +I: insert a row.
  - -U: retract the previous version of an updated row.

  Each row is:

  [window_start, window_end, page_id, page_views, page_rank]

  For example:

  +I[..., page-2, 3, 1]

  means page-2 was ranked #1 with 3 views in that 30-second window.

  The first four +I rows describe the initial ranking. The following -U rows
  retract that earlier result. The counts do not have to change for a -U to
  be emitted: when a newer window becomes the latest window, the previous
  window is removed from the result by the MAX(window_end) filter. Therefore,
  -U is not a count update; it is a retraction of the old latest-window result.

  The 6> and 15> rows are produced by different sink subtasks. The print sink
  displays Flink's changelog, so it can show intermediate inserts and
  retractions rather than one final, sorted snapshot.

  The 15> rows represent the currently selected latest window. They show:

  1. page-3 — 3 views
  2. page-4 — 3 views
  3. page-1 — 2 views
  4. page-2 — 2 views

  Ties are resolved alphabetically by page_id.


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
    parser.add_argument("--group-id", default="ex04-top-pages")
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
            'properties.enable.auto.commit' = 'false',
            'properties.commit.offsets.on.checkpoint' = 'false',
            -- Allow windows to close when a Kafka partition has no events.
            'scan.watermark.idle-timeout' = '5 s',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE top_pages_sink (
            window_start TIMESTAMP_LTZ(3),
            window_end TIMESTAMP_LTZ(3),
            page_id STRING,
            page_views BIGINT,
            page_rank BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # HOP creates overlapping windows: each window covers 30 seconds and a
    # new window starts every 5 seconds. Rank pages within each window and
    # retain only the ten most frequently visited pages. The final join keeps
    # only the latest completed window.
    table_env.execute_sql("""
        INSERT INTO top_pages_sink
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
        SELECT ranked.window_start,
               ranked.window_end,
               ranked.page_id,
               ranked.page_views,
               ranked.page_rank
        FROM ranked_pages AS ranked
        JOIN latest_window AS latest
          ON ranked.window_end = latest.window_end
        WHERE ranked.page_rank <= 10
    """).wait()


if __name__ == "__main__":
    main()
