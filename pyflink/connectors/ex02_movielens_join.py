from pathlib import Path

from pyflink.common import Configuration
from pyflink.table import (EnvironmentSettings, TableEnvironment)
import os.path

config = Configuration()
config.set_string("pipeline.name", "MyPyFlinkJob")
config.set_string("parallelism.default", "1")
config.set_string("sink.parallelism", "4")

"""
CSV connection properties
https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/csv/
"""

t_env = TableEnvironment.create(EnvironmentSettings.new_instance()
    .in_batch_mode()
    .with_configuration(config)
    .build())


def process(input_directory:str, output_path:str):
    path = os.path.join(input_directory, "movies.csv")
    t_env.execute_sql(f"""
        CREATE TABLE movies_raw (
              `movieId` STRING,
              `title` STRING,
              `genres` STRING
        ) WITH (
             'connector' = 'filesystem',
             'path' = '{path}',
             'format' = 'csv',
             'csv.ignore-parse-errors' = 'false'
        )
    """)
    path = os.path.join(input_directory, "ratings.csv")
    t_env.execute_sql(f"""
            CREATE TABLE ratings_raw (
                  `userId` STRING,
                  `movieId` STRING,
                  `rating` STRING,
                  `timestamp` STRING
            ) WITH (
                 'connector' = 'filesystem',
                 'path' = '{path}',
                 'format' = 'csv',
                 'csv.ignore-parse-errors' = 'false'
            )
        """)

    # Flink's CSV filesystem format reads the header as a record in this
    # version. Filter it while the fields are still strings, then cast the
    # actual data rows to their types.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW movies AS
        SELECT CAST(movieId AS INTEGER) AS movieId, title, genres
        FROM movies_raw
        WHERE movieId <> 'movieId'
    """)
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW ratings AS
        SELECT CAST(userId AS INTEGER) AS userId,
               CAST(movieId AS INTEGER) AS movieId,
               CAST(rating AS DOUBLE) AS rating,
               CAST(`timestamp` AS BIGINT) AS `timestamp`
        FROM ratings_raw
        WHERE userId <> 'userId'
    """)

    t_env.execute_sql(f"""
    CREATE TABLE csv_sink (
        movieId INTEGER,
        title STRING,
        rating_count BIGINT,
        avg_rating DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'csv',
        'csv.field-delimiter' = ',',
        'csv.quote-character' = '"',
        'csv.null-literal' = 'NULL',
        'csv.write-batch-size' = '1000',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '30 min',
        'sink.partition-commit.policy.kind' = 'success-file'
    )
    """)

    t_env.execute_sql("""
    CREATE TEMPORARY VIEW temp_results AS
    select 
        t1.movieId, 
        t1.title, 
        count(1) rating_count, 
        avg(t2.rating) avg_rating 
    FROM movies t1 left join ratings t2
    ON t1.movieId = t2.movieId
    GROUP BY 
        t1.movieId, 
        t1.title
    HAVING count(1)>100
    ORDER BY avg_rating DESC
    """)

    t_env.execute_sql("""
    INSERT INTO csv_sink
    SELECT * FROM temp_results
    """).wait()

    t_env.execute_sql("""
        SELECT * FROM temp_results limit 10
        """).print()

    print("Job is complete")

if __name__ == '__main__':
    input_path_ = str(Path(__file__).resolve().parents[1] / "data" / "ml-latest-small")
    output_path_ = "/tmp/topmovies"
    process(input_path_, output_path_)
