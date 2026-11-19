from pathlib import Path

from pyflink.common import Configuration
from pyflink.table import (EnvironmentSettings, TableEnvironment)
import os
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

    # The Parquet format is provided by the flink-parquet connector. Add the
    # dependency matching the installed PyFlink version, then put the copied
    # JARs on the client and TaskManager classpaths, for example:
    #
    #   mvn dependency:get \
    #     -Dartifact=org.apache.flink:flink-parquet:2.3.0
    #
    # For a cluster, copy flink-parquet-2.3.0.jar and its transitive Parquet
    # dependencies into FLINK_HOME/lib on every JobManager and TaskManager.
    movies_output = os.path.join(output_path, "movies")
    ratings_output = os.path.join(output_path, "ratings")
    t_env.execute_sql(f"""
        CREATE TABLE movies_parquet (
            movieId INTEGER,
            title STRING,
            genres STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{movies_output}',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY'
        )
    """)
    t_env.execute_sql(f"""
        CREATE TABLE ratings_parquet (
            userId INTEGER,
            movieId INTEGER,
            rating DOUBLE,
            `timestamp` BIGINT
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{ratings_output}',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY'
        )
    """)

    print(f"Writing movies to Parquet: {movies_output}")
    t_env.execute_sql("""
        INSERT INTO movies_parquet
        SELECT movieId, title, genres FROM movies
    """).wait()

    print(f"Writing ratings to Parquet: {ratings_output}")
    t_env.execute_sql("""
        INSERT INTO ratings_parquet
        SELECT userId, movieId, rating, `timestamp` FROM ratings
    """).wait()

    print("Job is complete")

if __name__ == '__main__':
    input_path_ = str(Path(__file__).resolve().parents[1] / "data" / "ml-latest-small")
    import argparse

    cli = argparse.ArgumentParser()
    cli.add_argument("--output", default="/tmp/movieslens_parquet", help="Output directory (file:// or s3://)")
    output_path_ = cli.parse_args().output
    process(input_path_, output_path_)
