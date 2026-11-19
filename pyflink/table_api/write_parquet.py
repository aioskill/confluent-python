from dateutil import parser
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes


def main(output_path: str):
    """
    Main function to demonstrate writing PyFlink Table data to Parquet format.

    Args:
        output_path (str): The local file system path where Parquet files will be written.



    The Parquet JARs are installed into PyFlink by ../setup_pyflink.sh
    (flinklibs/pom.xml), so no ``pipeline.jars`` setting is needed.
    """
    env_settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.execute_sql(f"""
    CREATE TABLE parquet_sink (
        user_id STRING,
        event_time TIMESTAMP(3),
        attributes STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'parquet',
        'parquet.compression' = 'SNAPPY', -- GZIP, LZO, ZSTD, UNCOMPRESSED
        'parquet.enable.dictionary' = 'true',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '30 min',
        'sink.partition-commit.policy.kind' = 'success-file'
    )
    """)

    # Create a table from Python data
    # The 'event_time' is parsed using dateutil.parser to ensure it's a datetime object,
    # which maps correctly to Flink's TIMESTAMP type.
    data = [
        ("user1", parser.parse("2023-01-01 10:00:00.000"), "{'action':'login'}"),
        ("user2", parser.parse("2023-01-02 11:00:00.000"), "{'action':'purchase'}"),
        ("user3", parser.parse("2023-01-03 12:30:00.000"), "{'item':'book', 'price': 25.5}"),
        ("user1", parser.parse("2023-01-01 10:15:00.000"), "{'action':'logout'}"),
        ("user4", parser.parse("2023-01-04 09:00:00.000"), "{'source':'web', 'device':'mobile'}")
    ]

    # Define the schema for the Python data
    fields = DataTypes.ROW([
        DataTypes.FIELD('user_id', DataTypes.STRING()),
        DataTypes.FIELD('event_time', DataTypes.TIMESTAMP(3)),  # Match TIMESTAMP(3) in DDL
        DataTypes.FIELD('attributes', DataTypes.STRING())
    ])

    # Create a temporary view from the Python data
    table = table_env.from_elements(data, fields)
    table_env.create_temporary_view("source_view", table)

    # Execute the INSERT INTO statement to write data to the Parquet sink
    print(f"Writing data to Parquet files in: {output_path}")
    table_env.execute_sql("""
    INSERT INTO parquet_sink SELECT * FROM source_view
    """).wait()

    print(f"Job is complete. Output files located at: {output_path}")


if __name__ == "__main__":
    import argparse

    cli = argparse.ArgumentParser()
    cli.add_argument("--output", default="/tmp/flink-parquet", help="Output directory (file:// or s3://)")
    output_path_ = cli.parse_args().output
    main(output_path_)
