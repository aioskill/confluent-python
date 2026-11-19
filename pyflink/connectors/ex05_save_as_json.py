from pyflink.table import (EnvironmentSettings, TableEnvironment)



def main(output_path:str):
    # Create table environment
    env_settings = EnvironmentSettings.in_batch_mode()  # or in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Define JSON sink table
    table_env.execute_sql(f"""
    CREATE TABLE json_sink (
        user_id STRING,
        event_time STRING,
        attributes STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_path}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.map-null-key.mode' = 'DROP',
        'json.encode.decimal-as-plain-number' = 'true',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '30 min',
        'sink.partition-commit.policy.kind' = 'success-file'
    )
    """)


    # Create a table from Python data
    data = [("user1", "2023-01-01 10:00:00", "{'action':'login'}"),
            ("user2", "2023-01-02 11:00:00", "{'action':'purchase'}")]

    table = table_env.from_elements(data, ["user_id", "event_time", "attributes"])
    table_env.create_temporary_view("source_view", table)

    # Write to JSON
    # table.execute_insert("json_sink").wait()

    table_env.execute_sql("""
    INSERT INTO json_sink SELECT * FROM source_view
    """).wait()


    print("Job is complete")

if __name__ == "__main__":
    import argparse

    cli = argparse.ArgumentParser()
    cli.add_argument("--output", default="/tmp/flink-json", help="Output directory (file:// or s3://)")
    output_path_ = cli.parse_args().output
    main(output_path_)