from pathlib import Path

from pyflink.table import (EnvironmentSettings, TableEnvironment)

def process(input_path):
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    # write all the data to one file
    t_env.get_config().set("parallelism.default", "1")
    t_env.execute_sql(f"""
        CREATE TABLE stocks (
              `date` DATE,
              `open` DOUBLE,
              `high` DOUBLE,
              `low` DOUBLE,
              `close` DOUBLE,
              `volume` DOUBLE,
              `adjclose` DOUBLE,
              `symbol` STRING
        ) WITH (
             'connector' = 'filesystem',
             'path' = '{input_path}',
             'format' = 'csv',
             'csv.header' = 'true',
             'csv.ignore-parse-errors' = 'true'
        )
    """)


    t_env.execute_sql("""
            CREATE TABLE console_sink (
              `date` DATE,
              `open` DOUBLE,
              `high` DOUBLE,
              `low` DOUBLE,
              `close` DOUBLE,
              `volume` DOUBLE,
              `adjclose` DOUBLE,
              `symbol` STRING
            ) WITH (
              'connector' = 'print'
            )
        """)

    # Option 1
    # table = t_env.from_path("stocks")
    # table_result = table.execute_insert("console_sink")
    # table_result.wait()

    # Option 2
    # t_env.execute_sql("""
    # CREATE TEMPORARY VIEW temp_view AS
    # SELECT * FROM stocks limit 10
    # """)
    # table = t_env.from_path("temp_view")
    # table_result = table.execute_insert("console_sink")
    # table_result.wait()

    # Option 3
    t_env.execute_sql("""
        INSERT INTO console_sink
        SELECT * FROM stocks WHERE symbol LIKE 'BAC'
        """).wait()



    print("Job is complete")

if __name__ == '__main__':
    input_path_ = Path(__file__).resolve().parents[1] / "data" / "stocks.csv"
    process(input_path_)
