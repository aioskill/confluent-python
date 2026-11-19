from datetime import date

import pandas as pd
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment


def process():
    """Demonstrate Table -> Pandas -> Table using synthetic data."""
    table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    # Create a small Flink Table in memory. No CSV file or external system is
    # required for this example.
    stock_rows = [
        (date(2024, 1, 2), 100.0, 104.0, 99.0, 103.0, 10000.0, "ACME"),
        (date(2024, 1, 3), 103.0, 105.0, 101.0, 102.0, 12000.0, "ACME"),
        (date(2024, 1, 4), 102.0, 106.0, 102.0, 105.0, 11000.0, "ACME"),
    ]
    stocks_table = table_env.from_elements(
        stock_rows,
        DataTypes.ROW([
            DataTypes.FIELD("date", DataTypes.DATE()),
            DataTypes.FIELD("open", DataTypes.DOUBLE()),
            DataTypes.FIELD("high", DataTypes.DOUBLE()),
            DataTypes.FIELD("low", DataTypes.DOUBLE()),
            DataTypes.FIELD("close", DataTypes.DOUBLE()),
            DataTypes.FIELD("volume", DataTypes.DOUBLE()),
            DataTypes.FIELD("symbol", DataTypes.STRING()),
        ]),
    )

    # 1. Convert a PyFlink Table to a local Pandas DataFrame.
    pandas_df = stocks_table.to_pandas()
    print("Table -> Pandas DataFrame")
    print(pandas_df)

    # Perform a small local Pandas transformation.
    pandas_df["date"] = pd.to_datetime(pandas_df["date"]).dt.date
    pandas_df["change"] = pandas_df["close"] - pandas_df["open"]

    # 2. Convert the modified Pandas DataFrame back to a PyFlink Table.
    pandas_table = table_env.from_pandas(
        pandas_df,
        schema=[
            DataTypes.DATE(),
            DataTypes.DOUBLE(),
            DataTypes.DOUBLE(),
            DataTypes.DOUBLE(),
            DataTypes.DOUBLE(),
            DataTypes.DOUBLE(),
            DataTypes.STRING(),
            DataTypes.DOUBLE(),
        ],
    )
    table_env.create_temporary_view("stocks_from_pandas", pandas_table)

    print("Pandas DataFrame -> Table")
    table_env.sql_query("""
        SELECT `date`, `symbol`, `open`, `close`, `change`
        FROM stocks_from_pandas
        ORDER BY `date`
    """).execute().print()

    print("Job is complete")


if __name__ == "__main__":
    process()
