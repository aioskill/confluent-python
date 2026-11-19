import os

# Resolve credentials before importing PyFlink so the Java gateway inherits
# the standard AWS environment variables.
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.environ.get("S3_SECRET_ACCESS_KEY") or os.environ.get(
    "AWS_SECRET_ACCESS_KEY"
)
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION")

missing = [
    name
    for name, value in {
        "S3_ACCESS_KEY_ID": ACCESS_KEY,
        "S3_SECRET_ACCESS_KEY": SECRET_KEY,
        "S3_BUCKET": BUCKET,
        "S3_REGION": REGION,
    }.items()
    if not value
]
if missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

os.environ.setdefault("AWS_ACCESS_KEY_ID", ACCESS_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
os.environ.setdefault("AWS_REGION", REGION)
os.environ.setdefault("AWS_DEFAULT_REGION", REGION)

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment


OBJECT_PATH = os.environ.get("S3_OBJECT_PATH", "ex08/synthetic-stocks")
S3_PATH = f"s3://{BUCKET}/{OBJECT_PATH}"


def process():
    """Write synthetic data to AWS S3 and read it back with Flink SQL."""
    table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    configuration = table_env.get_config().get_configuration()

    # Flink uses these settings for the AWS S3 filesystem connector.
    configuration.set_string("s3.access-key", ACCESS_KEY)
    configuration.set_string("s3.secret-key", SECRET_KEY)
    configuration.set_string("fs.s3a.endpoint.region", REGION)

    stock_type = DataTypes.ROW([
        DataTypes.FIELD("symbol", DataTypes.STRING()),
        DataTypes.FIELD("price", DataTypes.DOUBLE()),
        DataTypes.FIELD("quantity", DataTypes.INT()),
    ])
    synthetic_stocks = table_env.from_elements(
        [
            ("ACME", 101.25, 10),
            ("ACME", 102.50, 5),
            ("GLOBEX", 87.75, 8),
        ],
        stock_type,
    )
    table_env.create_temporary_view("synthetic_stocks", synthetic_stocks)

    table_env.execute_sql(f"""
        CREATE TABLE s3_sink (
            symbol STRING,
            price DOUBLE,
            quantity INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{S3_PATH}',
            'format' = 'csv',
            'csv.write-header' = 'true',
            'sink.parallelism' = '1'
        )
    """)

    print(f"Writing synthetic data to {S3_PATH}")
    table_env.execute_sql("""
        INSERT INTO s3_sink
        SELECT symbol, price, quantity
        FROM synthetic_stocks
    """).wait()

    table_env.execute_sql(f"""
        CREATE TABLE s3_source (
            symbol STRING,
            price DOUBLE,
            quantity INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{S3_PATH}',
            'format' = 'csv',
            'csv.header' = 'true',
            'csv.ignore-parse-errors' = 'true'
        )
    """)

    print(f"Reading data back from {S3_PATH}")
    table_env.sql_query("""
        SELECT symbol, price, quantity
        FROM s3_source
        ORDER BY symbol, price
    """).execute().print()


if __name__ == "__main__":
    process()
