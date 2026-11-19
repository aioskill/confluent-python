#!/usr/bin/env bash
# Install PyFlink, the Python libraries and the connector JARs used by the examples.
#
# Usage:
#   ./setup_pyflink.sh            # use the active virtualenv, or create ./.venv
#   source .venv/bin/activate     # then run the examples with "python <script>.py"
#
# What it does
#   1. creates ./.venv (only when no virtualenv is active),
#   2. pip installs requirements.txt (apache-flink 2.3.0, kafka-python, ...),
#   3. runs flinklibs/configure, which installs Maven (if missing, needs sudo) and
#      downloads the connector JARs (Kafka, JDBC/MySQL, Avro, Parquet, S3, Iceberg)
#      into the PyFlink "lib" directory of that environment,
#   4. creates sample data in ./data (sample text, stocks.csv, MovieLens).
#
# The JAR list is flinklibs/pom.xml, the same one used on the cluster.
# Safe to run again. Needs Python 3.12, Java 17+, curl.

set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

for tool in python3 java curl; do
  command -v "$tool" >/dev/null || { echo "Error: $tool was not found in PATH." >&2; exit 1; }
done

if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  VENV="$VIRTUAL_ENV"
  echo "==> Using the active virtualenv $VENV"
else
  VENV="$HERE/.venv"
  if [[ ! -x "$VENV/bin/python" ]]; then
    echo "==> Creating the virtualenv $VENV"
    python3 -m venv "$VENV"
  fi
  echo "==> Using the virtualenv $VENV"
fi
PY="$VENV/bin/python"

echo "==> Installing the Python packages from requirements.txt"
"$PY" -m pip install --quiet --upgrade pip
"$PY" -m pip install --quiet -r "$HERE/requirements.txt"

PYFLINK_DIR="$("$PY" -c 'import os, pyflink; print(os.path.dirname(pyflink.__file__))' | tail -1)"
LIB="$PYFLINK_DIR/lib"
echo "==> PyFlink: $PYFLINK_DIR"

echo "==> Downloading the connector JARs with Maven (flinklibs/pom.xml) into $LIB"
# flinklibs/configure resolves the dependencies of flinklibs/pom.xml (Kafka, JDBC,
# MySQL, Avro, Parquet, S3 filesystem, Hadoop client classes), adds the Iceberg
# runtime, and links every JAR into the PyFlink lib directory of the active Python.
PATH="$VENV/bin:$PATH" "$HERE/flinklibs/configure"

DATA="$HERE/data"
mkdir -p "$DATA"
echo "==> Preparing the sample data in $DATA"
"$PY" - "$DATA" <<'EOF2'
import csv, random, sys
from datetime import date, timedelta
from pathlib import Path

data = Path(sys.argv[1])
rng = random.Random(42)
words = "flink kafka stream window state checkpoint watermark table join sql python batch event time".split()
sample = data / "sample.txt"
if not sample.exists():  # ex04, ex05, ex15 (batch) and table_v2
    sample.write_text("\n".join(" ".join(rng.choices(words, k=8)) for _ in range(2000)) + "\n")
stocks = data / "stocks.csv"
if not stocks.exists():  # ex14 (batch) and ex06_stocks_csv
    with stocks.open("w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["date", "open", "high", "low", "close", "volume", "adjclose", "symbol"])
        for symbol, base in [("BAC", 30.0), ("AAPL", 150.0), ("MSFT", 300.0)]:
            price = base
            for day in range(250):
                price *= 1 + rng.uniform(-0.02, 0.02)
                o, c = price, price * (1 + rng.uniform(-0.01, 0.01))
                w.writerow([date(2025, 1, 1) + timedelta(days=day), round(o, 2),
                            round(max(o, c) * 1.01, 2), round(min(o, c) * 0.99, 2),
                            round(c, 2), int(rng.uniform(1e6, 5e7)), round(c, 2), symbol])
EOF2
if [[ ! -s "$DATA/ml-latest-small/ratings.csv" ]]; then
  echo "    get  MovieLens ml-latest-small (used by batch ex10, connectors ex02 and ex03)"
  curl --fail --silent --show-error --location --retry 3 -o "$DATA/ml-latest-small.zip" \
    https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
  "$PY" -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$DATA/ml-latest-small.zip" "$DATA"
  rm -f "$DATA/ml-latest-small.zip"
fi

echo "==> Checking the installation"
"$PY" - <<'EOF'
import pyflink, importlib.metadata as m
print("    apache-flink", m.version("apache-flink"))
from pyflink.table import EnvironmentSettings, TableEnvironment
t = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t.execute_sql("CREATE TABLE k (a STRING) WITH ('connector'='kafka','topic'='x',"
              "'properties.bootstrap.servers'='localhost:9092','format'='json')")
print("    Kafka connector: OK")
EOF

cat <<EOF

Done. Activate the environment and run an example from its directory:

    source "$VENV/bin/activate"
    cd "$HERE/streaming"
    export CONFLUENT_SASL_PASSWORD='<password from /app/confluent/etc/kafka/client.properties>'
    python ex01_data_validation.py
EOF
