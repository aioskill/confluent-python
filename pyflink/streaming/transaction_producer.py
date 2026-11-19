import argparse
import json
from datetime import datetime, timezone
from random import gauss, randint, choice, uniform
from time import sleep, time
from uuid import uuid1

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from kafka_auth import kafka_python_properties  # noqa: E402  (SASL/SCRAM settings)

from kafka import KafkaProducer

"""

Create topic
/app/confluent/bin/kafka-topics  --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic transactions --delete 
/app/confluent/bin/kafka-topics  --command-config /app/confluent/etc/kafka/client.properties --bootstrap-server sa01:9092 --topic transactions --create \
--replication-factor 1 --partitions 3



Run the application
$ python transaction_producer.py --start-date 2026-07-03T00:00:00Z --rate 60


Producer Properties
https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

"""

host = "sa01"
topic = "transactions"
port = 9092

# To send messages synchronously
producer = KafkaProducer(
    bootstrap_servers=f"{host}:{port}",
    **kafka_python_properties(),  # SASL/SCRAM login
)


def parse_start_date(value):
    """Parse an ISO-8601 date/datetime and return its UTC epoch timestamp."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def get_record(timestamp, num_customers:int, num_merchants:int):
    return {"id": str(uuid1())
            , "merchant_id": "m" + str(randint(0, num_merchants))
            , "customer_id": "c" + str(randint(0, num_customers))
            , "amount": gauss(100.0, 25.0)
            , "category": choice(["pos", "net", "app"])
            , "timestamp": int(timestamp)
            , 'timestamp_ts': datetime.fromtimestamp(timestamp).isoformat()}


def main():
    parser = argparse.ArgumentParser(description="Produce synthetic transactions to Kafka.")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Initial event timestamp in ISO-8601 format (for example, 2026-09-03T00:00:00Z).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=60.0,
        help="Average transactions per hour (default: 60.0).",
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=10,
        help="Number of customers (default: 10).",
    )
    parser.add_argument(
        "--num-merchants",
        type=int,
        default=10,
        help="Number of merchants (default: 10).",
    )
    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate must be greater than zero")

    num_merchants = args.num_merchants
    num_customers = args.num_customers
    simulated_timestamp = parse_start_date(args.start_date)
    index = 0

    try:
        while True:
            # Apply backpressure whenever the replay timeline is ahead of the
            # wall clock. This also handles a start date in the future.
            wait_time = simulated_timestamp - time()
            if wait_time > 0:
                sleep(wait_time)

            record = get_record(simulated_timestamp, num_customers, num_merchants)
            print(index, ": ", record)
            producer.send(topic, key=None, value=json.dumps(record).encode("utf-8"))

            # Vary each interval around the requested hourly rate. The same
            # interval advances event time, creating an uneven timestamp spread.
            interval = max(0.001, uniform(0.5, 1.5) * 3600.0 / args.rate)
            simulated_timestamp += interval
            index += 1
    except KeyboardInterrupt:
        print("Stopping producer...")

    producer.flush()
    print(f"Complete!!! All messages are published. Count: {index}")


if __name__ == "__main__":
    main()
