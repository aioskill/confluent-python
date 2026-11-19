"""Configure RocksDB and use managed keyed state in PyFlink.

This is a small bounded example so it can run locally without Kafka::

    python ex11_rocksdb_state.py

``state.backend.type=rocksdb`` makes Flink store keyed state in its embedded
RocksDB backend.  Checkpointing is configured separately: RocksDB is the local
state implementation, while the checkpoint directory is the durable recovery
location.  In a cluster, replace the local ``file:///tmp`` path with shared
storage such as S3.

To write checkpoints to S3:

* set ``state.checkpoints.dir`` to ``s3://<bucket>/<path>`` and keep
  ``execution.checkpointing.storage=filesystem`` (the cluster needs the
  ``flink-s3-fs-hadoop`` plugin and S3 credentials);
* keep ``state.backend.rocksdb.localdir`` on the local disk (RocksDB data is
  always local; only checkpoint snapshots go to S3).
"""

from __future__ import annotations

import json

from pyflink.common import Configuration, Types
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor


EVENTS = [
    ("alice", "NEW"),
    ("bob", "NEW"),
    ("alice", "PAID"),
    ("alice", "SHIPPED"),
    ("bob", "CANCELLED"),
    ("alice", "DELIVERED"),
]
EVENT_TYPE = Types.TUPLE([Types.STRING(), Types.STRING()])


class TrackCustomerState(KeyedProcessFunction):
    """Keep the latest status and per-status counts for each customer key."""

    def open(self, runtime_context):
        self.last_status = runtime_context.get_state(
            ValueStateDescriptor("last-status", Types.STRING())
        )
        self.status_counts = runtime_context.get_map_state(
            MapStateDescriptor("status-counts", Types.STRING(), Types.INT())
        )

    def process_element(self, event, _context):
        customer, status = event
        previous_status = self.last_status.value()
        old_count = self.status_counts.get(status) or 0
        self.status_counts.put(status, old_count + 1)
        self.last_status.update(status)

        # The state is read after the update to make the persisted state
        # visible in the output for every input event.
        counts = {
            state_status: count
            for state_status, count in self.status_counts.items()
        }
        yield json.dumps({
            "customer": customer,
            "status": status,
            "previous_status": previous_status,
            "status_counts": counts,
        }, sort_keys=True)


def build_environment() -> StreamExecutionEnvironment:
    """Create an environment with RocksDB and checkpointing configured."""
    configuration = Configuration()
    configuration.set_string("state.backend.type", "rocksdb")
    # RocksDB local storage expects filesystem paths, not URI strings.
    configuration.set_string("state.backend.rocksdb.localdir", "/tmp/pyflink-training/rocksdb")
    configuration.set_string("state.checkpoints.dir", "file:///tmp/pyflink-training/checkpoints")
    # S3 variant: swap the line above for an S3 URI and keep the
    # "filesystem" storage setting below, e.g.
    #     configuration.set_string(
    #         "state.checkpoints.dir",
    #         "s3://<bucket>/pyflink-training/checkpoints")
    configuration.set_string("execution.checkpointing.interval", "1s")
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")

    # Only other option is jobmanager (checkpoint kept in job manager's heap)
    configuration.set_string("execution.checkpointing.storage", "filesystem")

    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)
    env.enable_checkpointing(1000)
    return env


def main() -> None:
    env = build_environment()
    events = env.from_collection(EVENTS, type_info=EVENT_TYPE)
    tracked = (
        events
        .key_by(lambda event: event[0], key_type=Types.STRING())
        .process(TrackCustomerState(), output_type=Types.STRING())
    )

    print("state backend: rocksdb", flush=True)
    print("checkpoint interval: 1s", flush=True)
    for result in tracked.execute_and_collect("RocksDB Managed Keyed State"):
        print(result, flush=True)


if __name__ == "__main__":
    main()
