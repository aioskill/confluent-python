"""RocksDB managed keyed state with checkpoints written to a checkpoint directory.

Run locally::

    python ex12_rocksdb_state_checkpoints.py

or submit to a running JobManager::

    /app/flink/bin/flink run -m sa03:8967 --detached \\
       -py ex12_rocksdb_state_checkpoints.py

RocksDB stores keyed state on the TaskManager's local disk; the checkpoint
snapshots are written to the directory configured in ``state.checkpoints.dir``.
``execution.checkpointing.storage=filesystem`` already uses
``state.checkpoints.dir``, so only the directory URI changes.

``CHECKPOINT_DIR`` and ``SAVEPOINT_DIR`` below are local directories, which is
fine when everything runs on one machine. For a cluster with several machines
change them to S3, for example ``s3://my-bucket/flink/checkpoints`` (this needs
the ``flink-s3-fs-hadoop`` plugin and S3 credentials in the Flink configuration).
The user that runs the Flink job needs read/write access to the location.

A savepoint (a manually triggered snapshot) can be taken before the job ends:

* use ``env.execute_async()`` to get a ``JobClient`` (``execute_and_collect``
  blocks until the job ends and gives no handle);
* call ``job_client.trigger_savepoint(savepoint_dir)`` while the job is still
  running, then wait for the job to finish.

The job must be RUNNING when the savepoint is triggered, so this finite example
throttles the source to keep the job alive long enough for the trigger.

PyFlink 2.3's Python ``JobClient.trigger_savepoint`` still calls the removed
one-arg Java ``triggerSavepoint(String)``; the current Java signature also takes
a ``SavepointFormatType``.  This script calls the raw Java method with the new
signature (see ``_trigger_savepoint``).
"""

from __future__ import annotations

import json
import time

from pyflink.common import Configuration, Types
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.java_gateway import get_gateway

CHECKPOINT_DIR = "file:///tmp/pyflink-training/checkpoints"
SAVEPOINT_DIR = "file:///tmp/pyflink-training/savepoints"

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
    """Create an environment with RocksDB and filesystem checkpointing."""
    configuration = Configuration()
    configuration.set_string("state.backend.type", "rocksdb")
    # RocksDB local storage expects filesystem paths, not URI strings.
    configuration.set_string("state.backend.rocksdb.localdir", "/tmp/pyflink-training/rocksdb")
    # Checkpoint snapshots go to CHECKPOINT_DIR; "filesystem" storage reads this URI.
    configuration.set_string("state.checkpoints.dir", CHECKPOINT_DIR)
    configuration.set_string("state.savepoints.dir", SAVEPOINT_DIR)
    configuration.set_string("execution.checkpointing.interval", "1s")
    configuration.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    configuration.set_string("execution.checkpointing.storage", "filesystem")

    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)
    env.enable_checkpointing(1000)
    return env


def _keep_alive(event):
    """Slow the tiny finite source down so the job is still running when the
    savepoint is triggered.  Remove this for a real streaming source."""
    time.sleep(1)
    return event


def _trigger_savepoint(job_client, directory: str) -> str:
    """Trigger a savepoint on the raw Java ``JobClient``.

    PyFlink 2.3's Python ``trigger_savepoint`` calls the removed one-arg
    ``JobClient.triggerSavepoint(String)``; the current Java signature also
    takes a ``SavepointFormatType``, so call it directly.
    """
    format_type = get_gateway().jvm.org.apache.flink.core.execution.SavepointFormatType.CANONICAL
    return job_client._j_job_client.triggerSavepoint(directory, format_type).get()


def main() -> None:
    global CHECKPOINT_DIR, SAVEPOINT_DIR
    import argparse

    cli = argparse.ArgumentParser()
    cli.add_argument("--checkpoint-dir", default=CHECKPOINT_DIR, help="file:// or s3:// directory")
    cli.add_argument("--savepoint-dir", default=SAVEPOINT_DIR, help="file:// or s3:// directory")
    args = cli.parse_args()
    CHECKPOINT_DIR, SAVEPOINT_DIR = args.checkpoint_dir, args.savepoint_dir

    env = build_environment()
    events = (
        env.from_collection(EVENTS, type_info=EVENT_TYPE)
        .map(_keep_alive, output_type=EVENT_TYPE)
    )
    tracked = (
        events
        .key_by(lambda event: event[0], key_type=Types.STRING())
        .process(TrackCustomerState(), output_type=Types.STRING())
    )
    # Sink to the TaskManager stdout/log; execute_async has no collect iterator.
    tracked.print()

    print("state backend: rocksdb", flush=True)
    print(f"checkpoints dir: {CHECKPOINT_DIR}", flush=True)
    print(f"savepoints dir: {SAVEPOINT_DIR}", flush=True)
    print("checkpoint interval: 1s", flush=True)

    job_client = env.execute_async("RocksDB Managed Keyed State (checkpoints)")
    time.sleep(3)  # wait for the job to reach RUNNING state
    savepoint = _trigger_savepoint(job_client, SAVEPOINT_DIR)
    print(f"savepoint taken at: {savepoint}", flush=True)
    job_client.get_job_execution_result().result()
    print("job finished", flush=True)


if __name__ == "__main__":
    main()
