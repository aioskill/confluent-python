"""Run a slow PyFlink job so it can be inspected in the Flink Web UI.

Start the standalone cluster first, then submit this script from the client
machine with the JobManager REST endpoint:

Run locally (embedded Flink mini cluster, from this script's directory)::

    python ex02_submit_job.py

Submit to the cluster: copy this file to sa01 under the home directory and run
the command below:

scp ex02_submit_job.py ubuntu@sa01:~/

/app/flink/bin/flink run \
    -m sa03:8967 \
    --detached \
    -py ~/ex02_submit_job.py

Use ``-m sa01:8081`` when the JobManager is running on ``sa01``.

Open http://localhost:8081 while the job is running. Stop the cluster with
/app/flink/bin/stop-cluster.sh when finished.
"""

import time

from pyflink.common import Types
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment


# Purpose: run a deliberately slow batch job for Web UI inspection.
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)


def slow_double(value):
    time.sleep(1)
    return value * 2


(env.from_collection(range(60))
    .map(slow_double, output_type=Types.INT())
    .print())

env.execute("PyFlink Web UI Demo")
