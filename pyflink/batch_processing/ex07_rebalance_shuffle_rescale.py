"""Compare DataStream rebalance, shuffle, and rescale partitioning.

  - rebalance() — round-robin distribution across all downstream subtasks
  - shuffle() — random distribution
  - rescale() — round-robin distribution within connected downstream subsets

Run from the repository root: (do not use IDE debugger).

    python ex07_rebalance_shuffle_rescale.py

The output prefix printed by Flink (for example, ``1>`` or ``4>``) identifies
the downstream subtask that received the record. ``shuffle`` is random, so its
distribution can differ between runs.


How to interpret the output.

Each line has this structure:

  <partitioning>: <operator> (<subtask>/<parallelism>)#<attempt>, value=<record>

  For example:

  shuffle: Map (4/4)#0, value=5

  means value 5 was processed by subtask 4 of a Map operator with four parallel subtasks.
  #0 is the first execution attempt.

  ### Shuffle

  shuffle: Map (4/4)#0, value=5
  shuffle: Map (4/4)#0, value=15
  ...

  shuffle() randomly sends each record to one of the four downstream subtasks.
  The distribution and order can change between runs.

  ### Rescale

  The output is:
  - Subtask 1: 1, 5, 9, 13
  - Subtask 2: 3, 7, 11, 15
  - Subtask 3: 2, 6, 10, 14
  - Subtask 4: 4, 8, 12, 16

  With two upstream subtasks and four downstream subtasks, each upstream subtask connects only to a
  subset of downstream subtasks. This is why rescale() has locality and produces this predictable grouping.

  ### Rebalance

  The records are evenly distributed:

  - Subtask 1: 2, 10, 7, 15
  - Subtask 2: 4, 12, 1, 9
  - Subtask 3: 6, 14, 3, 11
  - Subtask 4: 8, 16, 5, 13

  rebalance() uses round-robin distribution across all downstream subtasks.
  The output order is not globally ordered because the four subtasks run concurrently.

  The key point is: the prefix identifies the downstream subtask that processed the record;
  the printed line order does not represent the original processing order.

"""

from pyflink.common import Types
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction


# Purpose: demonstrate how records are distributed between downstream subtasks.
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

# The collection source is non-parallel and must remain at parallelism 1.
# The identity map creates two upstream subtasks, which feed four downstream
# subtasks in each branch and makes the partitioning differences visible.
source = (env.from_collection(range(1, 17))
          .map(lambda value: value, output_type=Types.INT())
          .set_parallelism(2))


class LabelWithSubtask(MapFunction):
    """Add the receiving downstream subtask to each record."""

    def __init__(self, branch):
        self.branch = branch
        self.task_name = None

    def open(self, runtime_context):
        self.task_name = runtime_context.get_task_name_with_subtasks()

    def map(self, value):
        return f"{self.branch}: {self.task_name}, value={value}"


for branch_name, partitioned_stream in (
    ("rebalance", source.rebalance()),
    ("shuffle", source.shuffle()),
    ("rescale", source.rescale()),
):
    (partitioned_stream
        .map(
            LabelWithSubtask(branch_name),
            output_type=Types.STRING(),
        )
        .set_parallelism(4)
        .print())

env.execute("Rebalance Shuffle Rescale")
