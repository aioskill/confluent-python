from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

input_path = str(Path(__file__).resolve().parents[1] / "data" / "sample.txt")

class SplitInspector(ProcessFunction):
    def open(self, ctx):
        # ProcessFunction.open receives the RuntimeContext directly.
        # There is no get_runtime_context() method on that context.
        self.num_subtasks = ctx.get_number_of_parallel_subtasks()
        # The specific subtask index for this worker (0 to num_subtasks - 1)
        self.subtask_index = ctx.get_index_of_this_subtask()

    def process_element(self, value, ctx):
        yield f"Subtask {self.subtask_index}/{self.num_subtasks} processing: {value}"


class ZeroTimestampAssigner(TimestampAssigner):
    """Assign timestamp zero to every record in this bounded example."""

    def extract_timestamp(self, value, record_timestamp):
        return 0


class FinalRecordCount(KeyedProcessFunction):
    """Count records and emit once when the bounded stream finishes."""

    def open(self, runtime_context):
        self.count = runtime_context.get_state(
            ValueStateDescriptor("record-count", Types.LONG())
        )

    def process_element(self, value, ctx):
        self.count.update((self.count.value() or 0) + 1)
        # Timestamp zero is used for every input. The bounded source emits a
        # final max watermark, which fires this timer once at end-of-input.
        ctx.timer_service().register_event_time_timer(0)

    def on_timer(self, timestamp, ctx):
        yield f"records processed: {self.count.value() or 0}"


def count_records(data_stream):
    """Return a one-record stream containing the final input count."""
    return (
        data_stream
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
            .with_timestamp_assigner(ZeroTimestampAssigner())
        )
        .key_by(lambda _: 0, key_type=Types.INT())
        .process(FinalRecordCount(), output_type=Types.STRING())
    )


env = StreamExecutionEnvironment.get_execution_environment()


# Create a FileSource reading line by line
file_source = FileSource.for_record_stream_format(
    StreamFormat.text_line_format(),
    input_path
).build()

# Bind source to DataStream
ds = env.from_source(file_source,
                     watermark_strategy=WatermarkStrategy.no_watermarks(),
                     source_name="File Source")

# Set parallelism explicitly to see the stream split across partitions.
ds.process(SplitInspector(), output_type=Types.STRING()).set_parallelism(4).print()
count_records(ds).print()

env.execute("Inspect DataStream Partitions")
