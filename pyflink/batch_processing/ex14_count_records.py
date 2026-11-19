from pathlib import Path
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Configuration
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

input_path = str(Path(__file__).resolve().parents[1] / "data" / "stocks.csv")

class FinalRecordCount(KeyedProcessFunction):
    """Keep the count in managed state and emit it once at end-of-input."""

    def open(self, runtime_context):
        self.count = runtime_context.get_state(
            ValueStateDescriptor("record-count", Types.LONG())
        )

    def process_element(self, value, ctx):
        self.count.update((self.count.value() or 0) + 1)
        # All records use timestamp zero. A bounded source emits the final
        # maximum watermark, which fires this timer after the last record.
        ctx.timer_service().register_event_time_timer(0)

    def on_timer(self, timestamp, ctx):
        yield f"records processed: {self.count.value() or 0}"


class ZeroTimestampAssigner(TimestampAssigner):
    """Assign timestamp zero to every record in this bounded example."""

    def extract_timestamp(self, value, record_timestamp):
        return 0


def count_records(data_stream):
    """Return a stream containing one final total for the input records."""
    return (
        data_stream
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps()
            .with_timestamp_assigner(ZeroTimestampAssigner())
        )
        .key_by(lambda _: 0, key_type=Types.INT())
        .process(FinalRecordCount(), output_type=Types.STRING())
    )

config = Configuration()
config.set_string("rest.port", "60000")
config.set_string("rest.bind-address", "127.0.0.1")

env = StreamExecutionEnvironment.get_execution_environment(config)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

# Create a FileSource reading line by line
file_source = FileSource.for_record_stream_format(
    StreamFormat.text_line_format(),
    input_path
).build()

# Bind source to DataStream
ds = env.from_source(file_source,
                     watermark_strategy=WatermarkStrategy.no_watermarks(),
                     source_name="File Source")

ds = ds.set_parallelism(36)
count_records(ds).print()
print("Execution plan", env.get_execution_plan())
env.execute("Inspect DataStream Partitions")
