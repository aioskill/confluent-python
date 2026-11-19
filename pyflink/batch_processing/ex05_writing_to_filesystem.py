from pathlib import Path
import logging
import re
import sys

from pyflink.common import Types, WatermarkStrategy, Encoder
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, \
    RollingPolicy, BucketAssigner

# Purpose: extend file-based word count with parallelism and rolling FileSink output.

def word_count(input_path:str, output_path:str):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(4)

    current_parallelism = env.get_parallelism()
    print(f"Current parallelism: {current_parallelism}")


    ds = env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                   input_path)
        .process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    print(type(ds))

    # compute word count
    ds = (ds
            .flat_map(lambda line: re.compile(r"\s+").split(line.lower()))
            .filter(lambda r: not re.compile(r"\W+").search(r) and len(r)>3)
            .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
            .key_by(lambda i: i[0])
            .reduce(lambda i, j: (i[0], i[1] + j[1]))
        )
    # ds.print()


    """
       Creates a BucketAssigner that does not perform any bucketing of files. All files are
       written to the base path.
    """
    bucket_assigner1 = BucketAssigner.base_path_bucket_assigner()


    """
        Creates a BucketAssigner that assigns to buckets based on current system time.

        It will create directories of the following form: /{basePath}/{dateTimePath}/}.
        The basePath is the path that was specified as a base path when creating the new bucket.
        The dateTimePath is determined based on the current system time and the user provided format
        string.

        The Java DateTimeFormatter is used to derive a date string from the current system time and
        the date format string. The default format string is "yyyy-MM-dd--HH" so the rolling files
        will have a granularity of hours.

        :param format_str: The format string used to determine the bucket id.
        :param timezone_id: The timezone id, either an abbreviation such as "PST", a full name
                            such as "America/Los_Angeles", or a custom timezone_id such as
                            "GMT-08:00". Th e default time zone will b used if it's None.
    """
    bucket_assigner2 = BucketAssigner.date_time_bucket_assigner("yyyy-MM-dd-HH-mm", "UTC")

    output_file_config = (OutputFileConfig
                            .builder()
                            .with_part_prefix("part")
                            .with_part_suffix(".txt")
                            .build())

    ds.set_parallelism(1).sink_to(
        sink=FileSink.for_row_format(
            base_path=output_path,
            encoder=Encoder.simple_string_encoder())
        .with_output_file_config(output_file_config)
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .with_bucket_assigner(bucket_assigner2)
        .build()
    )
    env.execute()


    print("Job is complete")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    # https://www.gutenberg.org/cache/epub/76357/pg76357.txt
    input_path_ = str(Path(__file__).resolve().parents[1] / "data" / "sample.txt")
    import argparse

    cli = argparse.ArgumentParser()
    cli.add_argument("--output", default="/tmp/flink-output", help="Output directory (file:// or s3://)")
    output_path_ = cli.parse_args().output
    word_count(input_path_, output_path_)
