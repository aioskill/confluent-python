"""Read a bounded text file, uppercase each line, and write it back with FileSink.

The input and output can be local paths (``/tmp/sample.txt``, ``file:///tmp/out/``)
or S3 paths (``s3://my-bucket/sample.txt``). On a cluster with more than one
machine, use S3: local paths must exist on every TaskManager.

S3 needs the ``flink-s3-fs-hadoop`` plugin in Flink's ``plugins/`` directory and the
S3 credentials in the Flink configuration.

Local run::

    echo "hello flink" > /tmp/sample.txt
    python ex06_read_write_files.py --input /tmp/sample.txt --output /tmp/flink-output/

Submission to a running JobManager (the paths must be visible to every TaskManager)::

    /app/flink/bin/flink run -m sa03:8967 --detached \\
       -py ex06_read_write_files.py \\
       --input s3://my-bucket/sample.txt \\
       --output s3://my-bucket/flink-output/

The output is a directory containing Flink part files. Do not use an output
directory that contains committed results from an earlier run unless the
downstream process is designed for repeated part files.
"""

import argparse

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (
    Encoder,
    FileSink,
    FileSource,
    OutputFileConfig,
    StreamFormat,
    BucketAssigner
)


# Purpose: demonstrate bounded file read, DataStream transformation, and FileSink output.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        help="Input text file (local path or s3:// path)",
    )
    parser.add_argument(
        "--output",
        help="Output directory (local path or s3:// path)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Job parallelism (default: 1)",
    )
    args = parser.parse_args()

    if args.parallelism < 1:
        parser.error("--parallelism must be at least 1")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(args.parallelism)

    lines = env.from_source(
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            args.input,
        ).process_static_file_set().build(),
        WatermarkStrategy.no_watermarks(),
        "text_file_source",
    )
    transformed_lines = lines.map(
        lambda line: line.upper(),
        output_type=Types.STRING(),
    )

    output_file_config = (OutputFileConfig.builder()
                          .with_part_prefix("part")
                          .with_part_suffix(".txt")
                          .build())
    sink = (FileSink
            .for_row_format(args.output, Encoder.simple_string_encoder("UTF-8"))
            .with_output_file_config(output_file_config)
            .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())
            .build())

    transformed_lines.sink_to(sink)
    env.execute("PyFlink Read Write Files")


if __name__ == "__main__":
    main()
