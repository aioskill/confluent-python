from pathlib import Path
import logging
import re
import sys

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

# Purpose: read a bounded text file with FileSource and count normalized words.

def word_count(input_path:str):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)
    ds = env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                   input_path)
        .process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    # compute word count
    ds = (ds
            .flat_map(lambda line: re.compile(r"\s+").split(line.lower()))
            .filter(lambda r: not re.compile(r"\W+").search(r) and len(r)>3)
            .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
            .key_by(lambda i: i[0])
            .reduce(lambda i, j: (i[0], i[1] + j[1]))
        )
    ds.print()
    env.execute()
    print("Job is complete")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    # https://www.gutenberg.org/cache/epub/76357/pg76357.txt
    input_path_ = str(Path(__file__).resolve().parents[1] / "data" / "sample.txt")
    word_count(input_path_)
