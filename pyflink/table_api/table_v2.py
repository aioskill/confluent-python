from pathlib import Path
import logging
import sys

from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes)
from pyflink.table import expressions as F
from pyflink.table.udf import udtf


def word_count(input_path):
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    # write all the data to one file
    t_env.get_config().set("parallelism.default", "1")

    # define the source
    t_env.create_temporary_table(
        'source',
        TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .build())
        .option('path', input_path)
        .format('raw')
        .build())

    # define the sink
    print("Printing result to stdout.")
    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .column('count', DataTypes.BIGINT())
                .build())
        .build())

    @udtf(result_types=[DataTypes.STRING()])
    def split(line: Row):
        for s in line[0].split():
            yield Row(s)

    # compute word count
    out = (t_env
        .from_path('source')
        .flat_map(split).alias('word')
        .select(F.col("word").lower_case.alias("word"))
        .where(F.col("word").char_length > 3)
        .group_by(F.col('word'))
        .select(F.col('word'), F.lit(1).sum.alias("sum"))
        .order_by(F.col("sum").desc)
        .where(F.col("sum")>1)
        .limit(100)
    )
    schema = out.get_schema()
    print("Schema: ", schema)
    out.execute_insert('sink').wait()
    print("Job is complete")

if __name__ == '__main__':
    input_path_ = str(Path(__file__).resolve().parents[1] / "data" / "sample.txt")
    word_count(input_path_)