"""Top 10 movies by average rating, computed with PyFlink DataStream operators.

Run from the repository root::

    python ex10_movielens.py

The answer is computed entirely in the stream graph (map / filter / key_by /
reduce / union) and printed as a single JSON string.  No Python-side
post-processing is used.

Dataset is expected at ``pyflink/data/ml-latest-small`` (created by ``setup_pyflink.sh``) (movies.csv, ratings.csv);
override with ``--data-path``.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat

DATA_PATH = Path("~/data/ml-latest-small")

MOVIE_TYPE = Types.TUPLE([Types.INT(), Types.STRING(), Types.STRING()])       # (movieId, title, genres)
RATING_TYPE = Types.TUPLE([Types.INT(), Types.INT(), Types.FLOAT()])          # (userId, movieId, rating)
STAT_TYPE = Types.TUPLE([Types.INT(), Types.FLOAT(), Types.INT()])            # (movieId, avg, count)
STAT_LIST_TYPE = Types.TUPLE([Types.INT(), Types.LIST(STAT_TYPE)])
ENRICHED_TYPE = Types.TUPLE([Types.INT(), Types.STRING(), Types.FLOAT(), Types.INT()])  # (movieId,title,avg,count)
ENRICHED_LIST_TYPE = Types.TUPLE([Types.INT(), Types.LIST(ENRICHED_TYPE)])


def default_data_path() -> Path:
    candidates = (
        DATA_PATH.expanduser(),
        Path(__file__).resolve().parents[1] / "data" / "ml-latest-small",
    )
    for candidate in candidates:
        if (candidate / "movies.csv").is_file() and (candidate / "ratings.csv").is_file():
            return candidate
    return DATA_PATH.expanduser()


def read_text_file(env, path, name):
    return env.from_source(
        FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                            str(path)).process_static_file_set().build(),
        WatermarkStrategy.no_watermarks(),
        name,
    )


def read_movies(env, path):
    def parse(row):
        return int(row[0]), row[1], row[2]

    return (
        read_text_file(env, path / "movies.csv", "movies_csv")
        .map(lambda line: next(csv.reader([line])), output_type=Types.LIST(Types.STRING()))
        .filter(lambda row: row[0] != "movieId")
        .map(parse, output_type=MOVIE_TYPE)
    )


def read_ratings(env, path):
    def parse(row):
        return int(row[0]), int(row[1]), float(row[2])

    return (
        read_text_file(env, path / "ratings.csv", "ratings_csv")
        .map(lambda line: next(csv.reader([line])), output_type=Types.LIST(Types.STRING()))
        .filter(lambda row: row[0] != "userId")
        .map(parse, output_type=RATING_TYPE)
    )


def solve(data_path: Path | str) -> None:
    path = Path(data_path).expanduser()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(os.cpu_count() or 1)

    movies = read_movies(env, path)
    ratings = read_ratings(env, path)

    # Step 1: average rating per movie, from the ratings file.
    stats = (
        ratings
        .map(lambda r: (r[1], r[2], 1), output_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.INT()]))
        .key_by(lambda r: r[0], key_type=Types.INT())
        .reduce(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))
        .map(lambda r: (r[0], r[1] / r[2], r[2]), output_type=STAT_TYPE)
    )

    # Step 2: keep the top 10 by average rating (tie-break: count, then id).
    top10 = (
        stats
        .map(lambda r: (0, [r]), output_type=STAT_LIST_TYPE)
        .key_by(lambda r: r[0], key_type=Types.INT())
        .reduce(lambda a, b: (0, sorted(a[1] + b[1], key=lambda r: (-r[1], -r[2], r[0]))[:10]))
        .flat_map(lambda r: iter(r[1]), output_type=STAT_TYPE)
    )

    # Step 3: join the top 10 with movies to add the title.
    # A union + key_by + reduce merge: movies carry the title, stats carry avg/count.
    movie_side = movies.map(lambda m: (m[0], m[1], 0.0, 0), output_type=ENRICHED_TYPE)
    stat_side = top10.map(lambda r: (r[0], "", r[1], r[2]), output_type=ENRICHED_TYPE)
    result = (
        movie_side.union(stat_side)
        .key_by(lambda r: r[0], key_type=Types.INT())
        .reduce(lambda a, b: (a[0], a[1] or b[1], a[2] or b[2], a[3] or b[3]))
        .filter(lambda r: r[1] != "" and r[2] > 0)
        .map(lambda r: (0, [r]), output_type=ENRICHED_LIST_TYPE)
        .key_by(lambda r: r[0], key_type=Types.INT())
        .reduce(lambda a, b: (0, a[1] + b[1]))
        .map(lambda r: json.dumps(
            [{"movieId": m[0], "title": m[1], "average_rating": round(m[2], 3), "rating_count": m[3]}
             for m in sorted(r[1], key=lambda m: (-m[2], -m[3], m[0]))]),
            output_type=Types.STRING())
    )

    with result.execute_and_collect("top10_movies") as records:
        for value in records:
            print(value, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-path", type=Path, default=default_data_path(), help="MovieLens directory")
    args = parser.parse_args()
    solve(args.data_path)


if __name__ == "__main__":
    main()
