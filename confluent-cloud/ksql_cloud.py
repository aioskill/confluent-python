#!/usr/bin/env python3
"""Run ksqlDB statements on a Confluent Cloud ksqlDB cluster through its REST API.

The endpoint and the API key come from KSQLDB_ENDPOINT, KSQLDB_KEY and KSQLDB_SECRET
(environment or the .env file next to this script). Statements are separated by ';'.

    python3 ksql_cloud.py "SHOW STREAMS;"
    python3 ksql_cloud.py --file statements.sql
    python3 ksql_cloud.py - <<'SQL'
    SHOW STREAMS;
    SQL

SELECT and PRINT statements use the /query endpoint and show the rows. A push query
(EMIT CHANGES) needs a LIMIT, or --timeout, to end. Everything else goes to the /ksql
endpoint. The queries read topics from the start (auto.offset.reset=earliest), unlike
the SET command of the ksqlDB editor, which this script does not support.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import socket
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from client_config import ksqldb_config

CONTENT_TYPE = "application/vnd.ksql.v1+json"


def split_statements(text: str) -> list[str]:
    """Split on ';' outside single-quoted strings and drop -- comments."""
    text = re.sub(r"--[^\n]*", "", text)
    statements, current, quoted = [], [], False
    for char in text:
        if char == "'":
            quoted = not quoted
        if char == ";" and not quoted:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement + ";")
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        statements.append(tail + ";")
    return statements


def post(path: str, statement: str, timeout: float, partial: bool = False) -> tuple[bytes, bool]:
    """POST a statement. Returns (body, timed_out).

    With partial=True the response is read until it ends or until `timeout` seconds have
    passed in total (a push query without LIMIT never ends); what was read is returned."""
    config = ksqldb_config()
    token = base64.b64encode(f"{config['key']}:{config['secret']}".encode()).decode()
    body = json.dumps(
        {"ksql": statement, "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}}
    ).encode()
    request = Request(
        config["endpoint"] + path,
        data=body,
        headers={"Authorization": f"Basic {token}", "Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE},
        method="POST",
    )
    deadline = time.monotonic() + timeout
    chunks: list[bytes] = []
    try:
        with urlopen(request, timeout=timeout) as response:
            if not partial:
                return response.read(), False
            while True:
                try:
                    response.fp.raw._sock.settimeout(max(deadline - time.monotonic(), 0.1))
                    chunk = response.read1(65536)
                except (TimeoutError, socket.timeout):
                    return b"".join(chunks), True
                if not chunk:
                    return b"".join(chunks), False
                chunks.append(chunk)
                if time.monotonic() > deadline:
                    return b"".join(chunks), True
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(detail).get("message", detail)
        except ValueError:
            pass
        raise RuntimeError(f"HTTP {error.code}: {detail}") from error
    except (URLError, TimeoutError, socket.timeout) as error:
        raise RuntimeError(f"Could not reach ksqlDB: {error}") from error


def table(columns: list[str], rows: list[list]) -> None:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    print(" | ".join(c.ljust(w) for c, w in zip(columns, widths)))
    print("-+-".join("-" * w for w in widths))
    for row in cells:
        print(" | ".join(v.ljust(w) for v, w in zip(row, widths)))


def show_statement_result(result: dict) -> None:
    kind = result.get("@type")
    if kind == "currentStatus":
        status = result["commandStatus"]
        print(f"{status['status']}: {status['message']}")
    elif kind in ("streams", "tables"):
        table(["Name", "Topic", "Key format", "Value format"],
              [[r["name"], r["topic"], r["keyFormat"], r["valueFormat"]] for r in result[kind]])
    elif kind == "queries":
        table(["Query ID", "Status", "Sinks", "Query string"],
              [[r["id"], ", ".join(f"{k} x{v}" for k, v in r.get("statusCount", {}).items()),
                ",".join(r.get("sinks", [])), r["queryString"][:60]]
               for r in result["queries"]])
    elif kind == "kafka_topics":
        table(["Topic", "Partitions", "Replicas"],
              [[r["name"], len(r["replicaInfo"]), r["replicaInfo"][0] if r["replicaInfo"] else ""]
               for r in result["topics"]])
    elif kind == "sourceDescription":
        source = result["sourceDescription"]
        print(f"{source['type']} {source['name']} -> topic {source['topic']}")
        table(["Field", "Type", "Key"],
              [[f["name"], f["schema"]["type"], f.get("type", "")] for f in source["fields"]])
        for host, stats in (source.get("statisticsMap") or {}).items():
            print(f"Statistics: {stats}")
        for name in ("readQueries", "writeQueries"):
            for query in source.get(name, []):
                print(f"{name[:-7].capitalize()} query: {query['id']}")
    elif kind == "queryDescription":
        print(result["queryDescription"].get("statementText", ""))
        print(result["queryDescription"].get("topologyDescription", ""))
    else:
        print(json.dumps(result, indent=2))


def run_query(statement: str, timeout: float) -> None:
    body, timed_out = post("/query", statement, timeout, partial=True)
    raw = body.decode("utf-8")
    if re.match(r"(?is)\s*print\b", statement):  # PRINT returns plain text lines
        print(raw.rstrip())
        if timed_out:
            print(f"Stopped after {timeout:g} seconds (--timeout); the topic had no more rows to print.")
        return
    if timed_out:  # the response is a JSON array that was cut: close it
        raw = raw.rstrip(", \n")
        if raw and not raw.endswith("]"):
            raw += "]"
    items = json.loads(raw) if raw.strip() else []
    columns: list[str] = []
    rows: list[list] = []
    for item in items:
        if "header" in item:
            columns = re.findall(r"`([^`]+)`", item["header"]["schema"]) or ["VALUE"]
        elif "row" in item:
            rows.append(item["row"]["columns"])
        elif "errorMessage" in item:
            raise RuntimeError(item["errorMessage"].get("message", str(item["errorMessage"])))
        elif "finalMessage" in item:
            pass
    if columns:
        table(columns, rows)
        print(f"({len(rows)} row{'' if len(rows) == 1 else 's'})")
    if timed_out:
        print(f"Stopped after {timeout:g} seconds (--timeout); the query had not ended.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("statements", nargs="*", help="ksqlDB statements separated by ';', or - to read them from standard input")
    parser.add_argument("--file", help="read the statements from a file")
    parser.add_argument("--timeout", type=float, default=60, help="seconds to wait for a response")
    args = parser.parse_args()

    text = " ".join(args.statements)
    if text.strip() == "-":  # read the statements from standard input
        text = sys.stdin.read()
    if args.file:
        with open(args.file, encoding="utf-8") as handle:
            text += "\n" + handle.read()
    statements = split_statements(text)
    if not statements:
        parser.error("give a statement or --file")

    for statement in statements:
        print(f"ksql> {statement}", flush=True)
        try:
            if re.match(r"(?is)\s*(select|print)\b", statement):
                run_query(statement, args.timeout)
            else:
                for result in json.loads(post("/ksql", statement, args.timeout)[0]):
                    show_statement_result(result)
        except RuntimeError as error:
            print(f"Error: {error}", file=sys.stderr)
            return 1
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
