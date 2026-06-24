#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import random
import socket
import statistics
import sys
import time


class RmdbClient:
    def __init__(self, host: str, port: int, timeout: float = 30.0) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)

    def execute(self, sql: str) -> str:
        payload = sql if sql.endswith(";") else sql + ";"
        self.sock.sendall(payload.encode("utf-8"))
        data = self.sock.recv(1024 * 1024)
        if not data:
            raise RuntimeError(f"empty response for SQL: {sql}")
        return data.decode("utf-8", errors="replace")

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def execute_checked(client: RmdbClient, sql: str) -> str:
    response = client.execute(sql)
    lowered = response.strip().lower()
    if lowered.startswith("failure") or lowered.startswith("abort"):
        raise RuntimeError(f"SQL failed: {sql}\nresponse: {response}")
    return response


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = round((len(ordered) - 1) * pct / 100.0)
    return ordered[min(max(idx, 0), len(ordered) - 1)]


def time_phase(name: str, fn) -> dict:
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    return {"phase": name, "seconds": elapsed, "result": result}


def bench_insert(client: RmdbClient, table: str, rows: int) -> dict:
    start = time.perf_counter()
    for i in range(rows):
        if table == "bench_a":
            sql = (
                f"insert into bench_a values ({i}, {i % 97}, {i % 1000}, 'a{i % 10000}');"
            )
        else:
            sql = (
                f"insert into bench_b values ({i}, {i}, {i % 2000}, 'b{i % 10000}');"
            )
        execute_checked(client, sql)
    elapsed = time.perf_counter() - start
    return {"rows": rows, "rows_per_sec": rows / elapsed if elapsed > 0 else 0.0}


def bench_queries(client: RmdbClient, sqls: list[str]) -> dict:
    latencies: list[float] = []
    for sql in sqls:
        q_start = time.perf_counter()
        execute_checked(client, sql)
        latencies.append(time.perf_counter() - q_start)
    total = sum(latencies)
    count = len(latencies)
    return {
        "count": count,
        "qps": count / total if total > 0 else 0.0,
        "avg_ms": statistics.mean(latencies) * 1000.0 if latencies else 0.0,
        "p50_ms": percentile(latencies, 50.0) * 1000.0,
        "p95_ms": percentile(latencies, 95.0) * 1000.0,
        "p99_ms": percentile(latencies, 99.0) * 1000.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--point-selects", type=int, default=1000)
    parser.add_argument("--range-selects", type=int, default=300)
    parser.add_argument("--updates", type=int, default=600)
    parser.add_argument("--joins", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    result_dir = pathlib.Path(args.result_dir).resolve()
    result_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    client = RmdbClient(args.host, args.port)
    phases: list[dict] = []
    try:
        phases.append(
            time_phase(
                "ddl",
                lambda: [
                    execute_checked(client, "create table bench_a (id int, grp int, val int, name char(16))"),
                    execute_checked(client, "create table bench_b (id int, aid int, score int, tag char(16))"),
                ],
            )
        )
        phases.append(time_phase("insert_bench_a", lambda: bench_insert(client, "bench_a", args.rows)))
        phases.append(time_phase("insert_bench_b", lambda: bench_insert(client, "bench_b", args.rows)))
        phases.append(
            time_phase(
                "create_index",
                lambda: [
                    execute_checked(client, "create index bench_a(id)"),
                    execute_checked(client, "create index bench_b(aid)"),
                ],
            )
        )

        point_sqls = [
            f"select * from bench_a where id = {rng.randrange(args.rows)}"
            for _ in range(args.point_selects)
        ]
        range_sqls = []
        for _ in range(args.range_selects):
            left = rng.randrange(max(args.rows - 50, 1))
            right = left + 20
            range_sqls.append(
                f"select * from bench_a where id >= {left} and id <= {right}"
            )
        update_sqls = [
            f"update bench_a set val = {100000 + i} where id = {rng.randrange(args.rows)}"
            for i in range(args.updates)
        ]
        join_sqls = [
            (
                "select * from bench_a, bench_b "
                f"where bench_a.id = bench_b.aid and bench_a.id = {rng.randrange(args.rows)}"
            )
            for _ in range(args.joins)
        ]

        phases.append(time_phase("point_select", lambda: bench_queries(client, point_sqls)))
        phases.append(time_phase("range_select", lambda: bench_queries(client, range_sqls)))
        phases.append(time_phase("update", lambda: bench_queries(client, update_sqls)))
        phases.append(time_phase("join_select", lambda: bench_queries(client, join_sqls)))
    finally:
        client.close()

    data = {
        "rows": args.rows,
        "point_selects": args.point_selects,
        "range_selects": args.range_selects,
        "updates": args.updates,
        "joins": args.joins,
        "phases": phases,
    }
    json_path = result_dir / "common_sql_benchmark.json"
    md_path = result_dir / "common_sql_benchmark.md"
    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Common SQL Benchmark",
        "",
        f"- rows: {args.rows}",
        f"- point_selects: {args.point_selects}",
        f"- range_selects: {args.range_selects}",
        f"- updates: {args.updates}",
        f"- joins: {args.joins}",
        "",
    ]
    for phase in phases:
        lines.append(f"## {phase['phase']}")
        lines.append(f"- seconds: {phase['seconds']:.6f}")
        result = phase["result"]
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, float):
                    lines.append(f"- {key}: {value:.6f}")
                else:
                    lines.append(f"- {key}: {value}")
        else:
            lines.append(f"- result: {result}")
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(md_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
