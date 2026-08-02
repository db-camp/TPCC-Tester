#!/usr/bin/env python3
"""Compare official evaluation logs against a local perf_workflow record.

Usage:
  python3 compare_official.py <official_log> <local_record_dir>

Outputs a multi-metric comparison table (throughput, decay, latency, aborts,
abandoned, CPU, RSS, load time, 5s-bucket distribution) so local-vs-official
gaps can be narrowed jointly rather than on tpmC alone.
"""
import json
import os
import re
import sys


def parse_official(path):
    text = open(path, encoding="utf-8", errors="replace").read()
    m = re.search(r"Ranking metric\s*\n(\d+\.?\d*)\s*\nNewOrder/min", text)
    median = float(m.group(1)) if m else None
    m = re.search(r"Median NewOrder/min\s*\n(\d+\.?\d*)", text)
    if m and median is None:
        median = float(m.group(1))

    def num(pattern):
        mm = re.search(pattern, text)
        return float(mm.group(1)) if mm else None

    def num_of(pattern):
        mm = re.findall(pattern, text)
        return [float(x) for x in mm] if mm else None

    return {
        "source": "official",
        "median": median,
        "rounds": num_of(r"Round\s*\d+\s*[\d.]+\s*([\d.]+)\s*[\d.]+\s*[\d.]+\s*×"),
        "abort_rate": num(r"Abort rate\s*\n(\d+\.?\d*)%"),
        "p50": num(r"Pooled p50 latency\s*\n([\d.]+) ms"),
        "p99": num(r"Pooled p99 latency\s*\n([\d.]+) ms"),
        "avg_latency": num(r"Pooled average latency\s*\n([\d.]+) ms"),
        "max_latency": num(r"Pooled max latency\s*\n([\d.]+) ms"),
        "cpu_avg": num(r"TPC-C CPU avg \(1-core %\)\s*\n([\d.]+)%"),
        "cpu_host": num(r"TPC-C CPU avg \(host %\)\s*\n([\d.]+)% of 16 CPUs"),
        "rss_gb": num(r"Peak RMDB RSS\s*\n([\d.]+) GiB"),
        "load_time": num(r"Load time\s*\n([\d.]+)s"),
        "bucket_min": num(r"min (\d+\.00)\s*\nmax"),
        "bucket_max": num(r"max (\d+\.00)\s*\nmedian"),
        "disk_after_load": num(r"Database disk after loading\s*\n([\d.]+) GiB"),
    }


def parse_local(record_dir):
    rank_path = os.path.join(record_dir, "rank.log")
    text = open(rank_path, encoding="utf-8", errors="replace").read()
    rounds, lats = [], []
    for i in range(1, 4):
        m = re.search(
            rf"window{i}: new_order_per_min=([\d.]+), attempted=(\d+).*?abandoned=(\d+).*?"
            rf"new_order_latency_ms=avg:([\d.]+),p50:([\d.]+),p99:([\d.]+),max:([\d.]+).*?"
            rf"new_order_5s=avg_per_min:[\d.]+,cv_percent:([\d.]+),min_per_min:([\d.]+),max_per_min:([\d.]+).*?"
            rf"(?:peak_avg=([\d.]+),abort_rate=([\d.]+),)?",
            text,
            re.DOTALL,
        )
        if m:
            groups = m.groups()
            rate, attempted, abandoned, avg, p50, p99, mx, cv, mn5, mx5 = [
                float(x) for x in groups[:10]
            ]
            peak = float(groups[10]) if groups[10] is not None else 0.0
            abort = float(groups[11]) if groups[11] is not None else 0.0
            rounds.append({"rate": rate, "attempted": int(attempted), "abandoned": int(abandoned),
                           "peak_avg": peak, "abort_rate": abort, "min5": mn5, "max5": mx5, "cv": cv})
            lats.append({"avg": avg, "p50": p50, "p99": p99, "max": mx})
    res = {
        "source": "local",
        "record": os.path.basename(record_dir),
        "rounds": rounds,
        "latency": lats,
        "median": None,
    }
    if len(rounds) == 3:
        res["median"] = sorted(r["rate"] for r in rounds)[1]
    res["abandoned_total"] = sum(r["abandoned"] for r in rounds)
    res["attempted_total"] = sum(r["attempted"] for r in rounds)
    res["bucket_max"] = max((r["max5"] for r in rounds), default=None)

    try:
        rm = json.load(open(os.path.join(record_dir, "resource_metrics.json")))
        rc = rm.get("rank_cpu", {})
        res["cpu_host"] = rc.get("combined", {}).get("average_host_percent")
        res["cpu_peak"] = rc.get("combined", {}).get("peak_host_percent")
        res["rss_gb"] = rm.get("max_rss", {}).get("gb_decimal")
    except Exception:
        pass

    setup = open(os.path.join(record_dir, "setup.log"), encoding="utf-8", errors="replace").read()
    m = re.findall(r"物化完成 \(([\d.]+)s\)", setup)
    if m:
        res["load_time"] = float(m[-1])
    try:
        mf = json.load(open(os.path.join(record_dir, "manifest.json")))
        res["status"] = mf.get("status")
        res["conformance"] = mf.get("conformance")
    except Exception:
        pass
    return res


def row(label, off, loc, ratio=True, inv=False):
    def fmt(v, suffix=""):
        if v is None:
            return "n/a"
        if isinstance(v, float):
            return f"{v:.1f}{suffix}"
        return f"{v}{suffix}"
    if off is None and loc is None:
        return f"{label:28s} n/a  n/a  n/a"
    r = ""
    if off is not None and loc is not None and ratio and loc != 0:
        r = f"{off / loc:.2f}x" if not inv else f"{loc / off:.2f}x"
    return f"{label:28s} {fmt(off):>12s} {fmt(loc):>12s} {r:>10s}"


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    off = parse_official(sys.argv[1])
    loc = parse_local(sys.argv[2])
    print(f"对比: 官方 {sys.argv[1]}  vs  本地 {sys.argv[2]}")
    print(f"{'指标':28s} {'官方':>12s} {'本地':>12s} {'差距':>10s}")
    print("-" * 66)
    print(row("Median NewOrder/min", off.get("median"), loc.get("median")))
    if loc.get("rounds") and off.get("rounds"):
        for i, (o, l) in enumerate(zip(off["rounds"], loc["rounds"])):
            print(row(f"  Round{i+1} NewOrder/min", o, l["rate"]))
        o_decay = off["rounds"][0] / off["rounds"][2] if off["rounds"][2] else None
        l_decay = loc["rounds"][0]["rate"] / loc["rounds"][2]["rate"] if loc["rounds"][2]["rate"] else None
        print(f"R1/R3 衰减        {o_decay:>12.2f}x {l_decay:>12.2f}x")
    print(row("Abort rate %", off.get("abort_rate"), None))
    if loc.get("abandoned_total") is not None:
        abort_loc = None
        if loc.get("attempted_total"):
            abort_loc = loc["abandoned_total"] * 100.0 / loc["attempted_total"]
        print(f"本地 abandoned 总: {loc['abandoned_total']} ({abort_loc:.2f}% of attempted)" if abort_loc else f"本地 abandoned 总: {loc['abandoned_total']}")
    print(row("p50 latency ms", off.get("p50"), loc["latency"][0]["p50"] if loc.get("latency") else None))
    print(row("p99 latency ms", off.get("p99"), loc["latency"][0]["p99"] if loc.get("latency") else None))
    print(row("avg latency ms", off.get("avg_latency"), loc["latency"][0]["avg"] if loc.get("latency") else None))
    print(row("CPU avg (host%)", off.get("cpu_host"), loc.get("cpu_host")))
    print(row("Peak RSS GiB", off.get("rss_gb"), loc.get("rss_gb")))
    print(row("Load time s", off.get("load_time"), loc.get("load_time")))
    print(row("5s bucket max", off.get("bucket_max"), loc.get("bucket_max")))
    print(f"本地 conformance: {loc.get('conformance')}  status: {loc.get('status')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
