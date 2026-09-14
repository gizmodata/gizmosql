#!/usr/bin/env python3
"""Compare server binaries with completed work and sampled process resources.

Uses the same released GizmoSQL ADBC driver for every server. Emits raw samples
and workload results as JSON; repeat runs before treating ratios as regressions.
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import psutil
from adbc_driver_gizmosql import dbapi


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def percentile(values, p):
    return sorted(values)[min(len(values) - 1, int(len(values) * p))]


def run(binary, label, duration, license_path, scrape, output):
    port, health, metrics = free_port(), free_port(), free_port()
    with tempfile.TemporaryDirectory(prefix="gizmosql-benchmark-") as directory:
        log_path = output / (label + ".server.log")
        env = dict(os.environ)
        for key in (
            "GIZMOSQL_LICENSE_KEY",
            "GIZMOSQL_LICENSE_KEY_FILE",
            "GIZMOSQL_METRICS_PORT",
            "GIZMOSQL_METRICS_BIND_ADDRESS",
            "INIT_SQL_COMMANDS",
            "INIT_SQL_COMMANDS_FILE",
        ):
            env.pop(key, None)
        command = [
            str(binary),
            "--hostname",
            "127.0.0.1",
            "--port",
            str(port),
            "--health-port",
            str(health),
            "--username",
            "benchmark",
            "--password",
            "local_benchmark_password",
            "--enable-instrumentation",
            "false",
            "--log-level",
            "warning",
            "--database-filename",
            str(Path(directory) / "benchmark.duckdb"),
        ]
        if license_path:
            command += ["--license-key-file", str(license_path)]
        if label.startswith("candidate"):
            command += [
                "--enable-metrics",
                "true" if license_path else "false",
                "--metrics-port",
                str(metrics if scrape else 0),
                "--metrics-bind-address",
                "127.0.0.1",
            ]
        stop = threading.Event()
        samples, scrape_times, background_errors = [], [], []
        sampler = scraper = None
        with log_path.open("wb") as log:
            server = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        process = psutil.Process(server.pid)

        def connect():
            return dbapi.connect(
                f"grpc://127.0.0.1:{port}",
                username="benchmark",
                password="local_benchmark_password",
                autocommit=True,
            )

        try:
            deadline = time.monotonic() + 60
            while True:
                if server.poll() is not None:
                    raise RuntimeError(f"{label} startup failed: {log_path}")
                try:
                    with connect() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT 1")
                            assert cur.fetchone() == (1,)
                    break
                except Exception:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)

            def sample():
                while not stop.wait(0.2):
                    cpu = process.cpu_times()
                    samples.append(
                        {
                            "time": time.monotonic(),
                            "rss": process.memory_info().rss,
                            "cpu_seconds": cpu.user + cpu.system,
                            "threads": process.num_threads(),
                            "fds": process.num_fds()
                            if hasattr(process, "num_fds")
                            else None,
                        }
                    )

            def scrape_loop():
                try:
                    while not stop.wait(0.1):
                        start = time.monotonic()
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{metrics}/metrics", timeout=5
                        ) as r:
                            assert r.status == 200
                            assert b"gizmosql_build_info{" in r.read()
                        scrape_times.append(time.monotonic() - start)
                except Exception as error:
                    background_errors.append(str(error))

            sampler = threading.Thread(target=sample)
            sampler.start()
            scraper = threading.Thread(target=scrape_loop) if scrape else None
            if scraper:
                scraper.start()
            results = {}
            with connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE ledger(token BIGINT PRIMARY KEY, n BIGINT)")
                    cur.execute(
                        "CREATE TABLE numbers AS SELECT * FROM range(100000) t(n)"
                    )
                    # Warm execution, allocator and filesystem caches before measurement.
                    for _ in range(100):
                        cur.execute("SELECT sum(n) FROM numbers")
                        assert cur.fetchone()[0] == 4999950000
                    workloads = {
                        "read": ("SELECT sum(n) FROM numbers", None),
                        "bound_read": (
                            "SELECT sum(n) FROM numbers WHERE n < ?",
                            (50000,),
                        ),
                        "write": ("INSERT INTO ledger VALUES (?, ?)", "write"),
                    }
                    for name, (sql, params) in workloads.items():
                        times = []
                        cpu_start = process.cpu_times()
                        start = time.monotonic()
                        while time.monotonic() - start < duration:
                            t = time.monotonic()
                            if params == "write":
                                cur.execute(sql, (len(times), len(times) * 7))
                            else:
                                cur.execute(sql, params)
                                expected = 4999950000 if name == "read" else 1249975000
                                assert cur.fetchone()[0] == expected
                            times.append(time.monotonic() - t)
                        elapsed = time.monotonic() - start
                        cpu_end = process.cpu_times()
                        results[name] = {
                            "operations": len(times),
                            "seconds": elapsed,
                            "ops_per_second": len(times) / elapsed,
                            "p50_ms": statistics.median(times) * 1000,
                            "p95_ms": percentile(times, 0.95) * 1000,
                            "cpu_seconds": cpu_end.user
                            + cpu_end.system
                            - cpu_start.user
                            - cpu_start.system,
                        }
                        if name == "write":
                            cur.execute("SELECT count(*), sum(n) FROM ledger")
                            assert cur.fetchone() == (
                                len(times),
                                7 * len(times) * (len(times) - 1) // 2,
                            )
            # Independent sessions issue concurrent reads and confirmed writes.
            # Each worker owns its connection/cursor: no client-side serialization.
            for workers in (2, 8):
                ready = threading.Barrier(workers + 1)

                def concurrent_worker(worker):
                    times = []
                    with connect() as c:
                        with c.cursor() as q:
                            q.execute(
                                f"CREATE TABLE concurrent_{workers}_{worker}(n BIGINT PRIMARY KEY)"
                            )
                            ready.wait(timeout=30)
                            start = time.monotonic()
                            while time.monotonic() - start < duration:
                                t = time.monotonic()
                                q.execute("SELECT sum(n) FROM numbers")
                                assert q.fetchone() == (4999950000,)
                                q.execute(
                                    f"INSERT INTO concurrent_{workers}_{worker} VALUES (?)",
                                    (len(times),),
                                )
                                times.append(time.monotonic() - t)
                            q.execute(
                                f"SELECT count(*), sum(n) FROM concurrent_{workers}_{worker}"
                            )
                            assert q.fetchone() == (
                                len(times),
                                len(times) * (len(times) - 1) // 2,
                            )
                    return times

                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = [pool.submit(concurrent_worker, i) for i in range(workers)]
                    cpu_start = process.cpu_times()
                    start = time.monotonic()
                    ready.wait(timeout=30)
                    times = [t for future in futures for t in future.result()]
                    elapsed = time.monotonic() - start
                    cpu_end = process.cpu_times()
                results[f"concurrent_{workers}"] = {
                    "operations": len(times),
                    "seconds": elapsed,
                    "ops_per_second": len(times) / elapsed,
                    "p50_ms": statistics.median(times) * 1000,
                    "p95_ms": percentile(times, 0.95) * 1000,
                    "cpu_seconds": cpu_end.user
                    + cpu_end.system
                    - cpu_start.user
                    - cpu_start.system,
                }
            # Repeated connect/query/close cycles detect retained session resources.
            churn_rss = []
            for phase in range(5):

                def one_session(_):
                    with connect() as c:
                        with c.cursor() as q:
                            q.execute("SELECT 42")
                            assert q.fetchone() == (42,)

                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
                    list(pool.map(one_session, range(100)))
                time.sleep(1)
                churn_rss.append(process.memory_info().rss)
            stop.set()
            sampler.join()
            if scraper:
                scraper.join()
            if background_errors:
                raise RuntimeError(f"Background collection failed: {background_errors}")
            sample_gaps = [b["time"] - a["time"] for a, b in zip(samples, samples[1:])]
            largest_gap = max(sample_gaps, default=0)
            return {
                "label": label,
                "measurement_valid": largest_gap <= 2,
                "max_sample_gap_seconds": largest_gap,
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "workloads": results,
                "samples": samples,
                "churn_rss_bytes": churn_rss,
                "scrapes": len(scrape_times),
                "scrape_p95_ms": percentile(scrape_times, 0.95) * 1000
                if scrape_times
                else None,
            }
        finally:
            stop.set()
            for thread in (sampler, scraper):
                if thread:
                    thread.join(timeout=10)
            server.terminate()
            try:
                server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--baseline", type=Path, required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--metrics-license", type=Path)
    p.add_argument("--duration", type=float, default=30)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    runs = []
    modes = [
        (args.baseline, "baseline", None, False),
        (args.candidate, "candidate-disabled", None, False),
    ]
    if args.metrics_license:
        modes += [
            (args.candidate, "candidate-collection", args.metrics_license, False),
            (args.candidate, "candidate-scraped", args.metrics_license, True),
        ]
    for binary, label, license_path, scrape in modes:
        result = run(
            binary.resolve(), label, args.duration, license_path, scrape, args.output
        )
        runs.append(result)
        (args.output / "results.json").write_text(json.dumps(runs, indent=2))
        print(label, json.dumps(result["workloads"]), flush=True)


if __name__ == "__main__":
    main()
