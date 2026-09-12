# Server performance comparison

`compare_versions.py` runs identical workloads against a released baseline and
a locally compiled candidate, using the same released GizmoSQL ADBC driver. It
records completed operations, latency, server CPU time, RSS, thread/file-descriptor
counts, scrape latency, and memory after repeated connection churn.

Install the driver dependencies and process sampler:

```bash
python -m pip install -r tests/drivers/requirements.txt psutil==7.2.2
python tests/benchmarks/compare_versions.py \
  --baseline /path/to/released/gizmosql_server \
  --candidate build/gizmosql_server \
  --metrics-license /private/path/metrics-license.txt \
  --duration 30 --output build/performance/run-1
```

The four modes are baseline, candidate with metrics disabled, collection enabled
with HTTP disabled, and collection plus approximately ten HTTP scrapes/second.
Without a metrics license, only the first two modes run. Each mode uses an
isolated database. Workloads verify query results and write-ledger checksums;
throughput never counts unverified or merely submitted writes.

Use equally optimized builds on the same idle machine. Avoid builds, test suites,
or other heavy activity during measurement, and repeat the comparison. On macOS,
prefix the command with `caffeinate -i` to prevent idle sleep. Results mark a run
invalid when resource sampling pauses for more than two seconds; retain the raw
data and rerun invalid modes before comparing throughput.

Concurrency rates count a read/write pair as one operation; single-query rates
count one query. Compare like workloads. CPU seconds per completed operation
helps distinguish a lower operation rate from lower CPU overhead. RSS may retain
allocator caches after cleanup, so inspect its trend across churn phases rather
than interpreting any increase as a leak. This benchmark is a regression check,
not a proof that no memory leak can exist.

The output includes exact binary SHA-256 hashes and raw resource samples. Keep
license files and server signing keys out of version control and benchmark
artifacts. Performance checks are run deliberately on a quiet host; correctness
and quality checks run in ordinary CI.
