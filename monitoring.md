# Monitoring and load tests

Prometheus scrapes Go; Go does not connect to Prometheus and works without it.
Counters remain in process memory while monitoring is stopped; missed samples
cannot be reconstructed, and restarting Go resets its process counters.
Grafana queries Prometheus.

## Dashboards

Experimental encrypted attachment downloads expose
`e2ee_blob_downloads_total{method,result}`. Results are `complete`, `rejected` or
`interrupted`; a transfer interrupted after headers can still appear as HTTP200
in ordinary request metrics. Completion means the server finished sending, not
that the recipient durably saved the file or sent a file ACK. Labels contain no
account/message/blob IDs, filenames or credentials.

Encrypted attachment cleanup exposes `e2ee_blob_cleanup_*` counters with finite
`unfinished`, `expired` and `accepted` lanes. Compare `scanned_total`,
`reclaimed_total`, `failures_total` and `deferred_total{kind="item"|"query"}`:
continued scans with failures/deferrals and no reclamation indicate blocked work.
`retry_evictions_total` means the bounded retry cache dropped a scheduling hint;
the durable database work remains. Reclaimed counts include successful replay
observations, so they are not a count of unique files or freed bytes. Counters
are observational; they do not prove a recipient saved or decrypted a file.

Set a separate `GRAFANA_ADMIN_PASSWORD` in private `.env`, then:

```bash
make monitor-up
make monitor-stop
```

Grafana: http://localhost:3000 (`admin`, password from `.env`).
Prometheus: http://localhost:9090. Override ports with `GRAFANA_PORT` and
`PROMETHEUS_PORT`. An existing Grafana volume retains its original password.

Three dashboards are provisioned into the **Zchat** folder:

| Dashboard | Main signals |
| --- | --- |
| Backend and PostgreSQL | Request latency/errors, WS connections/queues/persistence, Go CPU/heap/goroutines, DB pool waits, PostgreSQL sessions/locks/I/O |
| Calls and LiveKit | Admission/signaling/management failures, rooms/tracks, RTP traffic/loss, RTT/jitter and SFU resources |
| Load test and resource limits | Delivery/history failures, latency, k6 dropped iterations, container usage/limits and sampler freshness |

Versioned dashboard JSON and datasource live in `monitoring/grafana/`.
Edit those files to persist changes. Choose the interval of the run and enough
samples for rate calculations; **No data/NaN is not a measured zero**. Completed
k6 series become stale, so inspect historical graphs. Alerts are in Prometheus;
external notifications need Alertmanager. An optional disabled SFU can appear DOWN.

The monitoring profile uses a 5-second scrape interval and bounded retention.
The backend shares an admission budget across authenticated HTTP/gRPC/WS work:
20 admitted operations, up to16 data operations and2 anonymous login/register
operations. Each account has4 data slots and1 separate control slot. Remaining
capacity serves logout/password/call checks; the PostgreSQL pool remains25/25.
Busy work gets HTTP429 or gRPC ResourceExhausted; ordinary WS gets a correlated
retryable server_busy error. Call gateway joins use503 for temporary check failure.
No persistence ACK is fabricated. Slots are released
before upload-body reads, authorized download streaming and idle socket/stream lifetime.

The Backend dashboard includes admitted work and decisions by class/result. Rejection
counts are backpressure; client retry time can increase tail latency. Limits bound
DB-bound work, not all transport allocations, incoming connections or RTP.

Metrics avoid message contents, tokens and per-user/room labels. nginx hides
`/metrics`; keep direct backend/exporter ports private.

## Isolated load lab

Requires Docker Compose, Python 3 and Make. k6/fixture Node run in pinned containers;
no host k6 installation is required. The lab has its own volumes, random credentials
and synthetic accounts under ignored `out/load/`; it does not load ordinary `.env`.

```bash
make load-init
make load-up
make load-seed LOAD_PAIRS=20
make load-check
make load-run LOAD_PROFILE=baseline
make load-info
```

| Service | Lab address |
| --- | --- |
| Chat / desktop server origin | http://localhost:38080 |
| Grafana | http://localhost:33000 |
| Prometheus | http://localhost:39090 |
| Direct Go HTTP / gRPC | `38081` / `35051` |
| PostgreSQL / Redis | `35432` / `36379` |

Grafana uses `admin` and `GRAFANA_ADMIN_PASSWORD` from `out/load/lab.env`.
`load-up` enables calls and monitoring. For manual desktop calls, point the client
at the lab origin and collect resources with `make load-observe LOAD_SECONDS=900`.
The core lab budgets Go and PostgreSQL at 2 CPU / 1 GB each; other services and
the generator have separate limits in `compose.load.yaml`.

## Workloads and interpretation

| Profile | Main duration | Minimum fixture pairs |
| --- | --- | --- |
| `smoke` | 20 s | 6 |
| `baseline` | 3 min | 20 |
| `stress` | 11 min 30 s | 240 |
| `spike` | 2 min 10 s | 120 |
| `soak` | 16 min | 40 |

A connected pair is two accounts/two WebSockets. Chat senders exercise private/group
messages; receivers check duplicates and a 5-second delivery deadline. Separate
HTTP traffic reads chats/history, with final stored-history checks. A fixture pair
is not necessarily connected throughout the run. Seeding stays outside measurement.

```bash
make load-seed LOAD_PAIRS=240
make load-run LOAD_PROFILE=stress
```

Inspect delivery/history failures, request tails, queue occupancy, DB pool waits,
locks/WAL waits, CPU/RAM, restarts and recovery. Check generator saturation and
sample age before attributing failure to Go/PostgreSQL. HTTP429 can be admission
protection; dropped iterations are unfinished workload, not successful requests.
Threshold failures return a nonzero exit code. Recovery after an aborted stress
run needs a separate baseline. Local WSL results do not establish VPS capacity.

Reports in `out/load/reports/` retain source/resource metadata, k6 summaries and
container samples; `out/load/last-run.txt` points to the latest. Keep these private.
The k6 profiles do not generate WebRTC RTP or test encrypted messaging. The short
mixed diagnostic below covers server-side E2EE paths separately. Container-memory graphs include runtime/cache overhead, not
only live Go heap. Runtime memory targets are softer than container limits.

## Profiling

After seeding at least 90 pairs, these commands enable profiling on the owned lab
and restart PostgreSQL with diagnostic extensions/settings:

```bash
python3 scripts/load/profile.py up
python3 scripts/load/profile.py run --kind mixed
```

Other kinds are `read-chats`, `read-history` and `write`. Compare identical workload
settings; instrumentation and samplers add overhead. Artifacts in
`out/load/profiling/` include the server executable/hash, Go profiles, SQL statistics,
waits and I/O counters. To inspect a saved CPU profile:

```bash
go tool pprof -http=127.0.0.1:36060 -no_browser \
  out/load/profiling/<run>/server out/load/profiling/<run>/cpu.pprof
```

Open `/ui/flamegraph` or `/ui/top`; graph rendering also needs Graphviz.
Raw pprof is opt-in and bound only to container loopback, without a published port.
Run `EXPLAIN (ANALYZE, BUFFERS)` separately from timed runs: ANALYZE executes the
query, so use deliberate synthetic data for writes. A plan alone does not prove
capacity. Return to the normal lab configuration with `make load-up`.

## Short mixed WS/E2EE server diagnostic

From WSL/Linux with Go, Python and Docker available, first create the private lab
configuration with `make load-init`, then:

```bash
make load-mixed-race
make load-mixed MIXED_SECONDS=4 MIXED_WORKERS=32 MIXED_ADMISSION=disabled
make load-mixed MIXED_SECONDS=4 MIXED_WORKERS=32 MIXED_ADMISSION=enabled
```

The runner compiles an opt-in Go fixture, starts only the owned lab PostgreSQL if
needed, creates a marked disposable database, then restores the original container
state and removes that database. Existing application/lab accounts and configuration
are preserved. A lock prevents overlapping mixed runs. Each steady phase accepts
1–30 seconds; the default runs roughly a minute of traffic, plus setup and draining.

Ordinary WebSockets and TLS E2EE gRPC share one production PostgreSQL pool (`25/25`).
The enabled mode matches production; disabled is a fixture-only comparison control.
Warmup finishes before the fixture enables the gate. Cases compare ordinary/E2EE/mixed traffic at low and selected concurrency, idle
polling, a reconnect burst and an explicitly **artificial** 500 ms pool stall.
E2EE cycles verify Send receipt → exact pending ciphertext → simulated recipient ACK
→ Stored status. Explicit overload retries preserve the request UUID/bytes, with
25–100ms test jitter and a3second cycle deadline; this aggressive pressure model
does not reproduce desktop scheduler cadence or its exact retry delays. Rejections
and retry counts remain visible. Ciphertext is synthetic; no libsignal, native durable receive,
WebRTC, HTTP history or key-maintenance cost is measured.

Private results live in `out/load/mixed/<run>/`: phase timing, exact outcome/DB wait
counters, sampled latency percentiles, server RPC concurrency, combined Go heap/
goroutines, PostgreSQL resources/waits and source/build metadata. The client and
server share one Go process, so these numbers do not establish independent server
capacity. This fixture does not start the full application or populate Grafana's
backend dashboard. Check `failedCycles` as well as the test exit code: pressure
errors are reported data; invalid delivery/receipt invariants fail the test.

The race variant checks correctness and existing epoch RPC regressions; use the
normal variant for timing. No stress fixture runs in CI unless explicitly opted in;
CI checks the runner safeguards and small statistics/configuration tests.

`make load-stop` preserves data/history. `make load-down` removes containers/network
and preserves volumes/reports. The explicit `lab.py reset` command destroys owned
lab data and configuration; it is not needed for ordinary shutdown.
