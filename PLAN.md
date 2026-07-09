# Nomios-Go — MySQL CDC at Scale, in Golang

Implementation plan / technical specification for rebuilding the **Nomios** CDC platform
(originally designed by the Catalog Platform team, documented in *"08 Nomios – Scaling MySQL CDC"*
and the *"How do we build NOMIOS"* presentation) as a Go service.

---

## 1. Problem & Motivation

CDC is an integral part of the catalog platform architecture. The incumbent solution —
Debezium MySQL connectors on Kafka Connect — has three structural limits:

1. **Capture rate limit.** Each connector tops out at ~5k change events/sec per table/topic.
2. **Operational complexity.** To gain throughput on one database we must deploy and manage
   many connectors, each covering a group of tables.
3. **Inefficient resource usage.** Debezium's single-threaded parse → serialize → publish
   pipeline cannot use available CPU cores.

Nomios solves this with a multi-threaded (in Go: goroutine-based) pipeline where a single
binlog stream fans out to a pool of parallel publishers, while still preserving per-entity
event ordering. Production results from the original system: **1M deals ≈ 3M binlog events**
handled at peak with headroom, far beyond the Debezium ceiling.

### Goals

- ≥ 50k change events/sec sustained per Hyperloop on commodity hardware (10× Debezium).
- **At-least-once** delivery with **per-key ordering** guaranteed.
- Crash/restart safe via persisted GTID-based state.
- One binary, many pipelines: run N independent CDC flows ("Hyperloops") per node.
- Operable: start/stop/monitor via API, Prometheus metrics, graceful shutdown.

### Non-goals (v1)

- Exactly-once delivery (accepted trade-off: duplicates possible on restart, consumers
  must be idempotent — same as original Nomios).
- Sources other than MySQL (interface is pluggable; only `MysqlSource` ships in v1).
- Full initial snapshot / incremental snapshot (roadmapped, see §10).

---

## 2. Architecture Overview

A single CDC flow (**Hyperloop**) looks like:

```
                        ┌──────────────────────────── Hyperloop ───────────────────────────┐
                        │                                                                  │
 MySQL master/replica   │  ┌────────┐   ┌─────────────┐    ┌───────────────────────────┐  │
 ┌─────────┐  binlog    │  │ Binlog │   │ EventMapper │    │       Consumer pool       │  │
 │  mysqld │ ─────────► │  │ client │ ─►│ parse + map │─┬─►│ queue[0] ─► Publisher[0] ─┼──┼─► Kafka
 └─────────┘ (replica   │  └────────┘   │ schema info │ │  │ queue[1] ─► Publisher[1] ─┼──┼─► Kafka
              protocol) │   Source      └─────────────┘ │  │   ...          ...        │  │
                        │  (1 goroutine)   NomiosEvent  │  │ queue[N] ─► Publisher[N] ─┼──┼─► Kafka
                        │                               │  └────────────┬──────────────┘  │
                        │                        KeyProvider            │ last event      │
                        │                        hash(key) % N          ▼ per publisher   │
                        │                                     ┌───────────────┐           │
                        │                                     │ State manager │──────────►│ persistent
                        │                                     │ min(positions)│  commit   │ state store
                        │                                     └───────────────┘  ckpt     │
                        └──────────────────────────────────────────────────────────────────┘
```

Pipeline stages: **stream event → parse → serialize → publish**, with checkpointing on the side.

Key design points carried over from the original Nomios:

- The **Source** runs on its own goroutine. Stream+parse speed of a single reader is
  sufficient (binlog is inherently a serial log); parallelism is applied where it pays:
  serialize + publish.
- Events are **partitioned into in-memory buffer queues by hashing an entity key**
  (provided by a `KeyProvider`). All changes for one entity land in the same queue →
  processed by the same publisher → **per-entity order preserved**.
- Each **Publisher** (consumer in the pool) drains its queue in batches, serializes and
  produces to Kafka on its own goroutine — this is the core improvement over Debezium.
- The **State manager** computes the global checkpoint as the **earliest (minimum)
  last-processed position across all publishers**, so a restart never skips events
  (it may replay a few — at-least-once).

---

## 3. Component Specifications

### 3.1 NomiosEvent

The standard data format used between all components in the stream.

```go
// pkg/event/event.go
type Op string

const (
    OpInsert Op = "c" // create
    OpUpdate Op = "u"
    OpDelete Op = "d"
)

type NomiosEvent struct {
    ID       string            // globally unique identifier (ULID)
    Op       Op                // insert | update | delete
    Before   map[string]any    // row image before change (nil for insert)
    After    map[string]any    // row image after change  (nil for delete)
    Source   SourceMeta        // metadata about the origin of the event
    OccurredAt time.Time       // event time from binlog header
    Position Position          // binlog position / GTID of this event
}

type SourceMeta struct {
    Connector string // "mysql"
    ServerID  uint32
    Database  string
    Table     string
    GTID      string // gtid of the enclosing transaction, if enabled
    File      string // binlog file
    Pos       uint32 // binlog offset
    TxOrder   int    // ordinal of the row event inside the transaction
}
```

Wire format (JSON serializer, v1) is Debezium-compatible in spirit
(`before`/`after`/`source`/`op`/`ts_ms`) to ease consumer migration.

### 3.2 Source (`MysqlSource`)

Responsible for producing the event stream. Nomios acts as a **MySQL replication slave**:
it registers with the server (`register_slave`, unique `server_id`) and streams binlog
events over the replication protocol.

- **Library:** [`github.com/go-mysql-org/go-mysql`](https://github.com/go-mysql-org/go-mysql)
  (`replication.BinlogSyncer` + `canal`-style schema tracking). It is the Go equivalent of
  `osheroff/mysql-binlog-connector-java` used by the original Nomios/Debezium.
- **Requirements on MySQL:** `binlog_format=ROW`, `binlog_row_image=FULL`,
  `gtid_mode=ON` + `enforce_gtid_consistency=ON` (GTID strongly preferred; file+offset
  supported as fallback).
- Runs on **one dedicated goroutine**. Not parallelized by design.
- Handles raw binlog events: `TABLE_MAP`, `WRITE_ROWS`, `UPDATE_ROWS`, `DELETE_ROWS`,
  `GTID`, `XID`, `ROTATE`, `QUERY` (DDL).
- **EventMapper** enriches raw row events with schema info (column names/types from an
  in-memory schema registry refreshed on DDL events + initial `INFORMATION_SCHEMA` load),
  filters by configured table include/exclude lists, and emits `NomiosEvent`s.

```go
// pkg/source/source.go
type Source interface {
    // Start begins streaming from the given position and pushes events
    // to out until ctx is cancelled or a fatal error occurs.
    Start(ctx context.Context, from state.Position, out chan<- event.NomiosEvent) error
    io.Closer
}
```

Pluggable: additional sources (Postgres WAL, MongoDB oplog) implement the same interface later.

### 3.3 Consumer Pool (buffer queues + publishers)

**Buffer queues.** To prevent the Source from blocking when downstream is slower, events
are held in N in-memory queues (Go: buffered channels, capacity configurable, default 4096).
The dispatcher routes each event:

```
queueIdx = hash(KeyProvider(event)) % N
```

`KeyProvider` extracts the partition key — by default the table's primary key value(s),
overridable per table (e.g. route by `seller_id`). Same entity → same queue → same
publisher → ordering per key holds end-to-end (Kafka partition key uses the same key).

**Backpressure:** when a queue is full the dispatcher blocks, which naturally throttles
the binlog reader instead of dropping events or OOMing.

**Publishers.** Each of the N pool members runs its own goroutine:

1. Drain currently available events from its queue (batch up to `maxBatch`, default 500,
   or `maxWait`, default 20 ms).
2. Serialize each event via the `Serializer` interface (v1: JSON; Avro/Protobuf later).
3. Produce the batch to Kafka with the entity key as the record key.
4. On producer ack of the batch, report its **last processed position** to the State manager.

```go
// pkg/serialize/serializer.go
type Serializer interface {
    Serialize(e event.NomiosEvent) (key []byte, value []byte, err error)
}

// pkg/publish/publisher.go
type Publisher interface {
    PublishBatch(ctx context.Context, events []event.NomiosEvent) error
    LastPosition() state.Position
    io.Closer
}
```

**Kafka client:** [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) —
high-throughput, pure Go, supports idempotent producer, batching and compression natively.
Producer config mirrors the benchmarked original: `acks=all`, `linger.ms` tuned,
`compression` configurable (lz4 default — original TODO item "try compression"),
`enable.idempotence=true`.

Topic routing is configurable per table (default `"{prefix}.{database}.{table}"`), with
support for custom topic/key/body overrides (a requested feature in the design review).

### 3.4 State Manager

Every event carries a `Position`. Nomios state = position of the last processed event,
continuously persisted so that restart/deploy/crash resumes from the right place.

Because many publishers run concurrently, the committed state must be the
**earliest position among all publishers' last-processed events** — guaranteeing no event
is lost on restart, accepting that a few already-published events may be re-sent
(**at-least-once**).

```go
// pkg/state/state.go
type Position struct {
    GTIDSet string // preferred: executed GTID set, e.g. "3E11FA47-...:1-77"
    File    string // fallback: binlog file + offset
    Offset  uint32
    SeqNo   uint64 // monotonic sequence assigned by Source, used for min() comparison
}

type Store interface {
    Load(ctx context.Context, hyperloopID string) (Position, error)
    Save(ctx context.Context, hyperloopID string, p Position) error
}
```

- Positions are compared via the Source-assigned monotonic `SeqNo` (GTID sets are not
  totally ordered by string comparison).
- **GTID handling:** the state keeps a full **GTID set**; on resume the syncer starts from
  `GTIDSet`, which is robust across MySQL failover (master change with same host URI — an
  explicit test scenario from the design review).
- Commit cadence: every `commitInterval` (default 1 s) and on graceful shutdown.
- `Store` implementations v1: **MySQL table** (default — no new infra) and **file** (dev).
  Redis/etcd can be added behind the interface.

### 3.5 Hyperloop — the unit of CDC

A Hyperloop is one complete CDC flow (stream → parse → serialize → publish) managed as a
single unit. One Nomios node can run multiple Hyperloops.

Responsibilities:

- Start all goroutines in dependency order and supervise them.
- Expose lifecycle API: **configure / start / monitor / stop**.
- **Graceful shutdown:** stop Source → drain buffer queues → flush publishers → commit
  final state → close Kafka clients. Bounded by `shutdownTimeout` (default 30 s).
- **Failure recovery:** on a child goroutine error, initiate graceful shutdown, then apply
  restart policy (exponential backoff, max attempts) depending on error class
  (recoverable: connection loss, ErrPipelineBackpressure; fatal: bad config, auth).

```go
// pkg/hyperloop/hyperloop.go
type Status string // "created" | "starting" | "running" | "stopping" | "stopped" | "failed"

type Hyperloop struct { /* wires Source, dispatcher, pool, state manager */ }

func New(cfg Config, deps Deps) (*Hyperloop, error)
func (h *Hyperloop) Start(ctx context.Context) error
func (h *Hyperloop) Stop(ctx context.Context) error   // graceful
func (h *Hyperloop) Status() StatusReport             // per-component health, lag, positions
```

**Start flow:** load state → connect Kafka producers → start publishers → start dispatcher
→ start Source from loaded position → mark running.
**Stop/terminate flow:** reverse order with drain (as above); on `ctx` deadline exceed,
hard-cancel and log the positions that will be replayed.

### 3.6 Control plane (Nomios server)

A thin HTTP/JSON API (chi or stdlib `net/http`) managing hyperloops on the node:

| Method | Path                          | Purpose                          |
|--------|-------------------------------|----------------------------------|
| POST   | `/v1/hyperloops`              | create from config               |
| POST   | `/v1/hyperloops/{id}/start`   | start                            |
| POST   | `/v1/hyperloops/{id}/stop`    | graceful stop                    |
| GET    | `/v1/hyperloops/{id}`         | status, positions, lag           |
| GET    | `/v1/hyperloops`              | list                             |
| GET    | `/metrics`                    | Prometheus                       |
| GET    | `/healthz`, `/readyz`         | probes                           |

Config is also loadable from YAML at boot for declarative deployments.

---

## 4. Delivery & Ordering Guarantees (Makesure)

The original design asks three questions; the Go implementation answers them the same way:

| Guarantee | Mechanism |
|---|---|
| **All messages are processed** | Checkpoint = min(position) over publishers; persisted before being advanced past; resume from checkpoint ⇒ no gaps. Kafka producer `acks=all` + idempotence + unlimited retries within delivery timeout. |
| **Payload is correct** | Schema registry kept in sync with DDL from the binlog stream itself (not a side channel), full row images (`binlog_row_image=FULL`), round-trip serializer tests, and the **Makesure auditor** (§below). |
| **Order per key** | Same key → same buffer queue → same publisher → same Kafka partition (key-hash). Single in-flight batch per partition sequence via idempotent producer. |

**Nomios-Makesure (auditor, phase 4):** a separate lightweight consumer that tails the
output topics and the source DB to detect abnormal events — gaps in per-key sequence,
payload mismatches on sampled keys, duplicate bursts — and exposes them as metrics/alerts.
V1 ships the metric hooks; the full auditor service is a later milestone.

---

## 5. Repository Layout

```
cdc-scaling/
├── cmd/
│   └── nomios/              # main: server + hyperloop runner
├── pkg/
│   ├── event/               # NomiosEvent, Op, SourceMeta
│   ├── source/
│   │   └── mysql/           # BinlogSyncer wrapper, EventMapper, schema registry
│   ├── dispatch/            # KeyProvider, hash partitioner, buffer queues
│   ├── publish/
│   │   └── kafka/           # franz-go publisher pool
│   ├── serialize/           # Serializer iface, json/
│   ├── state/               # Position, Store iface, mysqlstore/, filestore/
│   ├── hyperloop/           # lifecycle, supervision, graceful shutdown
│   ├── server/              # HTTP control plane
│   └── metrics/             # Prometheus collectors
├── internal/testutil/       # docker-based MySQL+Kafka harnesses
├── configs/                 # example YAML configs
├── deploy/                  # Dockerfile, compose for local stack, k8s manifests
└── docs/                    # this spec, runbooks, benchmark reports
```

Go ≥ 1.22, modules; `golangci-lint`; CI: lint + unit + integration (dockerized MySQL & Redpanda/Kafka).

Reference implementations to consult (from the original doc):
`tikivn/apollo-event-publisher`, `osheroff/mysql-binlog-connector-java`, `zzt93/syncer`,
`airbnb/SpinalTap`, Debezium's `ReadBinLogIT`, Netflix DBLog paper.

---

## 6. Configuration (example)

```yaml
hyperloops:
  - id: catalog-main
    source:
      mysql:
        host: mysql-replica-1:3306
        user: nomios
        password: ${NOMIOS_MYSQL_PASSWORD}
        serverID: 5501            # unique per replica client
        gtid: true
        include: ["catalog.deals", "catalog.products", "catalog.*_price"]
        exclude: ["catalog.tmp_*"]
    pipeline:
      queues: 8                   # buffer queue / publisher count (TODO: tune, see §10)
      queueCapacity: 4096
      batch: { maxSize: 500, maxWaitMs: 20 }
      keyOverrides:
        catalog.deals: ["seller_id", "deal_id"]
    sink:
      kafka:
        brokers: ["kafka-1:9092", "kafka-2:9092"]
        topicTemplate: "cdc.{database}.{table}"
        acks: all
        compression: lz4
        idempotent: true
    state:
      store: mysql
      dsn: ${NOMIOS_STATE_DSN}
      commitIntervalMs: 1000
    shutdownTimeoutSec: 30
```

---

## 7. Observability

Prometheus metrics (parity with the original Grafana dashboards):

- `nomios_source_events_total{hyperloop,table,op}` — raw binlog events parsed
- `nomios_source_lag_seconds{hyperloop}` — now − last event timestamp (replication lag)
- `nomios_queue_depth{hyperloop,queue}` / `nomios_queue_blocked_seconds_total`
- `nomios_publish_events_total{hyperloop,topic}` / `nomios_publish_errors_total`
- `nomios_publish_batch_size` (histogram), `nomios_publish_latency_seconds` (histogram)
- `nomios_state_commit_total`, `nomios_state_position{...}` (info gauge)
- `nomios_hyperloop_status{status}` — state machine gauge

Structured logging via `log/slog`; per-hyperloop logger fields. Optional OTel traces on
the publish path.

---

## 8. Testing Plan

**Unit:** EventMapper against captured binlog fixtures; hash partitioner distribution &
stability; min-position computation with concurrent publishers; serializer round-trips;
graceful-shutdown ordering with race detector (`-race` in CI always).

**Integration** (docker: MySQL 8 + Redpanda) — includes the scenarios listed in the
original design review:

1. Restart MySQL server mid-stream → resume, no gaps.
2. Stop MySQL, delay 30 s, start → reconnect with backoff, no gaps.
3. **Change master of a MySQL cluster behind the same host URI** → GTID-set resume works.
4. Update table schema (DDL), then update data → new columns mapped correctly.
5. Rename a table → routing/filtering behaves per config.
6. Kill -9 Nomios under load → restart replays ≥ checkpoint; consumer sees dups, no gaps.
7. Kafka broker unavailable → backpressure to source, recovery without loss.

**Verification harness (mini-Makesure):** test producer writes rows with monotonic
per-key sequence numbers; test consumer asserts per-key ordering, completeness, payload
equality against the DB.

**Benchmarks:** reproduce the original benchmark matrix — publisher pool size (1→16),
batch size, compression on/off, queue capacity — with `go test -bench` micro-benchmarks
plus an end-to-end load rig (sysbench-generated writes). Acceptance: ≥ 50k ev/s sustained,
p99 publish latency < 250 ms, checkpoint lag < 5 s.

---

## 9. Milestones

Mirrors the original roadmap shape (implement → test core → test deployment → UAT → prod, ~2 quarters):

| # | Milestone | Deliverables | Est. |
|---|-----------|--------------|------|
| 0 | Skeleton & CI | repo layout, config loading, lint/test pipeline, local docker stack | 1 w |
| 1 | **Source core** | MysqlSource on go-mysql: GTID + file/offset streaming, EventMapper, schema registry, DDL handling, table filters | 3 w |
| 2 | **Pipeline core** | KeyProvider, hash dispatcher, buffer queues, publisher pool, JSON serializer, franz-go sink | 2 w |
| 3 | **State & recovery** | Position/min-checkpoint logic, MySQL+file stores, resume-on-start, crash tests | 2 w |
| 4 | **Hyperloop & control plane** | lifecycle supervision, graceful shutdown, HTTP API, multi-hyperloop per node | 2 w |
| 5 | **Observability & hardening** | metrics, dashboards, failover/DDL integration suite, chaos tests | 2 w |
| 6 | **Benchmark & tuning** | pool-size/batch/compression sweeps, report vs. Debezium baseline | 1 w |
| 7 | **UAT → prod rollout** | shadow-run beside Debezium on one database, Makesure comparisons, cutover runbook, then other teams | 2–4 w |

---

## 10. Future Work (from original TODO + review comments)

- Tune consumer pool size dynamically (auto-scale publishers on queue depth).
- Benchmark with custom batch sizes; compression codecs beyond lz4.
- **Incremental snapshot** (DBLog-style watermark chunking) for bootstrapping existing tables.
- Full **Nomios-Makesure** auditor service (gap/duplicate/payload-drift detection & alerting).
- Advanced Kafka record configuration: custom body format, custom topic/key templates per table.
- Additional serializers (Avro + schema registry, Protobuf) and sources (Postgres).
- Event-store style sink support.
- HA fail-over between Nomios nodes (leader election per hyperloop, cf. debezium-examples/failover).
