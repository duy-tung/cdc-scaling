# Nomios-Go — Scaling MySQL CDC

A Go implementation of **Nomios**, the MySQL change-data-capture platform
designed to replace Debezium connectors (which cap at ~5k changes/sec, one
thread per connector). Nomios streams the MySQL binlog as a replication
slave and fans events out to a pool of parallel Kafka publishers while
preserving per-entity ordering and at-least-once delivery.

See [PLAN.md](PLAN.md) for the full design specification.

## Architecture

```
MySQL ─binlog─► Source (1 goroutine) ─► Dispatcher ─hash(key)─► buffer queues ─► Publisher pool ─► Kafka
                                                                                      │
                                                              State manager ◄── min(position)
```

- **Source** (`pkg/source/mysql`) — acts as a MySQL replication slave
  (GTID or file/offset), maps raw binlog rows to `NomiosEvent`s using a
  schema registry that invalidates on DDL seen in the stream.
- **Dispatcher** (`pkg/dispatch`) — hashes each event's entity key into one
  of N buffer queues; same entity → same queue → order preserved. Full
  queues block the source (backpressure, never drops).
- **Publisher pool** (`pkg/hyperloop` + `pkg/publish/kafka`) — one goroutine
  per queue drains batches, serializes (JSON) and produces to Kafka with
  acks=all + idempotence.
- **State manager** (`pkg/state`) — checkpoint = earliest position across
  all publishers (contiguous-prefix watermark), persisted to MySQL or file;
  restart resumes with no gaps, tolerating a few replays.
- **Hyperloop** (`pkg/hyperloop`) — one complete CDC flow managed as a
  unit: start/stop/monitor, graceful shutdown (stop source → drain → flush
  → final commit).
- **Control plane** (`pkg/server`, `cmd/nomios`) — HTTP API to create,
  start, stop and inspect hyperloops; `/metrics`, `/healthz`.

## Run

```sh
go build ./cmd/nomios
./nomios -config configs/nomios.yaml
```

MySQL requirements: `binlog_format=ROW` (8.0 default), `gtid_mode=ON`,
a user with `REPLICATION SLAVE, REPLICATION CLIENT, SELECT`.

## Test

```sh
# Unit tests
go test -race ./pkg/...

# Integration tests: full pipeline against an in-memory Kafka (kfake) and,
# when a local MySQL 8 is prepared (sudo ./scripts/setup-test-mysql.sh),
# real binlog capture: insert/update/delete, restart-resume, DDL mid-stream.
go test -tags integration -race ./test/integration/...
```

The MySQL tests skip with instructions if no server is available at
`127.0.0.1:3306` (override via `NOMIOS_TEST_MYSQL_ADDR`,
`NOMIOS_TEST_MYSQL_USER`, `NOMIOS_TEST_MYSQL_PASSWORD`).

## Benchmarks

```sh
# Micro: serializer, dispatcher, checkpoint tracker
go test -bench . -run xxx ./pkg/serialize/ ./pkg/dispatch/ ./pkg/state/

# Pipeline matrix: publisher pool size × batch size (design doc §8 sweep)
go test -bench BenchmarkHyperloop -run xxx -benchtime 200000x ./pkg/hyperloop/

# End to end with a real Kafka producer (in-memory broker): pool + codecs
go test -tags integration -bench BenchmarkPipeline -run xxx -benchtime 100000x ./test/integration/
```

Reference results (4-core CI container, race detector off, after the
optimization passes documented in [docs/OPTIMIZATION.md](docs/OPTIMIZATION.md)):

| Benchmark | Result |
|---|---|
| JSON serialize, 5-col row | 839 ns/event, 2 allocs (hand-rolled encoder) |
| Dispatcher routing | 8.8 ns/event, 0 allocs; ~22–34 ns/event through queues |
| Pipeline + JSON serialize (in-process sink) | ~1.19M events/s |
| End-to-end incl. Kafka producer, 1 publisher | ~686k events/s (async produce) |
| End-to-end incl. Kafka producer, 4 publishers | ~857k events/s; ~930–950k with lz4/snappy |

## HTTP API

| Method | Path                        | Purpose            |
|--------|-----------------------------|--------------------|
| GET    | `/v1/hyperloops`            | list + status      |
| POST   | `/v1/hyperloops`            | create from config |
| GET    | `/v1/hyperloops/{id}`       | status, checkpoint, queue depths |
| POST   | `/v1/hyperloops/{id}/start` | start              |
| POST   | `/v1/hyperloops/{id}/stop`  | graceful stop      |
| GET    | `/metrics`, `/healthz`      | ops                |

## Delivery guarantees

- **No gaps**: the committed checkpoint never passes an unpublished event;
  resume replays at most the in-progress transaction.
- **At-least-once**: consumers must dedupe (event `id` is deterministic
  from binlog coordinates).
- **Per-key order**: same key → same buffer queue → same publisher → same
  Kafka partition.
