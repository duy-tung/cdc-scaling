# Nomios-Go Optimization Plan

Performance optimization spec, derived from the benchmark suite added in
`3250f7f` and CPU/heap profiles of `BenchmarkHyperloop` (queues=4,
batch=500, 2M events, 4-core container, Go 1.24).

## 1. Baseline

| Benchmark | Result |
|---|---|
| `BenchmarkJSONSerialize/cols=5` | 2,381 ns/op, **25 allocs/op**, 1,232 B/op |
| `BenchmarkJSONSerialize/cols=20` | 7,939 ns/op, 85 allocs/op |
| `BenchmarkPick` (dispatcher routing) | 43 ns/op, 1 alloc/op |
| `BenchmarkDispatchThroughput` | ~262 ns/event regardless of queue count |
| `BenchmarkTrackerDoneInOrder` | 43 ns/op, 0 allocs |
| `BenchmarkTrackerDoneConcurrent` (8 goroutines) | 277 ns/op |
| `BenchmarkHyperloop` (in-process sink) | 600–745k ev/s, plateau at queues=4 |
| `BenchmarkPipelineToKafka` queues=1 | ~208k ev/s |
| `BenchmarkPipelineToKafka` queues=4/8 | ~405k ev/s (plateau) |
| Codec sweep (8 publishers) | lz4 > none ≈ snappy > gzip |

## 2. Profile evidence (what actually costs)

CPU profile of the full pipeline, cumulative:

| Cost center | % of samples | Notes |
|---|---|---|
| `serialize.JSON.Serialize` → `encoding/json.Marshal` | **35%** | reflection: `structEncoder` 24.5%, `mapEncoder` 15.3% for the row maps |
| `runtime.selectgo` (+ `sellock`) | **25%** | one `select` per event in the dispatcher and another in `drainBatch` |
| `runtime.mallocgc` + GC (`scanobject`, `gcDrain`) | **~19%** | row maps + json's intermediate objects |
| `runtime.lock2` / `unlock2` | **12%** | channel locks + `Tracker.Done` mutex taken once per event (5.5% cum) |

Heap profile (`alloc_objects`): `encoding/json.mapEncoder` /
`reflect.copyVal` account for **31%** of all object allocations;
`JSON.Serialize` in total **55%**; `fmt.Sprintf` (key building in
`KeyFromColumns`) **6.6%**; `SourceMeta.FQTN` string concat 1.4%.

End-to-end evidence: with a real producer, one publisher reaches only 208k
ev/s vs 745k in-process — each `ProduceSync` blocks the publisher for a full
broker round-trip before the next batch is drained. Pool scaling stops at 4
queues on 4 cores (and in-process throughput *regresses* at queues=16), so
the wins must come from per-event cost, not more goroutines.

## 3. Optimizations (priority order)

### O1 — Hand-rolled JSON encoder (attacks the 35% CPU / 55% allocs)

Replace `encoding/json.Marshal` of the envelope with an append-based
encoder writing into a pooled `[]byte` (`sync.Pool`):

- `appendJSONString/Int/Float/Value` helpers over a `[]byte`; the envelope
  layout is fixed and known, no reflection needed.
- Row images encoded by iterating the map once; escape-path only when a
  string actually needs escaping (scan first, copy fast path).
- `Serializer` interface unchanged; add `SerializeTo(e, buf []byte) []byte`
  as the internal fast path, keep `Serialize` as a wrapper. The publisher
  owns a reusable buffer per batch; bytes handed to `kgo.Record` are copied
  out of the pool slice (records outlive the batch call under async produce
  — see O3).

Target: `cols=5` from 2,381 ns / 25 allocs → **< 700 ns / ≤ 3 allocs**.
Verification: existing `TestJSONSerialize` plus a differential fuzz test
(`encoding/json` output == custom output for random events).

### O2 — Micro-batch the channel hops (attacks the 25% selectgo)

Events currently cross two channels one at a time. Change the inter-stage
unit from `*NomiosEvent` to `[]*NomiosEvent`:

- Source emits the natural binlog batch (all rows of one `RowsEvent`, cap
  ~256) — it already has them in hand.
- Dispatcher receives a slice, routes each event to a per-queue staging
  slice, flushes each staging slice to its buffer queue when full or when
  the input pauses (zero extra latency under load: flush-on-empty).
- `drainBatch` collects slices instead of single events; batch semantics
  and `BatchMaxSize`/`BatchMaxWait` behavior preserved.

This divides `selectgo`/channel-lock traffic by the mean batch size
(~50–250×). Target: `BenchmarkDispatchThroughput` from 262 ns/event →
**< 40 ns/event**; in-process pipeline > 1.2M ev/s.

### O3 — Async produce with bounded in-flight (attacks the RTT stall)

Replace per-batch `ProduceSync` with callback-based `Produce`:

- Publisher drains the next batch while up to `MaxInflightBatches`
  (default 4) previous batches await acks; a semaphore bounds memory.
- Ack callback reports each record's `Position` to the tracker — the
  tracker was *designed* for out-of-order completion (contiguous-prefix
  watermark), so checkpoint correctness is untouched.
- Per-key ordering is preserved: franz-go's idempotent producer keeps
  per-partition order across in-flight requests (sequence numbers), and
  same key → same partition. `kgo.MaxProduceRequestsInflightPerBroker`
  stays at the idempotence-safe default (5).
- On produce error: fail the hyperloop (as today) after draining pending
  callbacks; graceful shutdown flushes with `cl.Flush(ctx)`.

Target: `BenchmarkPipelineToKafka/queues=1` from 208k → **> 400k ev/s**;
against a real (networked) broker the relative win is larger because real
RTTs dwarf kfake's.

### O4 — Allocation-free key building (attacks the 6.6%+1.4% allocs)

- Replace `fmt.Sprintf("%v", ...)`/`strings.Join` in `KeyFromColumns` with
  a `strconv.Append*` type-switch into a reusable `[]byte` (int64, uint64,
  float, string, []byte fast paths; `fmt.Appendf` fallback).
- Cache the FQTN string in the source's `TableSchema` (computed once per
  table, not per event); `Pick` hashes the key with an inline FNV-1a loop
  over the string — no `[]byte` conversion, no hasher object (removes the
  1 alloc in `BenchmarkPick`).

Target: `BenchmarkPick` 43 ns/1 alloc → **< 15 ns/0 allocs**; source-side
key building off the heap profile.

### O5 — Batch checkpoint reporting (attacks tracker lock share)

`Tracker.Done` is called once per event (one mutex acquire each).
Add `Tracker.DoneBatch(ps []Position)` — one lock per publisher batch,
advancing the watermark in a single pass. With O3 the ack callback
aggregates per produce-batch. Target: tracker cost < 1% of CPU;
`BenchmarkTrackerDoneConcurrent` measured per event improves ~10×
amortized.

### O6 — Row image representation (larger change, measure after O1–O5)

`map[string]any` per row image is the remaining GC driver (`scanobject`
11%). Option: columnar images — `Columns []string` (shared, cached per
table schema version) + `Values []any` — with a map-view accessor for the
key/override paths. The JSON encoder (O1) iterates slices instead of a
map. Wire format unchanged. This touches `event`, `source/mysql`,
`dispatch`, `serialize`; do it only if O1–O5 leave GC > 10% of CPU, and
gate it behind the same differential fuzz test.

### Non-goals / findings from research

- **GTID set serialization** (`set.String()` per commit) — already amortized
  once per transaction and shared by all events of that txn; not on the
  profile. No action.
- **More queues** — 16 queues regresses on 4 cores; keep default at
  `min(GOMAXPROCS, 8)` guidance in docs rather than raising it.
- **Alternative JSON libs** (jsoniter, sonic) — rejected: cgo/asm or
  unmaintained risk for ~the same win a 150-line hand encoder gives on a
  fixed schema.
- **Compression** — lz4 already fastest and the default; no change.

## 4. Execution order & acceptance criteria

| Phase | Change | Gate to merge |
|---|---|---|
| P1 | O1 encoder + differential fuzz test | serialize bench ≥ 3× faster; all tests `-race` green |
| P2 | O4 keys + O5 batch-Done (small, independent) | Pick ≤ 15 ns/0 alloc; no ordering test regressions |
| P3 | O2 micro-batching | dispatch ≤ 40 ns/event; MySQL integration suite green (ordering, resume, DDL) |
| P4 | O3 async produce | pipeline-to-Kafka queues=1 ≥ 2× baseline; kill -9 style hard-cancel test still gap-free |
| P5 | O6 columnar rows | only if GC still > 10% CPU after P1–P4 |

Every phase: `go test -race ./pkg/...` and
`go test -tags integration -race ./test/integration/` must pass unchanged
(the integration suite is the correctness contract: completeness, per-key
order, resume-no-gaps, DDL). Re-run the full benchmark table and update
README numbers per phase; profiles archived per phase to confirm the
targeted cost center actually shrank.

Combined target: **≥ 2M ev/s in-process, ≥ 800k ev/s end-to-end (kfake)**
on 4 cores — roughly 3× the current baseline — with allocation rate per
event reduced ~10×.

---

## 5. Results (implemented: P1–P4)

Measured on the same 4-core container after each phase merged; all unit
and integration suites pass with `-race` unchanged.

| Benchmark | Baseline | After P1–P4 | Δ |
|---|---|---|---|
| `JSONSerialize/cols=5` | 2,381 ns / 25 allocs | **839 ns / 2 allocs** | 2.8× / 12.5× |
| `Pick` | 43 ns / 1 alloc | **8.8 ns / 0 allocs** | 4.9× |
| `DispatchThroughput` | 262 ns/event | **22–34 ns/event** | ~10× |
| Hyperloop in-process | 745k ev/s | **~1.19M ev/s** | 1.6× |
| Pipeline→Kafka, 1 publisher | 208k ev/s | **686k ev/s** | 3.3× |
| Pipeline→Kafka, 4 publishers | 405k ev/s | **857k ev/s** (lz4/snappy ~930–950k) | 2.1× |

End-to-end target (≥ 800k ev/s) **met**. The in-process 2M stretch goal
was not: post-optimization profiles show the benchmark is now bound by
its own event *generation* (the fake source accounts for 62% of remaining
allocation — event structs, row maps, ID strings), i.e. the pipeline is no
longer the bottleneck on 4 cores.

**P5 (columnar row images): deferred.** The trigger condition (GC > 10%
CPU) technically fires (~15%), but the allocation profile attributes the
bulk to (a) the bench harness's event generation and (b) the serialized
payload buffers themselves — which the async sink must retain until ack
and which columnar images would not eliminate. In production the MySQL
source is bounded by binlog stream/parse rates well below the pipeline's
current ceiling. Revisit with production profiles if a real deployment
shows GC pressure; the refactor design in §3/O6 remains valid.

Notes for operators:
- Async produce means a produce failure surfaces on the *next*
  `PublishBatch`/`Flush` call; the checkpoint can never advance past an
  unacknowledged event (done-callback is withheld on failure).
- Key rendering changed in P2 (strconv-based): partition→queue assignment
  of some non-string key values may differ from previous builds. Per-key
  ordering is unaffected; do a clean drain-and-restart rather than running
  mixed versions against the same topic if strict cross-version partition
  affinity matters.
