# DECISION — should a card-fraud team run its feature pipeline as a streaming/LINQ Arrow layer in .NET?

**Verdict: strong offline, viable-but-caveated online, and the train/serve-skew win is real.**

Expressing the velocity feature graph **once** and running it over both a pull stream (offline backfill) and a
push stream (online scoring) **eliminated train/serve skew structurally** (byte-identical features, S4), kept the
backfill's memory **~3× below** full-materialize (S1), and scored each authorization in **~0.4 µs median** (S3).
The honest limits: the online path's **per-card state has no durability/failover** (a feature store still owns
that), managed **GC produced a ~25 ms latency outlier** (S3 tail), Rx **push has no backpressure** so a surge
queues unbounded (S2), and **global group-by/joins still belong in DuckDB** (S5). Adopt the managed Arrow layer for
the windowed feature pass and the define-once contract; keep DuckDB for global aggregates and add a durable state
store for online serving.

Machine: 11th Gen Intel i9-11950H, 16 threads, .NET 10.0.100. Dataset: **5,003,653** transactions, PaySim-shaped
(real schema/semantics: per-account key, hourly `step`, `amount`, `isFraud`), **volume-synthesized** for offline
determinism, fraud injected as per-account bursts (**6,788 fraud rows, 0.136%**, 200,000 accounts, 744 steps).
Numbers are illustrative of one box and single runs, not a benchmark suite. Every number comes from a run on the
named data via the `spikes/*.cs` file-based apps.

---

## What each spike decided

### S1 — Offline feature backfill (the analytics/pull path) → **GO**
Stream the historical transactions as `IAsyncEnumerable<RecordBatch>`, compute per-account causal velocity features
via the shared `VelocityEngine`, fit the Track-1 logistic scorer — without materializing the raw dataset.

| Mode | Peak working set | Throughput | Model AUC |
|---|---:|---:|---:|
| **stream** (never materialize) | **176 MiB** | 1.4 M rows/s | 0.9515 |
| full-materialize (baseline) | 551 MiB | 1.4 M rows/s | 0.9515 |

Streaming holds peak **~3.1× lower** at the **same throughput** and **identical** model AUC (correctness parity).
AUC ≈ 0.95 confirms the velocity features carry real fraud signal, so the pipeline is meaningful, not a toy. A
.NET team can build the training feature table in-process instead of a Spark job — for the windowed-feature slice.

### S2 — Auth-surge robustness (push vs pull, slow consumer) → **GO for pull, CAUTION for push**
With a deliberately slow scorer (25 ms/batch):

| Path | Max in-flight backlog | Peak working set |
|---|---:|---:|
| **pull** (`IAsyncEnumerable`) | **1 batch** | 53 MiB |
| push (`IObservable` + `ObserveOn`) | 74 of 77 batches | 139 MiB |

Pull has **native backpressure** — the producer can't outrun the consumer, so memory stays flat through a surge.
Rx push has **no built-in backpressure**: a slow consumer lets nearly the whole dataset queue (2.6× the memory
here; an OOM on a long-enough surge). **If the online path is push, it must add bounded buffering / load-shedding.**

### S3 — Online velocity scoring (the hard one) → **VIABLE, with caveats**
Per-card rolling velocity over the live push stream, featurized (define-once) and scored by the Track-1 logistic
model. Timing is the per-event feature+score critical section only.

| Metric | Value |
|---|---:|
| p50 latency | **424 ns** |
| p99 latency | 1.28 µs |
| p99.9 latency | 8.1 µs |
| **max latency** | **25.5 ms** (single GC pause) |
| Throughput | 1.75 M events/s (single thread) |
| Active-card state | 200,000 cards ≈ 46 MiB in-process |

Median scoring is **sub-microsecond** — orders of magnitude under any auth budget. Two honest caveats decide the
real answer: (1) the **GC tail** (one ~25 ms outlier) is the managed-runtime risk an online SLA must budget for
(server GC / pre-sized state / pooling can shrink it, but it won't be zero); (2) the per-card window lives in an
**in-process dictionary with no durability, failover, or cross-process sharing** — exactly what a feature store
(Redis/Feast) provides. So: in-process .NET Arrow+Rx is a viable *compute* path for online features, but it does
**not** replace a feature store's *state-management* role.

### S4 — "Define once, run both" (the train/serve-skew test) → **GO (the headline win)**
The same `VelocityEngine` + `CardVelocityFeatures` method, run via pull (S1 shape) and via Rx push (S3 shape) over
the same step-ordered stream:

```
PULL : rows=5,003,653 checksum=481AA80E58622444
PUSH : rows=5,003,653 checksum=481AA80E58622444
RESULT: features are BYTE-IDENTICAL across pull and push => train/serve skew eliminated at the code level.
```

This is the strongest result. The single most expensive fraud-pipeline bug — offline (Spark) and online (Flink)
feature code drifting apart — is **structurally impossible** when one compiled method produces both. **Honest
caveat:** the guarantee holds because both paths feed the engine the **same step-ordered events**. If the online
path ever had to reorder, deduplicate, or approximate windows (late/out-of-order events at the real auth edge), the
byte-identical guarantee weakens to an approximate one. The define-once *code* is proven; define-once *under
real-world event disorder* is the open follow-up.

### S5 — Build-vs-buy boundary (managed vs embedded DuckDB) → **DRAW the seam**
The same global per-account group-by, in-process, two ways:

| Path | Group-by time | Ingest tax | Result |
|---|---:|---:|---|
| managed (streaming dictionary) | ~650–1,100 ms | none (reads Arrow directly) | parity |
| embedded DuckDB (SQL `GROUP BY`) | **~100 ms** | **~1.3 s** (row-by-row appender) | parity |

DuckDB's vectorized group-by is **~6–10× faster** once the data is resident — but DuckDB.NET has **no Arrow ingest
API**, so loading 5 M rows costs a **~1.3 s row-by-row tax** that erases the win for a single one-shot pass.
Aggregates match within FP summation-order tolerance (parity confirmed). **Seam:** windowed per-event features stay
in the managed Arrow/streaming layer (no shuffle, bounded memory, define-once); global group-by, dimension joins,
and the label join go to embedded DuckDB when the data is already resident or reused across many queries.

---

## The three business questions, answered

- **Does it kill train/serve skew?** **Yes, structurally** (S4), for in-order events. This is the headline reason a
  .NET fraud team would adopt it: one feature definition, provably identical in training and serving.
- **Does it shrink PCI scope?** **Plausibly yes** — the entire feature pass runs in-process .NET (Arrow →
  `TensorPrimitives` → score), so PAN/card data never leaves the existing .NET service boundary into a separate
  Python/Spark/GPU data plane. (This spike validates the *compute* claim; the PCI-scope claim is an architecture
  argument the numbers support, not a thing the spikes "measure".)
- **Does it remove Spark + Flink + feature store?** **Partially.** It can own the **windowed feature compute** for
  both backfill and serving. It does **not** replace DuckDB/Spark for global group-by/joins (S5), and it does
  **not** replace a feature store's **durable, shareable online state** (S3).

## What it does NOT solve (read this before adopting)
- **Online state durability/failover/sharing** — the in-process per-card dictionary is not a feature store (S3).
- **GC latency tail** — a ~25 ms outlier exists; an online SLA must budget for managed-GC pauses (S3).
- **Backpressure on the push path** — Rx does not provide it; a surge queues unbounded without explicit bounding (S2).
- **Global group-by / joins at scale** — slower than DuckDB once resident; hand those off (S5).
- **Event disorder** — the define-once guarantee is proven for in-order events; late/out-of-order events at the
  real auth edge are an open follow-up (S4).
- **Model realism** — the scorer is a cheap fitted logistic placeholder (Track 1); GBDT-level realism would need
  the gated ONNX seam (S7, not run).

## Generalization
The exact shape — per-key velocity/rolling aggregates computed identically offline and online — recurs in **AML**
(per-account transaction velocity), **logistics/telematics** (per-vehicle velocity/geofence), and **telecom**
(per-SIM usage velocity). The define-once + in-boundary-compute argument transfers directly; fraud is just the
deepest, most-regulated anchor.

## Recommendation
Adopt the managed Arrow streaming/LINQ layer for the **windowed feature pass** and the **define-once contract**
(S1, S4) — that is where it clearly wins. For **online serving** (S3), use it as the compute path but pair it with
a **durable state store** and budget for GC tails and explicit backpressure. Keep **embedded DuckDB** for **global
group-by/joins/label-join** (S5). Revisit the **ONNX scoring seam** (Track 2 / S7) only if GBDT-level model realism
is required — the Arrow→ONNX zero-copy bridge is already proven elsewhere and would otherwise just add deps.
