# DECISION — should a card-fraud team run its feature pipeline as a streaming/LINQ Arrow layer in .NET?

> **New here? Read [`WHY.md`](WHY.md) first.** This memo is the *what we chose*. WHY.md is the *so what*: why the
> pull/push duality, why LINQ as one query algebra, why Arrow as the shared substrate, and the G1–G7 gap map.

**Verdict: strong offline, viable-but-caveated online, and the train/serve-skew win is real.**

Expressing the velocity feature graph **once** and running it over both a pull stream (offline backfill) and a
push stream (online scoring) **eliminated train/serve skew structurally** (byte-identical features, S4), kept the
backfill's memory **~3× below** full-materialize (S1), and scored each authorization in **~0.4 µs median** (S3).
The honest limits: the online path's **per-card state has no durability/failover** (a feature store still owns
that), managed **GC produced a ~25 ms latency outlier** (S3 tail), a **decoupled** Rx push has **no demand
backpressure** so it queues unbounded unless you add a bounded buffer (S2), and **global group-by/joins still
belong in DuckDB** (S5). Adopt the managed Arrow layer for
the windowed feature pass and the define-once contract; keep DuckDB for global aggregates and add a durable state
store for online serving.

Machine: 11th Gen Intel i9-11950H, 16 threads, .NET 10.0.100. Dataset: **5,003,653** transactions, PaySim-shaped
(real schema/semantics: per-account key, hourly `step`, `amount`, `isFraud`), **volume-synthesized** for offline
determinism, fraud injected as per-account bursts (**6,788 fraud rows, 0.136%**, 200,000 accounts, 744 steps).
Numbers are illustrative of one box and single runs, not a benchmark suite. Every number comes from a run on the
named data via the `spikes/*.cs` file-based apps. The why-spikes (S8–S10) and the AI track run on a regenerated
dataset that adds a **`merchant` descriptor text column** (**5,008,756** rows; numeric-only baseline AUC **0.9544**),
so the text-feature lift in S9 is measured against that same baseline.

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

### S2 — Auth-surge robustness, and the precise truth about Rx backpressure → **GO for pull / bounded-bridge; the unbounded queue is a *decoupling* artifact, not "Rx can't throttle"**
The careful claim: **Rx.NET has no _demand-based_ (Reactive-Streams) backpressure** — an observer cannot tell the
producer to slow down. But that is not the same as "Rx always queues unbounded." With a deliberately slow scorer
(25 ms/batch), five configurations over the same stream:

| Mode | What it is | Max backlog | Dropped | Peak working set |
|---|---|---:|---:|---:|
| **pull** (`IAsyncEnumerable`) | `await foreach`, lock-step | **1 batch** | 0 | **55 MiB** |
| push (decoupled) | `ObserveOn` + async producer | 71 of 77 | 0 | 204 MiB |
| **push-sync** | synchronous `Observable.Create`, no scheduler | **1 batch** | 0 | 61 MiB |
| **push-bounded** | bounded `System.Threading.Channels` bridge (cap 4) | 6 batches | 0 | 73 MiB |
| push-lossy | Rx `Sample`/`Throttle` | n/a (dropped) | **73 of 77** | 51 MiB |

What the numbers actually say:
- **The unbounded queue is caused by _decoupling_, not by Rx push per se.** The `push` row blows up to 204 MiB
  only because we add `ObserveOn(EventLoopScheduler)` plus an async producer, which is exactly what a real live
  feed does. Take the scheduler away (`push-sync`) and **Rx is lock-step by default** — `OnNext` is a blocking
  call, so the producer can't outrun the consumer (backlog 1, flat memory), just like pull. ("Isn't there a way to
  subscribe and throttle?" Yes: stay synchronous.)
- **Rx's own flow-control operators are lossy.** `push-lossy` keeps memory flat (51 MiB) but **drops 73 of 77
  batches** (`Sample` keeps only the latest per window). Fine for telemetry; unacceptable for fraud, where every
  authorization must be scored.
- **The non-lossy fix for a decoupled pipeline is a bounded buffer, not an Rx operator.** `push-bounded` bridges
  the async producer through a bounded `Channel<RecordBatch>`; the producer `await`s `WriteAsync` when full, giving
  real backpressure across the thread boundary — **flat memory, zero loss.**

So: if the online path is a decoupled Rx push, it must add bounded buffering (`System.Threading.Channels`) or fall
back to pull (`IAsyncEnumerable`); `Subscribe` controls the subscription's *lifetime*, not its *rate*. Lossy
shedding is the only thing Rx gives you in the box, and fraud can't use it.

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

### S8 — Ergonomics: why LINQ, where (hand-rolled vs LINQ.Async vs Rx) → **LINQ for composition, kernel for the hot loop**
The same per-card velocity feature, three ways over the same stream:

| Approach | Throughput | Feature-logic LOC | Checksum |
|---|---:|---:|---|
| hand-rolled `VelocityEngine` batch loop | **1.23 M rows/s** | 16 | `B6DB07E9B0425424` |
| pull — `System.Linq.Async` `GroupBy` | 0.54 M rows/s | 34 | `B6DB07E9B0425424` |
| push — Rx `GroupBy` + `Scan` | 0.42 M rows/s | 36 | `B6DB07E9B0425424` |

All three produce **byte-identical** features (over 5,008,756 rows). LINQ is one query algebra across the pull/push
duality: the same `GroupBy(card)` pipeline reads almost identically whether the source is `IAsyncEnumerable` (pull)
or `IObservable` (push). That symmetry is the .NET-native unlock. The cost is honest: operators run **2–3× slower**
(per-element, allocating) and there is **no Arrow-batch-aware causal window operator** in the box, so you hand-roll
the window inside `Scan` (gap **G6**). Use LINQ to *compose the graph*; drop to the kernel for the throughput-critical
inner loop.

### S9 — AI-primitive composition (text feature on the same Arrow row) → **GO; the lift is real, the seam is G7**
Merchant-descriptor text → `Microsoft.ML.Tokenizers` (BertTokenizer) → ONNX Runtime (MiniLM) → mean-pool + L2-norm →
cosine similarity to fraud-cluster centroids → fused as a 6th feature → refit the logistic scorer:

| Metric | Numeric only (5 feats) | Numeric + text (6 feats) |
|---|---:|---:|
| Model AUC | 0.9544 | **0.9835** (+0.0291) |

| Nearest-cluster lookup | Time | Result |
|---|---:|---|
| `TensorPrimitives` cosine scan (contiguous `float[]`) | **33 ms** | 28/28 agree |
| vector-store-shaped object lookup (MEVD shape) | 251 ms | 28/28 agree |

The first-party AI building blocks **compose**: Tokenizers + ONNX + `System.Numerics.Tensors` snap together behind a
`Microsoft.Extensions.AI` `IEmbeddingGenerator`, and the text signal buys real accuracy. For small candidate sets a
`TensorPrimitives` scan beats reaching for a store (**~7.6×**, identical results). **Seam (G7):** MEAI/MEVD are not
Arrow-native — embeddings live in `float[]`/record objects, so you marshal out of the columnar substrate and back.
(`Microsoft.Extensions.VectorData` in-memory connector is preview-only and fought version pinning; S9 used a shim
with the same abstraction shape — itself a finding.)

### S10 — Columnar-LINQ → Arrow provider sketch (the G1 proof) → **the highest-leverage missing piece**
The same `Where(amount > 200).Sum/Max`, two ways over the same Arrow file:

| Approach | Throughput | Time | Result |
|---|---:|---:|---|
| row — `IEnumerable<T>` LINQ-to-Objects | 7.42 M rows/s | 0.68 s | identical |
| **column — `ArrowQuery` lowered to spans + kernels** | **18.10 M rows/s** | **0.28 s** | identical |

A deferred query that lowers `Where/Select/Aggregate` straight onto column spans and `TensorPrimitives` ran **2.44×
faster** than idiomatic row-LINQ on the same bytes, with **identical answers** (unfiltered count of all 5,008,756 rows:
0.11 s as one kernel pass per batch). The row path can't take it: it rehydrates a managed object and invokes a delegate
per element by construction. .NET has **no columnar LINQ provider** (gap **G1**) — building one (plus a fuller kernel
set, G2) would make this fast path the *default* path. The sketch enumerates what a real `IQueryable` Arrow provider
still needs (expression-tree parsing, predicate fusion, validity propagation, typed-kernel dispatch, GroupBy/Join
lowering, Arrow-in/Arrow-out).

---

## The gap map (G1–G7)
The honest map of where the first-party primitives stop and you hand-roll. Full version in [`WHY.md`](WHY.md).

- **G1** No columnar LINQ provider — row LINQ over Arrow is 2.44× slower than a column lowering (S10).
- **G2** Arrow .NET compute kernels are sparse (Sum/Min/Max/Mean only; no filter/take/sort/group/join) (S10, S8).
- **G3** No complete pure-managed columnar IO story (used Arrow IPC).
- **G4** No Arrow-native ingest into the embedded engine — DuckDB pays a ~1.3 s ingest tax (S5).
- **G5** ML.NET `IDataView` ↔ Arrow impedance (scoring sidesteps `IDataView` via `TensorPrimitives.Dot`).
- **G6** Rx/LINQ have no *demand* backpressure and no Arrow-batch-aware window operator; a decoupled push needs a bounded `Channel` (S2, S8).
- **G7** MEAI/MEVD don't speak Arrow — embeddings/vectors live outside the columnar substrate (S9).

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
- **Backpressure on a decoupled push path** — Rx has no *demand* backpressure, so a scheduler-decoupled producer
  queues unbounded; bound it with `System.Threading.Channels` or use pull. (A *synchronous* Rx pipeline is
  lock-step, and Rx's built-in `Sample`/`Throttle` shedding is lossy — unusable when every auth must be scored.) (S2)
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
