# WHY — what a streaming/LINQ Arrow layer actually unlocks in .NET

This is the conceptual companion to [`DECISION.md`](DECISION.md). The decision memo says *what we
chose* for the fraud pipeline. This document answers the harder question: *so what?* If you have
never thought about `IObservable`, LINQ, or Apache Arrow as a single design, this is written for you.
The fraud scenario is just the ground. The point is the architecture and where it fits.

---

## So what, in one paragraph

Modern .NET ships a set of foundational primitives that were built by different teams, at different
times, for different reasons: `IAsyncEnumerable` and `IObservable` for streams, LINQ as a query
algebra, `System.Numerics.Tensors` for SIMD math, Apache Arrow for columnar memory, and the new
AI building blocks (Tokenizers, ONNX Runtime, `Microsoft.Extensions.AI`, `Microsoft.Extensions.VectorData`).
Each is useful alone. The bet this experiment tests is that they **compose into one in-process data
plane** if you give them a shared substrate (Arrow) and a shared query language (LINQ). When that
works, a .NET team gets something the usual stack cannot give them: write the computation **once**,
run it over both historical and live data, at SIMD speed, without ever leaving the .NET service or
copying card data into Spark, Flink, or pandas. The spikes show this is real for the windowed-feature
core, and they show exactly where the substrate still has seams.

---

## The thing every fraud pipeline does twice

A card processor computes the same per-card velocity features two times. Once **offline**, over months
of history, to build the training table. Once **online**, on each incoming authorization, inside a tight
latency budget. Today those are two codebases on two engines (Spark for the backfill, Flink plus a
feature store for serving). Two codebases that must agree forever is the classic, expensive
**train/serve skew** bug: the model trains on one definition of "velocity" and scores on a subtly
different one, and nobody notices until the fraud rate moves.

Hold that picture. Every "why" below maps back to it.

---

## Why two stream shapes: the pull/push duality

`IAsyncEnumerable<T>` and `IObservable<T>` are not competitors. They are the two halves of one idea,
the **duality between pull and push**:

- **Pull** (`IAsyncEnumerable`): the consumer asks for the next item when it is ready. The historical
  backfill is pull. You read the file as fast as you can process it, and no faster.
- **Push** (`IObservable`): the producer hands you items as they arrive. Live authorizations are push.
  The card network decides when the next event happens, not you.

The offline job is naturally pull. The online job is naturally push. That is **why both exist**, and
why a pipeline that has to live in both worlds needs both shapes rather than picking one.

**What this unlocks, and where:** the choice is not cosmetic. It has a hard operational consequence we
measured. Under a slow consumer (an auth surge), **pull gives you backpressure for free**: the producer
cannot outrun the consumer, so memory stays flat (S2: 1 batch in flight, 53 MiB). **Push does not**:
stock Rx queues the backlog unbounded (S2: 74 of 77 batches in flight, 139 MiB, an OOM on a long enough
surge). So the duality is not just elegant. It tells you *pull belongs on the backfill, and push on the
edge only if you add bounded buffering or load-shedding*. That is a design rule you get from taking the
two shapes seriously, not from a framework.

## Why LINQ: one query algebra across both shapes

Here is the part that is easy to miss. LINQ is not "a way to filter lists." LINQ is a **query algebra**:
a small fixed set of operators (`Where`, `Select`, `GroupBy`, `SelectMany`, aggregate) that is defined
the same way over **both** pull and push. `System.Linq.Async` gives those operators over
`IAsyncEnumerable`. Rx gives the *same operator names with the same shapes* over `IObservable`.

That means you can write the per-card feature pipeline as `GroupBy(card).Window(...).Select(velocity)`
and it reads **almost identically** whether the source is the historical file (pull) or the live feed
(push). We verified this directly (S8): the hand-rolled engine, the pull-LINQ version, and the push-Rx
version all produced **byte-identical features** (FNV-1a checksum `B6DB07E9B0425424` across all three,
over 5,008,756 rows). The cross-dual symmetry is real and it is the .NET-native unlock: **one mental
model, one definition, two runtimes.** That is the structural cure for train/serve skew (S4 proves the
features are byte-identical pull vs push), and it is the reason a reviewer's instinct to reach for
`IObservable` + LINQ was worth testing.

**Where LINQ fits, honestly:** LINQ shines as the *composition surface* (declare the graph once, compose
operators, get pull/push for free). It is not free. The operator versions ran 2 to 3 times slower than
the hand-rolled batch loop (S8: 1.23 M rows/s hand-rolled vs 0.54 pull and 0.42 push), because stock
operators go one element at a time and allocate per element. And there is no Arrow-batch-aware window
operator in the box, so you still hand-roll the causal window inside a `Scan` (this is gap **G6**).
LINQ buys you readability and the pull/push symmetry. It charges you per-element overhead and the lost
batch-level SIMD granularity. For the *backfill's* throughput-critical inner loop you may still drop to
the kernel; for the *composition* of the graph, LINQ is the right altitude.

## Why Arrow: the substrate that makes the rest meet

Now the keystone. None of the above forces a particular memory layout. So why Arrow specifically?

Because **Arrow is the standard columnar substrate**, and being a standard is the whole point. A
`RecordBatch` is a set of typed, contiguous column buffers. That single fact is what lets every other
primitive plug in without a translation layer:

- The SIMD aggregation kernels (`System.Numerics.Tensors`, the kind in arrow-dotnet#379) want a
  contiguous `ReadOnlySpan<T>`. An Arrow column *is* one. Zero-copy.
- ONNX Runtime wants contiguous tensors. Arrow buffers can be pinned and handed over without a copy.
- DuckDB, Spark, Polars, and pandas all already speak Arrow, so an Arrow boundary is an
  *interoperability* boundary, not a dead end.

This is the difference between "a .NET data model" and "the data model the ecosystem already agreed on."
We lean on Arrow not to reinvent a columnar format but because **standards compound**: every tool that
speaks Arrow becomes composable for free. The whole pipeline in this repo runs `Arrow batch →
TensorPrimitives aggregates → TensorPrimitives dot → fraud score` as one cohesive SIMD layer precisely
because the column buffer never has to change shape.

**What this unlocks (S10):** because the data is already columnar, a deferred query can lower
`Where/Select/Aggregate` straight onto column spans and SIMD kernels, instead of pulling one managed
row object per element. On the same Arrow file, the same filtered `Where + Sum + Max` ran **2.44x
faster** column-at-a-time than the idiomatic LINQ-to-Objects row version (18.1 vs 7.42 M rows/s),
with **identical answers**, and an unfiltered count of all 5,008,756 rows took 0.11 s as one
`TensorPrimitives` pass per batch. The row version cannot take that path: it rehydrates a row and
invokes a delegate per element by construction.

---

## Composing .NET's AI primitives on the same substrate: what's real, what's a wash

The second half of the bet is that the **AI** primitives compose on this substrate too, so a fraud
feature can be *text-derived* (the merchant descriptor) and live next to the numeric features on the
same Arrow row. We wired the real first-party stack (S9):

`merchant string column → Microsoft.ML.Tokenizers (BertTokenizer) → ONNX Runtime (MiniLM) →
mean-pool + L2-normalize → 384-dim embedding → cosine similarity to fraud-cluster centroids →
fused as a 6th feature → refit the logistic scorer.`

**What's real and works:**
- The pieces compose. Tokenizers, ONNX Runtime, and `System.Numerics.Tensors` snap together cleanly,
  and wrapping the embedder behind `Microsoft.Extensions.AI`'s `IEmbeddingGenerator` is the right
  seam: the pipeline depends on an abstraction, not on a specific model.
- The text signal is not decorative. Adding the merchant-similarity feature lifted model **AUC from
  0.9544 (numeric only) to 0.9835** on the same sample. Composition bought real accuracy.
- For nearest-cluster lookup, a brute-force `TensorPrimitives` cosine scan over a contiguous `float[]`
  matched the vector-store result exactly (28/28 queries) and ran **~7.6x faster** (33 ms vs 251 ms).
  For small candidate sets, the standard tensor primitive beats reaching for a store.

**What's a wash or a seam (this is the honest part):**
- The AI primitives are **not Arrow-native**. Embeddings live in `float[]` / `ReadOnlyMemory<float>`,
  and the vector store wants record *objects*. So you marshal: Arrow `StringArray` column → strings →
  ONNX tensors → float arrays → store records, and back. The "one substrate" story has a real seam
  right here (gap **G7**). The columnar world and the AI-object world do not yet share a memory model.
- `Microsoft.Extensions.VectorData` in-memory connector is preview-only and fought version pinning, so
  S9 fell back to a small in-memory shim with the same abstraction shape. That friction is itself a
  finding: the abstraction exists, the production-grade local implementation does not yet.

The takeaway is not "AI in .NET is broken." It is the opposite: **the building blocks are here and they
compose**, and the only thing missing is an Arrow-native memory bridge so embeddings stop leaving the
columnar substrate.

---

## The gap map: where the .NET / AI ecosystem is still thin

Every gap below is tagged to the spike that evidences it. "Real" means we relied on it and it worked.
"Missing" means we had to hand-roll around it. This is the honest map of where the first-party
primitives stop and you are on your own.

| Gap | What's missing | Evidence | Why it matters |
|---|---|---|---|
| **G1** | No columnar **LINQ provider**. LINQ over Arrow falls back to one managed row per element. | S10: a hand-built deferred query lowered to column spans ran 2.44x faster than row LINQ on the same file, identical results. | This is the single highest-leverage gap. A real `IQueryable` Arrow provider would make the fast path the *default* path. |
| **G2** | Arrow .NET **compute kernels are sparse**. Sum/Min/Max/Mean exist (arrow-dotnet#379); filter, take, sort, group, join do not. | Used the vendored aggregation kernel throughout; had to hand-write masked folds (S10) and windows (S8). | Without a kernel library, every operator above aggregate is hand-rolled, which is exactly what blocks G1. |
| **G3** | No **pure-managed Parquet/Arrow IO** story that is first-party and complete. | We used Arrow IPC; broader columnar IO still leans on native or third-party. | A self-contained .NET service shouldn't need native glue to read its own columnar files. |
| **G4** | No **Arrow-native ingest into the embedded engine**. DuckDB is great but you pay an ingest tax. | S5: DuckDB group-by ~100 ms but ~1.3 s to ingest; managed stayed ~650 ms to 1.1 s with no copy. | The "drop to DuckDB for global aggregates" answer is real, but the Arrow→engine boundary is not free. |
| **G5** | **ML.NET `IDataView` ↔ Arrow** impedance. The ML stack has its own columnar view that is not Arrow. | Scoring here is a hand-fit logistic on `TensorPrimitives.Dot`, sidestepping `IDataView` entirely. | Two columnar models in one runtime that don't share memory is a copy and a concept tax. |
| **G6** | **Rx / LINQ have no backpressure-aware, Arrow-batch-aware window operator.** | S2: push queued unbounded under load. S8: the causal window was hand-rolled inside `Scan` in both pull and push. | The pull/push symmetry is real, but the streaming windowing that fraud features need is not in the box. |
| **G7** | **MEAI / MEVD don't speak Arrow.** Embeddings and vector records live outside the columnar substrate. | S9: constant marshaling Arrow column → strings → ONNX → `float[]` → store records; AUC lift was real but the seam was manual. | This is what stops "AI features on the same Arrow row" from being seamless. An Arrow-native embedding/vector path would close it. |

---

## The honest bottom line

What is **real** today: the pull/push duality with its backpressure consequence (S2), LINQ as one query
algebra that makes pull and push byte-identical (S8, S4), Arrow as the standard substrate that lets SIMD
kernels and ONNX share buffers with no copy (S10, S9), and a genuine accuracy lift from composing
Tokenizers + ONNX + Tensors behind the MEAI abstraction (S9, AUC 0.954 → 0.984).

What is **still missing** is plumbing, not physics: a columnar LINQ provider (G1) and a fuller kernel set
(G2) would make the fast path the default, and an Arrow-native bridge for the AI primitives (G7) would
erase the last seam. None of these are fundamental limits. Arrow already hands us the vectorizable
columns; the ecosystem just hasn't finished wiring LINQ and the AI building blocks onto them.

That is the unlock this experiment is really about: **not a fraud feature, but a single, .NET-native,
in-process data-and-AI plane built by composing standards instead of reinventing them** — and a precise
list of the seven wires that still need soldering before it is seamless.
