# arrow-streaming-compose

A throwaway **validation experiment**: can a streaming / LINQ composition layer over [Apache Arrow .NET](https://github.com/apache/arrow-dotnet)
SIMD kernels serve a real **card-fraud feature pipeline** entirely in-process in .NET — killing **train/serve skew**
and keeping card data inside the .NET service boundary?

It does **not** ship a framework. It runs five small spikes against one concrete business process and produces an
honest [`results/DECISION.md`](results/DECISION.md): where this layer helps, where it doesn't, and where it must
hand off to a real engine.

> Origin: a reviewer of [apache/arrow-dotnet#379](https://github.com/apache/arrow-dotnet/pull/379) (a SIMD
> aggregation-kernel PR) asked *"what if this was IObservable and projected into some fancy LINQ?"* This repo tests
> that idea on a real workload instead of assuming it's a win. **PR #379 is untouched** — the aggregation kernel is
> [vendored standalone](src/ArrowStreamingCompose/Kernels/VENDORED.md) so this experiment has zero dependency on it.

## The scenario

A card issuer / payment processor computes the **same per-card velocity features twice**: offline to build the
training table (today: Spark), and online to score each authorization in a tight latency budget (today: Flink + a
feature store). Writing those two paths separately is the classic, expensive **train/serve skew** bug. The
hypothesis: express the feature graph **once** in LINQ over Arrow `RecordBatch` streams and run it **both** ways,
in-process, at SIMD speed.

## One numeric backbone

Everything runs in .NET on **`System.Numerics.Tensors` (`TensorPrimitives`)** end to end — the vendored aggregation
kernels, the velocity features, and the logistic model scorer — so the whole pipeline is one cohesive SIMD layer:
`Arrow batch → TensorPrimitives aggregates → TensorPrimitives dot → fraud score`.

## Layout

```
src/ArrowStreamingCompose/      # the small shared library
  Kernels/Aggregations.cs       #   vendored SIMD Sum/Min/Max/Mean (copy of arrow-dotnet#375)
  Features/CardVelocityFeatures #   THE single feature definition (define-once)
  Streaming/VelocityEngine.cs   #   per-card stateful window — the literal define-once artifact (S1 == S3)
  Streaming/ArrowStream.cs      #   IPC → IAsyncEnumerable (pull) + IObservable (push, Rx)
  Streaming/PartialAggregates   #   combinable streaming aggregates
  Scoring/LogisticScorer.cs     #   Track-1 scorer: sigmoid(TensorPrimitives.Dot(features,w)+b), weights FIT
  Data/Transactions.cs          #   PaySim-shaped generator + Arrow IPC writer
spikes/                         # .NET 10 file-based apps (dotnet run x.cs)
  gen-data.cs                   #   produce the offline Arrow IPC dataset
  s1-offline-backfill.cs        #   S1: streaming backfill vs full-materialize
  s2-push-vs-pull.cs            #   S2: backpressure under a slow consumer
  s3-online-scoring.cs          #   S3: per-event latency p50/p99 + state footprint
  s4-define-once.cs             #   S4: byte-identical features pull vs push (skew test)
  s5-build-vs-buy.cs            #   S5: managed group-by vs embedded DuckDB
data/                           # generated/fetched data (gitignored; see fetch-paysim.sh)
results/DECISION.md             # S6: the honest decision memo
```

## Run it

Requires the .NET 10 SDK.

```bash
# 1. generate the offline dataset (~5M PaySim-shaped rows -> data/transactions.arrow)
cd spikes
dotnet run gen-data.cs -- 5000000 ../data/transactions.arrow

# 2. the spikes (each prints its own decision + numbers)
dotnet run s1-offline-backfill.cs -- ../data/transactions.arrow stream
dotnet run s1-offline-backfill.cs -- ../data/transactions.arrow materialize
dotnet run s2-push-vs-pull.cs     -- ../data/transactions.arrow pull
dotnet run s2-push-vs-pull.cs     -- ../data/transactions.arrow push
dotnet run s3-online-scoring.cs   -- ../data/transactions.arrow
dotnet run s4-define-once.cs      -- ../data/transactions.arrow
dotnet run s5-build-vs-buy.cs     -- ../data/transactions.arrow
```

The spikes run on a **PaySim-shaped, volume-synthesized** dataset by default (real schema and feature semantics,
synthesized volume, labeled as such — no fabricated numbers). To validate against the real distribution, drop the
actual [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) CSV in via [`data/fetch-paysim.sh`](data/fetch-paysim.sh);
the schema lines up.

## Headline findings

See [`results/DECISION.md`](results/DECISION.md) for the full memo. In short, on one box / single runs:

- **Train/serve skew → eliminated** (S4): the same compiled feature method yields **byte-identical** features in
  the pull (offline) and push (online) paths.
- **Offline backfill → go** (S1): streaming holds peak memory **~3× below** full-materialize at the same throughput
  and identical model AUC (≈ 0.95).
- **Online scoring → viable, caveated** (S3): **~0.4 µs median** per-event score, but a GC tail (~25 ms outlier)
  and no durable/shared per-card state — it's a compute path, not a feature store.
- **Surge robustness** (S2): pull has native backpressure (flat memory); Rx push does not (unbounded queue).
- **Build-vs-buy** (S5): global group-by/joins still belong in embedded DuckDB; windowed features stay managed.

## Scope

This is an experiment, not a product. The model is a cheap fitted logistic placeholder; the data is volume-
synthesized; the numbers are illustrative of one machine. The point is the **decision**, on real-shaped data, for
each step of the fraud feature pipeline.
