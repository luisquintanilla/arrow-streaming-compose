#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0

// S1 — Offline feature backfill (the analytics/pull path).
//
// Business step: a fraud team builds the TRAINING feature table over months of historical authorizations. Here we
// stream the transactions Arrow IPC file as IAsyncEnumerable<RecordBatch> (native backpressure), compute per-card
// causal velocity features via the SHARED VelocityEngine (the same code S3 runs online), and fit the Track-1
// logistic scorer on the labels — WITHOUT ever holding the whole raw dataset in memory.
//
// Decision it informs: can a .NET team build the training feature table in-process instead of a Spark job?
// Numbers: throughput, peak working set (stream vs full-materialize), and a model AUC that proves the velocity
// features carry real signal (so the pipeline is meaningful, not a toy).
//
// Run BOTH modes (separate processes => clean PeakWorkingSet64):
//   dotnet run s1-offline-backfill.cs -- ../data/transactions.arrow stream
//   dotnet run s1-offline-backfill.cs -- ../data/transactions.arrow materialize

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Features;
using ArrowStreamingCompose.Scoring;
using ArrowStreamingCompose.Streaming;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
string mode = args.Length > 1 ? args[1] : "stream";
const int WindowSteps = 24;          // "this card's activity in the last ~24 hourly steps"
const int ReservoirCap = 300_000;    // cap the in-memory training set; the rest only updates counters

Console.WriteLine($"S1 backfill | mode={mode} | window={WindowSteps} steps | {path}");

var engine = new VelocityEngine(WindowSteps);
var featBuf = new double[CardVelocityFeatures.Count];

// Capped reservoir of (features,label) for fitting — keeps memory bounded even on huge inputs.
var sampleX = new List<double[]>(ReservoirCap);
var sampleY = new List<int>(ReservoirCap);
var rng = new Random(7);
long rows = 0, fraud = 0;

var sw = Stopwatch.StartNew();

if (mode == "materialize")
{
    // BASELINE: load every transaction into a list first (what "just pull it all into memory" looks like).
    var all = new List<Txn>();
    await foreach (var batch in ArrowStream.ReadIpc(path))
    {
        var acct = Transactions.Account(batch);
        var step = Transactions.Step(batch);
        var amt = Transactions.Amount(batch);
        var isf = Transactions.IsFraud(batch);
        for (int i = 0; i < batch.Length; i++)
            all.Add(new Txn(acct.GetValue(i)!.Value, step.GetValue(i)!.Value, amt.GetValue(i)!.Value, isf.GetValue(i)!.Value));
        batch.Dispose();
    }
    foreach (var t in all)
    {
        engine.Process(t.Account, t.Step, t.Amount, featBuf);
        Admit(featBuf, t.IsFraud);
    }
}
else
{
    // STREAMING: one batch in flight at a time; features emitted as we go; raw input never fully materialized.
    await foreach (var batch in ArrowStream.ReadIpc(path))
    {
        var acct = Transactions.Account(batch);
        var step = Transactions.Step(batch);
        var amt = Transactions.Amount(batch);
        var isf = Transactions.IsFraud(batch);
        for (int i = 0; i < batch.Length; i++)
        {
            engine.Process(acct.GetValue(i)!.Value, step.GetValue(i)!.Value, amt.GetValue(i)!.Value, featBuf);
            Admit(featBuf, isf.GetValue(i)!.Value);
        }
        batch.Dispose();
    }
}

sw.Stop();

// Fit the Track-1 scorer on the sampled feature table and confirm it learned signal.
var scorer = LogisticScorer.Fit(sampleX, sampleY, epochs: 300, lr: 0.2f);
var scores = new float[sampleX.Count];
for (int i = 0; i < sampleX.Count; i++) scores[i] = scorer.Score(sampleX[i]);
double auc = LogisticScorer.AreaUnderRoc(scores, sampleY);

long peak = Process.GetCurrentProcess().PeakWorkingSet64;
Console.WriteLine($"rows={rows:N0} fraud={fraud:N0} ({100.0 * fraud / rows:N3}%) activeCards={engine.ActiveCards:N0}");
Console.WriteLine($"throughput={rows / sw.Elapsed.TotalSeconds / 1e6:N2} M rows/s in {sw.Elapsed.TotalSeconds:N2}s");
Console.WriteLine($"peakWorkingSet={peak / 1024.0 / 1024.0:N0} MiB | trainSample={sampleX.Count:N0}");
Console.WriteLine($"model AUC={auc:N4}  (>0.5 means velocity features carry fraud signal)");

void Admit(ReadOnlySpan<double> f, sbyte label)
{
    rows++;
    if (label == 1) fraud++;
    // Reservoir sampling (Algorithm R): keep a representative, bounded training set across ALL steps.
    if (sampleX.Count < ReservoirCap)
    {
        sampleX.Add(f.ToArray());
        sampleY.Add(label);
    }
    else
    {
        long j = (long)(rng.NextDouble() * rows);
        if (j < ReservoirCap)
        {
            sampleX[(int)j] = f.ToArray();
            sampleY[(int)j] = label;
        }
    }
}
