#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package System.Reactive@6.0.1

// S3 — Online velocity scoring (the hard, honest one).
//
// Business step: at AUTHORIZATION time, recompute this card's velocity features over its recent window and score
// the swipe within a tight latency budget (auth round-trips are well under a few hundred ms, and the model is one
// hop in that budget). This spike confronts the per-key stateful-window problem head-on: the live auth stream is
// pushed (Rx IObservable<RecordBatch>); the SAME VelocityEngine keeps a rolling window PER CARD; each event is
// featurized (define-once) and scored by the Track-1 logistic model.
//
// Decision it informs (may be a partial NO): is in-process .NET Arrow+Rx a viable ONLINE feature path, or must
// this hand off to Flink + a feature store? Numbers: per-event scoring latency (p50/p99/p99.9), sustained
// throughput, and the per-card state footprint (the thing a feature store would otherwise own).
//
// Run:  dotnet run s3-online-scoring.cs -- ../data/transactions.arrow

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
int windowSteps = 24;

Console.WriteLine($"S3 online scoring | window={windowSteps} steps | {path}");

// --- Pass 1: fit the Track-1 scorer offline on a sample (reusing the define-once engine) ---
var scorer = await FitScorer(path, windowSteps);
Console.WriteLine($"fitted logistic scorer on {CardVelocityFeatures.Count} velocity features");

// --- Pass 2: simulate the live auth stream (push) and measure per-event score latency ---
var engine = new VelocityEngine(windowSteps);
var latenciesNs = new List<double>(6_000_000);
long scored = 0, flagged = 0;
double scoreSink = 0;
var done = new TaskCompletionSource();

var observable = ArrowStream.ToObservable(ArrowStream.ReadIpc(path));
var wall = Stopwatch.StartNew();
var sub = observable.Subscribe(
    onNext: batch =>
    {
        Span<double> f = stackalloc double[CardVelocityFeatures.Count];
        var acct = Transactions.Account(batch);
        var step = Transactions.Step(batch);
        var amt = Transactions.Amount(batch);
        for (int i = 0; i < batch.Length; i++)
        {
            long a = acct.GetValue(i)!.Value;
            int s = step.GetValue(i)!.Value;
            double amount = amt.GetValue(i)!.Value;

            // The online critical section: featurize this card's window + score. Time exactly this.
            long t0 = Stopwatch.GetTimestamp();
            engine.Process(a, s, amount, f);
            float p = scorer.Score(f);
            long t1 = Stopwatch.GetTimestamp();

            latenciesNs.Add((t1 - t0) * 1e9 / Stopwatch.Frequency);
            scoreSink += p;
            if (p > 0.5f) flagged++;
            scored++;
        }
        batch.Dispose();
    },
    onError: ex => done.SetException(ex),
    onCompleted: () => done.SetResult());

await done.Task;
sub.Dispose();
wall.Stop();

latenciesNs.Sort();
double P(double q) => latenciesNs[(int)Math.Min(latenciesNs.Count - 1, q * latenciesNs.Count)];
long stateBytes = EstimateStateBytes(engine.ActiveCards);

Console.WriteLine($"scored={scored:N0} flagged={flagged:N0} ({100.0 * flagged / scored:N2}%) sink={scoreSink:N0}");
Console.WriteLine($"throughput={scored / wall.Elapsed.TotalSeconds / 1e6:N2} M events/s (single thread)");
Console.WriteLine($"per-event score latency: p50={P(0.50):N0}ns  p99={P(0.99):N0}ns  p99.9={P(0.999):N0}ns  max={latenciesNs[^1]:N0}ns");
Console.WriteLine($"active-card state: {engine.ActiveCards:N0} cards ~ {stateBytes / 1024.0 / 1024.0:N0} MiB in-process (what a feature store would otherwise hold)");
Console.WriteLine();
Console.WriteLine("READ: latency is the per-event feature+score critical section only (not I/O). The open question");
Console.WriteLine("the memo must answer is DURABILITY/FAILOVER and cross-process sharing of this per-card state —");
Console.WriteLine("which is exactly what a feature store (Redis/Feast) provides and an in-process dictionary does not.");

static async Task<LogisticScorer> FitScorer(string path, int windowSteps)
{
    var engine = new VelocityEngine(windowSteps);
    var f = new double[CardVelocityFeatures.Count];
    var x = new List<double[]>(400_000);
    var y = new List<int>(400_000);
    var rng = new Random(7);
    long rows = 0;
    const int cap = 300_000;
    await foreach (var batch in ArrowStream.ReadIpc(path))
    {
        var acct = Transactions.Account(batch);
        var step = Transactions.Step(batch);
        var amt = Transactions.Amount(batch);
        var isf = Transactions.IsFraud(batch);
        for (int i = 0; i < batch.Length; i++)
        {
            engine.Process(acct.GetValue(i)!.Value, step.GetValue(i)!.Value, amt.GetValue(i)!.Value, f);
            rows++;
            int label = isf.GetValue(i)!.Value;
            if (x.Count < cap) { x.Add((double[])f.Clone()); y.Add(label); }
            else { long j = (long)(rng.NextDouble() * rows); if (j < cap) { x[(int)j] = (double[])f.Clone(); y[(int)j] = label; } }
        }
        batch.Dispose();
    }
    return LogisticScorer.Fit(x, y, epochs: 300, lr: 0.2f);
}

// Rough per-card state: dictionary entry + CardState object + two small arrays (start at 8 slots).
static long EstimateStateBytes(int cards) => (long)cards * (48 + 32 + (8 * (4 + 8)) + 64);
