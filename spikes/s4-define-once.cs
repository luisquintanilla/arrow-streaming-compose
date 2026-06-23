#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package System.Reactive@6.0.1

// S4 — "Define once, run both" (the train/serve-skew test).
//
// The single most expensive fraud-pipeline bug is train/serve SKEW: the offline feature code (Spark SQL) and the
// online feature code (Flink/Java) are written separately and silently drift, degrading the model. This spike
// runs the EXACT SAME feature definition (VelocityEngine + CardVelocityFeatures) two ways:
//   - PULL  : IAsyncEnumerable<RecordBatch>  (the S1 offline/backfill shape)
//   - PUSH  : IObservable<RecordBatch> via Rx (the S3 online shape)
// ...over the same step-ordered stream, and asserts the emitted feature vectors are BYTE-IDENTICAL.
//
// Decision it informs: does expressing the feature graph once eliminate skew structurally? If the checksums match,
// yes — the same compiled method produced training and serving features. Honest caveat (reported below): this
// holds because BOTH paths feed the engine the same step-ordered events; if the online path had to reorder or
// approximate windows, the guarantee would weaken — that's the open risk S3 probes.
//
// Run:  dotnet run s4-define-once.cs -- ../data/transactions.arrow

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Features;
using ArrowStreamingCompose.Streaming;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
int windowSteps = 24;

Console.WriteLine($"S4 define-once | window={windowSteps} steps | {path}");

var pull = await RunPull(path, windowSteps);
Console.WriteLine($"PULL : rows={pull.Rows:N0} checksum={pull.Checksum:X16}");
PrintSamples("PULL ", pull.Samples);

var push = await RunPush(path, windowSteps);
Console.WriteLine($"PUSH : rows={push.Rows:N0} checksum={push.Checksum:X16}");
PrintSamples("PUSH ", push.Samples);

bool identical = pull.Rows == push.Rows && pull.Checksum == push.Checksum;
Console.WriteLine();
Console.WriteLine(identical
    ? "RESULT: features are BYTE-IDENTICAL across pull and push => train/serve skew eliminated at the code level."
    : "RESULT: MISMATCH => the same definition produced different features; skew NOT eliminated. Investigate.");

static async Task<Result> RunPull(string path, int windowSteps)
{
    var engine = new VelocityEngine(windowSteps);
    var f = new double[CardVelocityFeatures.Count];
    var r = new Result();
    await foreach (var batch in ArrowStream.ReadIpc(path))
    {
        var acct = Transactions.Account(batch);
        var step = Transactions.Step(batch);
        var amt = Transactions.Amount(batch);
        for (int i = 0; i < batch.Length; i++)
        {
            engine.Process(acct.GetValue(i)!.Value, step.GetValue(i)!.Value, amt.GetValue(i)!.Value, f);
            r.Admit(f);
        }
        batch.Dispose();
    }
    return r;
}

static async Task<Result> RunPush(string path, int windowSteps)
{
    var engine = new VelocityEngine(windowSteps);
    var f = new double[CardVelocityFeatures.Count];
    var r = new Result();
    var done = new TaskCompletionSource();
    var observable = ArrowStream.ToObservable(ArrowStream.ReadIpc(path));

    var sub = observable.Subscribe(
        onNext: batch =>
        {
            var acct = Transactions.Account(batch);
            var step = Transactions.Step(batch);
            var amt = Transactions.Amount(batch);
            for (int i = 0; i < batch.Length; i++)
            {
                engine.Process(acct.GetValue(i)!.Value, step.GetValue(i)!.Value, amt.GetValue(i)!.Value, f);
                r.Admit(f);
            }
            batch.Dispose();
        },
        onError: ex => done.SetException(ex),
        onCompleted: () => done.SetResult());

    await done.Task;
    sub.Dispose();
    return r;
}

static void PrintSamples(string tag, List<double[]> samples)
{
    for (int s = 0; s < samples.Count; s++)
        Console.WriteLine($"  {tag} sample[{s}] = [{string.Join(", ", System.Array.ConvertAll(samples[s], v => v.ToString("0.##")))}]");
}

sealed class Result
{
    public long Rows;
    public ulong Checksum = 14695981039346656037UL;   // FNV-1a offset basis; order-sensitive over feature bits
    public readonly List<double[]> Samples = new();
    private long _next;

    public void Admit(ReadOnlySpan<double> f)
    {
        for (int i = 0; i < f.Length; i++)
        {
            ulong bits = (ulong)BitConverter.DoubleToInt64Bits(f[i]);
            Checksum = (Checksum ^ bits) * 1099511628211UL;
        }
        if (Rows == _next && Samples.Count < 3) { Samples.Add(f.ToArray()); _next += 1_000_000; }
        Rows++;
    }
}
