#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package System.Linq.Async@6.0.1
#:package System.Reactive@6.0.1

// S8 — Ergonomics: why LINQ / why IObservable, and where.
//
// Business step: a fraud team wants one .NET-native query shape for both historical backfills and live scoring.
// This spike computes the SAME per-card 24-step velocity feature three ways:
//   - HAND : the explicit VelocityEngine batch loop (baseline; keeps RecordBatch/SIMD granularity)
//   - PULL : System.Linq.Async over IAsyncEnumerable rows (bulk/backfill shape)
//   - PUSH : Rx over IObservable rows (live-edge shape)
//
// Decision it informs: LINQ is the single query algebra across the pull/push duality. GroupBy(account) plus a
// per-key pipeline reads almost identically for IAsyncEnumerable and IObservable. Honest caveat / G6: stock
// LINQ/Rx do NOT provide an Arrow-batch-aware causal window operator, so the per-key window is still hand-rolled
// inside the per-key pipeline; flattening to rows also gives up the RecordBatch SIMD granularity the baseline keeps.
//
// Run:  dotnet run s8-ergonomics.cs -- ../data/transactions.arrow

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Features;
using ArrowStreamingCompose.Streaming;
using System.Reactive.Disposables;
using System.Reactive.Linq;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
const int WindowSteps = 24;

Console.WriteLine($"S8 ergonomics | window={WindowSteps} steps | {path}");
Console.WriteLine();

var hand = await RunHandRolled(path, WindowSteps);
PrintRun(hand);

var pull = await RunPullLinq(path, WindowSteps);
PrintRun(pull);

var push = await RunPushRx(path, WindowSteps);
PrintRun(push);

bool pass =
    hand.Rows == pull.Rows && hand.Rows == push.Rows &&
    hand.Checksum == pull.Checksum && hand.Checksum == push.Checksum;

Console.WriteLine();
Console.WriteLine("feature logic LOC (approx):");
Console.WriteLine("  hand-rolled VelocityEngine batch loop : 16");
Console.WriteLine("  pull LINQ GroupBy + per-key helper    : 34");
Console.WriteLine("  push Rx GroupBy + Scan helper         : 36");
Console.WriteLine();
Console.WriteLine($"checksum: {(pass ? "PASS" : "FAIL")} (rows={hand.Rows:N0}, fnv1a={hand.Checksum:X16})");
Console.WriteLine();
Console.WriteLine("Conclusion:");
Console.WriteLine("  LINQ is useful where the feature graph is query-shaped: GroupBy(account) + per-key composition is the same idea in pull and push.");
Console.WriteLine("  IAsyncEnumerable fits bulk backfill because the consumer pulls batches with natural backpressure.");
Console.WriteLine("  IObservable/Rx fits the live edge because sources push events and Rx gives the same GroupBy/SelectMany/Scan vocabulary.");
Console.WriteLine("  G6: stock LINQ/Rx did not give an Arrow-batch-aware causal 24-step window, so the per-key window state is still hand-rolled.");
Console.WriteLine("  The operator paths gain composability and pull/push symmetry, but pay per-row allocation/dispatch and lose the SIMD batch path.");

if (!pass)
    throw new InvalidOperationException("S8 checksum mismatch: operator feature output was not byte-identical to VelocityEngine.");

static async Task<RunResult> RunHandRolled(string path, int windowSteps)
{
    var sw = Stopwatch.StartNew();
    var engine = new VelocityEngine(windowSteps);
    var featureBuffer = new double[CardVelocityFeatures.Count];
    var result = new ChecksumResult();

    await foreach (var batch in ArrowStream.ReadIpc(path))
    {
        try
        {
            var account = Transactions.Account(batch);
            var step = Transactions.Step(batch);
            var amount = Transactions.Amount(batch);
            for (int i = 0; i < batch.Length; i++)
            {
                engine.Process(account.GetValue(i)!.Value, step.GetValue(i)!.Value, amount.GetValue(i)!.Value, featureBuffer);
                result.Admit(featureBuffer);
            }
        }
        finally
        {
            batch.Dispose();
        }
    }

    sw.Stop();
    return new RunResult("HAND VelocityEngine", result.Rows, result.Checksum, sw.Elapsed);
}

static async Task<RunResult> RunPullLinq(string path, int windowSteps)
{
    var sw = Stopwatch.StartNew();
    var rowCounter = new RowCounter();
    var ordered = new OrderedFeatureCollector();

    // The LINQ-shaped expression is the point: group by key, then run a per-key feature pipeline.
    // G6 remains visible because the causal window itself is a helper, not a stock Arrow-aware operator.
    var groups = PullRows(path, rowCounter).GroupBy(static row => row.Account);
    await foreach (var group in groups)
    {
        await foreach (var feature in ComputePullGroup(group, windowSteps))
            ordered.Admit(feature);
    }

    var result = ordered.ToChecksumResult();
    sw.Stop();
    return new RunResult("PULL LINQ.Async", result.Rows, result.Checksum, sw.Elapsed);
}

static async Task<RunResult> RunPushRx(string path, int windowSteps)
{
    var sw = Stopwatch.StartNew();
    var rowCounter = new RowCounter();
    var ordered = new OrderedFeatureCollector();
    var done = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

    // This is intentionally near-isomorphic to the pull query above: flatten, GroupBy(account),
    // per-key Scan with the same hand-rolled causal window helper, then merge.
    var query =
        ArrowStream.ToObservable(ArrowStream.ReadIpc(path))
            .SelectMany(batch => PushRows(batch, rowCounter))
            .GroupBy(static row => row.Account)
            .SelectMany(group =>
                group.Scan(
                        new RxWindowScan(windowSteps),
                        static (scan, row) =>
                        {
                            scan.Last = scan.Window.Process(row);
                            return scan;
                        })
                    .Select(static scan => scan.Last));

    using var subscription = query.Subscribe(
        onNext: ordered.Admit,
        onError: ex => done.TrySetException(ex),
        onCompleted: () => done.TrySetResult(null));

    await done.Task;

    var result = ordered.ToChecksumResult();
    sw.Stop();
    return new RunResult("PUSH Rx", result.Rows, result.Checksum, sw.Elapsed);
}

static async IAsyncEnumerable<TxnRow> PullRows(
    string path,
    RowCounter counter,
    [EnumeratorCancellation] CancellationToken ct = default)
{
    await foreach (var batch in ArrowStream.ReadIpc(path, ct).WithCancellation(ct))
    {
        try
        {
            var account = Transactions.Account(batch);
            var step = Transactions.Step(batch);
            var amount = Transactions.Amount(batch);
            for (int i = 0; i < batch.Length; i++)
            {
                yield return new TxnRow(
                    counter.Next(),
                    account.GetValue(i)!.Value,
                    step.GetValue(i)!.Value,
                    amount.GetValue(i)!.Value);
            }
        }
        finally
        {
            batch.Dispose();
        }
    }
}

static IObservable<TxnRow> PushRows(RecordBatch batch, RowCounter counter)
    => Observable.Create<TxnRow>(observer =>
    {
        try
        {
            var account = Transactions.Account(batch);
            var step = Transactions.Step(batch);
            var amount = Transactions.Amount(batch);
            for (int i = 0; i < batch.Length; i++)
            {
                observer.OnNext(new TxnRow(
                    counter.Next(),
                    account.GetValue(i)!.Value,
                    step.GetValue(i)!.Value,
                    amount.GetValue(i)!.Value));
            }
            observer.OnCompleted();
        }
        catch (Exception ex)
        {
            observer.OnError(ex);
        }
        finally
        {
            batch.Dispose();
        }

        return Disposable.Empty;
    });

static async IAsyncEnumerable<FeatureBits> ComputePullGroup(IAsyncEnumerable<TxnRow> rows, int windowSteps)
{
    var window = new PerKeyWindow(windowSteps);
    await foreach (var row in rows)
        yield return window.Process(row);
}

static void PrintRun(RunResult r)
{
    Console.WriteLine(
        $"{r.Name,-19} rows={r.Rows,12:N0} checksum={r.Checksum:X16} " +
        $"time={r.Elapsed.TotalSeconds,7:N2}s throughput={r.ThroughputMRowsPerSecond,6:N2} M rows/s");
}

sealed class PerKeyWindow
{
    private readonly int _windowSteps;
    private int[] _steps = new int[8];
    private double[] _amounts = new double[8];
    private double[] _scratch = new double[256];
    private readonly double[] _features = new double[CardVelocityFeatures.Count];
    private int _head;
    private int _count;

    public PerKeyWindow(int windowSteps) => _windowSteps = windowSteps;

    public FeatureBits Process(TxnRow row)
    {
        Evict(row.Step - _windowSteps);

        if (_count > _scratch.Length)
            System.Array.Resize(ref _scratch, Math.Max(_count, _scratch.Length * 2));

        for (int i = 0; i < _count; i++)
            _scratch[i] = _amounts[(_head + i) % _amounts.Length];

        CardVelocityFeatures.Compute(_scratch.AsSpan(0, _count), row.Amount, _features);
        var bits = FeatureBits.From(row.Ordinal, _features);
        Add(row.Step, row.Amount);
        return bits;
    }

    private void Add(int step, double amount)
    {
        if (_count == _steps.Length)
            Grow();

        int tail = (_head + _count) % _steps.Length;
        _steps[tail] = step;
        _amounts[tail] = amount;
        _count++;
    }

    private void Evict(int minStepInclusive)
    {
        while (_count > 0 && _steps[_head] < minStepInclusive)
        {
            _head = (_head + 1) % _steps.Length;
            _count--;
        }
    }

    private void Grow()
    {
        int nextLength = _steps.Length * 2;
        var steps = new int[nextLength];
        var amounts = new double[nextLength];
        for (int i = 0; i < _count; i++)
        {
            int source = (_head + i) % _steps.Length;
            steps[i] = _steps[source];
            amounts[i] = _amounts[source];
        }

        _steps = steps;
        _amounts = amounts;
        _head = 0;
    }
}

sealed class RxWindowScan
{
    public RxWindowScan(int windowSteps) => Window = new PerKeyWindow(windowSteps);
    public PerKeyWindow Window { get; }
    public FeatureBits Last { get; set; }
}

sealed class OrderedFeatureCollector
{
    private readonly List<FeatureBits> _features = new();

    public void Admit(FeatureBits feature) => _features.Add(feature);

    public ChecksumResult ToChecksumResult()
    {
        _features.Sort(static (left, right) => left.Ordinal.CompareTo(right.Ordinal));
        var result = new ChecksumResult();
        foreach (var feature in _features)
            result.Admit(feature);
        return result;
    }
}

sealed class ChecksumResult
{
    private const ulong OffsetBasis = 14695981039346656037UL;
    private const ulong Prime = 1099511628211UL;

    public long Rows { get; private set; }
    public ulong Checksum { get; private set; } = OffsetBasis;

    public void Admit(ReadOnlySpan<double> features)
    {
        for (int i = 0; i < features.Length; i++)
            AdmitBits(BitConverter.DoubleToInt64Bits(features[i]));
        Rows++;
    }

    public void Admit(FeatureBits feature)
    {
        AdmitBits(feature.B0);
        AdmitBits(feature.B1);
        AdmitBits(feature.B2);
        AdmitBits(feature.B3);
        AdmitBits(feature.B4);
        Rows++;
    }

    private void AdmitBits(long bits)
        => Checksum = (Checksum ^ (ulong)bits) * Prime;
}

sealed class RowCounter
{
    private long _next;
    public long Next() => _next++;
}

readonly record struct TxnRow(long Ordinal, long Account, int Step, double Amount);

readonly record struct FeatureBits(long Ordinal, long B0, long B1, long B2, long B3, long B4)
{
    public static FeatureBits From(long ordinal, ReadOnlySpan<double> features)
        => new(
            ordinal,
            BitConverter.DoubleToInt64Bits(features[0]),
            BitConverter.DoubleToInt64Bits(features[1]),
            BitConverter.DoubleToInt64Bits(features[2]),
            BitConverter.DoubleToInt64Bits(features[3]),
            BitConverter.DoubleToInt64Bits(features[4]));
}

readonly record struct RunResult(string Name, long Rows, ulong Checksum, TimeSpan Elapsed)
{
    public double ThroughputMRowsPerSecond => Rows / Elapsed.TotalSeconds / 1_000_000.0;
}
