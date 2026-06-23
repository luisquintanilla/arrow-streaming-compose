using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Compute;   // the vendored kernel: PrimitiveArray<T>.Sum/Min/Max/Mean
using Apache.Arrow.Ipc;
using ArrowStreamingCompose.Data;

namespace ArrowStreamingCompose.Streaming;

/// <summary>
/// The streaming substrate. The unit is ALWAYS a RecordBatch (a chunk of columns) — never a scalar/row — so the
/// vendored SIMD kernel still runs vectorized inside each batch. Two surfaces over the same source:
///   - PULL: IAsyncEnumerable&lt;RecordBatch&gt; (native backpressure) — the analytics/backfill default.
///   - PUSH: IObservable&lt;RecordBatch&gt; (Rx) — for live event sources (no built-in backpressure).
/// </summary>
public static class ArrowStream
{
    /// <summary>Read an Arrow IPC stream file as a pull-based batch stream.</summary>
    public static async IAsyncEnumerable<RecordBatch> ReadIpc(
        string path, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var reader = new ArrowStreamReader(fs);
        while (true)
        {
            RecordBatch? batch = await reader.ReadNextRecordBatchAsync(ct);
            if (batch is null) yield break;
            yield return batch;
        }
    }

    /// <summary>
    /// Adapt a pull stream into a push stream, optionally pacing emissions to simulate a live feed. This is a
    /// deliberately naive hot observable (it pushes as fast as it reads): exactly the shape spike S2 stresses to
    /// show that push has NO built-in backpressure.
    /// </summary>
    public static IObservable<RecordBatch> ToObservable(
        IAsyncEnumerable<RecordBatch> source, TimeSpan perBatchDelay = default)
        => new AsyncEnumerableObservable(source, perBatchDelay);

    /// <summary>Fold the contiguous <c>amount</c> column of one batch into running stats using the vendored kernel.</summary>
    public static void FoldAmount(RecordBatch batch, ref RunningStats stats)
    {
        var amount = Transactions.Amount(batch);
        if (amount.Length == 0) return;
        double sum = amount.Sum();          // SIMD via TensorPrimitives when NullCount == 0
        double min = amount.Min();
        double max = amount.Max();
        stats.Combine(sum, amount.Length, min, max);
    }

    private sealed class AsyncEnumerableObservable : IObservable<RecordBatch>
    {
        private readonly IAsyncEnumerable<RecordBatch> _source;
        private readonly TimeSpan _delay;
        public AsyncEnumerableObservable(IAsyncEnumerable<RecordBatch> source, TimeSpan delay)
        {
            _source = source; _delay = delay;
        }

        public IDisposable Subscribe(IObserver<RecordBatch> observer)
        {
            var cts = new CancellationTokenSource();
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var b in _source.WithCancellation(cts.Token))
                    {
                        if (_delay > TimeSpan.Zero) await Task.Delay(_delay, cts.Token);
                        observer.OnNext(b);   // push immediately — no awaiting the consumer (no backpressure)
                    }
                    observer.OnCompleted();
                }
                catch (OperationCanceledException) { }
                catch (Exception ex) { observer.OnError(ex); }
            }, cts.Token);
            return new Cancel(cts);
        }

        private sealed class Cancel : IDisposable
        {
            private readonly CancellationTokenSource _cts;
            public Cancel(CancellationTokenSource cts) => _cts = cts;
            public void Dispose() { _cts.Cancel(); _cts.Dispose(); }
        }
    }
}
