using System;
using System.Collections.Generic;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;

namespace ArrowStreamingCompose.Streaming;

/// <summary>
/// A deliberately small, deferred, column-at-a-time query surface over a stream of Arrow
/// <see cref="RecordBatch"/>es. It exists to make ONE point concrete (gap G1): .NET has no
/// columnar LINQ provider, so idiomatic <c>IEnumerable&lt;T&gt;</c> LINQ over Arrow forces you to
/// materialize one managed row object per element and invoke a delegate per element — which throws
/// away the contiguous, SIMD-friendly column buffers Arrow already handed you.
///
/// <para>
/// <see cref="ArrowQuery"/> instead builds a tiny plan (a list of column predicates + one projected
/// numeric column) and only executes when a terminal aggregate is awaited. Execution streams
/// batches, evaluates predicates against typed column spans (no per-row object), and folds the
/// projected column. When there are no filters the fold is a single
/// <see cref="TensorPrimitives"/> kernel call over the column's values buffer — exactly the
/// vectorized path the Apache.Arrow.Compute kernels (PR #379) expose.
/// </para>
///
/// <para>
/// This is a SKETCH, not a real provider. It does NOT parse expression trees, fuse predicates,
/// reorder a plan, propagate validity bitmaps through projections, or lower joins / group-bys.
/// <see cref="ProviderGaps"/> enumerates what a production <c>IQueryable</c> Arrow provider would
/// still need. The value here is showing that the columnar lowering is real and measurably faster
/// than row LINQ on the same bytes.
/// </para>
/// </summary>
public sealed class ArrowQuery
{
    public enum Op { GreaterThan, GreaterOrEqual, LessThan, LessOrEqual, Equal, NotEqual }

    private readonly IAsyncEnumerable<RecordBatch> _source;
    private readonly List<(string Column, Op Op, double Value)> _filters = new();
    private string? _projection;

    private ArrowQuery(IAsyncEnumerable<RecordBatch> source) => _source = source;

    /// <summary>Starts a deferred query over a batch stream. Nothing executes yet.</summary>
    public static ArrowQuery From(IAsyncEnumerable<RecordBatch> source)
        => new(source ?? throw new ArgumentNullException(nameof(source)));

    /// <summary>Adds a predicate on a numeric column. Deferred; predicates AND together.</summary>
    public ArrowQuery Where(string column, Op op, double value)
    {
        if (string.IsNullOrEmpty(column)) throw new ArgumentNullException(nameof(column));
        _filters.Add((column, op, value));
        return this;
    }

    /// <summary>Selects the numeric column the terminal aggregate reduces. Deferred.</summary>
    public ArrowQuery Select(string column)
    {
        _projection = column ?? throw new ArgumentNullException(nameof(column));
        return this;
    }

    /// <summary>Executes the plan, returning (sum, count, max) of the projected column over matching rows.</summary>
    public async Task<(double Sum, long Count, double Max)> AggregateAsync(CancellationToken ct = default)
    {
        if (_projection is null)
            throw new InvalidOperationException("Call Select(column) before a terminal aggregate.");

        double sum = 0;
        long count = 0;
        double max = double.NegativeInfinity;
        bool[]? mask = null;

        await foreach (RecordBatch batch in _source.WithCancellation(ct))
        {
            using (batch)
            {
                int rows = batch.Length;
                ReadOnlySpan<double> values = ReadDoubleColumn(batch, _projection);

                // Fast path: no predicates -> reduce the whole column with one vectorized kernel pass.
                if (_filters.Count == 0)
                {
                    if (rows == 0) continue;
                    sum += TensorPrimitives.Sum(values);
                    count += rows;
                    double batchMax = TensorPrimitives.Max(values);
                    if (batchMax > max) max = batchMax;
                    continue;
                }

                // Filtered path: build a boolean mask column-by-column (still no per-row object),
                // then fold the projection under the mask.
                if (mask is null || mask.Length < rows) mask = new bool[rows];
                Span<bool> keep = mask.AsSpan(0, rows);
                keep.Fill(true);

                foreach ((string column, Op op, double value) in _filters)
                    ApplyPredicate(batch, column, op, value, keep);

                for (int i = 0; i < rows; i++)
                {
                    if (!keep[i]) continue;
                    double v = values[i];
                    sum += v;
                    count++;
                    if (v > max) max = v;
                }
            }
        }

        if (count == 0) max = 0;
        return (sum, count, max);
    }

    public async Task<double> SumAsync(CancellationToken ct = default) => (await AggregateAsync(ct)).Sum;

    public async Task<long> CountAsync(CancellationToken ct = default) => (await AggregateAsync(ct)).Count;

    public async Task<double> MeanAsync(CancellationToken ct = default)
    {
        (double sum, long count, _) = await AggregateAsync(ct);
        return count == 0 ? 0 : sum / count;
    }

    public async Task<double> MaxAsync(CancellationToken ct = default) => (await AggregateAsync(ct)).Max;

    // --- columnar lowering helpers -------------------------------------------------------------

    private static void ApplyPredicate(RecordBatch batch, string column, Op op, double value, Span<bool> keep)
    {
        ReadOnlySpan<double> col = ReadDoubleColumn(batch, column);
        for (int i = 0; i < keep.Length; i++)
        {
            if (!keep[i]) continue; // already excluded by an earlier predicate
            keep[i] = Compare(col[i], op, value);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Compare(double a, Op op, double b) => op switch
    {
        Op.GreaterThan => a > b,
        Op.GreaterOrEqual => a >= b,
        Op.LessThan => a < b,
        Op.LessOrEqual => a <= b,
        Op.Equal => a == b,
        Op.NotEqual => a != b,
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };

    /// <summary>
    /// Reads any numeric column as a contiguous <c>double</c> span. For a true double column this is
    /// zero-copy; for integer columns it widens into a pooled buffer. A real provider would keep the
    /// native type and dispatch typed kernels instead of widening — see <see cref="ProviderGaps"/>.
    /// </summary>
    private static ReadOnlySpan<double> ReadDoubleColumn(RecordBatch batch, string column)
    {
        IArrowArray array = batch.Column(column);
        switch (array)
        {
            case DoubleArray d:
                return d.Values;
            case Int64Array l:
                return Widen(l.Values);
            case Int32Array i:
                return Widen(i.Values);
            case Int8Array s:
                return Widen(s.Values);
            default:
                throw new NotSupportedException(
                    $"Column '{column}' has type {array.Data.DataType.Name}; the sketch only lowers numeric columns.");
        }

        static double[] Widen<T>(ReadOnlySpan<T> src) where T : unmanaged
        {
            var dst = new double[src.Length];
            for (int i = 0; i < src.Length; i++) dst[i] = Convert.ToDouble(src[i]);
            return dst;
        }
    }

    /// <summary>
    /// What this sketch does NOT do, and a real columnar <c>IQueryable</c> Arrow provider would need.
    /// Printed by the S10 spike as the concrete statement of gap G1.
    /// </summary>
    public static readonly string[] ProviderGaps =
    {
        "Parse System.Linq.Expressions trees so callers write normal LINQ (q.Where(t => t.Amount > X)), not a string DSL.",
        "Fuse stacked predicates into one masked pass and push them below projections (predicate/projection pushdown).",
        "Propagate Arrow validity bitmaps through every operator so nulls follow LINQ semantics end to end.",
        "Keep native column types and dispatch typed SIMD kernels instead of widening ints to double.",
        "Lower GroupBy/Join to columnar hash/merge operators over dictionary-encoded keys, not row rehydration.",
        "Honor batch boundaries as the vectorization unit and respect backpressure from the IAsyncEnumerable source.",
        "Emit results back as Arrow (RecordBatch out, RecordBatch in) so the provider composes with the rest of the stack.",
    };
}
