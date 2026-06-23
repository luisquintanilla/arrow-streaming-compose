using System.Numerics.Tensors;

namespace ArrowStreamingCompose.Streaming;

/// <summary>
/// Combinable partial aggregate state. The whole point: a streaming Mean over many RecordBatches is NOT
/// "average of per-batch averages" — you must carry (sum, count) and combine. Sum is widened to double/long so
/// it cannot overflow across millions of rows. Min/Max combine trivially; Mean/Sum need this carrier.
/// </summary>
public struct SumCount
{
    public double Sum;
    public long Count;

    public static SumCount Zero => new() { Sum = 0d, Count = 0 };

    public void Add(double value)
    {
        Sum += value;
        Count++;
    }

    /// <summary>Fold one batch's partial (computed via the vendored SIMD kernel) into the running total.</summary>
    public void Combine(double batchSum, long batchCount)
    {
        Sum += batchSum;
        Count += batchCount;
    }

    public void Combine(in SumCount other)
    {
        Sum += other.Sum;
        Count += other.Count;
    }

    public readonly double Mean => Count == 0 ? double.NaN : Sum / Count;
}

/// <summary>
/// Running min/max/sum/count over a stream, fed one batch-partial at a time. Used to prove a streaming wrapper
/// produces identical results to a single-shot kernel over the whole (never-materialized) column.
/// </summary>
public struct RunningStats
{
    public SumCount SumCount;
    public double Min;
    public double Max;
    private bool _set;

    public static RunningStats Empty => new() { SumCount = SumCount.Zero, Min = double.PositiveInfinity, Max = double.NegativeInfinity, _set = false };

    public void Combine(double batchSum, long batchCount, double batchMin, double batchMax)
    {
        SumCount.Combine(batchSum, batchCount);
        if (batchCount == 0) return;
        if (!_set) { Min = batchMin; Max = batchMax; _set = true; }
        else { if (batchMin < Min) Min = batchMin; if (batchMax > Max) Max = batchMax; }
    }

    public readonly double Mean => SumCount.Mean;
    public readonly long Count => SumCount.Count;
    public readonly double Sum => SumCount.Sum;
}
