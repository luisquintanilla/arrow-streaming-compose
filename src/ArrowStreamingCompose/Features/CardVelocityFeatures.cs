using System.Numerics.Tensors;

namespace ArrowStreamingCompose.Features;

/// <summary>
/// THE single, canonical card-velocity feature definition. This is the heart of the "define once, run both"
/// claim (spike S4 / train-serve skew): the SAME method is called by the offline batch backfill (S1) and the
/// online live scorer (S3). If both paths feed it identical windows, they get byte-identical features.
///
/// Given a card's recent window of transaction amounts (most-recent-N, already selected by the caller) and the
/// current transaction amount, it produces a fixed-length feature vector. Aggregates use System.Numerics.Tensors
/// (TensorPrimitives) — the same SIMD backbone as the vendored kernel and the scorer.
/// </summary>
public static class CardVelocityFeatures
{
    /// <summary>Number of features emitted. Keep in sync with <see cref="Names"/> and <see cref="Compute"/>.</summary>
    public const int Count = 5;

    public static readonly string[] Names =
    {
        "velocity_count",   // how many txns in the window — the classic velocity signal
        "window_sum",       // rolling spend in the window
        "window_mean",      // average ticket in the window
        "window_max",       // largest ticket in the window
        "amount_over_mean", // current amount relative to the card's recent average (spike detector)
    };

    /// <summary>
    /// Compute the feature vector into <paramref name="dest"/> (length >= <see cref="Count"/>).
    /// <paramref name="windowAmounts"/> is the card's recent history (excluding the current txn);
    /// <paramref name="currentAmount"/> is the txn being scored/backfilled.
    /// </summary>
    public static void Compute(ReadOnlySpan<double> windowAmounts, double currentAmount, Span<double> dest)
    {
        double count = windowAmounts.Length;
        double sum = windowAmounts.Length == 0 ? 0d : TensorPrimitives.Sum(windowAmounts);
        double mean = windowAmounts.Length == 0 ? 0d : sum / windowAmounts.Length;
        double max = windowAmounts.Length == 0 ? 0d : TensorPrimitives.Max(windowAmounts);
        double ratio = mean <= 0d ? 1d : currentAmount / mean;

        dest[0] = count;
        dest[1] = sum;
        dest[2] = mean;
        dest[3] = max;
        dest[4] = ratio;
    }

    public static double[] Compute(ReadOnlySpan<double> windowAmounts, double currentAmount)
    {
        var dest = new double[Count];
        Compute(windowAmounts, currentAmount, dest);
        return dest;
    }
}
