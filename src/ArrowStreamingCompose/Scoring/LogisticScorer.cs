using System.Numerics.Tensors;

namespace ArrowStreamingCompose.Scoring;

/// <summary>
/// A logistic-regression fraud scorer on the SAME numeric backbone as the kernels: scoring is
/// sigmoid(TensorPrimitives.Dot(features, weights) + bias). Weights are FIT on labeled data (cheap gradient
/// descent) — they are not hand-invented. This is "Track 1": cohesive, tiny, no ONNX/ML.NET session on the hot
/// path, so the online latency it adds (spike S3) is honest and minimal.
///
/// Features are standardized (z-score) because velocity_count and window_sum live on wildly different scales;
/// the standardizer is fit on the training set and applied identically at score time.
/// </summary>
public sealed class LogisticScorer
{
    private readonly float[] _weights;
    private readonly float _bias;
    private readonly float[] _mean;
    private readonly float[] _std;

    public int FeatureCount => _weights.Length;

    private LogisticScorer(float[] weights, float bias, float[] mean, float[] std)
    {
        _weights = weights;
        _bias = bias;
        _mean = mean;
        _std = std;
    }

    /// <summary>Score one already-standardized-internally feature vector. Returns fraud probability in [0,1].</summary>
    public float Score(ReadOnlySpan<double> rawFeatures)
    {
        Span<float> z = stackalloc float[_weights.Length];
        for (int i = 0; i < z.Length; i++)
            z[i] = ((float)rawFeatures[i] - _mean[i]) / _std[i];

        float logit = TensorPrimitives.Dot(z, _weights) + _bias;
        return 1f / (1f + MathF.Exp(-logit));
    }

    /// <summary>
    /// Fit weights with mini-batch-free full-batch gradient descent on binary cross-entropy. Small and
    /// deterministic; the point is a real fitted model over real labels, not a state-of-the-art trainer.
    /// </summary>
    public static LogisticScorer Fit(IReadOnlyList<double[]> x, IReadOnlyList<int> y, int epochs = 200, float lr = 0.1f)
    {
        if (x.Count == 0) throw new ArgumentException("No training rows.", nameof(x));
        int n = x.Count;
        int d = x[0].Length;

        // Fit standardizer.
        var mean = new float[d];
        var std = new float[d];
        for (int j = 0; j < d; j++)
        {
            double s = 0; for (int i = 0; i < n; i++) s += x[i][j];
            double m = s / n; mean[j] = (float)m;
            double v = 0; for (int i = 0; i < n; i++) { double e = x[i][j] - m; v += e * e; }
            std[j] = (float)Math.Max(Math.Sqrt(v / n), 1e-9);
        }

        // Pre-standardize into a contiguous matrix for TensorPrimitives.Dot per row.
        var z = new float[n][];
        for (int i = 0; i < n; i++)
        {
            var row = new float[d];
            for (int j = 0; j < d; j++) row[j] = ((float)x[i][j] - mean[j]) / std[j];
            z[i] = row;
        }

        var w = new float[d];
        float b = 0f;
        var grad = new float[d];

        for (int epoch = 0; epoch < epochs; epoch++)
        {
            Array.Clear(grad);
            float gb = 0f;
            for (int i = 0; i < n; i++)
            {
                float logit = TensorPrimitives.Dot(z[i].AsSpan(), w.AsSpan()) + b;
                float p = 1f / (1f + MathF.Exp(-logit));
                float err = p - y[i];
                var zi = z[i];
                for (int j = 0; j < d; j++) grad[j] += err * zi[j];
                gb += err;
            }
            float invN = 1f / n;
            for (int j = 0; j < d; j++) w[j] -= lr * grad[j] * invN;
            b -= lr * gb * invN;
        }

        return new LogisticScorer(w, b, mean, std);
    }

    /// <summary>Area under ROC, used to confirm the fitted model actually learned signal (sanity, not a benchmark).</summary>
    public static double AreaUnderRoc(IReadOnlyList<float> scores, IReadOnlyList<int> labels)
    {
        int n = scores.Count;
        var idx = Enumerable.Range(0, n).OrderBy(i => scores[i]).ToArray();
        long pos = labels.Count(l => l == 1);
        long neg = n - pos;
        if (pos == 0 || neg == 0) return double.NaN;
        // Rank-sum (Mann–Whitney) AUC.
        double rankSumPos = 0;
        for (int r = 0; r < n; r++) if (labels[idx[r]] == 1) rankSumPos += r + 1;
        return (rankSumPos - pos * (pos + 1) / 2.0) / (pos * (double)neg);
    }
}
