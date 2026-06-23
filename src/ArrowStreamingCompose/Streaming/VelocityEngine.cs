using ArrowStreamingCompose.Features;

namespace ArrowStreamingCompose.Streaming;

/// <summary>
/// THE per-card stateful velocity operator — the literal "define once, run both" artifact (spike S4). Both the
/// offline backfill (S1, pull) and the online scorer (S3, push) call <see cref="Process"/> for every transaction,
/// in step (time) order, so they compute byte-identical features and train/serve skew is structurally impossible.
///
/// Honest finding it surfaces: even the BATCH backfill needs per-key state to compute a CAUSAL window without
/// label leakage — so the streaming shape is natural for both paths, not a mismatch. What batch engines (Spark)
/// add on top is the distributed shuffle/group-by across millions of cards; here that's just an in-process
/// hashmap of small per-card windows. That boundary is exactly what spike S5 prices against DuckDB.
///
/// Window semantics (fixed, so batch == stream): for a txn of card c at step s, the window is that card's earlier
/// txns with step in [s - WindowSteps, s). Causal (past-only), time-based, no leakage.
/// </summary>
public sealed class VelocityEngine
{
    private readonly int _windowSteps;
    private readonly Dictionary<long, CardState> _cards;
    private double[] _scratch = new double[256];

    public VelocityEngine(int windowSteps, int expectedCards = 1024)
    {
        _windowSteps = windowSteps;
        _cards = new Dictionary<long, CardState>(expectedCards);
    }

    /// <summary>Number of cards currently holding window state (online memory footprint signal for S3).</summary>
    public int ActiveCards => _cards.Count;

    /// <summary>
    /// Compute the velocity features for one transaction over its causal window, into <paramref name="dest"/>
    /// (length >= <see cref="CardVelocityFeatures.Count"/>), then admit the transaction into the card's window.
    /// </summary>
    public void Process(long account, int step, double amount, Span<double> dest)
    {
        if (!_cards.TryGetValue(account, out var state))
        {
            state = new CardState();
            _cards[account] = state;
        }

        state.Evict(step - _windowSteps);

        int w = state.Count;
        if (w > _scratch.Length) Array.Resize(ref _scratch, Math.Max(w, _scratch.Length * 2));
        state.CopyAmounts(_scratch);

        CardVelocityFeatures.Compute(_scratch.AsSpan(0, w), amount, dest);

        state.Add(step, amount);
    }

    /// <summary>A card's recent (step, amount) window as parallel ring-ish queues. Kept tiny per card.</summary>
    private sealed class CardState
    {
        private int[] _steps = new int[8];
        private double[] _amounts = new double[8];
        private int _head;   // index of oldest
        private int _count;

        public int Count => _count;

        public void Add(int step, double amount)
        {
            if (_count == _steps.Length) Grow();
            int tail = (_head + _count) % _steps.Length;
            _steps[tail] = step;
            _amounts[tail] = amount;
            _count++;
        }

        public void Evict(int minStepInclusive)
        {
            while (_count > 0 && _steps[_head] < minStepInclusive)
            {
                _head = (_head + 1) % _steps.Length;
                _count--;
            }
        }

        public void CopyAmounts(double[] dest)
        {
            for (int i = 0; i < _count; i++)
                dest[i] = _amounts[(_head + i) % _amounts.Length];
        }

        private void Grow()
        {
            int n = _steps.Length * 2;
            var s = new int[n];
            var a = new double[n];
            for (int i = 0; i < _count; i++)
            {
                int src = (_head + i) % _steps.Length;
                s[i] = _steps[src];
                a[i] = _amounts[src];
            }
            _steps = s; _amounts = a; _head = 0;
        }
    }
}
