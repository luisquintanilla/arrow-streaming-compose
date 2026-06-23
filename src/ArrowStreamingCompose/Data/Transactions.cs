using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace ArrowStreamingCompose.Data;

/// <summary>One transaction event, time-ordered by <see cref="Step"/>. Schema mirrors PaySim's relevant columns,
/// plus a <see cref="Merchant"/> descriptor string (the real text field an auth carries) for the S9 text path.</summary>
public readonly record struct Txn(long Account, int Step, double Amount, sbyte IsFraud, string Merchant);

/// <summary>
/// PaySim-SHAPED transaction generator + Arrow IPC writer. The schema and feature semantics mirror the real
/// PaySim dataset (account key = nameOrig, step = hourly time unit, amount, isFraud); the VOLUME is synthesized
/// so spikes run offline and deterministically. Fraud is injected as per-account BURSTS (many large txns in a
/// short step window) so the velocity features carry real signal — i.e. a fitted model gets AUC well above 0.5.
///
/// Drop in the real PaySim CSV via data/fetch-paysim.sh when Kaggle credentials are available; the schema lines up.
/// </summary>
public static class Transactions
{
    public static Schema Schema { get; } = new Schema.Builder()
        .Field(new Field("account", Int64Type.Default, nullable: false))
        .Field(new Field("step", Int32Type.Default, nullable: false))
        .Field(new Field("amount", DoubleType.Default, nullable: false))
        .Field(new Field("isFraud", Int8Type.Default, nullable: false))
        .Field(new Field("merchant", StringType.Default, nullable: false))
        .Build();

    // Merchant-descriptor pools. Normal txns draw from everyday merchants; fraud bursts draw mostly from a small
    // set of "risky" descriptors (gift-card / crypto / wire / reload language) so a text embedding of the merchant
    // carries a learnable fraud signal — the whole point of the S9 text-similarity feature.
    private static readonly string[] NormalMerchants =
    {
        "WALMART GROCERY", "AMZN MKTP US", "STARBUCKS STORE", "SHELL OIL", "TARGET STORE",
        "COSTCO WHSE", "UBER TRIP", "NETFLIX SUBSCRIPTION", "MCDONALDS", "HOME DEPOT",
        "BEST BUY", "DELTA AIR LINES", "WHOLE FOODS MKT", "CVS PHARMACY", "SPOTIFY USA",
        "APPLE STORE", "CHIPOTLE", "TRADER JOES", "LYFT RIDE", "EXXONMOBIL",
    };

    private static readonly string[] RiskyMerchants =
    {
        "GIFTCARD SUPPLY LTD", "CRYPTO EXCHANGE GLOBAL", "WIRE TRANSFER SVC", "PREPAID RELOAD CENTER",
        "OFFSHORE BETTING CO", "INSTANT CASHOUT INC", "DIGITAL WALLET TOPUP", "ANON VPN SERVICES",
    };


    public sealed class Options
    {
        public long TargetRows = 5_000_000;
        public int Accounts = 200_000;
        public int Steps = 744;            // ~ one month of hourly steps, like PaySim
        public double FraudAccountFraction = 0.002;
        public int Seed = 12345;
    }

    /// <summary>Yields transactions in STEP (time) order, low-memory. Card txns are interleaved with others —
    /// which is exactly why velocity needs per-key windowing.</summary>
    public static IEnumerable<Txn> Generate(Options opt)
    {
        var rng = new Random(opt.Seed);
        double normalPerStep = (double)opt.TargetRows / opt.Steps;

        // Pre-schedule fraud bursts: a small set of accounts, each bursts once.
        int fraudAccounts = Math.Max(1, (int)(opt.Accounts * opt.FraudAccountFraction));
        var burstAtStep = new Dictionary<int, List<long>>();
        for (int f = 0; f < fraudAccounts; f++)
        {
            long acct = rng.Next(opt.Accounts);
            int start = rng.Next(opt.Steps);
            int len = 1 + rng.Next(3);                 // burst spans 1–3 steps
            int countPerStep = 4 + rng.Next(10);       // 4–13 txns per burst step
            for (int s = start; s < Math.Min(opt.Steps, start + len); s++)
            {
                if (!burstAtStep.TryGetValue(s, out var l)) { l = new List<long>(); burstAtStep[s] = l; }
                for (int c = 0; c < countPerStep; c++) l.Add(acct);
            }
        }

        for (int step = 0; step < opt.Steps; step++)
        {
            int k = Poisson(rng, normalPerStep);
            for (int i = 0; i < k; i++)
            {
                // Skew account selection so some accounts recur (non-trivial windows).
                double u = rng.NextDouble();
                long acct = (long)(opt.Accounts * u * u);
                double amount = LogNormal(rng, meanLog: 5.0, sigma: 1.1);   // ~ a few hundred, heavy tail
                string merchant = NormalMerchants[rng.Next(NormalMerchants.Length)];
                yield return new Txn(acct, step, Math.Round(amount, 2), 0, merchant);
            }

            if (burstAtStep.TryGetValue(step, out var fraudList))
            {
                foreach (var acct in fraudList)
                {
                    double amount = LogNormal(rng, meanLog: 7.0, sigma: 0.8); // larger tickets during fraud
                    // 85% of fraud txns hit a risky merchant; the rest look normal (so text isn't a perfect tell).
                    string merchant = rng.NextDouble() < 0.85
                        ? RiskyMerchants[rng.Next(RiskyMerchants.Length)]
                        : NormalMerchants[rng.Next(NormalMerchants.Length)];
                    yield return new Txn(acct, step, Math.Round(amount, 2), 1, merchant);
                }
            }
        }
    }

    /// <summary>Write the generated stream to an Arrow IPC stream file, one RecordBatch per <paramref name="batchSize"/>.</summary>
    public static long WriteIpc(string path, IEnumerable<Txn> txns, int batchSize = 65_536)
    {
        var alloc = new NativeMemoryAllocator();
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write);
        using var writer = new ArrowStreamWriter(fs, Schema);

        long total = 0;
        var acct = new Int64Array.Builder();
        var step = new Int32Array.Builder();
        var amount = new DoubleArray.Builder();
        var fraud = new Int8Array.Builder();
        var merchant = new StringArray.Builder();
        int n = 0;

        void Flush()
        {
            if (n == 0) return;
            var batch = new RecordBatch(Schema, new IArrowArray[]
            {
                acct.Build(alloc), step.Build(alloc), amount.Build(alloc), fraud.Build(alloc), merchant.Build(alloc),
            }, n);
            writer.WriteRecordBatch(batch);
            batch.Dispose();
            acct.Clear(); step.Clear(); amount.Clear(); fraud.Clear(); merchant.Clear();
            n = 0;
        }

        foreach (var t in txns)
        {
            acct.Append(t.Account); step.Append(t.Step); amount.Append(t.Amount); fraud.Append(t.IsFraud);
            merchant.Append(t.Merchant);
            total++;
            if (++n >= batchSize) Flush();
        }
        Flush();
        writer.WriteEnd();
        return total;
    }

    // --- Column readers (typed access to a RecordBatch by field name) ---
    public static Int64Array Account(RecordBatch b) => (Int64Array)b.Column("account");
    public static Int32Array Step(RecordBatch b) => (Int32Array)b.Column("step");
    public static DoubleArray Amount(RecordBatch b) => (DoubleArray)b.Column("amount");
    public static Int8Array IsFraud(RecordBatch b) => (Int8Array)b.Column("isFraud");
    public static StringArray Merchant(RecordBatch b) => (StringArray)b.Column("merchant");

    private static int Poisson(Random rng, double lambda)
    {
        // For small lambda use Knuth's exact algorithm; for large lambda exp(-lambda) underflows, so use the
        // normal approximation Poisson(λ) ≈ Normal(λ, λ). Volume is synthetic anyway; this keeps counts honest.
        if (lambda < 30.0)
        {
            double l = Math.Exp(-lambda);
            int k = 0; double p = 1.0;
            do { k++; p *= rng.NextDouble(); } while (p > l);
            return k - 1;
        }
        double z = Gaussian(rng);
        return Math.Max(0, (int)Math.Round(lambda + Math.Sqrt(lambda) * z));
    }

    private static double Gaussian(Random rng)
    {
        double u1 = 1.0 - rng.NextDouble();
        double u2 = 1.0 - rng.NextDouble();
        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
    }

    private static double LogNormal(Random rng, double meanLog, double sigma)
    {
        double z = Gaussian(rng);
        return Math.Exp(meanLog + sigma * z);
    }
}
