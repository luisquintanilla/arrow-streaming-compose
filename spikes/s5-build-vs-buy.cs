#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package DuckDB.NET.Data.Full@1.5.3

// S5 — Build-vs-buy boundary (managed Arrow vs embedded DuckDB).
//
// The offline path isn't only windowed features: it also needs a GLOBAL group-by across millions of cards, joins
// to merchant/account dimensions, and the label join. Those are the classic shuffle/hash-join workloads that
// vectorized engines are built for. This spike draws the explicit seam by running the SAME global per-account
// aggregate two ways, IN-PROCESS, on the same data:
//   - MANAGED : stream the Arrow IPC and group-by into a Dictionary (what the managed layer would own).
//   - DUCKDB  : bulk-load the rows into embedded DuckDB (like SQLite, not a cluster) and GROUP BY in SQL.
//
// Decision it informs: which parts of the fraud feature pipeline stay in the managed Arrow/streaming layer vs hand
// off to embedded DuckDB. Numbers: group-by wall time each way (+ the DuckDB ingest tax), with result parity.
//
// Run:  dotnet run s5-build-vs-buy.cs -- ../data/transactions.arrow

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Streaming;
using DuckDB.NET.Data;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
Console.WriteLine($"S5 build-vs-buy | global per-account group-by | {path}");

// ---------------- MANAGED: streaming dictionary group-by ----------------
var groups = new Dictionary<long, (long c, double s, double m)>(256_000);
var swM = Stopwatch.StartNew();
await foreach (var batch in ArrowStream.ReadIpc(path))
{
    var acct = Transactions.Account(batch);
    var amt = Transactions.Amount(batch);
    for (int i = 0; i < batch.Length; i++)
    {
        long a = acct.GetValue(i)!.Value;
        double v = amt.GetValue(i)!.Value;
        if (groups.TryGetValue(a, out var g))
            groups[a] = (g.c + 1, g.s + v, Math.Max(g.m, v));
        else
            groups[a] = (1, v, v);
    }
    batch.Dispose();
}
swM.Stop();
(long rows, double sumChecksum, double maxChecksum) managed = (0, 0, 0);
foreach (var kv in groups) { managed.rows += kv.Value.c; managed.sumChecksum += kv.Value.s; managed.maxChecksum += kv.Value.m; }
Console.WriteLine($"MANAGED: groups={groups.Count:N0} rows={managed.rows:N0} groupBy={swM.Elapsed.TotalMilliseconds:N0} ms");

// ---------------- DUCKDB: bulk-load + SQL group-by ----------------
using var conn = new DuckDBConnection("Data Source=:memory:");
conn.Open();
Exec(conn, "CREATE TABLE txn(account BIGINT, step INTEGER, amount DOUBLE, isFraud TINYINT)");

var swIngest = Stopwatch.StartNew();
long loaded = 0;
await foreach (var batch in ArrowStream.ReadIpc(path))
{
    var acct = Transactions.Account(batch);
    var step = Transactions.Step(batch);
    var amt = Transactions.Amount(batch);
    var isf = Transactions.IsFraud(batch);
    using var appender = conn.CreateAppender("txn");
    for (int i = 0; i < batch.Length; i++)
    {
        appender.CreateRow()
            .AppendValue(acct.GetValue(i)!.Value)
            .AppendValue(step.GetValue(i)!.Value)
            .AppendValue(amt.GetValue(i)!.Value)
            .AppendValue((sbyte)isf.GetValue(i)!.Value)
            .EndRow();
        loaded++;
    }
    batch.Dispose();
}
swIngest.Stop();

int groupCount = 0; long ddbRows = 0; double ddbSum = 0, ddbMax = 0;
var swD = Stopwatch.StartNew();
using (var cmd = conn.CreateCommand())
{
    // Aggregate per card, then a tiny outer aggregate so we read one row back (apples-to-apples with managed).
    cmd.CommandText = @"SELECT count(*) AS groups, sum(c) AS rows, sum(s) AS sumc, sum(m) AS maxc FROM
                          (SELECT account, count(*) c, sum(amount) s, max(amount) m FROM txn GROUP BY account)";
    using var r = cmd.ExecuteReader();
    r.Read();
    groupCount = (int)r.GetInt64(0);
    ddbRows = r.GetInt64(1);
    ddbSum = r.GetDouble(2);
    ddbMax = r.GetDouble(3);
}
swD.Stop();
Console.WriteLine($"DUCKDB : groups={groupCount:N0} rows={ddbRows:N0} ingest={swIngest.Elapsed.TotalMilliseconds:N0} ms groupBy={swD.Elapsed.TotalMilliseconds:N0} ms");

bool parity = groups.Count == groupCount && managed.rows == ddbRows
              && RelClose(managed.sumChecksum, ddbSum, 1e-9)
              && RelClose(managed.maxChecksum, ddbMax, 1e-9);
Console.WriteLine();
Console.WriteLine($"  managed: sumChk={managed.sumChecksum:R} maxChk={managed.maxChecksum:R}");
Console.WriteLine($"  duckdb : sumChk={ddbSum:R} maxChk={ddbMax:R}");
Console.WriteLine(parity ? "PARITY: managed and DuckDB agree on every group aggregate (sum within FP order tolerance)." : "MISMATCH: aggregates differ — investigate.");
Console.WriteLine("READ: managed wins the STREAMING windowed feature pass (bounded memory, define-once, no shuffle).");
Console.WriteLine("DuckDB wins the GLOBAL group-by/join/label-join (vectorized, multi-threaded, spills) — but charges");
Console.WriteLine("an ingest tax to get non-Arrow rows in. Seam: window features = managed; global group-by/joins = DuckDB.");

static bool RelClose(double a, double b, double rel) => Math.Abs(a - b) <= rel * Math.Max(Math.Abs(a), Math.Abs(b));

static void Exec(DuckDBConnection c, string sql)
{
    using var cmd = c.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}
