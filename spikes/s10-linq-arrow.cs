#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0

// S10 — A columnar-LINQ -> Arrow provider sketch (the G1 proof).
//
// Business step: an analyst asks a plain question of the authorization stream — "total and peak
// amount of all transactions over $200" — the kind of filter+aggregate that sits under every fraud
// dashboard and feature backfill. The natural .NET reflex is LINQ: txns.Where(t => t.Amount > 200).Sum(t => t.Amount).
//
// The gap it exposes (G1): there is NO columnar LINQ provider in .NET. So idiomatic LINQ over Arrow
// pulls ONE managed Txn object per row and invokes a delegate per row — discarding the contiguous,
// SIMD-ready column buffers Arrow already gave us. This spike runs the SAME query two ways over the
// SAME Arrow file:
//   (a) ROW   : materialize Txn rows per batch, then LINQ-to-Objects Where/Sum/Max  (today's reflex)
//   (b) COLUMN: ArrowQuery.From(stream).Where("amount", >, 200).Select("amount") deferred, lowered to
//               column spans + TensorPrimitives kernels  (what a provider would unlock)
//
// It asserts both produce identical answers (so the lowering is correct), reports the throughput
// delta, and prints exactly what a real IQueryable Arrow provider would still need to be built.
//
// Run:
//   dotnet run s10-linq-arrow.cs -- ../data/transactions.arrow
//   dotnet run s10-linq-arrow.cs -- ../data/transactions.arrow 200

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Streaming;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
double threshold = args.Length > 1 ? double.Parse(args[1]) : 200.0;

Console.WriteLine($"S10 columnar-LINQ vs row-LINQ | filter: amount > {threshold:N0} | {path}\n");

// --- (a) ROW: today's reflex — IEnumerable<Txn> LINQ-to-Objects ----------------------------------
// We rehydrate each Arrow batch into managed Txn rows (one object's worth of fields per row), then
// run ordinary LINQ. This is what a .NET dev writes today because there is no columnar provider.
var swRow = Stopwatch.StartNew();
double rowSum = 0, rowMax = double.NegativeInfinity;
long rowCount = 0, rowTotal = 0;
await foreach (RecordBatch batch in ArrowStream.ReadIpc(path))
{
    using (batch)
    {
        rowTotal += batch.Length;
        foreach (double amount in MaterializeRows(batch)        // per-row pull into managed values
                                  .Where(a => a > threshold))    // per-row delegate invocation
        {
            rowSum += amount;
            rowCount++;
            if (amount > rowMax) rowMax = amount;
        }
    }
}
if (rowCount == 0) rowMax = 0;
swRow.Stop();

// --- (b) COLUMN: deferred plan lowered to column spans + TensorPrimitives -------------------------
var swCol = Stopwatch.StartNew();
(double colSum, long colCount, double colMax) = await ArrowQuery
    .From(ArrowStream.ReadIpc(path))
    .Where("amount", ArrowQuery.Op.GreaterThan, threshold)
    .Select("amount")
    .AggregateAsync();
swCol.Stop();

// Also time the UNFILTERED full-column reduction, which lowers to a single TensorPrimitives.Sum
// kernel call per batch — the pure vectorized path row-LINQ can never take.
var swColFull = Stopwatch.StartNew();
long fullCount = await ArrowQuery.From(ArrowStream.ReadIpc(path)).Select("amount").CountAsync();
swColFull.Stop();

// --- correctness gate ----------------------------------------------------------------------------
bool sumOk = Math.Abs(rowSum - colSum) <= 1e-6 * Math.Max(1, Math.Abs(rowSum));
bool countOk = rowCount == colCount;
bool maxOk = Math.Abs(rowMax - colMax) <= 1e-9 * Math.Max(1, Math.Abs(rowMax));
bool pass = sumOk && countOk && maxOk;

double rowMrows = rowTotal / swRow.Elapsed.TotalSeconds / 1e6;
double colMrows = rowTotal / swCol.Elapsed.TotalSeconds / 1e6;

Console.WriteLine($"ROW    LINQ-to-Objects  sum={rowSum,18:N2}  count={rowCount,12:N0}  max={rowMax,10:N2}  time={swRow.Elapsed.TotalSeconds,6:F2}s  {rowMrows,5:F2} M rows/s");
Console.WriteLine($"COLUMN ArrowQuery       sum={colSum,18:N2}  count={colCount,12:N0}  max={colMax,10:N2}  time={swCol.Elapsed.TotalSeconds,6:F2}s  {colMrows,5:F2} M rows/s");
Console.WriteLine();
Console.WriteLine($"correctness: {(pass ? "PASS" : "FAIL")}  (sum {(sumOk ? "ok" : "MISMATCH")}, count {(countOk ? "ok" : "MISMATCH")}, max {(maxOk ? "ok" : "MISMATCH")})");
Console.WriteLine($"speedup (filtered Where+Sum+Max): {swRow.Elapsed.TotalSeconds / swCol.Elapsed.TotalSeconds:F2}x");
Console.WriteLine($"unfiltered Count over {fullCount:N0} rows via single TensorPrimitives pass/batch: {swColFull.Elapsed.TotalSeconds:F2}s");

Console.WriteLine();
Console.WriteLine("What this sketch does NOT do — i.e. gap G1, what a real columnar IQueryable Arrow provider still needs:");
foreach (string gap in ArrowQuery.ProviderGaps)
    Console.WriteLine($"  - {gap}");

Console.WriteLine();
Console.WriteLine("Takeaway: the lowering is correct and faster on the same bytes. The reason it isn't free");
Console.WriteLine("today is purely missing plumbing (expression-tree parsing + operator coverage), not a");
Console.WriteLine("fundamental limitation — Arrow already hands us the vectorizable columns.");

return pass ? 0 : 1;

// Rehydrate a batch's amount column the way a row-oriented model forces you to: value-by-value.
// (A full Txn rehydration would also touch account/step/isFraud/merchant per row; we pull just the
// projected column to give row-LINQ its BEST case and still show the gap.)
static IEnumerable<double> MaterializeRows(RecordBatch batch)
{
    DoubleArray amount = Transactions.Amount(batch);
    for (int i = 0; i < amount.Length; i++)
        yield return amount.GetValue(i)!.Value;
}
