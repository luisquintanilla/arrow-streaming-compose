#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0

// gen-data: produce the offline Arrow IPC dataset the spikes read.
//
// Writes a PaySim-SHAPED, volume-synthesized transaction stream to data/transactions.arrow. The schema and
// feature semantics are real PaySim (account key, hourly step, amount, isFraud); the volume is synthesized so
// spikes are deterministic and run offline. Fraud is injected as per-account BURSTS so velocity features carry
// real signal. Labeled "volume-synthesized, shape-real" in the decision memo — no fabricated numbers.
//
// Run:  dotnet run gen-data.cs -- [rows] [out.arrow]
//   e.g. dotnet run gen-data.cs -- 5000000 ../data/transactions.arrow

using System;
using System.Diagnostics;
using System.IO;
using ArrowStreamingCompose.Data;

long rows = args.Length > 0 ? long.Parse(args[0]) : 5_000_000;
string outPath = args.Length > 1
    ? args[1]
    : Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "data", "transactions.arrow");
outPath = Path.GetFullPath(outPath);
Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);

var opt = new Transactions.Options { TargetRows = rows };
Console.WriteLine($"Generating ~{rows:N0} rows -> {outPath}");

var sw = Stopwatch.StartNew();
long written = Transactions.WriteIpc(outPath, Transactions.Generate(opt));
sw.Stop();

long bytes = new FileInfo(outPath).Length;
Console.WriteLine($"Wrote {written:N0} rows, {bytes / 1024.0 / 1024.0:N1} MiB in {sw.Elapsed.TotalSeconds:N1}s " +
                  $"({written / sw.Elapsed.TotalSeconds / 1e6:N2} M rows/s)");
