#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package System.Reactive@6.0.1

// S2 — Auth-surge robustness (push vs pull under a slow consumer).
//
// Business step: the auth stream BURSTS (holiday peak, or a credential-stuffing attack). If the model scorer is
// momentarily slow, what happens to memory? This spike pins a deliberately slow consumer and compares:
//   - PULL : IAsyncEnumerable<RecordBatch> — the consumer `await`s each batch, so the producer is naturally
//            throttled (native backpressure). Only a couple of batches are ever in flight.
//   - PUSH : IObservable<RecordBatch> via Rx with ObserveOn (decoupled producer/consumer) — the producer runs
//            free and the unconsumed batches QUEUE. No built-in backpressure => the backlog (and memory) grows.
//
// Decision it informs: can the online path survive a burst without OOM, and what does each model cost? Numbers:
// max in-flight backlog (batches) and peak working set, per mode (run each in its own process).
//
// Run BOTH (separate processes => clean PeakWorkingSet64):
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow pull
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow push

using System;
using System.Diagnostics;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Streaming;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
string mode = args.Length > 1 ? args[1] : "pull";
int slowMsPerBatch = args.Length > 2 ? int.Parse(args[2]) : 25;

Console.WriteLine($"S2 surge | mode={mode} | slowConsumer={slowMsPerBatch}ms/batch | {path}");

long produced = 0, consumed = 0, maxBacklog = 0;
void Note() { long b = Interlocked.Read(ref produced) - Interlocked.Read(ref consumed); if (b > maxBacklog) maxBacklog = b; }

var sw = Stopwatch.StartNew();

if (mode == "pull")
{
    // PULL: consumer awaits each batch => producer cannot run ahead. Backlog stays ~1.
    await foreach (var batch in ArrowStream.ReadIpc(path))
    {
        Interlocked.Increment(ref produced); Note();
        await Task.Delay(slowMsPerBatch);          // the slow scorer
        Interlocked.Increment(ref consumed); Note();
        batch.Dispose();
    }
}
else
{
    // PUSH: ObserveOn decouples producer from consumer => producer runs free, batches queue unbounded.
    var done = new TaskCompletionSource();
    var observable = ArrowStream.ToObservable(ArrowStream.ReadIpc(path))
        .Do(_ => { Interlocked.Increment(ref produced); Note(); })   // counted as it is PUSHED (produced)
        .ObserveOn(new EventLoopScheduler());                        // hand off to a single consumer thread

    var sub = observable.Subscribe(
        onNext: batch =>
        {
            Thread.Sleep(slowMsPerBatch);          // the slow scorer (synchronous on the consumer thread)
            Interlocked.Increment(ref consumed); Note();
            batch.Dispose();
        },
        onError: _ => done.TrySetResult(),
        onCompleted: () => done.TrySetResult());

    await done.Task;
    sub.Dispose();
}

sw.Stop();
long peak = Process.GetCurrentProcess().PeakWorkingSet64;
Console.WriteLine($"produced={produced:N0} consumed={consumed:N0} maxBacklog={maxBacklog:N0} batches");
Console.WriteLine($"peakWorkingSet={peak / 1024.0 / 1024.0:N0} MiB in {sw.Elapsed.TotalSeconds:N1}s");
Console.WriteLine(mode == "pull"
    ? "READ: pull keeps backlog ~1 and memory flat — native backpressure absorbs the surge."
    : "READ: push lets the backlog (and retained Arrow buffers) grow — no backpressure; a long-enough surge OOMs.");
