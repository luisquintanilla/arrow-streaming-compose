#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package System.Reactive@6.0.1

// S2 — Auth-surge robustness (push vs pull under a slow consumer), and the PRECISE truth about Rx backpressure.
//
// Business step: the auth stream BURSTS (holiday peak, or a credential-stuffing attack). If the model scorer is
// momentarily slow, what happens to memory — and can we survive the surge WITHOUT dropping any authorization?
//
// The careful claim: Rx.NET has no *demand-based* (Reactive-Streams) backpressure — an observer cannot tell the
// producer to slow down. But that does NOT mean "Rx always queues unbounded." Two things matter, and this spike
// measures all of them so the claim is backed by numbers, not hand-waving:
//
//   pull         IAsyncEnumerable<RecordBatch> — the consumer `await`s each batch; the producer is throttled in
//                lock-step (native, non-lossy backpressure). Backlog ~1, flat memory.
//   push         IObservable + ObserveOn(EventLoopScheduler) + an async producer. We DELIBERATELY decouple the
//                producer from the consumer, so unconsumed batches queue on the scheduler — UNBOUNDED. Backlog
//                grows. This is the surge-OOM case.
//   push-sync    A synchronous cold Observable.Create with NO scheduler. OnNext is a blocking call, so the slow
//                consumer blocks the producer loop — Rx is lock-step BY DEFAULT. Backlog ~1, flat memory. (This
//                is the "isn't there a way to subscribe and throttle?" answer: yes, if you stay synchronous.)
//   push-bounded The decoupled async producer, but bridged through a BOUNDED System.Threading.Channels channel.
//                The producer `await`s WriteAsync when the channel is full => real backpressure, flat memory,
//                ZERO loss. This is the non-lossy fix when you must decouple threads.
//   push-lossy   Rx's own built-in flow control: Sample/Throttle. Memory stays bounded, but consumed < produced
//                => it bounds memory by DROPPING data. Fine for telemetry; unacceptable when you can't drop auths.
//
// Decision it informs: the surge path must be pull, a bounded-Channel bridge, or synchronous — NOT a decoupled
// Rx push with no bound. Numbers: max in-flight backlog (batches), peak working set, and dropped count, per mode.
//
// Run each in its own process (clean PeakWorkingSet64):
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow pull
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow push
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow push-sync
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow push-bounded
//   dotnet run s2-push-vs-pull.cs -- ../data/transactions.arrow push-lossy

using System;
using System.Diagnostics;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Streaming;

string path = args.Length > 0 ? args[0] : "../data/transactions.arrow";
string mode = args.Length > 1 ? args[1] : "pull";
int slowMsPerBatch = args.Length > 2 ? int.Parse(args[2]) : 25;

Console.WriteLine($"S2 surge | mode={mode} | slowConsumer={slowMsPerBatch}ms/batch | {path}");

long produced = 0, consumed = 0, dropped = 0, maxBacklog = 0;
void Note() { long b = Interlocked.Read(ref produced) - Interlocked.Read(ref consumed) - Interlocked.Read(ref dropped); if (b > maxBacklog) maxBacklog = b; }

var sw = Stopwatch.StartNew();

switch (mode)
{
    case "pull":
    {
        // PULL: consumer awaits each batch => producer cannot run ahead. Backlog stays ~1.
        await foreach (var batch in ArrowStream.ReadIpc(path))
        {
            Interlocked.Increment(ref produced); Note();
            await Task.Delay(slowMsPerBatch);          // the slow scorer
            Interlocked.Increment(ref consumed); Note();
            batch.Dispose();
        }
        break;
    }

    case "push":
    {
        // PUSH (decoupled): ObserveOn + an async producer => producer runs free, batches queue UNBOUNDED.
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
        break;
    }

    case "push-sync":
    {
        // PUSH (synchronous): a cold Observable.Create with NO scheduler. The subscribe delegate iterates the
        // source on the SUBSCRIBER's thread and calls OnNext inline. Because OnNext is a blocking call, the slow
        // consumer blocks the producer loop => Rx is lock-step BY DEFAULT. Backlog ~1, flat memory, zero loss.
        IObservable<RecordBatch> sync = Observable.Create<RecordBatch>(observer =>
        {
            var e = ArrowStream.ReadIpc(path).GetAsyncEnumerator();
            try
            {
                // Console app has no SynchronizationContext, so blocking on the async reader is deadlock-safe.
                while (e.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                {
                    Interlocked.Increment(ref produced); Note();
                    observer.OnNext(e.Current);        // BLOCKS here until the consumer's OnNext returns
                }
                observer.OnCompleted();
            }
            catch (Exception ex) { observer.OnError(ex); }
            finally { e.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
            return Disposable.Empty;
        });

        sync.Subscribe(
            onNext: batch =>
            {
                Thread.Sleep(slowMsPerBatch);          // the slow scorer, inline on the producer's thread
                Interlocked.Increment(ref consumed); Note();
                batch.Dispose();
            });
        break;
    }

    case "push-bounded":
    {
        // PUSH (decoupled) bridged through a BOUNDED channel. The async producer awaits WriteAsync when the
        // channel is full => real backpressure across the thread boundary, flat memory, ZERO loss. This is the
        // non-lossy fix when you must decouple producer and consumer threads.
        const int capacity = 4;
        var channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait    // producer waits instead of dropping
        });

        var producer = Task.Run(async () =>
        {
            await foreach (var batch in ArrowStream.ReadIpc(path))
            {
                Interlocked.Increment(ref produced); Note();
                await channel.Writer.WriteAsync(batch);   // AWAITS when full => backpressure
            }
            channel.Writer.Complete();
        });

        await foreach (var batch in channel.Reader.ReadAllAsync())
        {
            await Task.Delay(slowMsPerBatch);          // the slow scorer
            Interlocked.Increment(ref consumed); Note();
            batch.Dispose();
        }
        await producer;
        break;
    }

    case "push-lossy":
    {
        // PUSH with Rx's OWN flow control: Sample drops all but the most recent batch per interval. Memory stays
        // bounded, but consumed < produced => it bounds memory by DROPPING data. Acceptable for telemetry; NOT for
        // fraud, where every authorization must be scored.
        var done = new TaskCompletionSource();
        var observable = ArrowStream.ToObservable(ArrowStream.ReadIpc(path))
            .Do(b => { Interlocked.Increment(ref produced); b.Dispose(); })  // dropped batches are released immediately
            .Sample(TimeSpan.FromMilliseconds(slowMsPerBatch))                        // keep only the latest per window
            .ObserveOn(new EventLoopScheduler());

        var sub = observable.Subscribe(
            onNext: _ =>
            {
                Thread.Sleep(slowMsPerBatch);          // the slow scorer
                Interlocked.Increment(ref consumed); Note();
            },
            onError: _ => done.TrySetResult(),
            onCompleted: () => done.TrySetResult());

        await done.Task;
        sub.Dispose();
        Interlocked.Exchange(ref dropped, produced - consumed);   // everything produced but not consumed was dropped
        break;
    }

    default:
        Console.Error.WriteLine($"unknown mode '{mode}' (pull|push|push-sync|push-bounded|push-lossy)");
        return 1;
}

sw.Stop();
long peak = Process.GetCurrentProcess().PeakWorkingSet64;
Console.WriteLine($"produced={produced:N0} consumed={consumed:N0} dropped={dropped:N0} maxBacklog={maxBacklog:N0} batches");
Console.WriteLine($"peakWorkingSet={peak / 1024.0 / 1024.0:N0} MiB in {sw.Elapsed.TotalSeconds:N1}s");
Console.WriteLine(mode switch
{
    "pull"         => "READ: pull awaits each batch (lock-step) — native non-lossy backpressure, flat memory.",
    "push"         => "READ: decoupled push (ObserveOn + async producer) queues UNBOUNDED — no demand backpressure; a long surge OOMs.",
    "push-sync"    => "READ: synchronous Rx is lock-step — OnNext blocks the producer, so backlog ~1 and memory is flat (no scheduler = natural throttle).",
    "push-bounded" => "READ: a bounded Channel bridge gives real backpressure across threads — flat memory, ZERO loss. The non-lossy fix.",
    "push-lossy"   => "READ: Sample/Throttle bound memory by DROPPING batches (consumed < produced) — fine for telemetry, unacceptable for fraud.",
    _              => string.Empty,
});

return 0;
