#:project ../src/ArrowStreamingCompose/ArrowStreamingCompose.csproj
#:package Apache.Arrow@23.0.0
#:package Microsoft.ML.OnnxRuntime@1.27.0
#:package Microsoft.ML.Tokenizers@2.0.0
#:package System.Numerics.Tensors@10.0.9
#:package Microsoft.Extensions.AI.Abstractions@9.10.0

// S9 — AI-primitive composition on the same Arrow row.
//
// Business step: a fraud feature pipeline already streams numeric velocity features from Arrow. This spike adds a
// TEXT-derived feature from the same transaction row: embed the merchant descriptor with MiniLM, compare it to known
// risky merchant descriptors, and fuse that max similarity beside the numeric features before fitting the scorer.
//
// Decision it informs: do the modern first-party .NET AI primitives compose with an Arrow-centered pipeline, and does
// the merchant-text signal lift the numeric-only fraud model enough to justify the seam they introduce?
//
// Run:  dotnet run s9-ai-composition.cs -- ../data/transactions.arrow

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics.Tensors;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using ArrowStreamingCompose.Data;
using ArrowStreamingCompose.Features;
using ArrowStreamingCompose.Scoring;
using ArrowStreamingCompose.Streaming;
using Microsoft.Extensions.AI;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;
using Microsoft.ML.Tokenizers;

const int WindowSteps = 24;
const int ReservoirCap = 300_000;
const int EmbeddingSize = 384;
const int SearchRepeats = 2_000;

string dataPath = args.Length > 0 ? args[0] : "../data/transactions.arrow";
string modelPath = args.Length > 1 ? args[1] : "../models/model.onnx";
string vocabPath = args.Length > 2 ? args[2] : "../models/vocab.txt";

string[] riskyMerchants =
[
    "GIFTCARD SUPPLY LTD", "CRYPTO EXCHANGE GLOBAL", "WIRE TRANSFER SVC", "PREPAID RELOAD CENTER",
    "OFFSHORE BETTING CO", "INSTANT CASHOUT INC", "DIGITAL WALLET TOPUP", "ANON VPN SERVICES",
];

Console.WriteLine($"S9 AI composition | window={WindowSteps} steps | sampleCap={ReservoirCap:N0}");
Console.WriteLine($"data={dataPath}");
Console.WriteLine($"model={modelPath}");
Console.WriteLine($"vocab={vocabPath}");

var engine = new VelocityEngine(WindowSteps);
var featBuf = new double[CardVelocityFeatures.Count];
var sampleX5 = new List<double[]>(ReservoirCap);
var sampleMerchants = new List<string>(ReservoirCap);
var sampleY = new List<int>(ReservoirCap);
var distinctMerchants = new HashSet<string>(StringComparer.Ordinal);
var rng = new Random(7);
long rows = 0, fraud = 0;

var streamSw = Stopwatch.StartNew();
await foreach (var batch in ArrowStream.ReadIpc(dataPath))
{
    var acct = Transactions.Account(batch);
    var step = Transactions.Step(batch);
    var amt = Transactions.Amount(batch);
    var isf = Transactions.IsFraud(batch);
    var merchant = Transactions.Merchant(batch);

    for (int i = 0; i < batch.Length; i++)
    {
        string merchantText = merchant.GetString(i) ?? string.Empty;
        distinctMerchants.Add(merchantText);
        engine.Process(acct.GetValue(i)!.Value, step.GetValue(i)!.Value, amt.GetValue(i)!.Value, featBuf);
        Admit(featBuf, merchantText, isf.GetValue(i)!.Value);
    }

    batch.Dispose();
}
streamSw.Stop();

Console.WriteLine($"streamed rows={rows:N0} fraud={fraud:N0} ({100.0 * fraud / rows:N3}%) activeCards={engine.ActiveCards:N0}");
Console.WriteLine($"Arrow pass={streamSw.Elapsed.TotalSeconds:N2}s | distinct merchants={distinctMerchants.Count:N0} | train sample={sampleX5.Count:N0}");

using var embedder = new MiniLmEmbeddingGenerator(vocabPath, modelPath);
var embedSw = Stopwatch.StartNew();
var merchantEmbeddings = await EmbedAll(embedder, distinctMerchants.OrderBy(static m => m, StringComparer.Ordinal));
var centroidEmbeddings = await EmbedAll(embedder, riskyMerchants);
embedSw.Stop();
Console.WriteLine($"MEAI embedder: embedded {merchantEmbeddings.Count:N0} distinct merchants + {centroidEmbeddings.Count:N0} fraud centroids once in {embedSw.Elapsed.TotalSeconds:N2}s");

var store = new MevdFallbackStore();
foreach (var merchantName in riskyMerchants)
    store.Upsert(new CentroidRecord(merchantName, centroidEmbeddings[merchantName]));

string[] centroidIds = riskyMerchants.ToArray();
float[] centroidMatrix = FlattenCentroids(centroidIds, centroidEmbeddings);
var similarityByMerchant = new Dictionary<string, float>(StringComparer.Ordinal);
int agree = 0;
double maxScoreDelta = 0;

foreach (var (merchantName, vector) in merchantEmbeddings.OrderBy(static kvp => kvp.Key, StringComparer.Ordinal))
{
    var storeHit = store.SearchTop1(vector);
    var tensorHit = TensorTop1(vector.Span, centroidMatrix, centroidIds);
    if (storeHit.Id == tensorHit.Id) agree++;
    maxScoreDelta = Math.Max(maxScoreDelta, Math.Abs(storeHit.Score - tensorHit.Score));
    similarityByMerchant[merchantName] = (float)tensorHit.Score;
}

var queryVectors = merchantEmbeddings.OrderBy(static kvp => kvp.Key, StringComparer.Ordinal).Select(static kvp => kvp.Value).ToArray();
var storeTiming = TimeSearches(queryVectors, SearchRepeats, v => store.SearchTop1(v).Score);
var tensorTiming = TimeSearches(queryVectors, SearchRepeats, v => TensorTop1(v.Span, centroidMatrix, centroidIds).Score);

Console.WriteLine($"MEVD fallback top-1 vs TensorPrimitives top-1: {agree}/{merchantEmbeddings.Count} agree | max score delta={maxScoreDelta:E2}");
Console.WriteLine($"Search timing over {queryVectors.Length * SearchRepeats:N0} top-1 queries: MEVD-shaped object store={storeTiming.Elapsed.TotalMilliseconds:N2} ms, TensorPrimitives contiguous float[]={tensorTiming.Elapsed.TotalMilliseconds:N2} ms");
Console.WriteLine($"Timing checksum sink={storeTiming.Sink + tensorTiming.Sink:N2} (prevents dead-code elimination)");

var sampleX6 = new List<double[]>(sampleX5.Count);
for (int i = 0; i < sampleX5.Count; i++)
{
    var row = new double[CardVelocityFeatures.Count + 1];
    System.Array.Copy(sampleX5[i], row, CardVelocityFeatures.Count);
    row[CardVelocityFeatures.Count] = similarityByMerchant[sampleMerchants[i]];
    sampleX6.Add(row);
}

var fitSw = Stopwatch.StartNew();
var scorer5 = LogisticScorer.Fit(sampleX5, sampleY, epochs: 300, lr: 0.2f);
var scores5 = ScoreAll(scorer5, sampleX5);
double auc5 = LogisticScorer.AreaUnderRoc(scores5, sampleY);
var scorer6 = LogisticScorer.Fit(sampleX6, sampleY, epochs: 300, lr: 0.2f);
var scores6 = ScoreAll(scorer6, sampleX6);
double auc6 = LogisticScorer.AreaUnderRoc(scores6, sampleY);
fitSw.Stop();

double aucLift = auc6 - auc5;
Console.WriteLine($"fit+score time={fitSw.Elapsed.TotalSeconds:N2}s");
Console.WriteLine();
Console.WriteLine("Findings");
Console.WriteLine($"AUC numeric-only (5 feats)     = {auc5:N4}");
Console.WriteLine($"AUC numeric+text (6 feats)     = {auc6:N4} (lift {aucLift:+0.0000;-0.0000;0.0000})");
Console.WriteLine($"Vector agreement               = {agree}/{merchantEmbeddings.Count} merchant queries, max score delta {maxScoreDelta:E2}");
Console.WriteLine($"Vector timing/simplicity       = TensorPrimitives direct scan was {(tensorTiming.Elapsed.TotalMilliseconds <= storeTiming.Elapsed.TotalMilliseconds ? "faster or tied" : "slower")} and simpler: one contiguous float[] plus Dot; the MEVD fallback needs record objects and an upsert/search wrapper.");
Console.WriteLine("G7 gap                         = Arrow RecordBatch/StringArray values were marshaled into .NET strings, MEAI returned ReadOnlyMemory<float>/float[] embeddings, the vector path wrapped centroids as records, and model rows copied five Arrow-derived doubles plus one text scalar into double[]. MEAI/MEVD are not Arrow-native here; embeddings live beside, not inside, the columnar substrate.");
Console.WriteLine("MEVD note                      = The SK InMemory/MEVD connector is intentionally not referenced in this spike because preview package/version friction has produced VectorSearchFilter TypeLoadException in this repo family. The fallback keeps the abstraction shape (upsert + top-1 vector search) and the TensorPrimitives comparison working end-to-end.");
Console.WriteLine();
Console.WriteLine("Conclusion");
Console.WriteLine("1. MEAI composes cleanly as a small local IEmbeddingGenerator around Tokenizers + ONNX Runtime.");
Console.WriteLine("2. The text feature is operationally cheap only because merchant strings are low-cardinality and cached once.");
Console.WriteLine("3. The Arrow story has a real seam: strings and embeddings must leave RecordBatch columns for object/vector APIs.");
Console.WriteLine("4. TensorPrimitives is the clearest path for tiny centroid scans; a vector-store abstraction adds ceremony here.");
Console.WriteLine($"5. On this sample, merchant text {(aucLift > 0.0005 ? "lifts" : aucLift < -0.0005 ? "does not lift" : "is effectively tied with")} the numeric baseline (AUC delta {aucLift:+0.0000;-0.0000;0.0000}).");

void Admit(ReadOnlySpan<double> f, string merchantText, sbyte label)
{
    rows++;
    if (label == 1) fraud++;

    if (sampleX5.Count < ReservoirCap)
    {
        sampleX5.Add(f.ToArray());
        sampleMerchants.Add(merchantText);
        sampleY.Add(label);
    }
    else
    {
        long j = (long)(rng.NextDouble() * rows);
        if (j < ReservoirCap)
        {
            sampleX5[(int)j] = f.ToArray();
            sampleMerchants[(int)j] = merchantText;
            sampleY[(int)j] = label;
        }
    }
}

static async Task<Dictionary<string, ReadOnlyMemory<float>>> EmbedAll(
    IEmbeddingGenerator<string, Embedding<float>> embedder,
    IEnumerable<string> texts)
{
    string[] batch = texts.ToArray();
    var generated = await embedder.GenerateAsync(batch);
    var result = new Dictionary<string, ReadOnlyMemory<float>>(StringComparer.Ordinal);
    for (int i = 0; i < batch.Length; i++)
        result[batch[i]] = generated[i].Vector;
    return result;
}

static float[] ScoreAll(LogisticScorer scorer, IReadOnlyList<double[]> rows)
{
    var scores = new float[rows.Count];
    for (int i = 0; i < rows.Count; i++)
        scores[i] = scorer.Score(rows[i]);
    return scores;
}

static float[] FlattenCentroids(string[] ids, IReadOnlyDictionary<string, ReadOnlyMemory<float>> vectors)
{
    var matrix = new float[ids.Length * EmbeddingSize];
    for (int i = 0; i < ids.Length; i++)
        vectors[ids[i]].Span.CopyTo(matrix.AsSpan(i * EmbeddingSize, EmbeddingSize));
    return matrix;
}

static SearchHit TensorTop1(ReadOnlySpan<float> query, ReadOnlySpan<float> centroidMatrix, IReadOnlyList<string> ids)
{
    string bestId = string.Empty;
    double bestScore = double.NegativeInfinity;
    for (int i = 0; i < ids.Count; i++)
    {
        float score = TensorPrimitives.Dot(query, centroidMatrix.Slice(i * EmbeddingSize, EmbeddingSize));
        if (score > bestScore)
        {
            bestScore = score;
            bestId = ids[i];
        }
    }
    return new SearchHit(bestId, bestScore);
}

static (TimeSpan Elapsed, double Sink) TimeSearches(ReadOnlyMemory<float>[] queries, int repeats, Func<ReadOnlyMemory<float>, double> search)
{
    double sink = 0;
    var sw = Stopwatch.StartNew();
    for (int r = 0; r < repeats; r++)
    {
        for (int i = 0; i < queries.Length; i++)
            sink += search(queries[i]);
    }
    sw.Stop();
    return (sw.Elapsed, sink);
}

sealed class MiniLmEmbeddingGenerator : IEmbeddingGenerator<string, Embedding<float>>, IDisposable
{
    private const int MaxTokenLength = 128;
    private const int EmbeddingSize = 384;
    private readonly BertTokenizer _tokenizer;
    private readonly InferenceSession _session;
    private readonly string _modelId;
    private readonly bool _hasTokenTypeIds;

    public MiniLmEmbeddingGenerator(string vocabPath, string modelPath)
    {
        _tokenizer = BertTokenizer.Create(vocabPath, new BertOptions { LowerCaseBeforeTokenization = true });
        _session = new InferenceSession(modelPath);
        _modelId = System.IO.Path.GetFileName(modelPath);
        _hasTokenTypeIds = _session.InputMetadata.ContainsKey("token_type_ids");
    }

    public Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
        IEnumerable<string> values,
        EmbeddingGenerationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(values);
        var embeddings = new List<Embedding<float>>();
        foreach (string value in values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var embedding = new Embedding<float>(EmbedOne(value))
            {
                CreatedAt = DateTimeOffset.UtcNow,
                ModelId = _modelId,
            };
            embeddings.Add(embedding);
        }

        return Task.FromResult(new GeneratedEmbeddings<Embedding<float>>(embeddings));
    }

    public object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        return serviceKey is null && serviceType.IsInstanceOfType(this) ? this : null;
    }

    public void Dispose() => _session.Dispose();

    private float[] EmbedOne(string text)
    {
        string? normalizedText;
        int charsConsumed;
        IReadOnlyList<int> ids = _tokenizer.EncodeToIds(text, MaxTokenLength, true, out normalizedText, out charsConsumed)
            ?? throw new InvalidOperationException("Tokenizer returned no token ids.");
        int tokenCount = Math.Min(ids.Count, MaxTokenLength);

        var inputIds = new long[MaxTokenLength];
        var attentionMask = new long[MaxTokenLength];
        var tokenTypeIds = new long[MaxTokenLength];
        System.Array.Fill(inputIds, (long)_tokenizer.PaddingTokenId);
        for (int i = 0; i < tokenCount; i++)
        {
            inputIds[i] = ids[i];
            attentionMask[i] = 1;
        }

        int[] shape = [1, MaxTokenLength];
        var inputs = new List<NamedOnnxValue>
        {
            NamedOnnxValue.CreateFromTensor("input_ids", new DenseTensor<long>(inputIds, shape)),
            NamedOnnxValue.CreateFromTensor("attention_mask", new DenseTensor<long>(attentionMask, shape)),
        };
        if (_hasTokenTypeIds)
            inputs.Add(NamedOnnxValue.CreateFromTensor("token_type_ids", new DenseTensor<long>(tokenTypeIds, shape)));

        using IDisposableReadOnlyCollection<DisposableNamedOnnxValue> outputs = _session.Run(inputs);
        Microsoft.ML.OnnxRuntime.Tensors.Tensor<float> lastHidden =
            outputs.First(static o => o.Name == "last_hidden_state").AsTensor<float>();
        ReadOnlySpan<float> hidden = lastHidden.ToArray();

        var pooled = new float[EmbeddingSize];
        int nonPaddingTokens = Math.Max(1, tokenCount);
        for (int token = 0; token < tokenCount; token++)
        {
            ReadOnlySpan<float> tokenEmbedding = hidden.Slice(token * EmbeddingSize, EmbeddingSize);
            for (int dim = 0; dim < EmbeddingSize; dim++)
                pooled[dim] += tokenEmbedding[dim];
        }

        for (int dim = 0; dim < EmbeddingSize; dim++)
            pooled[dim] /= nonPaddingTokens;

        float norm = MathF.Sqrt(TensorPrimitives.Dot(pooled, pooled));
        if (norm > 0)
        {
            for (int dim = 0; dim < EmbeddingSize; dim++)
                pooled[dim] /= norm;
        }

        return pooled;
    }
}

sealed class MevdFallbackStore
{
    private readonly List<CentroidRecord> _records = new();

    public void Upsert(CentroidRecord record)
    {
        int existing = _records.FindIndex(r => r.Id == record.Id);
        if (existing >= 0) _records[existing] = record;
        else _records.Add(record);
    }

    public SearchHit SearchTop1(ReadOnlyMemory<float> query)
    {
        string bestId = string.Empty;
        double bestScore = double.NegativeInfinity;
        foreach (var record in _records)
        {
            float score = TensorPrimitives.Dot(query.Span, record.Vector.Span);
            if (score > bestScore)
            {
                bestScore = score;
                bestId = record.Id;
            }
        }
        return new SearchHit(bestId, bestScore);
    }
}

readonly record struct CentroidRecord(string Id, ReadOnlyMemory<float> Vector);
readonly record struct SearchHit(string Id, double Score);
