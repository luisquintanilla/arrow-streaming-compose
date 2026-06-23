# Vendored kernel

`Aggregations.cs` is a **vendored copy** of the Apache Arrow .NET compute kernel
(`Sum`/`Min`/`Max`/`Mean` over `PrimitiveArray<T>`, SIMD via `System.Numerics.Tensors`).

It is copied here **by value** so this experiment is fully self-contained and has **no dependency** on any
upstream PR or fork. The kernel is Apache-2.0 licensed (header preserved in the file). When `NullCount == 0`
it dispatches to `TensorPrimitives` for one SIMD pass over the contiguous values; with nulls it falls back to a
validity-aware scalar loop.

Namespace: `Apache.Arrow.Compute`.
