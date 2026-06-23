#!/usr/bin/env bash
# Fetch the all-MiniLM-L6-v2 sentence-embedding model (ONNX) + its WordPiece vocab for the S9 text path.
#
# Used by spikes/s9-ai-composition.cs to embed the merchant-descriptor string via Microsoft.ML.Tokenizers
# (BertTokenizer from vocab.txt) + Microsoft.ML.OnnxRuntime, behind a Microsoft.Extensions.AI IEmbeddingGenerator.
# Model: sentence-transformers/all-MiniLM-L6-v2 (Apache-2.0). Binaries are gitignored — run this to materialize them.
#
# Usage:  ./fetch-minilm.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MODEL_URL="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx"
VOCAB_URL="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/vocab.txt"

fetch_file() {
  local url="$1" output="$2" partial="${2}.part"
  if [[ -f "$output" ]]; then
    echo "Found $(basename "$output") - skipping download"
  else
    echo "Downloading $(basename "$output")"
    rm -f "$partial"
    if ! curl -L --fail --show-error --progress-bar "$url" -o "$partial"; then
      rm -f "$partial"; echo "Failed to download $url" >&2; return 1
    fi
    [[ -s "$partial" ]] || { rm -f "$partial"; echo "Downloaded file is empty: $url" >&2; return 1; }
    mv "$partial" "$output"
  fi
  echo "$(basename "$output"): $(du -h "$output" | cut -f1)"
}

fetch_file "$MODEL_URL" "$SCRIPT_DIR/model.onnx"
fetch_file "$VOCAB_URL" "$SCRIPT_DIR/vocab.txt"
