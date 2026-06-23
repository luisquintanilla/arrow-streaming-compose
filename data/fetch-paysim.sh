#!/usr/bin/env bash
# Fetch the real PaySim dataset (synthetic mobile-money transactions, ~6.3M rows) from Kaggle.
#
# PaySim is the schema/semantics anchor for these spikes: account key (nameOrig), hourly time `step`, `amount`,
# `isFraud` label. The spikes run on a PaySim-SHAPED generator by default (see src/.../Data/Transactions.cs) so
# they work offline; drop the real CSV here to validate against the actual distribution.
#
# Requires the Kaggle CLI + API token (~/.kaggle/kaggle.json). Dataset: ealaxi/paysim1 (CC BY-SA 4.0).
#
# Usage:  ./fetch-paysim.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$SCRIPT_DIR/paysim.csv"

if [[ -f "$OUT" ]]; then
  echo "Found $(basename "$OUT") - skipping download"
  exit 0
fi

if ! command -v kaggle >/dev/null 2>&1; then
  echo "kaggle CLI not found. Install with: pip install kaggle" >&2
  echo "Then place your API token at ~/.kaggle/kaggle.json (chmod 600)." >&2
  exit 1
fi

echo "Downloading PaySim (ealaxi/paysim1) ..."
kaggle datasets download -d ealaxi/paysim1 -p "$SCRIPT_DIR" --unzip
# Kaggle unzips to a CSV with a long name; normalize it.
CSV=$(find "$SCRIPT_DIR" -maxdepth 1 -name '*.csv' | head -n1 || true)
if [[ -n "$CSV" && "$CSV" != "$OUT" ]]; then
  mv "$CSV" "$OUT"
fi
echo "PaySim ready at $OUT"
