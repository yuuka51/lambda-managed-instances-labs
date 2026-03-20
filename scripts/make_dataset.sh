#!/usr/bin/env bash
set -euo pipefail

OUTDIR=${OUTDIR:-data/run-small}
ROWS=${ROWS:-100000}
NUM_FILES=${NUM_FILES:-10}
SEED=${SEED:-42}
MISMATCH_RATE=${MISMATCH_RATE:-0.01}
MISSING_RATE=${MISSING_RATE:-0.01}
DST_PREFIX=${DST_PREFIX:-inputs/run-small/}

python -m lmi_lab generate \
  --rows "${ROWS}" \
  --num-files "${NUM_FILES}" \
  --outdir "${OUTDIR}" \
  --seed "${SEED}" \
  --mismatch-rate "${MISMATCH_RATE}" \
  --missing-rate "${MISSING_RATE}" \
  --gzip true

if [[ -n "${BUCKET:-}" ]]; then
  python -m lmi_lab upload-s3 \
    --bucket "${BUCKET}" \
    --src "${OUTDIR}" \
    --dst-prefix "${DST_PREFIX}" \
    --include-manifest true
else
  echo "BUCKET が未指定のため upload-s3 はスキップしました"
fi
