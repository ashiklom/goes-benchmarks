#!/usr/bin/env bash

OUTDIR="results/final-compressed"
mkdir -p $OUTDIR

find results/final -type f -name "*.json"
for f in $(find results/final -type f -name "*.json"); do
  fname="$(basename $f).zst"
  target="results/final-compressed/$fname"
  if [[ -f $target ]]; then
    echo "Target already exists. Skipping."
    continue
  fi
  echo "Generating $target..."
  zstd $f -o $target
done
