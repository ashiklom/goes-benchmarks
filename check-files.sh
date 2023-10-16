#!/usr/bin/env bash

for i in $(seq 1 365); do
  if [[ ! -f $(printf "results/final/2022-%03d.json" $i) ]]; then
    echo "Missing file for day $i"
  fi
done
