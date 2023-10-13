#!/usr/bin/env bash

if [[ $(hostname) =~ "discover" ]]; then
  srun --account s2826 --cpus-per-task 16 --time 00:59:00 --pty bash
else
  # eso-c5n18xlarge-spot-dy-c5n18xlarge-[1-100]
  srun -c 36 -p eso-c5n18xlarge-spot,eso-c5n18xlarge-demand --pty bash
  # salloc -c 36 -p eso-c5n18xlarge-spot,eso-c5n18xlarge-demand
fi
