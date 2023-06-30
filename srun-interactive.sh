#!/usr/bin/env bash

# eso-c5n18xlarge-spot-dy-c5n18xlarge-[1-100]
srun -c 36 -p eso-c5n18xlarge-spot,eso-c5n18xlarge-demand --pty bash
# salloc -c 36 -p eso-c5n18xlarge-spot,eso-c5n18xlarge-demand
