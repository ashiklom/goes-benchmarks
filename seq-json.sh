#!/usr/bin/env bash
#SBATCH --account=s2826
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=12

source ~/.bash_functions
mod_py39
source activate eso

for i in $(seq 1 365); do
  echo "Day $i"
  python kerchunk-dask-byhand.py --year=2022 --doy=$i
done
