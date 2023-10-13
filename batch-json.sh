#!/usr/bin/env bash
#SBATCH --account s2826
#SBATCH --time 00:59:00
#SBATCH --cpus-per-task 8
#SBATCH --array=1-365

source ~/.bash_functions
mod_py39
source activate eso

python kerchunk-dask-byhand.py --year=2022 --doy=$SLURM_ARRAY_TASK_ID
