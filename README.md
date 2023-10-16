# Generating Kerchunk reference files

Data are stored at `/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/`.

We generate one Kerchunk reference file per day, in chunks, to balance inodes (avoid too many files) and file size (create references that aren't too big to store in memory).
The script that does this is `kerchunk-dask-byhand.py`.
Generate daily files for all of 2022 via SLURM using `batch-json.sh`.

Optionally, compress the JSON files using `compress-files.sh` (requires `zstd` command line; reading will probably also require Python `zstandard` library).

Try reading one file and extracting a time series in `test-read.py`.
