#!/usr/bin/env python

# Generate individual kerchunk JSON files for GOES data.

import __main__
INTERACTIVE = not hasattr(__main__, '__file__')

import os
import glob
import xarray as xr

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr

from dask.distributed import Client, LocalCluster, progress
import dask

import ujson
import math
import argparse
import re

if INTERACTIVE:
    import importlib
    import ref_utils
    importlib.reload(ref_utils)
from ref_utils import gen_refs, process_chunk

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int)
parser.add_argument('--doy', type=int)
if INTERACTIVE:
    args = parser.parse_args(["--year", "2022", "--doy", "1"])
else:
    args = parser.parse_args()

DDIR = os.path.join(f"{args.year:04d}", f"{args.doy:03d}")
RESULTS = os.path.join("results")
BADFILES = os.path.join(RESULTS, "badfiles")
CHUNKS = os.path.join(RESULTS, "chunks", DDIR)

finaldir = os.path.join(RESULTS, "final")
os.makedirs(finaldir, exist_ok=True)
finalfile = os.path.join(finaldir, f"{args.year:04d}-{args.doy:03d}.json")

BASEPATH = os.path.join("/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD", DDIR) 
assert os.path.exists(BASEPATH)

if __name__ == '__main__':

    if not INTERACTIVE:
        assert not os.path.exists(finalfile), f"{finalfile} already exists. Exiting..."

    import time
    print(f"Starting at {time.strftime('%c', time.localtime())}")

    os.makedirs(BADFILES, exist_ok=True)
    os.makedirs(CHUNKS, exist_ok=True)

    # Identify files to process
    all_paths = sorted(glob.glob(os.path.join(BASEPATH, '**/*.nc'), recursive=True))
    print(f"Processing {len(all_paths)} files from {BASEPATH}")
    print(f"Results will be stored in {finalfile}")

    # Launch Dask cluster for parallelization
    if not "cluster" in locals():
        print("Launching dask cluster")
        cluster = LocalCluster()
        client = Client(cluster)
    else:
        print("Using existing dask cluster")

    # Group paths by band
    unique_bands = sorted(set([re.search(r'M\d+C\d+', p).group() for p in all_paths]))
    files_by_band = {band: [f for f in all_paths if band in f] for band in unique_bands}

    makename = lambda x: os.path.join(CHUNKS, x + ".json")
    pathtasks = [dask.delayed(process_chunk)(files, makename(band), BADFILES) for band, files in files_by_band.items()]

    print("Beginning processing individual chunks...")
    start_time = time.time()
    pb = dask.persist(pathtasks)
    progress(pb)
    # dask.compute(pathtasks)
    end_time = time.time()

    elapsed = end_time - start_time
    nfiles = len(all_paths)
    rate = float(nfiles) / elapsed
    print(f"Processed {nfiles} files in {elapsed:.2f} seconds ({rate:.2f} files/sec)")
    print(f"Done at {time.strftime('%c', time.localtime())}")

    chunk_results = dask.compute(pathtasks)

    # Identify common dimensions from the first result
    d0 = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
        "consolidated": False,
        "storage_options": {"fo": chunk_results[0][0]}
    })
    identical_dims = list(d0.dims.keys())
    # identical_dims.remove("band")

    # NOTE: Consolidation here doesn't work neatly because times aren't identical.
    print("Consolidating chunks into one result")
    band_rxp = re.compile(r'.*/M\d+C(\d+).json$')
    fstart_time = time.time()
    day_result = MultiZarrToZarr(
        chunk_results[0],
        coo_map={"bandn": band_rxp},
        coo_dtypes={"bandn": "i4"},
        concat_dims=["bandn"],
        # concat_dims=["band"],
        identical_dims=identical_dims
    ).translate()
    with open(finalfile, "wb") as f:
        f.write(ujson.dumps(day_result).encode())

    fend_time = time.time()
    felapsed = fend_time - fstart_time
    print(f"Time spent consolidating: {felapsed:.2f} seconds")
    print(f"Total time elapsed: {fend_time - start_time:.2f} seconds")
    print(f"All done at {time.strftime('%c', time.localtime())}")
