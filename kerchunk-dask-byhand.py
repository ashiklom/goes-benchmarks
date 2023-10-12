#!/usr/bin/env python

# Generate individual kerchunk JSON files for GOES data.

import os
import glob
import xarray as xr

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr

from dask.distributed import Client, LocalCluster, progress
import dask

import ujson
import math

RESULTS = "results"
BADFILES = os.path.join(RESULTS, "badfiles")
CHUNKS = os.path.join(RESULTS, "chunks")
CHUNKSIZE = 1000

CACHEFILE = "goesfiles-all"
BASEPATH = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/'
# BASEPATH = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001'

def gen_refs(f):
    """
    Generate a single Kerchunk reference dictionary for a given NetCDF file
    """
    try:
        with open(f, 'rb') as infile:
            refs = SingleHdf5ToZarr(infile, url=f).translate()
        return {"file": f, "success": True, "refs": refs}
    except OSError as error:
        return {"file": f, "success": False, "error": str(error)}

def process_chunk(paths, outfile):
    """
    Generate a combined Kerchunk reference for a list of NetCDF files 
    and dump as JSON to `outfile`.
    """
    results = [gen_refs(f) for f in paths]

    good = [r for r in results if r["success"]]
    bad = [r for r in results if not r["success"]]
    for item in bad:
        fname = os.path.basename(item["file"]) + ".json"
        fpath = os.path.join(BADFILES, fname)
        with open(fpath, "w") as f:
            f.write(ujson.dumps(item))

    # Load first dataset to get common dimensions
    d0 = xr.open_dataset(good[0]["file"])
    identical_dims = list(d0.dims.keys())

    refs = [r["refs"] for r in good]
    multi = MultiZarrToZarr(
        refs,
        coo_map={"t": "cf:t"},
        concat_dims=["t"],
        identical_dims=identical_dims
    ).translate()
    with open(outfile, "wb") as f:
        f.write(ujson.dumps(multi).encode())
    return outfile

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

if __name__ == '__main__':

    import time
    print(f"Starting at {time.strftime('%c', time.localtime())}")

    os.makedirs(BADFILES, exist_ok=True)
    os.makedirs(CHUNKS, exist_ok=True)

    # Read the file list (or generate it if it doesn't exist)
    if os.path.exists(CACHEFILE):
        print("Using cached list of goes files.")
        with open(CACHEFILE, 'r') as f:
            all_paths = sorted(f.read().splitlines())
            all_paths = [f for f in all_paths if f.endswith(".nc")]
    else:
        print("Generating new GOES file list")
        all_paths = sorted(glob.glob(os.path.join(BASEPATH, '**/*.nc'), recursive=True))
        with open(CACHEFILE, 'w') as f:
            f.write("\n".join(all_paths))

    print(f"Processing {len(all_paths)} files")

    # Launch Dask cluster for parallelization
    if not "cluster" in locals():
        print("Launching dask cluster")
        cluster = LocalCluster()
        client = Client(cluster)
    else:
        print("Using existing dask cluster")

    all_paths_chunked = list(chunk_list(all_paths, CHUNKSIZE))
    nchunks = len(all_paths_chunked)
    digits = int(math.log10(nchunks)) + 1
    chunknames = [os.path.join(CHUNKS, str(i).zfill(digits) + ".json") for i in range(nchunks)]
    assert len(all_paths_chunked) == len(chunknames)

    pathtasks = [dask.delayed(process_chunk)(path, name) for path, name in zip(all_paths_chunked, chunknames)]

    print("Beginning processing...")
    start_time = time.time()
    # pb = dask.persist(pathtasks)
    # progress(pb)
    dask.compute(pathtasks)
    end_time = time.time()

    elapsed = end_time - start_time
    nfiles = len(all_paths)
    rate = float(nfiles) / elapsed
    print(f"Processed {nfiles} files in {elapsed:.2f} seconds ({rate:.2f} files/sec)")
    print(f"Done at {time.strftime('%c', time.localtime())}")
