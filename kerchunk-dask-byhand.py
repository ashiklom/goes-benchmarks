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
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, default=2022)
parser.add_argument('--doy', type=int, default=1)
parser.add_argument('--chunksize', type=int, default=50)
args = parser.parse_args()
# args = parser.parse_args(["--year", "2022", "--doy", "1"])

DDIR = os.path.join(f"{args.year:04d}", f"{args.doy:03d}")
RESULTS = os.path.join("results")
BADFILES = os.path.join(RESULTS, "badfiles")
CHUNKS = os.path.join(RESULTS, "chunks", DDIR)
CHUNKSIZE = args.chunksize

finaldir = os.path.join(RESULTS, "final")
os.makedirs(finaldir, exist_ok=True)
finalfile = os.path.join(finaldir, f"{args.year:04d}-{args.doy:03d}.json")

CACHEFILE = "goesfiles-all"
BASEPATH = os.path.join("/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD", DDIR) 
assert os.path.exists(BASEPATH)

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

    assert not os.path.exists(finalfile), f"{finalfile} already exists. Exiting..."

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
    identical_dims.remove("t")

    print("Consolidating chunks into one result")
    fstart_time = time.time()
    day_result = MultiZarrToZarr(
        chunk_results[0],
        concat_dims=["t"],
        identical_dims=identical_dims
    ).translate()
    with open(finalfile, "wb") as f:
        f.write(ujson.dumps(day_result).encode())

    fend_time = time.time()
    felapsed = fend_time - fstart_time
    print(f"Time spent consolidating: {felapsed:.2f} seconds")
    print(f"Total time elapsed: {fend_time - start_time:.2f} seconds")
    print(f"All done at {time.strftime('%c', time.localtime())}")
