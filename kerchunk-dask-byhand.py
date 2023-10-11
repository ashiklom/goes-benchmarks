#!/usr/bin/env python

# Generate individual kerchunk JSON files for GOES data. To reduce inode 
# pressure, this script uses REDIS to store the individual reference dicts.

import os
import glob
import xarray as xr

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr

from dask.distributed import Client, LocalCluster, progress
import dask.bag as db

import redis
import subprocess
import signal
import ujson

def gen_json(f):
    # Successes use database 0 (default)
    # Failures are stored in index 1
    rgood = redis.Redis(db=0)
    rbad = redis.Redis(db=1)
    outkey = os.path.basename(f)
    if rgood.exists(outkey) or rbad.exists(outkey):
        return None
    try:
        with open(f, 'rb') as infile:
            h5chunks = SingleHdf5ToZarr(infile, url=f)
            rgood.set(outkey, ujson.dumps(h5chunks.translate()).encode())
    except OSError as error:
        rbad.set(f, str(error))

if __name__ == '__main__':

    import time
    print(f"Starting at {time.strftime('%c', time.localtime())}")
    # Launch Dask cluster for parallelization
    if not "cluster" in locals():
        print("Launching dask cluster")
        cluster = LocalCluster()
        client = Client(cluster)
    else:
        print("Using existing dask cluster")

    # Start redis server
    if not "rpx" in locals():
        rpx = subprocess.Popen(["redis-server"], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        # ...and connect to redis client

    # Stop redis server
    # rpx.send_signal(signal.SIGINT)
    rgood = redis.Redis(db=0)
    rbad = redis.Redis(db=1)

    # Read the file list (or generate it if it doesn't exist)
    cachefile = "goesfiles-all"
    if os.path.exists(cachefile):
        print("Using cached list of goes files.")
        with open(cachefile, 'r') as f:
            all_paths = sorted(f.read().splitlines())
    else:
        print("Generating new GOES file list")
        base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/'
        # base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001'
        all_paths = sorted(glob.glob(os.path.join(base_path, '**/*.nc'), recursive=True))
        with open(cachefile, 'w') as f:
            f.write("\n".join(all_paths))

    # # Try 20 days' worth of files
    all_paths = [f for f in all_paths if "2022/00" in f or "2022/01" in f]
    print(f"Processing {len(all_paths)} files")

    # Load first dataset to get common dimensions
    d0 = xr.open_dataset(all_paths[0])
    common_dims = list(d0.dims.keys())

    pathbag = db.from_sequence(all_paths, npartitions=sum(client.ncores().values()))
    # pathbag = db.from_sequence(all_paths)
    pathtasks = pathbag.map(gen_json)

    print("Beginning processing...")
    start_time = time.time()

    pb = pathtasks.persist()
    progress(pb)

    end_time = time.time()

    elapsed = end_time - start_time
    nfiles = len(all_paths)
    rate = float(nfiles) / elapsed
    print(f"Processed {nfiles} files in {elapsed:.2f} seconds ({rate:.2f} files/sec)")
    print(f"Done at {time.strftime('%c', time.localtime())}")
