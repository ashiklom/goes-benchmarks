#!/usr/bin/env python

import ujson
import glob
import time

import xarray as xr

from kerchunk.combine import MultiZarrToZarr

chunkfiles = sorted(glob.glob("results/final/*.json"))

# Read first file to get common dimensions
d0 = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {"fo": chunkfiles[0]}
})
identical_dims = list(d0.dims.keys())
identical_dims.remove("t")

start_time = time.time()
dofiles = chunkfiles[0:20]
result = MultiZarrToZarr(
    dofiles,
    concat_dims=["t"],
    identical_dims=identical_dims
).translate()
end_time = time.time()
elapsed = end_time - start_time

print(f"Processed {len(dofiles)} in {elapsed:.03f} sec.")

import zstandard as zstd
import os

wstart_time = time.time()
with zstd.open("results/test.json.zst", "wb") as f:
    f.write(ujson.dumps(result).encode())
wend_time = time.time()
welapsed = wend_time - wstart_time
print(f"Writing output took f{welapsed:.03f} sec.")
