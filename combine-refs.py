#!/usr/bin/env python

import ujson
import time
import pathlib

import xarray as xr

from kerchunk.combine import MultiZarrToZarr

resultsdir = pathlib.Path("results") / "final"
chunkfiles = sorted([str(x) for x in resultsdir.glob("2022-*.json")])

# Try reading the first file and get list of identical dims
d0 = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {"fo": chunkfiles[0]}
})
identical_dims = list(d0.dims.keys())
identical_dims.remove("time")

# Read the second file, just to check that time steps are not identical
d1 = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {"fo": chunkfiles[1]}
})
assert d0.time[0].values != d1.time[0].values

start_time = time.time()
dofiles = chunkfiles
result = MultiZarrToZarr(
    dofiles,
    concat_dims=["time"],
    identical_dims=identical_dims
).translate()
end_time = time.time()
elapsed = end_time - start_time

print(f"Processed {len(dofiles)} in {elapsed:.03f} sec.")

# Check that we've stored the correct number of time steps
assert ujson.loads(result["refs"]["time/.zarray"])['shape'][0] == len(d0.time) * len(dofiles)

outfile = resultsdir / "consolidated.json"
with open(outfile, "wb") as f:
    f.write(ujson.dumps(result).encode())

# Try opening the file
import fsspec
target = fsspec.filesystem("reference", fo=str(outfile)).get_mapper()
dtest = xr.open_zarr(target, consolidated=False)
