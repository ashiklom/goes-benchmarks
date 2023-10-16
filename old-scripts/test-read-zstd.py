#!/usr/bin/env python

import xarray as xr
import fsspec

from dask.distributed import Client, LocalCluster
cluster = LocalCluster()
client = Client(cluster)

target = fsspec.filesystem("reference", fo="results/final/2022-002.json").get_mapper()
# target = fsspec.filesystem(
#     "reference",
#     "fo": "results/final-compressed/2022-002.json.zst",
#     "compression": 'zstd'
# )
dat = xr.open_zarr(target, consolidated=False)

# Try calculating a global mean
%time globmean = dat.isel(t=slice(200, 300)).Rad.mean(["t"]).values()
globmean.values()

%time globmean = dat.Rad.mean(["t"])
