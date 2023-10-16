#!/usr/bin/env python

import xarray as xr
import fsspec

from dask.distributed import Client, LocalCluster, progress
cluster = LocalCluster()
client = Client(cluster)

target = fsspec.filesystem("reference", fo="results/final/2022-002.json").get_mapper()
# target = fsspec.filesystem(
#     "reference",
#     fo="results/final-compressed/2022-002.json.zst",
#     compression='zstd'
# )
dat = xr.open_zarr(target, consolidated=False)

# Try extracting a time series for a random location
tseries = dat.isel(x=5000, y=5000, t=slice(0, 100)).Rad
result = tseries.persist()
progress(result)

# Try calculating a global mean
%time globmean = dat.isel(t=slice(200, 300)).Rad.mean(["t"]).values()
globmean.values()

%time globmean = dat.Rad.mean(["t"])
