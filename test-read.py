#!/usr/bin/env python

import xarray as xr
import fsspec

# from dask.distributed import Client, LocalCluster, progress
# cluster = LocalCluster()
# client = Client(cluster)

target = fsspec.filesystem("reference", fo="results/final/2022-001.json").get_mapper()
# target = fsspec.filesystem(
#     "reference",
#     fo="results/final-compressed/2022-002.json.zst",
#     compression='zstd'
# )
dat = xr.open_zarr(target, consolidated=False)

# Does this work? No idea..
# targets = [fsspec.filesystem("reference", fo=f).get_mapper() for f in json_files]
# datmf = xr.open_mfdataset(targets)

# Try extracting a time series for a random location
dat
tseries = dat.isel(bandn=1, x=4000, y=7000, time=slice(50,100)).Rad
tseries.values

result = tseries.persist()
progress(result)
# ~70 sec to read 100 time steps

# Try calculating a global mean
globmean = dat.isel(t=slice(200, 300)).Rad.mean(["t"])
rmean = globmean.persist()
progress(rmean)

%time globmean = dat.Rad.mean(["t"])
