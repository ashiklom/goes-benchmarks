#!/usr/bin/env python

import xarray as xr
import fsspec

from dask.distributed import Client, LocalCluster, progress
cluster = LocalCluster()
client = Client(cluster)

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
tseries = dat.isel(bandn=1, x=4000, y=7000, time=slice(50,100)).Rad

# 50 time steps for 1 band
%time tseries.values
# CPU times: user 2.41 s, sys: 327 ms, total: 2.74 s
# Wall time: 3.24 s

# 1 day's worth of data for one band and location
%time dat.isel(bandn=2, x=4000, y=7000).Rad.values
# CPU times: user 2.41 s, sys: 327 ms, total: 2.74 s
# Wall time: 3.24 s

result = tseries.persist()
progress(result)
# ~70 sec to read 100 time steps

# Try calculating a global mean for a single timestep
globmean = dat.isel(time=0).Rad.mean(["bandn"])
rmean = globmean.persist()
progress(rmean)

# Try calculating a global mean time series for the entire day
dailymean = dat.Rad.isel(bandn=0).mean(["x", "y"])
rmean = dailymean.persist()
progress(rmean)

%time globmean = dat.Rad.mean(["t"])
