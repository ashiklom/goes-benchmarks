#!/usr/bin/env python

import xarray as xr

dat = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {
        "fo": "results/final-compressed/2022-001.json.zst",
        "target_options": {"compression": 'zstd'}
    }
})

# Try calculating a global mean
%time globmean = dat.Rad.mean(["t"])
