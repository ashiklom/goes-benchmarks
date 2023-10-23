import os
import ujson
import re

import datetime

import xarray as xr
import pandas as pd
import numpy as np

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr

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

def parse_date(path):
    # path = paths[0]
    match = re.search(r'_s(\d{11})', path)
    dstring = match.groups()[0]
    dt = datetime.datetime.strptime(dstring, "%Y%j%H%M")
    return np.datetime64(dt)

def process_chunk(paths, outfile, BADFILES):
    """
    Generate a combined Kerchunk reference for a list of NetCDF files 
    and dump as JSON to `outfile`.
    """
    # paths = files_by_band[unique_bands[0]]
    # paths = paths[0:10]
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
    dates = np.array([parse_date(r["file"]) for r in good])
    multi = MultiZarrToZarr(
        refs,
        # coo_map={"t": "cf:t"},
        coo_map={"time": dates},
        concat_dims=["time"],
        identical_dims=identical_dims
    ).translate()
    with open(outfile, "wb") as f:
        f.write(ujson.dumps(multi).encode())
    return outfile

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]
