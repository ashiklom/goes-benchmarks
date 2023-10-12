#!/usr/bin/env python

# In case you accidentally generate a bunch of Kerchunk reference files without 
# filenames in the keys, this will re-insert the file names back in.

import redis
import ujson

from tqdm import tqdm

import xarray as xr

def fname_from_key(key):
    import os
    pieces = key.split("_")
    dpiece = pieces[3] 
    year = dpiece[1:5]
    doy = dpiece[5:8]
    hr = dpiece[8:10]
    fname = f"/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/{year}/{doy}/{hr}/{key}"
    assert os.path.exists(fname)
    return fname

def fix_paths(dct, fname):
    import re
    refs = dct["refs"]
    all_keys = list(refs.keys())
    pattern = re.compile("[A-Za-z_]+/\d+\\.\d+")
    chunk_keys = [k for k in all_keys if pattern.match(k)]
    for key in chunk_keys:
        val = refs[key]
        if type(val) == list and len(val) == 3 and val[0] is None:
            val[0] = fname
    return dct

rgood = redis.Redis(db=0)
rfixed = redis.Redis(db=2)

all_keys = sorted(rgood.keys())

key = all_keys[0]
for key in tqdm(all_keys):
    path = fname_from_key(key.decode())
    raw_dict = ujson.loads(rgood.get(key))
    fixed_dict = fix_paths(raw_dict, path)
    rfixed.set(key, ujson.dumps(fixed_dict).encode())
