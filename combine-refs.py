#!/usr/bin/env python

import redis
import ujson

import xarray as xr

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr

rgood = redis.Redis(db=2)
all_keys = sorted(rgood.keys())

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

all_keys_chunked = list(chunk_list(all_keys, 50))

chunk = all_keys_chunked[0]

def read_ref(key):
    rgood = redis.Redis(db=2)
    raw = rgood.get(key)
    dct = ujson.loads(raw)
    return dct

# Read an initial dict to get the commmon dims
# c0 = ujson.loads(rgood.get(chunk[0]))
ref0 = read_ref(chunk[0])
f0 = ref0["refs"]["Rad/1.30"][0]
dat = xr.open_dataset(f0)
common_dims = list(dat.dims.keys())

def process_chunk(chunk):
    refs = list(map(read_ref, chunk))
    result = MultiZarrToZarr(
        refs,
        coo_map={"t": "cf:t"},
        concat_dims=["t"],
        identical_dims=common_dims
    ).translate()
    return result

r1 = process_chunk(chunk)
