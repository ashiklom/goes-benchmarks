# List the files
import glob

base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/'
outdir = "./results/g17l1b"

# all_paths = sorted(glob.glob(base_path + '*/*/*.nc'))
with open('goesfiles', 'r') as f:
    all_paths = f.read().splitlines()
all_paths[:5]
all_paths[-5:]
len(all_paths)

import ujson
from kerchunk.hdf import SingleHdf5ToZarr
import os

from dask.distributed import Client, LocalCluster, progress
cluster = LocalCluster()
client = Client(cluster)

if not os.path.exists(outdir):
    os.mkdir(outdir)

def gen_json(f):
    try:
        with open(f, 'rb') as infile:
            h5chunks = SingleHdf5ToZarr(infile, f)
            outf = f'{outdir}/{os.path.basename(f)}.json'
            with open(outf, 'wb') as outfile:
                outfile.write(ujson.dumps(h5chunks.translate()).encode())
            return outf
    except:
        return None

import dask.bag as db

pathbag = db.from_sequence(all_paths).map(gen_json)
pb = pathbag.persist()
progress(pb)

# Consolidate results
import xarray as xr
d0 = xr.open_dataset(all_paths[0])
from kerchunk.combine import MultiZarrToZarr
json_list = glob.glob(f"{outdir}/*.json")
mzz = MultiZarrToZarr(
    json_list,
    concat_dims = "t",
    identical_dims = list(d0.dims.keys())
)
