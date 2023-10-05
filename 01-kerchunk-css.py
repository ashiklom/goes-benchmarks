# List the files
import glob
import os

base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001'
outdir = "./results/g17l1b"

os.makedirs(outdir, exist_ok=True)

# Cache the full list of GOES files --- otherwise, this takes a while
# if os.path.exists('goesfiles'):
#     with open('goesfiles', 'r') as f:
#         all_paths = f.read().splitlines()
# else:
    # all_paths = sorted(glob.glob(base_path + '*/*/*.nc'))
    # with open('goesfiles', 'w') as f:
    #     f.write("\n".join(all_paths))

all_paths = sorted(glob.glob(base_path + '*/*/*.nc'))
all_paths[:5]
all_paths[-5:]
len(all_paths)

import ujson
from kerchunk.hdf import SingleHdf5ToZarr

def gen_json(f):
    outf = f'{outdir}/{os.path.basename(f)}.json'
    if os.path.exists(outf):
        return outf
    try:
        # Read the chunks locally (infile).
        # But...actually point to the files on S3, assuming they are identical!
        out_url = 's3://eso-west2-curated' + f
        with open(f, 'rb') as infile:
            h5chunks = SingleHdf5ToZarr(
                infile,
                out_url
            )
            with open(outf, 'wb') as outfile:
                outfile.write(ujson.dumps(h5chunks.translate()).encode())
            return outf
    except:
        return None

from dask.distributed import Client, LocalCluster, progress
cluster = LocalCluster()
client = Client(cluster)

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
