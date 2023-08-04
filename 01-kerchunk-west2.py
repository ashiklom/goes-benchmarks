from dask.distributed import Client, LocalCluster, progress
cluster = LocalCluster(n_workers=2)
client = Client(cluster)

import s3fs
import itertools
from fsspec.implementations.local import LocalFileSystem

import dask
from dask.delayed import delayed

#us-west-2: 's3://eso-west2-curated/Shared/geostationary/GOES-16/L1B/2019/203' ~~~DONE~~~
#us-east-1: 
#rechunked-data: 's3://eso-west2-curated/Shared/GOES-rechunked/GOES-16-ABI-L1B-FULLD-rc1000/2019/203/'
#rechunked-data: 's3://eso-west2-curated/Shared/GOES-rechunked/GOES-16-ABI-L1B-FULLD-rc500/2019/203/'

base_path = 's3://eso-west2-curated/Shared/GOES-rechunked/GOES-16-ABI-L1B-FULLD-rc1000/2019/203/'

#us-west-2 outdir: './data/results/kerchunk-west2' ~~~DONE~~~
#us-east-1: './data/results/kerchunk-east1'
#rechunked-data-path1: './data/results/rc1000'
#rechunked-data-path2: './data/results/rc500'

outdir =  './data/results/kerchunk-rc1000'
anon=False

# import shutil
# shutil.rmtree(outdir)

def lsf(path):
    paths = fs.ls(path)
    return [p for p in paths if p.endswith(".nc")]

def ls_recursive(paths):
    it = [delayed(lsf)(p) for p in paths]
    return list(itertools.chain.from_iterable(dask.compute(*it)))

fs = s3fs.S3FileSystem(anon=anon)

hrs = fs.ls(base_path)
# This step can take a while if done sequentially...so we accelerate it with dask
all_paths_raw = ls_recursive(hrs)

# In case both RadC and RadF files are present, use only the RadF files.
all_paths = sorted([p for p in all_paths_raw if "ABI-L1b-RadF" in p])

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 't')

from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, CacheFSSpecTarget, MetadataTarget

target = FSSpecTarget(LocalFileSystem(), f'{outdir}'/'result')
cache = CacheFSSpecTarget(LocalFileSystem(), f'{outdir}'/'cache')
metadata = MetadataTarget(LocalFileSystem(), f'{outdir}'/'metadata')
rec = HDFReferenceRecipe(
    pattern,
    storage_config = StorageConfig(target, cache, metadata),
    netcdf_storage_options={"anon": anon}
)

delayed = rec.to_dask()
result = delayed.persist()
progress(result)
