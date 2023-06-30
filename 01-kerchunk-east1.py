from dask.distributed import Client, LocalCluster, progress
cluster = LocalCluster()
client = Client(cluster)

import s3fs
import itertools
from fsspec.implementations.local import LocalFileSystem

import dask
from dask.delayed import delayed

base_path = 's3://noaa-goes16/ABI-L1b-RadF/2019/203'
outdir = "./data/results/kerchunk-east1"
anon=True

def lsf(path):
    paths = fs.ls(path)
    return [p for p in paths if p.endswith(".nc")]

def ls_recursive(paths):
    it = [delayed(lsf)(p) for p in paths]
    return list(itertools.chain.from_iterable(dask.compute(*it)))

fs = s3fs.S3FileSystem(anon=anon)

hrs = fs.ls(base_path)
# This step can take a while if done sequentially...so we accelerate it with dask
all_paths = ls_recursive(hrs)
all_paths[:5]
all_paths[-5:]
len(all_paths)

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 't')

from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, CacheFSSpecTarget, MetadataTarget

# import shutil
# shutil.rmtree(outdir)
target = FSSpecTarget(LocalFileSystem(), f"{outdir}/result")
cache = CacheFSSpecTarget(LocalFileSystem(), f"{outdir}/cache")
metadata = MetadataTarget(LocalFileSystem(), f"{outdir}/metadata")
rec = HDFReferenceRecipe(
    pattern,
    storage_config = StorageConfig(target, cache, metadata),
    netcdf_storage_options={"anon": anon}
)

delayed = rec.to_dask()
result = delayed.persist()
progress(result)
