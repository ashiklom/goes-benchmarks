from dask.distributed import Client, LocalCluster
cluster = LocalCluster()
client = Client(cluster)

import s3fs
import itertools
from fsspec.implementations.local import LocalFileSystem

import dask
from dask.delayed import delayed

def ls_recursive(paths):
    it = [delayed(fs.ls)(p) for p in paths]
    return list(itertools.chain.from_iterable(dask.compute(*it)))

fs = s3fs.S3FileSystem(anon=True)
base_path = 's3://noaa-goes17/ABI-L2-FDCC/2018/'
doys = fs.ls(base_path)
hrs = ls_recursive(doys)
# This step can take ages if done sequentially...so we accelerate it with dask
all_paths = ls_recursive(hrs)
all_paths[:5]
all_paths[-5:]
len(all_paths)

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 't')

from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, CacheFSSpecTarget, MetadataTarget

outdir = "./goes"
# import shutil
# shutil.rmtree(outdir)
target = FSSpecTarget(LocalFileSystem(), f"{outdir}/result")
cache = CacheFSSpecTarget(LocalFileSystem(), f"{outdir}/cache")
metadata = MetadataTarget(LocalFileSystem(), f"{outdir}/metadata")
rec = HDFReferenceRecipe(
    pattern,
    storage_config = StorageConfig(target, cache, metadata),
    netcdf_storage_options={"anon": True}
)

from dask.diagnostics.progress import ProgressBar
delayed = rec.to_dask()
with ProgressBar():
    delayed.compute()

################################################################################
# Now, try loading the result
import intake
cat = intake.open_catalog(f"./goes/result/reference.yaml")
ds = cat.data.to_dask()

dsub = ds.sel(t="2018-12-05")
%time dsub["Temp"].mean(("x", "y")).compute()

import plotext as plt
x = dsub.t
y = dsub["Temp"].mean(("x", "y"))
