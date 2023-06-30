import s3fs
import fsspec
from fsspec.implementations.local import LocalFileSystem

fs = s3fs.S3FileSystem(anon=True)
base_path = 's3://esgf-world/CMIP6/OMIP/NOAA-GFDL/GFDL-CM4/omip1/r1i1p1f1/Omon/thetao/gr/v20180701/'
all_paths = fs.ls(base_path)
all_paths

from pangeo_forge_recipes.patterns import pattern_from_file_sequence

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 'time')

from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe
from pangeo_forge_recipes.storage import StorageConfig, FSSpecTarget, CacheFSSpecTarget, MetadataTarget

target = FSSpecTarget(LocalFileSystem(), "./result")
cache = CacheFSSpecTarget(LocalFileSystem(), "./cache")
metadata = MetadataTarget(LocalFileSystem(), "./metadata")
rec = HDFReferenceRecipe(
    pattern,
    storage_config = StorageConfig(target, cache, metadata),
    netcdf_storage_options={"anon": True}
)

from dask.diagnostics import ProgressBar
delayed = rec.to_dask()
with ProgressBar():
    delayed.compute()

# Try the output
import intake
cat = intake.open_catalog(f"{rec.target}/reference.yaml")
ds = cat.data.to_dask()
