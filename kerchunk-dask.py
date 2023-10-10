# Read the file list (or generate it if it doesn't exist)
import os
cachefile = "goesfiles-all"
if os.path.exists(cachefile):
    print("Using cached list of goes files.")
    with open(cachefile, 'r') as f:
        all_paths = sorted(f.read().splitlines())
else:
    print("Generating new GOES file list")
    import glob
    base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/'
    # base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001'
    all_paths = sorted(glob.glob(os.path.join(base_path, '**/*.nc'), recursive=True))
    with open(cachefile, 'w') as f:
        f.write("\n".join(all_paths))

# Optionally, start with a subset of the paths
# all_paths = all_paths[:1000]

# Try 20 days' worth of files
all_paths = [f for f in all_paths if "2022/00" in f or "2022/01" in f]
print(f"Processing {len(all_paths)} files")

# Load first dataset to get common dimensions
import xarray as xr
d0 = xr.open_dataset(all_paths[0])
common_dims = list(d0.dims.keys())

# Try using kerchunk combine
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import auto_dask

if __name__ == '__main__':
    print("Launching dask cluster")
    # Launch Dask cluster for parallelization
    from dask.distributed import Client, LocalCluster, progress
    cluster = LocalCluster()
    client = Client(cluster)
    print("Beginning processing...")
    result = auto_dask(
        urls=all_paths,
        single_driver=SingleHdf5ToZarr,
        single_kwargs={},
        mzz_kwargs={
            "coo_map": {"t": "cf:t"},
            "concat_dims": ["t"],
            "identical_dims": common_dims
        },
        n_batches=sum(client.ncores().values()),
        filename="daskresult.json"
    )
    print("Done!")
