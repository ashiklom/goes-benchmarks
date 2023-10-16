import os
import glob
import xarray as xr

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import auto_dask, MultiZarrToZarr

from dask.distributed import Client, LocalCluster, progress

def safe_auto_dask(
    urls,
    single_driver,
    single_kwargs,
    mzz_kwargs,
    n_batches,
    remote_protocol=None,
    remote_options=None,
    filename=None,
    output_options=None,
):
    """Batched tree combine using dask.

    If you wish to run on a distributed cluster (recommended), create
    a client before calling this function.

    Parameters
    ----------
    urls: list[str]
        input dataset URLs
    single_driver: class
        class with ``translate()`` method
    single_kwargs: to pass to single-input driver
    mzz_kwargs: passed to ``MultiZarrToZarr`` for each batch
    n_batches: int
        Number of MZZ instances in the first combine stage. Maybe set equal
        to the number of dask workers, or a multple thereof.
    remote_protocol: str | None
    remote_options: dict
        To fsspec for opening the remote files
    filename: str | None
        Ouput filename, if writing
    output_options
        If ``filename`` is not None, open it with these options

    Returns
    -------
    reference set
    """
    import dask

    # Use logging for thread-safe error reporting
    # import logging
    # logpath = 'kerchunk-dask.log'
    # logger = logging.getLogger('log')
    # logger.setLevel(logging.INFO)
    # ch = logging.FileHandler(logpath, 'w')
    # ch.setFormatter(logging.Formatter('%(message)s'))
    # logger.addHandler(ch)

    # make delayed functions
    def safe_single(x):
        try:
            single_driver(x, **single_kwargs).translate()
        except OSError as e:
            # logger.info(f"File: <{x}>, Error: <{e}>")
            print(f"File: <{x}>, Error: <{e}>")
            return None

    single_task = dask.delayed(safe_single)
    post = mzz_kwargs.pop("postprocess", None)
    inline = mzz_kwargs.pop("inline_threshold", None)
    # TODO: if single files produce list of reference sets (e.g., grib2)

    def safe_batch(u_all, x_all):
        # First, remove empty results
        good = [i for i in range(len(x_all)) if x_all[i] is not None]
        u = [u_all[i] for i in good]
        x = [x_all[i] for i in good]
        return MultiZarrToZarr(
            u,
            indicts=x,
            remote_protocol=remote_protocol,
            remote_options=remote_options,
            **mzz_kwargs,
        ).translate()

    batch_task = dask.delayed(safe_batch)

    # sort out kwargs
    dims = mzz_kwargs.get("concat_dims", [])
    dims += [k for k in mzz_kwargs.get("coo_map", []) if k not in dims]
    kwargs = {"concat_dims": dims}
    if post:
        kwargs["postprocess"] = post
    if inline:
        kwargs["inline_threshold"] = inline
    for field in ["remote_protocol", "remote_options", "coo_dtypes", "identical_dims"]:
        if field in mzz_kwargs:
            kwargs[field] = mzz_kwargs[field]
    final_task = dask.delayed(
        lambda x: MultiZarrToZarr(
            x, remote_options=remote_options, remote_protocol=remote_protocol, **kwargs
        ).translate(filename, output_options)
    )

    # make delayed calls
    tasks = [single_task(u) for u in urls]
    tasks_per_batch = -(-len(tasks) // n_batches)
    tasks2 = []
    for batch in range(n_batches):
        ibatch = slice(batch * tasks_per_batch, (batch + 1) * tasks_per_batch)
        in_tasks = tasks[ibatch]
        u = urls[ibatch]
        if in_tasks:
            # skip if on last iteration and no remaining tasks
            tasks2.append(batch_task(u, in_tasks))
    return dask.compute(final_task(tasks2))[0]

if __name__ == '__main__':

    import time
    print(f"Starting at {time.strftime('%c', time.localtime())}")
    start_time = time.time()
    # Launch Dask cluster for parallelization
    if not "cluster" in locals():
        print("Launching dask cluster")
        cluster = LocalCluster()
        client = Client(cluster)
    else:
        print("Using existing dask cluster")

    # Read the file list (or generate it if it doesn't exist)
    cachefile = "goesfiles-all"
    if os.path.exists(cachefile):
        print("Using cached list of goes files.")
        with open(cachefile, 'r') as f:
            all_paths = sorted(f.read().splitlines())
    else:
        print("Generating new GOES file list")
        base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/'
        # base_path = '/css/geostationary/BackStage/GOES-17-ABI-L1B-FULLD/2022/001'
        all_paths = sorted(glob.glob(os.path.join(base_path, '**/*.nc'), recursive=True))
        with open(cachefile, 'w') as f:
            f.write("\n".join(all_paths))

    # # Try 20 days' worth of files
    all_paths = [f for f in all_paths if "2022/00" in f or "2022/01" in f]
    print(f"Processing {len(all_paths)} files")

    # Load first dataset to get common dimensions
    d0 = xr.open_dataset(all_paths[0])
    common_dims = list(d0.dims.keys())

    print("Beginning processing...")
    result = safe_auto_dask(
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

    end_time = time.time()
    elapsed = end_time - start_time
    nfiles = len(all_paths)
    rate = float(nfiles) / elapsed
    print(f"Processed {nfiles} files in {elapsed:.2f} seconds ({rate:.2f} files/sec)")
    print(f"Done at {time.strftime('%c', time.localtime())}")
