if __name__ == "__main__":
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)

    import time
    import intake
    cat = intake.open_catalog("/efs/goes-kerchunked/catalog.yml")

    print("Opening east1 from catalog")
    east1 = cat["goes16_east1"].to_dask()
    print("Opening west2 from catalog")
    west2 = cat["goes16_west2"].to_dask()

    # Confirm that I can just read values from both
    t1 = time.time()
    e1val = east1["Rad"].isel(t=3, x=500, y=500).values
    t2 = time.time()
    print(e1val)
    print(f"Time elapsed: {t2 - t1}")
    t1 = time.time()
    w2val = west2["Rad"].isel(t=3, x=500, y=500).values
    t2 = time.time()
    print(w2val)
    print(f"Time elapsed: {t2 - t1}")

    # Spatial average for a single time step
    # %time east1.isel(t=2)["Rad"].mean().values
    # 12.2 sec with a 36-core cluster
    # 46.7 sec on the login node (4 cores)

    # Time series at a location
    # %time east1["Rad"].isel(x=1800, y=1200).mean().values
    # 3min, 1sec
