from dask.distributed import Client, LocalCluster
cluster = LocalCluster()
client = Client(cluster)

import intake

# cat = intake.open_catalog("data/results/kerchunk-east1/result/reference.yaml")
cat = intake.open_catalog("/efs/goes-kerchunked/catalog.yml")

east1 = cat["goes16_east1"].to_dask()

# Spatial average for a single time step
%time east1.isel(t=2)["Rad"].mean().values
# 12.2 sec with a 36-core cluster
# 46.7 sec on the login node (4 cores)

# Time series at a location
%time east1["Rad"].isel(x=1800, y=1200).mean().values
# 3min, 1sec
