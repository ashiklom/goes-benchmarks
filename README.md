# Benchmarking different GOES datasets

## Instructions

1. Start with 1 day of geo data on AWS-West-2, original chunking

	- Data located in: `s3://eso-west2-curated/Shared/geostationary/GOES-16/L1B/2019/203`
	- Kerchunk script: `01-kerchunk-west2.py`

2. Time the reading of 1 day of data  (say Rad array) with kerchunk for all channels. For a simple workflow, compute min and max of 2D array Rad in each file. Try to read directly from S3, without the mounted file system because this will be the most common use case.

3. Repeat 2) but now reading from NOAA's S3 on AWS-East-1. We really need to understand the added latency of reading data across the country.

	- Data located in `s3://noaa-goes16/ABI-L1b-RadC/2019/203`
	- Browse the data at https://noaa-goes16.s3.amazonaws.com/index.html
  - (Replace 16 with 17 to get GOES 17)

4. Use `ncks` to re-chunk the data to 500,500 and 1000,1000 and repeat 2. Notice that you will need to regenerate the sidecar files for these new chunks.
