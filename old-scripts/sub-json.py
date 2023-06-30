infile = "/efs/goes-kerchunked/GOES-16-L1B_us-west-2/reference.json"
outfile = "/efs/goes-kerchunked/GOES-16-L1B_us-west-2/reference-subbed.json"

with open(infile, "r") as f:
    instring = f.read()

outstring = instring.replace(
    "s3://noaa-goes16/ABI-L1b-RadC/",
    "s3://eso-west2-curated/Shared/geostationary/GOES-16/L1B/"
)

with open(outfile, "w") as f:
    f.write(outstring)
