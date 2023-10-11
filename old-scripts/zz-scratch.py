import pandas as pd
import humanfriendly
# import os

# with open("goesfiles-all", "r") as f:
#     all_files = sorted(f.read().splitlines())
#
# sizes = [os.path.getsize(f) for f in all_files]

fname = "goes-2022-10"

dat = pd.read_table(
    fname,
    sep='\s+',
    header=None,
    names=['permission', 'depth', 'user', 'group', 'size_str',
           'month', 'day', 'year_time', 'path']
)

dfiles = dat[dat['path'].apply(lambda x: '.nc' in x)]
dfiles = dfiles.assign(
    size = dfiles["size_str"].apply(humanfriendly.parse_size)
).sort_values('size')

from kerchunk.hdf import SingleHdf5ToZarr

testout = SingleHdf5ToZarr(dfiles.iloc[1].path).translate()
len(testout['refs'].keys())

import xarray as xr
badfiles = []
for fname in tqdm.tqdm(dfiles['path']):
    try:
        xr.open_dataset(fname)
    except OSError as err:
        print(err)
        badfiles.append(fname)

xd = xr.open_dataset(dfiles['path'].iloc[0])

import subprocess
subprocess.run(['ncinfo', dfiles['path'].iloc[0]])

import tqdm

################################################################################

import xarray as xr

# dat = xr.open_dataset("data/input/OR_ABI-L1b-RadC-M6C10_G16_s20192030916392_e20192030919176_c20192030919218.nc")
# dat = xr.open_dataset("data/input/OR_ABI-L1b-RadC-M6C10_G16_s20192030921392_e20192030924176_c20192030924221.nc")
