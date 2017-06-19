"""
Read the pre-compiled netcdf data, and write individual BC and pli files
for each source, and then concatenate all of those to global BC and pli 
files.
"""
from __future__ import print_function

import os

import xarray as xr

from stompy import utils
from stompy.spatial import wkb2shp, proj_utils

import stompy.model.delft.io as dio

opj=os.path.join

##

# configure paths:
nc_path="../outputs/sfbay_freshwater.nc"

out_dir="../outputs/dflow"

##


ds=xr.open_dataset(nc_path)

os.path.exists(out_dir) or os.mkdir(out_dir)


##
pli_files=[]
bc_files=[]

for stni in range(len(ds.station)):
    stn_ds=ds.isel(station=stni)

    # kind of a pain to get scalar values back out...
    src_name=stn_ds.station.item()

    # At least through the GUI, pli files must have more than one node.
    # Don't get too big for our britches, just stick a second node 50m east
    # if the incoming data is a point
    if 1: #-- Write a PLI file
        pnts=np.array( [[stn_ds.utm_x,stn_ds.utm_y],
                        [stn_ds.utm_x + 50.0,stn_ds.utm_y]] )
        pli_data=[ (src_name,pnts) ]
        pli_fn=opj(out_dir,"%s.pli"%src_name)

        dio.write_pli(pli_fn,pli_data)
        pli_files.append(pli_fn)

    if 1: #-- Write a BC file

        df=stn_ds.to_dataframe().reset_index()

        df['unix_time']=utils.to_unix(df.time.values)
        
        bc_fn=opj(out_dir,"%s.bc"%src_name)
        with open(bc_fn,'wt') as fp:
            # I think the 0001 has to be there, as it is used to
            # specify different values at different nodes of the pli
            # seems better to assume that incoming data is a daily average,
            # and keep it constant across the day
            # block-from means that the timestamp of a datum marks the beginning
            # of a period, and the value is held constant until the next timestamp
            # how about unix epoch for time units?
            fp.write("[forcing]\n")
            # This Name needs to match the name in the pli
            fp.write("Name               = %s_0001\n"%src_name)
            fp.write("Function           = timeseries\n")
            fp.write("Time-interpolation = block-from\n") # or linear, block-to 
            fp.write("Quantity           = time\n")
            fp.write("Unit               = seconds since 1970-01-01 00:00:00\n")
            fp.write("Quantity           = dischargebnd\n")
            fp.write("Unit               = m³/s\n")

            df.to_csv(fp,sep=' ',index=False, header=False, columns=['unix_time','flow_cms'])
        bc_files.append(bc_fn)


def concat_files(srcs,dest):        
    with open( dest,'wt') as fp_out:
        for src in srcs:
            with open(src,'rt') as fp_in:
                fp_out.write(fp_in.read())
    
concat_files( bc_files,
              opj(out_dir,'sfbay_freshwater-combined.bc') )
concat_files( pli_files,
              opj(out_dir,'sfbay_freshwater-combined.pli') )



