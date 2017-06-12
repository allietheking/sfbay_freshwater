"""
Read in BAHM outputs and USGS data, compile to pli and bc files
for reading into D-Flow FM.
"""

import os
import glob
import pandas as pd

from stompy import utils
from stompy.spatial import wkb2shp, proj_utils
import stompy.model.delft.io as dio

opj=os.path.join

##

# configure paths:
bahm_dir="../ModelforNutrient"
flow_dir=opj(bahm_dir,"BAHM Flow")
out_dir="../outputs/dflow"

os.path.exists(out_dir) or os.makedirs(out_dir)

## 

reload(wkb2shp)

# Load name and location data for the watersheds which
# enter the Bay
shp_fn=opj(flow_dir,
           "PourPointsforBAHydroModel",
           "Watershed2_CustomOutFlowPts.shp")

pour_points=wkb2shp.shp2geom( shp_fn,
                              target_srs='EPSG:26910' )
## 

# reading the BAHM output files:
def load_bahm_flow(src_fn):
    df=pd.read_fwf(src_fn,
                   [ (0,4), (5,7), (8,10),(10,23) ],
                   skiprows=5,
                   names=['year','month','day','flow_cfs'],
                   parse_dates={'date': [0,1,2] } )

    df['flow_cms']=0.028316847*df.flow_cfs
    df['unix_time'] = utils.to_unix( df.date.values )

    return df

## 
for rec in pour_points:
    name=rec['immediatec']

    # Find the text data file that goes with this point
    src_name=name.replace(' ','')
    # Fix a few mismatches in naming
    if src_name=='UALAMEDAg':
        src_name='UALAMEDA'
    elif src_name=='COYOTEd':
        src_name='COYOTE'
        
    src_fn=opj(flow_dir,"%s.txt"%src_name)

    assert os.path.exists(src_fn)
    print "name: %s fn: %s"%(name,src_fn)
    df=load_bahm_flow(src_fn)

    # At least through the GUI, pli files must have more than on node.
    # Don't get too big for our britches, just stick a second node 50m east
    # if the incoming data is a point
    if 0: #-- Write a PLI file
        pnts=np.atleast_2d(np.array(rec['geom']))
        if pnts.shape[0]==1:
            pnts=np.concatenate( [pnts,pnts])
            pnts[1,0] += 50.0 

        pli_data=[ (src_name,pnts) ]
        pli_fn=opj(out_dir,"%s.pli"%src_name)

        dio.write_pli(pli_fn,pli_data)

        
    if 1: #-- Write a BC file
        
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
            fp.write("Name               = Bnd_%s_0001\n"%src_name)
            fp.write("Function           = timeseries\n")
            fp.write("Time-interpolation = block-from\n") # or linear, block-to 
            fp.write("Quantity           = time\n")
            fp.write("Unit               = seconds since 1970-01-01 00:00:00\n")
            fp.write("Quantity           = dischargebnd\n")
            fp.write("Unit               = m³/s\n")

            df.to_csv(fp,sep=' ',index=False, header=False, columns=['unix_time','flow_cms'])





## 


