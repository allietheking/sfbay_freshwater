"""
Read in BAHM outputs and USGS data, compile to pli and bc files
for reading into D-Flow FM.
"""

import os
import glob
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

# Load name and location data for the watersheds which
# enter the Bay
pour_points=wkb2shp.shp2geom( opj(flow_dir,
                                  "PourPointsforBAHydroModel",
                                  "Watershed2_CustomOutFlowPts.shp") )

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

    # HERE:
    # Can pli file have a single node?  or do I have to fabricate
    # a second node from the point data in the shapefile?
    assert False # HERE - fix projection for this:
    pnts=np.atleast_2d(np.array(rec['geom']))

    pli_data=[ (src_name,pnts) ]
    pli_fn=opj(out_dir,"%s.pli"%src_name)
    
    dio.write_pli(pli_fn,pli_data)
