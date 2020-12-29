"""
Read in BAHM outputs and USGS data, compile to netcdf ready for ERDDAP,
and hopefully the master dataset for things like DFM bcs.
"""
from __future__ import print_function

import os
import glob
import pandas as pd
import xarray as xr
import numpy as np

from stompy import utils
from stompy.spatial import wkb2shp, proj_utils
from stompy.io import rdb

import stompy.model.delft.io as dio

opj=os.path.join

##

# configure paths:
flow_dir_bahm="../ModelforNutrient/BAHM Flow"
flow_dir_tan="../TanModel_v1_Flow"
flow_dir_usgs = "../USGS_flow"
shp_fn_dir = "../ModelforNutrient/BAHM Flow/PourPointsforBAHydroModel"

## 

# Load name and location data for the watersheds which
# enter the Bay
shp_fn=opj(shp_fn_dir,
           "Watershed2_CustomOutFlowPts.shp")

target_srs='EPSG:26910'
pour_points=wkb2shp.shp2geom( shp_fn,
                              target_srs=target_srs)
## 


def df_post(df):
    """ add cms, and unix time, in place.
    """
    df['flow_cms']=0.028316847*df.flow_cfs
    df['unix_time'] = utils.to_unix( df.date.values )
    
# reading the BAHM output files:
def load_bahm_flow(src_fn):
    """
    parse the BAHM output format (fixed width text) into
    pandas dataframe, add the extra fields via df_post()
    and return
    """
    df=pd.read_fwf(src_fn,
                   [ (0,4), (5,7), (8,10),(10,23) ],
                   skiprows=5,
                   names=['year','month','day','flow_cfs'],
                   parse_dates={'date': [0,1,2] } )
    df_post(df)

    return df
    
# reading the Tan new model output files:
def load_tan_flow(src_fn):
    """
    parse the new model output format (fixed width text) into
    pandas dataframe, add the extra fields via df_post()
    and return
    """
    df=pd.read_fwf(src_fn,
                   [ (0,4), (5,7), (8,10),(10,23) ],
                   skiprows=1,
                   names=['year','month','day','flow_cfs'],
                   parse_dates={'date': [0,1,2] } )
    df_post(df)

    return df

##
utm2ll=proj_utils.mapper(target_srs,'WGS84')

# collect datasets for all the sources
all_ds=[]

for rec in pour_points:
    name=rec['immediatec']
    
    # Find the text data file that goes with this point
    src_name=name.replace(' ','')
    # Fix a few mismatches in naming
    if src_name=='UALAMEDAg':
        src_name='UALAMEDA'
    elif src_name=='COYOTEd':
        # HERE - COYOTE needs to come from USGS data anyway, not to mention that
        # that the src_name shouldn't have the 'd'.
        src_name='COYOTE'

    ds=xr.Dataset()
    ds['station']= ( ('station',), [src_name])
    ds['source'] = ( ('station',), ['BAHM (1999-2017)/Tan''s New Model (2018-2019)'])
    
    if src_name=='COYOTE':
        # special handling
        usgs_coyote_fn=opj(flow_dir_usgs,'11172175.txt')
        df0=rdb.rdb_to_dataset(usgs_coyote_fn).to_dataframe()
        df1=df0.reset_index()
        df=df1.rename(columns={'stream_flow_mean_daily':'flow_cfs','time':'date'})
        df_post(df)
        ds['source'] = ( ('station',), ['USGS'])
    else:
        # bahm data
        src_fn_1=opj(flow_dir_bahm,"%s.txt"%src_name)
        assert os.path.exists(src_fn_1)
        print("name: %s fn: %s"%(name,src_fn_1))
        df_1=load_bahm_flow(src_fn_1)
        # tan's model data
        src_fn_2=opj(flow_dir_tan,"%s.txt"%src_name)
        assert os.path.exists(src_fn_2)
        print("name: %s fn: %s"%(name,src_fn_2))
        df_2=load_tan_flow(src_fn_2)
        # splice them together
        df = df_1.append(df_2).reset_index(drop=True)

    # convert to dataset, ready for concatenation along a "station" dimension
    ds['time']= ( ('time',), df.date)
    ds['flow_cfs']=( ('station','time'), [df.flow_cfs])
    ds['flow_cms']=( ('station','time',), [df.flow_cms])

    pnts=np.atleast_2d(np.array(rec['geom']))
    pnt=pnts.mean(axis=0)
    ds['utm_x']=pnt[0]
    ds['utm_y']=pnt[1]
    ll=utm2ll(pnt)
    ds['longitude']=( ('station',), [ll[0]])
    ds['latitude']=( ('station',), [ll[1]])
    
    all_ds.append(ds)

##  

merged=xr.concat(all_ds,dim='station')


# add some nice metadata
merged['station'].attrs['cf_role']="timeseries_id"
merged['flow_cfs'].attrs['units']='ft3 s-1'
merged['flow_cms'].attrs['units']='m3 s-1'
for fld in ['utm_x','utm_y']:
    merged[fld].attrs['units']='m'
    merged[fld].attrs['ioos_category']='Location'
    merged[fld].attrs['long_name']='Projection %s Coordiante'%(fld[-1].upper())
    merged[fld].attrs['standard_name']='projection_%s_coordinate'%(fld[-1])
merged['longitude'].attrs.update(dict(standard_name='longitude',
                                      units='degrees_east'))
merged['latitude'].attrs.update( dict(standard_name='latitude',
                                      units='degrees_north') )

merged.attrs['featureType']='timeSeries'
             
##

#fn='/opt/data/sfei/sfbay_freshwater.nc'
fn="../outputs/sfbay_freshwater.nc"
os.path.exists(fn) and os.unlink(fn)
merged.to_netcdf(fn)
