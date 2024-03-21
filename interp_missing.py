#!/usr/bin/env python
# coding: utf-8
######################################################################################################
#
# Interpolate missing time steps
#
#
# Usage:
#   - can be used stand-alone, but intended for use from main.py
#
# Authors: Bert Kruyt, NCAR RAl 2023
######################################################################################################

import argparse
import xarray as xr
import dask
import numpy as np
import glob
import os
import pandas as pd
import cftime
import multiprocessing as mp
import time
import sys
from datetime import datetime, timedelta

dask.config.set(**{'array.slicing.split_large_chunks': True})


def find_intp_missing(ds):
    """find missing timesteps and interpolate them. Return fixes dataset"""


    # Find out what is missing
    idx = np.where(ds.time.diff(dim='time').values > min(ds.time.diff(dim='time').values ) )[0] #? [0]
    if idx[0]!=0:
        print(ds.time[idx[0]-1:idx[0]+2].values )
    else:
        print(ds.time[idx[0]:idx[0]+3].values )
    # if idx[0]!=0:     print(ds.time[idx[0]-1:idx[0]+2].values )



    try:
        t = np.arange( ds.time[idx[0]].values , ds.time[idx[0]+1].values , timedelta(hours=3))
    except:
        t = np.arange( ds.time[idx[0]].values.astype('datetime64[h]') ,
                    ds.time[idx[0]+1].values.astype('datetime64[h]'),
                    timedelta(hours=3))
    # else:
    #     print('need to think about this case')
    #     t = np.arange( ds.time[idx[0]].values , ds.time[idx[0]+1].values , timedelta(hours=3))



    missing_time = t[1:]
    print(f" missing_time = {missing_time}")


    # Create a full time period:
    t1 = ( f"{str(ds.time[0].dt.year.values)}-{str(ds.time[0].dt.month.values).zfill(2)}-{str(ds.time[0].dt.day.values).zfill(2)} {str(ds.time[0].dt.hour.values).zfill(2)}:00:00")
    t2 = f"{str(ds.time[-1].dt.year.values)}-{str(ds.time[-1].dt.month.values).zfill(2)}-{str(ds.time[-1].dt.day.values).zfill(2)} {str(ds.time[-1].dt.hour.values).zfill(2)}:00:00"

    print( t1, " - ",t2)


    if ds.time.dt.calendar=='noleap':
        ds.time[0]
        time2 = xr.cftime_range(start=t1 , #ds.time[0].values.astype('datetime64[h]'),
                    end=t2, #ds.time[-1].values.astype('datetime64[h]'),
                    freq='3H', calendar='noleap' )

    elif ds.time.dt.calendar=='proleptic_gregorian':
        time2 = pd.date_range(start=t1 , #ds.time[0].values.astype('datetime64[h]'),
                    end=t2, #ds.time[-1].values.astype('datetime64[h]'),
                    freq='3H' )
    # print(time2)

    # turn the full time into a dataArray:
    full_time = xr.DataArray(
        data=time2,
        dims=["time"],
    # coords=dict( time=time ),
    # attrs=dict(
    #     description="time.",
    #     units="degC",
    #     )
    )


    # _____ ReIndex ______
    full = ds.reindex(time=full_time, fill_value=np.nan).sortby("time")

    #Check no more missing:
    idx2 =np.where( full.time.diff(dim='time').values.astype(float) > full.time.diff(dim='time').values.astype(float)[0] )[0]
    if len(idx2)>0:
        print(f"  ! More missing values, at:")
        if idx2[0]!=0:
            print(ds.time[idx2[0]-1:idx2[0]+2].values )
        else:
            print(ds.time[idx2[0]:idx2[0]+3].values )
    else:
        print(f"  No more missing values, :) ")


    # _______ Interpolate _______
    #### do the interpolation:
    ds_int = full.interpolate_na(dim="time", method="linear")

    # check ta2m
    print( ds_int.ta2m[idx[0]-1:idx[0]+len(missing_time)+2, 10,10].values  )

    # check pcp
    print(full.precip_dt[idx[0]-1:idx[0]+len(missing_time)+2, 10,10].values  )
    print(ds_int.precip_dt[idx[0]-1:idx[0]+len(missing_time)+2, 10,10].values  )

    return ds_int


def overwrite(ds_out, path, filename):
    """overwrite existing file"""
    # ________ overwrite exisitng _____
    # rename old, incomplete file first:
    os.rename( f"{path_in}/{file2fix}", f"{path_in}/{file2fix}".replace(".nc", "_SHRT.nc") )

    ds_out.to_netcdf(f"{path_in}/{file2fix}")



if __name__=="main":

    path_in="/glade/campaign/ral/hap/bert/CMIP5/WUS_icar_livBC/CCSM4_rcp85/3hr"
    file2fix="icar_3hr_livgrid_CCSM4_rcp85_2010-2014.nc"


    ds_in =xr.open_dataset(f"{path_in}/{file2fix}")

    ds_fix=find_intp_missing( ds_in )

    #__ overwrite? ___

    overwrite(ds=ds_fix, path=path_in, filename=file2fix)