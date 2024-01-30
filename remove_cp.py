#!/usr/bin/env python
# coding: utf-8
######################################################################################################
#
# Remove GCM cp from 3hr ICAR dataset and add noise
#    - requires GCM cp regridded to ICAR grid (in GCM path)
#    -
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
import datetime
import pandas as pd
import cftime
import multiprocessing as mp
import time
import sys

dask.config.set(**{'array.slicing.split_large_chunks': True})


#########################################
#         SETTTINGS
#######################################

# the variables to remove:
vars_to_drop=["swe", "soil_water_content", "hfls", "hus2m",
            "runoff_surface","runoff_subsurface",
            "soil_column_total_water","soil_column_total_water",
            "ivt","iwv","iwl","iwi", "snowfall_dt", "cu_precip_dt", "graupel_dt"]



# # #   NOISE  # # # #
noise_path = "/glade/derecho/scratch/bkruyt/CMIP6"
NOISE_u = xr.open_dataset(f"{noise_path}/uniform_noise_480_480.nc" )
u_noise = NOISE_u.uniform_noise  #.load() # 55000 x 480 x480




##########################
#   FUNCTIONS
##########################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='remove GCM convective precip, add noise')
    # parser.add_argument('path_in',   help='path 3h/daily files (should have these as subdirs)')
    # parser.add_argument('path_out',   help='path to write to')
    parser.add_argument('year',      help='year to process')
    parser.add_argument('model',     help='model')
    parser.add_argument('scenario',  help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")
    parser.add_argument('drop_vars',      help="drop variables that are not wanted (3hr only)", default=False,nargs="?")
    return parser.parse_args()


def myround(x, base=5):
    """ returns a number rounded to lowest (base)"""
    return int(base * np.floor(float(x)/base))


def drop_unwanted_vars(ds_in, vars_to_drop=vars_to_drop):

    for v in vars_to_drop:
        if v not in ds_in.data_vars:
            print(f"var to drop {v} not in ds_in.data_vars")
            continue

    ds_out = ds_in.drop_vars(vars_to_drop)

    return ds_out


##############################
#  remove convective pcp 3hr
##############################
def remove_3hr_cp(ds_in, m, year, model, scen,
                  GCM_path='/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid_3hr',
                  drop_vars=False,
                  vars_to_drop=vars_to_drop
                  ):

    # for legacy code?
    dt='3hr'

    #___________ GCM cp ____________
    yr_10 = myround(year, base=10)  # the decade in which the year falls
    ds_cp1  = xr.open_mfdataset(
        f'{GCM_path}/{scen}/{model}_cp_3hr_Igrid_{yr_10}-*.nc'
        )
    if year==2019: # somehow the year 2019 is spread over the files 2010-2019 and 2020-2029, so:
        print(f"   merging 2019 from 2 files...")
        yr_10 = myround(year+1, base=10)  # the (next) decade
        ds_cp2 = xr.open_mfdataset(
            f'{GCM_path}/{scen}/{model}_cp_3hr_Igrid_{yr_10}-*.nc'
            )
        ds_convective_p = xr.concat( [ds_cp1, ds_cp2], dim='time')
        print(f"  2019 length: {len(ds_convective_p.time.sel(time='2019').values)}")
    else:
        ds_convective_p = ds_cp1

    ds_convective_p = ds_convective_p.sel(time=ds_convective_p.time.dt.year==int(year))

    # ______ ICAR _______
    if 'precip_dt' in ds_in.data_vars:
        precip_var='precip_dt'
    elif 'precipitation' in ds_in.data_vars:
        precip_var='precipitation'
    elif 'Prec' in ds_in.data_vars:
        precip_var='Prec'
    else:
        print( f"! ! !  ERROR:  define precip variable in ICAR  dataset")
        sys.exit()


    print(year, str(year))
    ds_convective_p_sub = ds_convective_p.cp.sel(time=ds_convective_p.time.dt.month==int(m)).load()  #?

    print(f"    ...loaded GCM convective precipitation for year {year} month{m}")
    print(f"    GCM cp shape: {ds_convective_p_sub.shape }")
    print(f"    ICAR pcp shape: {ds_in[precip_var].shape }")
    print(f"    GCM times: {ds_convective_p_sub.time.values.min() } to {ds_convective_p_sub.time.values.max()}")
    print(f"    ICAR times: {ds_in.time.values.min() } to {ds_in.time.values.max()}")


    #----------------- subtract -----------------
    # Units are modified rom GCM kg m-2 s-1 to kg m-2
    # if dt =="daily":
    #     dsP_out=ds_in[precip_var]-ds_convective_p_sub * 60*60*24
    #     print(f"   timestep is {dt}, so multiplying GCM-cp by 60*60*24")
    # elif dt =="3hr":
    dsP_out=ds_in[precip_var]-ds_convective_p_sub * 60*60*3
    print(f"   timestep is {dt}, so multiplying GCM-cp by 60*60*3")

    #----------------- add noise -----------------
    noise_val= 0.01
    # noise_arr = noise_val * u_noise[:dsP_out.shape[0], :dsP_out.shape[1], :dsP_out.shape[2] ]
    t=len(ds_in.time)
    # if dt=="daily":
    #     noise_arr = noise_val * u_noise[(int(year) - 1950) * t : (int(year)-1950+1) * t , :dsP_out.shape[1], :dsP_out.shape[2] ]
    # elif dt=="3hr":
    noise_arr = noise_val * u_noise[0 : t , :dsP_out.shape[1], :dsP_out.shape[2] ]
        # need 8 times bigger noise array, so we repeat per year.... not ideal?

    # if(  (int(args.year)-1950+1) *t > u_noise.shape[0] ):
    #      noise_arr = noise_val * u_noise[(int(args.year) - 1950) * t : (int(args.year)-1950+1) *t , :dsP_out.shape[1], :dsP_out.shape[2] ]
    # else:
    #     noise_arr = noise_val * u_noise[(int(args.year) - 1950)  * t : (int(args.year)-1950+1) *t , :dsP_out.shape[1], :dsP_out.shape[2] ]


    # only the values that are 0 should get noise added, the values that aren’t 0 should get 0.1 (or whatever) added to them
    dsP_out = xr.where( dsP_out>0, dsP_out + noise_val, noise_arr.values)

    # # somehow subtracting the GCM cp does introduce negative values again, so we make sure those are set to zero: (this was probably because we subtracted 60*60*24 iso 60*60*6)?
    dsP_out=xr.where(dsP_out<0,0, dsP_out)  # not in all daily data! only files processed after November 1st 2023

    try:
        print("   Min ICAR (out) prec, after adding noise: ", np.nanmin(dsP_out.values)   ," kg/m-2"   )
        print("   Max ICAR (out) prec, after adding noise: ", np.nanmax(dsP_out.values)   ," kg/m-2"   )
    except:
        print("   Max prec slice gives error! ")

    # ----------- Remove unwanted vars (optional)  --------------
    if drop_vars:
        ds_in = drop_unwanted_vars(ds_in, vars_to_drop=vars_to_drop)

    # ------- add corrected precip to dataset ----------
    ds_in[precip_var].values = dsP_out.values
    ds_in[precip_var].attrs = {"note":"Removed GCM's convective precipitation from total precipitation"}

    print("   Min ICAR (out) prec: ", np.nanmin(ds_in[precip_var].values) ," kg/m-2"   )
    print("   Max ICAR (out) prec: ", np.nanmax(ds_in[precip_var].values) ," kg/m-2"   )

    # ----- return result  -----
    return ds_in




# -  --- - -- -- - - - - - -

##############################
#  remove convective pcp 24hr
##############################
def remove_24hr_cp(ds_in, m, year, model, scen,
                  GCM_path='/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid',
                  drop_vars=False,
                  vars_to_drop=vars_to_drop
                  ):

    # ______ ICAR pcp var _______
    if 'precip_dt' in ds_in.data_vars:
        precip_var='precip_dt'
    elif 'precipitation' in ds_in.data_vars:
        precip_var='precipitation'
    elif 'Prec' in ds_in.data_vars:
        precip_var='Prec'
    else:
        print( f"! ! !  ERROR:  define precip variable in ICAR  dataset")
        sys.exit()

    # -----------------open GCM on ICARgrid --------------
    if "CMIP6" in GCM_path: # one file per scen
        if scen=='hist': # hist is included in the scen folder
            ds_convective_p=xr.open_dataset(
                f'{GCM_path}/ssp245/{ model}_bias_corr_convective_prec_regrid_ssp245_1950-2099.nc'
                )
        else:
            ds_convective_p=xr.open_dataset(
                f'{GCM_path}/{scen}/{ model}_bias_corr_convective_prec_regrid_{scen}_1950-2099.nc'
                )
    elif "CMIP5" in GCM_path: # one file per scen
        # # CMIP5 DAILY :
        if scen == 'rcp45' and  model=='MRI-CGCM3':
            GCM_path='/glade/scratch/bkruyt/CMIP5/GCM_Igrid_3hr/rcp45' # this one was missing and is on my scratch iso Ryan's
        else:
            GCM_path='/glade/campaign/ral/hap/currierw/icar/output'
        print(f"   opening GCM cp from: {GCM_path}/{ model}_bias_corr_convective_prec_regrid_{scen}_1950-2099.nc")
        scen_temp="rcp85" if scen=="historical" else scen
        print(f"  scen_temp={scen_temp}")

        ds_convective_p=xr.open_dataset(
                f'{GCM_path}/{ model}_bias_corr_convective_prec_regrid_{scen_temp}_1950-2099.nc'
                ) # e.g.  MRI-CGCM3_bias_corr_convective_prec_regrid_rcp85_1950-2099.nc
        if "y" in ds_convective_p.dims:
            ds_convective_p=ds_convective_p.rename({"y":"lat_y", "x":"lon_x"})
            print(f"   dimensions changed from y to lat_y and x to lon_x")


    ds_convective_p_sub = ds_convective_p.cp.sel(time=ds_convective_p.time.dt.year==int(year)).load()  #?
    print(f"    ...loaded GCM convective precipitation for year {year}")
    print(f"    GCM cp shape: {ds_convective_p_sub.shape }")
    print(f"    ICAR pcp shape: {ds_in[precip_var].shape }")

    # Interpolate one more timestep at the end of GCM file.

    print(f"    GCM times: {ds_convective_p_sub.time.values.min() } to {ds_convective_p_sub.time.values.max()}")
    print(f"    ICAR times: {ds_in.time.values.min() } to {ds_in.time.values.max()}")


    #----------------- subtract -----------------
    # Units are modified rom GCM kg m-2 s-1 to kg m-2
    dsP_out=ds_in[precip_var]-ds_convective_p_sub * 60*60*24
    print(f"   timestep is 24hr, so multiplying GCM-cp by 60*60*24")

    # print(dsP_out)
    #----------------- add noise -----------------
    noise_val= 0.01
    # noise_arr = noise_val * u_noise[:dsP_out.shape[0], :dsP_out.shape[1], :dsP_out.shape[2] ]
    t=len(ds_in.time)

    noise_arr = noise_val * u_noise[(int(year) - 1950) * t : (int(year)-1950+1) * t , :dsP_out.shape[1], :dsP_out.shape[2] ]


    # # only the values that are 0 should get noise added, the values that aren’t 0 should get 0.1 (or whatever) added to them
    dsP_out = xr.where( dsP_out>0, dsP_out + noise_val, noise_arr.values)

    # # somehow subtracting the GCM cp does introduce negative values again, so we make sure those are set to zero: (this was probably because we subtracted 60*60*24 iso 60*60*6)?
    dsP_out=xr.where(dsP_out<0,0, dsP_out)  # not in all daily data! only files processed after November 1st 2023

    try:
        print("   Min ICAR (out) prec, after adding noise: ", np.nanmin(dsP_out.values)   ," kg/m-2"   )
        print("   Max ICAR (out) prec, after adding noise: ", np.nanmax(dsP_out.values)   ," kg/m-2"   )
    except:
        print("   Max prec slice gives error! ")


    # ----------- Remove unwanted vars (optional - not for daily)  --------------
    # if args.drop_vars and dt=="3hr":
    #     ds_in = drop_unwanted_vars(ds_in)



    # ------- save ----------
    ds_in[precip_var].values = dsP_out.values
    ds_in[precip_var].attrs = {"note":"Removed GCM's convective precipitation from total precipitation"}

    print("   Max ICAR (out) prec: ", np.nanmax(ds_in[precip_var].values) ," kg/m-2"   )

    #-----  return  -----
    return ds_in







##############################
#    Main
##############################
if __name__=="__main__":
    """Stand_alone not implemented yet"""


    # process command line
    args = process_command_line()  # dt should be argument!!
    path_in  = args.path_in
    path_out  = args.path_out
    year     = int(args.year)
    model    = args.model
    scenario = args.scenario
    scen    = args.scenario.split('_')[0]  # drop the year from sspXXX_year

    # Not tested in stand-alone:
    ds_in = xr.open_mfdataset( f"{path_in}/{model}_{scenario}/{year}/*.nc")

    for m in range(1,13):

        # aggregate in dict and concat?
        ds3hr_month = remove_3hr_cp(ds_in, m=m, year=year, model=model, scen=scen, drop_vars=False)

    # ds3hr = xr.concat( ....)