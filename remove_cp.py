#!/usr/bin/env python
# coding: utf-8
######################################################################################################
#
# Remove GCM cp from 3hr ICAR dataset
#    - requires GCM cp regridded to ICAR grid (in GCM path)
#    -
#
# ToDO:
#    - not properly tested for CMIP5 24hr (GCM path is different)
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

# # the variables to remove: (moved to main_3hr_from3hinput)
# vars_to_drop=["swe", "soil_water_content", "hfls", # "hus2m",
#             "runoff_surface","runoff_subsurface",
#             "soil_column_total_water","soil_column_total_water",
#             "ivt","iwv","iwl","iwi", "snowfall_dt", "cu_precip_dt", "graupel_dt"]



##########################
#   FUNCTIONS
##########################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='remove GCM convective precip')
    # parser.add_argument('path_in',   help='path 3h/daily files (should have these as subdirs)')
    # parser.add_argument('path_out',   help='path to write to')
    parser.add_argument('year',      help='year to process')
    parser.add_argument('model',     help='model')
    parser.add_argument('scenario',  help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")
    # parser.add_argument('drop_vars',      help="drop variables that are not wanted (3hr only)", default=False,nargs="?")
    return parser.parse_args()


def myround(x, base=5):
    """ returns a number rounded to lowest (base)"""
    return int(base * np.floor(float(x)/base))


def drop_unwanted_vars(ds_in, vars_to_drop=None):

    drp=[]
    for v in vars_to_drop:
        if v not in ds_in.data_vars:
            print(f"var to drop {v} not in ds_in.data_vars")
            continue
        else:
            drp.append(v)

    ds_out = ds_in.drop_vars(drp)

    return ds_out


##############################
#  remove convective pcp 3hr
##############################
def remove_3hr_cp(ds_in, m, year, model, scen,
                  GCM_path='/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid',
                  noise_path = "/glade/derecho/scratch/bkruyt/CMIP6/uniform_noise_480_480.nc", # remove from func call in main.py
                  vars_to_drop=None,
                #   drop_vars=False, #  should just check for vars_to_drop=None ?
                  ):

    # for legacy code?
    dt='3hr'

    # ## N.B> Noise is only added if Noise path is not None
    # if noise_path is not None:
    #     print(f"\n   Adding noise from {noise_path} \n")
    #     u_noise = xr.open_dataset(f"{noise_path}" ).uniform_noise  #.load() # 55000 x 480 x480


    #___________ GCM cp ____________
    print("GCM_path[-4:]: ", GCM_path[-4:], " scen=", scen)

    if "CMIP5" in GCM_path:
        if scen=="historical" or scen=="historical/3hr": # no historical subfolder, in rcp45/85
            print(f'historical; opening  "{GCM_path}/rcp45/{model}/{model}*_cp_3hr_Igrid_{year}.nc')
            ds_convective_p= xr.open_mfdataset(
                f"{GCM_path}/rcp45/{model}/{model}*_cp_3hr_Igrid_{year}.nc"
            )
        else:
            print(f' opening  "{GCM_path}/{scen}/{model}/{model}*_cp_3hr_Igrid_{year}.nc')
            ds_convective_p= xr.open_mfdataset(
                f"{GCM_path}/{scen}/{model}/{model}*_cp_3hr_Igrid_{year}.nc"
            )
    else:  #CMIP6
        yr_10 = myround(year, base=10)  # the decade in which the year falls
        ds_cp1  = xr.open_mfdataset(
            f'{GCM_path}/3hr/{scen}/{model}*_cp_3hr_Igrid_{yr_10}-*.nc'
            )
        if year==2019 and ("ssp" in scen or scen=="hist"): # somehow the year 2019 is spread over the files 2010-2019 and 2020-2029 (CMIP6 only), so:
            print(f"   merging 2019 from 2 files...")
            yr_10 = myround(year+1, base=10)  # the (next) decade
            ds_cp2 = xr.open_mfdataset(
                f'{GCM_path}/3hr/{scen}/{model}*_cp_3hr_Igrid_{yr_10}-*.nc'
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


    ds_convective_p_sub = ds_convective_p.cp.sel(time=ds_convective_p.time.dt.month==int(m)).load()  #?

    print(f"    ...loaded GCM convective precipitation for year {year} month{m}")
    print(f"    GCM cp shape: {ds_convective_p_sub.shape }")
    print(f"    ICAR pcp shape: {ds_in[precip_var].shape }")
    print(f"    GCM times: {ds_convective_p_sub.time.values.min() } to {ds_convective_p_sub.time.values.max()}")
    print(f"    ICAR times: {ds_in.time.values.min() } to {ds_in.time.values.max()}")


    #----------------- subtract -----------------
    # Units are modified rom GCM kg m-2 s-1 to kg m-2
        # 2024 08 14: notebook GCM_cp_check.ipynb shows comparison CMIP5/6 cp and how the units are different
    if "CMIP6" in GCM_path:
        units_conv = 60*60*3 # no idea why this is reduced to 1h, maybe by Abby?
    elif "CMIP5" in GCM_path:
        units_conv = 60*60*3/4 # Ryan's GCM cp has different units?

    dsP_out=ds_in[precip_var]-ds_convective_p_sub * units_conv
    print(f"   timestep is {dt}, so multiplying GCM-cp by {units_conv} to obtain kg m-2")
    print(f"   Be sure to check GCM cp units in input!!!!! ")
    print(f"    GCM cp sum for {year} {m}: {ds_convective_p_sub.sum().values} kg m-2 s-1 or {ds_convective_p_sub.sum().values*units_conv/10e3} 10e3 kg m-2 ")
    print(f"    ICAR pcp sum (inc GCMcp): {ds_in[precip_var].sum().values/10e3 } 10e3 kg m-2")
    # # somehow subtracting the GCM cp does introduce negative values again, so we make sure those are set to zero: (this was probably because we subtracted 60*60*24 iso 60*60*6)?
    dsP_out=xr.where(dsP_out<0,0, dsP_out)  # not in all daily data! only files processed after November 1st 2023

    try:
        print("   Min ICAR (out) prec: ", np.nanmin(dsP_out.values)   ," kg/m-2"   )
        print("   Max ICAR (out) prec: ", np.nanmax(dsP_out.values)   ," kg/m-2"   )
    except:
        print("   Max prec slice gives error! ")

    # ----------- Remove unwanted vars (optional)  --------------
    # if drop_vars:
    if vars_to_drop is not None:
        print(f"   dropping {len(vars_to_drop)} variables from dataset")
        ds_in = drop_unwanted_vars(ds_in, vars_to_drop=vars_to_drop)

    # ------- add corrected precip to dataset ----------
    ds_in[precip_var].values = dsP_out.values
    ds_in[precip_var].attrs["processing_note3"] = "Removed GCM's convective precipitation from total precipitation"

    # ----- return result  -----
    return ds_in




# -  --- - -- -- - - - - - -

##############################
#  remove convective pcp 24hr
##############################
def remove_24hr_cp(ds_in, year, model, scen,
                  GCM_path='/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid',
                #   noise_path = "/glade/derecho/scratch/bkruyt/CMIP6/uniform_noise_480_480.nc",
                #   drop_vars=False,
                  vars_to_drop=None
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
    print(f"   opening GCM cp from: {GCM_path}"   )
    print(f"   ! ! !   CHECK input units of GCM cp thoroughly  ! ! ! " )
    if "CMIP6" in GCM_path: # one file per scen
        if scen=='hist': # hist is included in the scen folder
            ds_convective_p=xr.open_dataset(
                f'{GCM_path}/daily/ssp245/{ model}_bias_corr_convective_prec_regrid_ssp245_1950-2099.nc'
                )
        else:
            ds_convective_p=xr.open_dataset(
                f'{GCM_path}/daily/{scen}/{ model}_bias_corr_convective_prec_regrid_{scen}_1950-2099.nc'
                )
    elif "CMIP5" in GCM_path: # one file per scen
        if scen == 'rcp45' and  model=='MRI-CGCM3':
            # GCM_path='/glade/derecho/scratch/bkruyt/CMIP5/GCM_Igrid_3hr/rcp45' # this one was missing and is on my scratch iso Ryan's
            GCM_path='/glade/derecho/scratch/bkruyt/CMIP5/GCM_Igrid_daily'
        else:
            GCM_path='/glade/campaign/ral/hap/currierw/icar/output'
        scen_temp="rcp85" if scen=="historical" else scen
        print(f"  scen_temp={scen_temp}")

        ds_convective_p=xr.open_dataset(
                f'{GCM_path}/{model}_bias_corr_convective_prec_regrid_{scen_temp}_1950-2099.nc'
                ) # e.g.  MRI-CGCM3_bias_corr_convective_prec_regrid_rcp85_1950-2099.nc
        if "y" in ds_convective_p.dims:
            ds_convective_p=ds_convective_p.rename({"y":"lat_y", "x":"lon_x"})
            print(f"   dimensions changed from y to lat_y and x to lon_x")


    ds_convective_p_sub = ds_convective_p.cp.sel(time=ds_convective_p.time.dt.year==int(year)).load()  #?
    print(f"    ...loaded GCM convective precipitation for year {year}")
    print(f"    GCM cp shape: {ds_convective_p_sub.shape }")
    print(f"    ICAR pcp shape: {ds_in[precip_var].shape }")

    print(f"    GCM times: {ds_convective_p_sub.time.values.min() } to {ds_convective_p_sub.time.values.max()}")
    print(f"    ICAR times: {ds_in.time.values.min() } to {ds_in.time.values.max()}")


    #----------------- subtract -----------------
    # Units are modified rom GCM kg m-2 s-1 to kg m-2
    # 2024 08 14: notebook GCM_cp_check.ipynb shows comparison CMIP5/6 cp and how the units are different
    if "CMIP6" in GCM_path:
        units_conv = 60*60*24 # no idea why this is reduced to 1h, maybe by Abby?
    elif "CMIP5" in GCM_path or "currierw" in GCM_path :
        units_conv = 60*60*6 # org GCM data has 6h timestep, daily is summed(?) so to convert to kg m-2 mult. by 6

    dsP_out=ds_in[precip_var]-ds_convective_p_sub * units_conv

    print(f"   multiplying GCM-cp by {units_conv} to go from kg m-2 s-1 to kg m-2")
    print(f"    GCM cp sum for year {year}: {ds_convective_p_sub.sum().values} kg m-2 s-1 or {ds_convective_p_sub.sum().values*units_conv/10e6} 10e6 kg m-2 ")
    print(f"    ICAR pcp sum (inc GCMcp): {ds_in[precip_var].sum().values/10e6 } 10e6 kg m-2")


    # # somehow subtracting the GCM cp does introduce negative values again, so we make sure those are set to zero: (this was probably because we subtracted 60*60*24 iso 60*60*6)?
    dsP_out=xr.where(dsP_out<0,0, dsP_out)  # not in all daily data! only files processed after November 1st 2023

    try:
        print("   Min ICAR (out) prec, after post processing: ", np.nanmin(dsP_out.values)   ," kg/m-2"   )
        print("   Max ICAR (out) prec, after post processing: ", np.nanmax(dsP_out.values)   ," kg/m-2"   )
    except:
        print("   Max prec slice gives error! ")


    # # ----------- Remove unwanted vars (optional - not for daily)  --------------
    # if vars_to_drop is not None:
    #     print(f"   dropping {len(vars_to_drop)} variables")
    #     ds_in = drop_unwanted_vars(ds_in, vars_to_drop=vars_to_drop)


    # ------- save ----------
    ds_in[precip_var].values = dsP_out.values
    ds_in[precip_var].attrs["processing_note3"] = "Removed GCM's convective precipitation from total precipitation"

    # print("   Max ICAR (out) prec: ", np.nanmax(ds_in[precip_var].values) ," kg/m-2"   )

    #-----  return  -----
    return ds_in







##############################
#    Main
##############################
if __name__=="__main__":
    """Stand_alone not implemented yet"""


    # process command line
    args = process_command_line()  # dt should be argument!!
    path_in     = args.path_in
    path_out    = args.path_out
    year        = int(args.year)
    model       = args.model
    scenario    = args.scenario
    scen        = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    # CMIP        = args.CMIP

    # Not tested in stand-alone:
    ds_in = xr.open_mfdataset( f"{path_in}/{model}_{scenario}/{year}/*.nc")

    for m in range(1,13):

        # aggregate in dict and concat?
        ds3hr_month = remove_3hr_cp(ds_in, m=m, year=year, model=model, scen=scen) #, drop_vars=False)

    # ds3hr = xr.concat( ....)