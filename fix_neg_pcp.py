#!/usr/bin/env python
# coding: utf-8
######################################################################################################
#
# Fix negative precipitation values, by:
#    - calculating timestep precipitation (precip_dt) (and snowfall_dt, etc)
#    - finding the timesteps where there are negative precip_dt values,
#    - set ALL values at those timesteps to NaN (this is version -2, first version only did the neg values)
#    - interpolate those NaN's (in time)
#
# 3h by month ONLY
#
# Usage:
#   - can be used stand-alone, but intended for use from main.py
#
# Authors: Bert Kruyt, NCAR RAl 2023
#
# changes:
#    2023-12-06: version 2, which interpolates all values at weird timestep, not just negative values.
#    2024-01-28: version to be called from main.py
######################################################################################################

import argparse
import xarray as xr
import numpy as np
import glob
import os
import datetime
import pandas as pd
import cftime
import multiprocessing as mp
import time
import sys

##################################        USER SETTINGS        ##################################
# # where the 3hr hourly data is:
# base_path = '/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3hr'    # {base_path}/CMIP6_{scen}/{model}/3hr/icar_3hr_*.nc'
# # base_path = '/glade/campaign/ral/hap/abby/ICAR'
# out_path  = '/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3h_pcpfix'
#
# !!!   N.B. settings in batch submit script!!!!

neg_thrsh = -0.0001 # threshold below which negative values get interpolated (setting to 0 leads to half the data being flagged, not good.)

overwrite=True

########################################################################
#                Subroutines / Functions                               #
# #######################################################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='Aggregate 1 day files to month(3h) and year (24h), while also fixing neg precip')
    parser.add_argument('path_in',   help='path 3h/daily files (should have these as subdirs)')
    parser.add_argument('path_out',   help='path to write to') # maybe 2 paths, one for 3h one for day?
    parser.add_argument('year',   help='year to process')
    parser.add_argument('model',     help='model')
    parser.add_argument('scenario', help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')

    return parser.parse_args()


##############################################################################################
#      calculate timestep amount from cumulative variable, and correct negative values       #
##############################################################################################
def correct_var(ds1, varname, varname_dt, ds2=None):
    """ takes a cumulative variable and returns the timestep version of that variable without negative values"""

    print(f"\n   correcting negative {varname} ...")

    if not varname in ds1.data_vars:
        # print(f" \n ! ! !   {varname} not found in data_vars. Already corrected?   ! ! ! \n")
        # return ds1
        sys.exit(f" \n ! ! !   {varname} not found in data_vars. Already corrected?   ! ! ! \n")

    remove_last=False
    # add last timestep of next file so we can calculate the difference: (need one chunk along time dim for interpolation)
    # if has_nextyearfiles:
    if ds2 is not None:
        pcp = xr.concat([ds1[varname], ds2[varname][0]], dim='time').chunk({"time": -1, "lat_y": "auto", "lon_x": "auto"})
    else: # year=2099 or 2049 without 2050
        pcp = ds1[varname].chunk({"time": -1, "lat_y": "auto", "lon_x": "auto"})

    # calc timestep amount from cumulative:
    pcp_dt = pcp.diff(dim='time', label='lower')#.load()

    # 1. where are the negative values?
    idx = np.where( pcp_dt.values < neg_thrsh ) #-0.0001 )
    print(f'      {len(np.unique(idx[0]))} negative timesteps found:')
    for i in range(len( np.unique(idx[0]))):
        print( '      ', pcp_dt.time[np.unique(idx[0])[i]].values, '   ',np.round(pcp_dt[np.unique(idx[0])[i]].min().values,2)  )

    # # 2.A replace w nans (only neg values)
    # pcp_na       = xr.where(  pcp_dt< neg_thrsh,    #-0.000001 ,       # cond
    #                           np.nan ,              # when cond is T
    #                           pcp_dt                # when cond is F
    #                         )
    # print(f'      negative {varname} replaced w NaNs' )


    print(f"   nans before: {np.sum(pcp_dt.isnull().values)}")
    # # 2.B replace w nans (all values in suspect timestep)
    idx = np.where( pcp_dt.values < neg_thrsh ) # return indices
    a = np.empty(pcp_dt.isel(time=0).shape)
    a[:] = np.nan
    for i in range(len(np.unique(idx[0]))):
        # pcp_dt[np.unique(idx[0])[i],:,:].values = a
        pcp_dt[np.unique(idx[0])[i],:,:] = a

    print(f"   nans afer: {np.sum(pcp_dt.isnull().values)}")
    # print(f"   equals #timesteps: {np.sum(pcp_dt.isnull().values)/439/319 }")

    # 3. check?
    # Think about edge cases:
       # a. first value idx[0][0]==0 is negative? -> load previous file and redo?
       # b. last value idx[0][-1]==-1 is negative? -> ...
    if len(np.unique(idx[0]))>0 and  pcp_dt.time[np.unique(idx[0])[-1]].values ==  pcp_dt.time[-1].values :
        print( "\n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n !!!    last timestep is negative, loading one more timestep...  !!! \n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" )
        if ds2 is not None: # otherwise not much we can do..
            # redo the calculation with one additional timestep
            pcp = xr.concat([ds1[varname], ds2[varname][:2]], dim='time').chunk({"time": -1, "lat_y": "auto", "lon_x": "auto"})
            pcp_dt = pcp.diff(dim='time', label='lower')
            remove_last=True # flag to remember to take the last timestep off again later.

            # look for neg values again:
            idx = np.where( pcp_dt.values < neg_thrsh ) # return indices
            a = np.empty(pcp_dt.isel(time=0).shape)
            a[:] = np.nan
            for i in range(len(np.unique(idx[0]))):
                pcp_dt[np.unique(idx[0])[i],:,:] = a

    # 4. interpolate nans
    # pcp_dt_pos = pcp_na.interpolate_na(dim="time", method='linear'   )
    pcp_dt_pos = pcp_dt.interpolate_na(dim="time", method='linear'   )
    print('      NaNs interpolated' )

    # check final reslult:
    idx2 = np.where( pcp_dt_pos< neg_thrsh) # -0.0001 )
    print(f'      {len(np.unique(idx2[0]))} negative timesteps found in corrected data:')
    for i in range(len( np.unique(idx2[0]))):
        print( '      ', pcp_dt_pos.time[np.unique(idx2[0])[i]].values, '   ',np.round(pcp_dt[np.unique(idx2[0])[i]].min().values,2) ) # time should be same(?)

    # 5. there can still be very small neg values (between 0 and neg_thresh). These we set to zero:
    pcp_dt_pos = xr.where(pcp_dt_pos<0,0, pcp_dt_pos)

    # 6. add corrected timestep-precipitation to dataset
    if remove_last or (ds2 is None):
        ds1[varname_dt] = pcp_dt_pos[:-1]
    else:
        ds1[varname_dt] = pcp_dt_pos

    # write attrs:
    ds1[varname_dt].attrs['processing_note'] = f'From the cumulative {varname}, calculated difference with diff(dim="time",label="lower"). Negative values due to restart errors were replaced with interpolated values (linearly interpolated in time).'
    ds1[varname_dt].attrs['units']           = 'kg m-2'
    ds1[varname_dt].attrs['standard_name']   = f'{varname}_amount_dt'
    ds1[varname_dt].attrs['long_name']       = f'timestep {varname} amount '

    # return dataset without the original cumulative variable:
    return ds1.drop_vars([varname])




######################################################################################
######      open 3hr icar files and correct negative precipitation          ##########
######################################################################################
def open_and_remove_neg_pcp(files_in, nextmonth_file_in, vars_to_correct):

    """ remove negative precipitation and return a 3h dataset with a new variable precip_dt (3hr precipitation amount). Files_in can be a string (path) or xr.dataset. vars_to_correct a dict with cumulative names as keys, dt vars as values"""

    # ________ open one month/year of 1h/3h files  _______

    # take ds or path_to_files as input:
    if isinstance(files_in, str ) :
        print(f'   loading: {files_in}')
        try:
            ds1 = xr.open_mfdataset( files_in).chunk({"time": -1, "lat_y": "auto", "lon_x": "auto"}) # , parallel=True
        except OSError:
            # if not model in errlist.keys():errlist[model]=[] # initiate a errorlist for this model if it does not yet exist.
            err_file = files_in # err_file.append(ftime)
            print('\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(  '   !!! OSError in files /year: ',files_in,' !!!')
            print(  '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n')
            # with open(f'errors_{args.model}_{args.scenario}.txt', 'a+') as f:
            #     f.write(f"{files_in} \n")
            return
        except ValueError:
            # if not model in errlist.keys():err_file=[] # initiate a errorlist for this model if it does not yet exist.
            err_file = files_in   # err_file.append(ftime)
            print('\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(  '   !!! ValueError in files/year: ',files_in,' !!!')
            print(  '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n')
            # with open(f'errors_{args.model}_{args.scenario}.txt', 'a+') as f:
                # f.write(f"{files_in} \n")
            return
    elif isinstance( files_in, xr.core.dataset.Dataset):
        print(f"   input is ds with {(files_in.time.shape)} timesteps")
        ds1 = files_in

    # _______ open next month/year's first file:  ___________
    # we also need next year's first file to calc daily prec
    if not nextmonth_file_in is None:
        if isinstance(files_in, str ) :
            try:
                ds2 = xr.open_dataset( nextmonth_file_in ).chunk({"time": -1, "lat_y": "auto", "lon_x": "auto"}) #, parallel=True)
                print("   loaded next month's 1st file")
                has_nextyearfiles=True
            except OSError:
                # err_file.append(file2load_next.split('icar_out_')[-1])
                print('\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                print(  '   !!! OSError in file ',nextmonth_file_in.split('/')[-1],' !!!')
                print(  '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  \n')
                # with open(f'errors_{args.model}_{args.scenario}.txt', 'a+') as f:
                #     f.write(f"{nextmonth_file_in} \n")
                # print(f'\n   !!! no next year file for year {int(yr1)+1} \n')
                has_nextyearfiles=False
                ds2=None
        elif isinstance( nextmonth_file_in, xr.core.dataset.Dataset):
            print(f"   nextmonth_file_in is ds with {nextmonth_file_in.time.shape} time steps ") #{list(nextmonth_file_in.data_vars)}")
            ds2 = nextmonth_file_in
    else:
        has_nextyearfiles=False
        ds2 = None    # has_nextyearfiles=False ;




    # returns a dataset with the dt-version of thr variable iso the cumulative variable:
    for varname, varname_dt in vars_to_correct.items():
        # print(f'   correcting {varname} into {varname_dt}' )
        ds1 = correct_var(ds1, varname, varname_dt, ds2)

    #  return corrected dataset:
    return ds1


###########################
#     MAIN
###########################
# def main_pcp_fix():
if __name__=="__main__":

    """ fix negative precipitation"""

    # argparse ...

    # # make output dirs:
    # if (not os.path.exists( f"{args.path_out}/{args.model}_{scenario_out}/3hr" )):
    #     os.makedirs( f"{args.path_out}/{args.model}_{scenario_out}/3hr" )
    # print( f"writing to:   {args.path_out}/{args.model}_{scenario_out}/3hr")


    # # # #   monthly  # # # # #
    for m in range(1, 13):   #!!! start month dynamical based on glob.glob!!

        print(f"\n processing {year}-{str(m).zfill(2)}")

        # # where the output has to go:
        # out_filestring_3h  =  f"{args.path_out}/{args.model}_{scenario_out}/3hr/icar_3hr_{args.model}_{scen}_{year}-{str(m).zfill(2)}.nc"

        # # if out file exists, do not process:
        # if os.path.isfile(out_filestring_3h) & (not overwrite) :

        #     # open the file and check the nr of timesteps and variables  !!!! MODIFY!!!!
        #     ds_out=xr.open_dataset(out_filestring_3h)
        #     if len(ds_out.time) > 220 and len(ds_out.data_vars)>20: # februari is 224 (28*8)
        #         print( f"   !!! output file exists, skipping....")
        #         continue

        files_in_month = f"{base_path}_{year}-{str(m).zfill(2)}*.nc"

        # print( "\n")
        # print( len(files_in_month) )
        # print( files_in_month)

        # if there are no input files (e.g. 2005-01 in sspXX_2004), move to next month
        if len(glob.glob(files_in_month))==0:
            print(f"\n no input files for {year}-{str(m).zfill(2)}. ")
            continue

        try:
            if m<12:
                nextmonth_file_in = sorted(glob.glob(f"{base_path}_{year}-{str(m+1).zfill(2)}*.nc"))[0]
            elif m==12:
                nextmonth_file_in = sorted(glob.glob(f"{base_path}_{str(int(year)+1)}-01*.nc"))[0]
        except:
                nextmonth_file_in=None  # should catch all fringe cases, ie 2005 in hist, 2050 in sspXXX_2004
        print( "   nextmonth_file_in ", nextmonth_file_in )

        #%%%%%%%%%%%%%%%%    Open 3h dataset and correct precipitation:   %%%%%%%%%%%%%%%%%
        ###################################################################################
        t1=time.time()

        ds3hr = open_and_remove_neg_pcp(files_in_month, nextmonth_file_in)
        print(f"\n   correcting took: {time.time()-t1} sec")
