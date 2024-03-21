#!usr/bin/env python

#####################################################################################
#
# Aggregate ICAR postprocessing
#
# takes 1h / 3h ICAR output files and:
#   - check incomplete
#       - stop or interpolate / fill?  ( ToDo)
#   - corrects neg pcp
#   - aggregates to monthly (3hr) files / yearly (24hr) files
#   - removes GCM cp (requires GCM cp on ICAR grid AND missing timesteps in ICAR to be filled in )
#
#
# Usage:
#       - called from submit_postprocess(_SLURM).sh
#       - takes arguments: path_in, path_out, year, model, scenario, remove_cp, GCM_path
#
# Author:
#       Bert Kruyt, NCAR RAL 2024
#####################################################################################
import pandas as pd
from datetime import datetime, timedelta
import xarray as xr
import numpy as np
import glob
import os
import multiprocessing as mp
import sys
import cftime
import argparse
import time

# import functions
import check_complete as check
import aggregate_in_time as change_temporal_res
import fix_neg_pcp as fix
import remove_cp as cp





#################################
#       FUNCTIONS
#################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='remove GCM convective precip, add noise')
    parser.add_argument('path_in',          help='path input files (should have years as subdirs)')
    parser.add_argument('path_out',         help='path to write 3hr output to')
    # parser.add_argument('path_out_nocp',    help='path to write output with GCM cp to')
    parser.add_argument('year',             help='year to process')
    parser.add_argument('model',            help='model')
    parser.add_argument('scenario',         help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    parser.add_argument('remove_cp',        help='remove GCM cp from ICAR data, requires GCM_cp_path') # bool
    parser.add_argument('GCM_cp_path',      help='path with the GCM cp on ICAR grid')

    return parser.parse_args()


######################  3hr files are corrected by month   #########################
#
###################################################################################
def correct_to_monthly_3hr_files( path_in, path_out_3hr, model, scenario, year,
                                 GCM_path  = '/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid',
                                 drop_vars = False
                                ):
    '''Post process hourly ICAR output to monthly files with 3hr timestep'''

    # determine start month (for first year in run starting on month 10)
    if year==2005 and (scenario[:3]=='ssp' or scenario[:3]=='rcp') :
        m_start = 10
    elif year==2050 and scenario[:3]=='ssp' and scenario[-5:]=='_2049':
        m_start = 10
    elif year==2050 and scenario[:3]=='rcp' and scenario[-5:]=='_2050':
        m_start = 10
    else:
        m_start = 1

    for m in range(m_start,13):
        t1 = time.time()

        # path_m = f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(m).zfill(2)}*.nc"
        path_m = f"{path_in}/{model}_{scenario}/icar_*_{year}-{str(m).zfill(2)}*.nc"

        # __________  check files for completeness  ______
        print(f"\n**********************************************")
        print(f"   checking {year}-{str(m).zfill(2)}")
        check_result = check.check_month( path_to_files=path_m, m=m, ts_p_day=ts_per_day )

        # ________ interpolate / fill missing timesteps  __________
        # if check_result == not None:
            #   ... call interpolation routine?
            # .......

        # ____________       corr neg pcp      _____________
        #
        #     make timestep precip vars  &  fix neg precip
        #__________________________________________________
        print(f"\n   **********************************************")
        print(f"   fixing neg {vars_to_correct_3hr.keys()}  for {year}-{str(m).zfill(2)}")
        t0 = time.time()

        # find next month's file (needed to calculate timestep pcp (diff))
        try:
            if m<12:
                nextmonth_file_in = sorted(glob.glob(f"{path_in}/{model}_{scenario}/icar_*_{year}-{str(m+1).zfill(2)}*.nc"))[0]
            elif m==12:
                # nextmonth_file_in = sorted(glob.glob(f"{path_in}/{model}/{scenario}/{str(int(year)+1)}/icar_out_{str(int(year)+1)}-01*.nc"))[0]
                nextmonth_file_in = sorted(glob.glob(f"{path_in}/{model}_{scenario}/icar_*_{str(int(year)+1)}-01*.nc"))[0]
        except:
                nextmonth_file_in=None  # should catch all fringe cases, ie 2005 in hist, 2050 in sspXXX_2004
        print( "   nextmonth_file_in ", nextmonth_file_in )

        # call the correction functions
        ds_fxd = fix.open_and_remove_neg_pcp( path_m,
                                             nextmonth_file_in,
                                             vars_to_correct=vars_to_correct_3hr
                                             )
        print(f"\n   correcting  neg pcp took: {time.time()-t0} sec")


        # ____________ aggregate to 3hr monthly files __________
        print(f"\n   **********************************************")
        print(f"   aggregating to monthly 3hr files: {year}-{str(m).zfill(2)}")

        if int(24/ts_per_day)==1 :
            ds3hr = change_temporal_res.make_3h_monthly_file( ds_fxd ) #, directory_3hr=path_out_3hr)
        elif int(24/ts_per_day)==3 :  # if we already have 3hourly data, just aggregate to monthly?
            print(f"      input data already has 3hr timestep!")
            ds3hr = ds_fxd   #???  aggregate


        # _________  remove cp  ____________
        if remove_cp:
            print(f"\n   **********************************************")
            print( f'   removing GCM cp  {year}-{str(m).zfill(2)}  \n')
            t0 =time.time()
            ds3hr = cp.remove_3hr_cp( ds_in       = ds3hr,
                                        m           = m,
                                        year        = year,
                                        model       = model,
                                        scen        = scenario.split('_')[0],
                                        GCM_path    = GCM_path,
                                        noise_path  = noise_path,
                                        drop_vars   = drop_vars
                                        )
            print(f"\n   removing cp took: {np.round(time.time()-t0,1)} sec")


        # __________  save output  _______________
        # save 3hr dataset to disk:
        file_out_3hr  = f"{path_out_3hr}/{model}_{scenario}/3hr/icar_3hr_{model}_{scenario.split('_')[0]}_{year}-{str(m).zfill(2)}.nc"
        print(f"\n   **********************************************")
        print( '   writing 3hfile to ', file_out_3hr )

        if not os.path.exists(f"{path_out_3hr}/{model}_{scenario}/3hr"):
            os.makedirs(f"{path_out_3hr}/{model}_{scenario}/3hr")

        ds3hr.to_netcdf(file_out_3hr, encoding={'time'      :{'units':"days since 1900-01-01"},
                                                'precip_dt' :{'dtype':"float32"}
                                                } )


        # end month loop:
        print(f"\n   - - - - -   month {m} done in {np.round((time.time()-t1)/60,1)} min - - - - - ")



#################################
#           Main
#################################
if __name__ == '__main__':

    t00 = time.time()

    # process command line
    args = process_command_line()  # dt should be argument!!
    path_in         = args.path_in
    path_out_3hr    = args.path_out
    model           = args.model
    scenario        = args.scenario
    # scen    = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    year            = int(args.year)
    remove_cp       = True if args.remove_cp=="True" else False
    GCM_path        = args.GCM_cp_path if args.remove_cp=="True" else None



    ########          correct negative variables          ########
    vars_to_correct_3hr = {'precipitation'   : 'precip_dt',
                            'snowfall'        : 'snowfall_dt',
                            'cu_precipitation': 'cu_precip_dt',
                            'graupel'         : 'graupel_dt'
                            }

    vars_to_correct_24hr = {'precipitation'   : 'precip_dt' }

    # noise_path  = '/pscratch/sd/b/bkruyt/CMIP/uniform_noise_480_480.nc'
    # noise_path   = '/glade/derecho/scratch/bkruyt/CMIP6/uniform_noise_480_480.nc'
    noise_path   = None
    drop_vars    = True

    print(f"\n##############################################  ")
    print(f"   Making 3-hourly corrected ICAR files for: " )
    print(f"      {model}   {scenario}   {year}         \n")
    print(f"   remove GCM cp:           {remove_cp}       ")
    if noise_path is not None:
        print(f"   and adding noise from:   {noise_path}       ")
    else:
        print(f" !  NOT adding noise !!! ")
    print(f"   drop unwanted variables: {drop_vars}       ")
    print(f"##############################################  \n")

    # determine timestep (nr of timesteps per day): (currently diagnostic only)
    try:
        ts_per_day = check.determine_time_step(f"{path_in}/{model}_{scenario}/icar_*_{year}-{str(10).zfill(2)}*.nc")
    except:  # if we don;t have month 10 (2005 / 2050 at end of period)
        ts_per_day = check.determine_time_step(f"{path_in}/{model}_{scenario}/icar_*_{year}-{str(1).zfill(2)}*.nc")
    # print(f"  input timestep is {int(24/ts_per_day)} hr")

    if ts_per_day is not None:
        correct_to_monthly_3hr_files( path_in, path_out_3hr, model, scenario, year,
                                    GCM_path   = GCM_path,
                                    drop_vars  = drop_vars
                                    )
    else:
        print(f" could not determine input timestep")



    print(f"\n------------------------------------------------------ ")
    print(f"     {model} {scenario.split('_')[0]} {year} took {np.round((time.time()-t00)/60,1)} min ")
    print(f"------------------------------------------------------ \n ")



