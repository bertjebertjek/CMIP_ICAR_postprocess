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
import aggregate_in_time as mon3hfiles
import fix_neg_pcp as fix
import remove_cp as cp


#################################
#       FUNCTIONS
#################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='remove GCM convective precip, add noise')
    parser.add_argument('path_in',          help='path input files (should have years as subdirs)')
    parser.add_argument('path_out',         help='path to write to')
    parser.add_argument('path_out_nocp',    help='path to write to')
    parser.add_argument('year',             help='year to process')
    parser.add_argument('model',            help='model')
    parser.add_argument('scenario',         help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    # parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")

    return parser.parse_args()


######################  3hr files are corrected by month   #########################
#
###################################################################################
def correct_to_monthly_3hr_files( path_in, path_out_3hr, model, scenario, year,
                                 GCM_path='/glade/scratch/bkruyt/CMIP6/GCM_Igrid_3hr',
                                 drop_vars=True
                                ):
    '''Post process hourly ICAR output to monthly files with 3hr timestep'''

    for m in range(1,13):
        path_m = f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(m).zfill(2)}*.nc"

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
        #     fix neg pcp on hourly files
        #__________________________________________________
        print(f"\n   **********************************************")
        print(f"   fixing neg {vars_to_correct_3hr.keys()}  for {year}-{str(m).zfill(2)}")
        t0 = time.time()

        # find next month's file (needed to calculate timestep pcp (diff))
        try:
            if m<12:
                nextmonth_file_in = sorted(glob.glob(f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(m+1).zfill(2)}*.nc"))[0]
            elif m==12:
                nextmonth_file_in = sorted(glob.glob(f"{path_in}/{model}/{scenario}/{str(int(year)+1)}/icar_out_{str(int(year)+1)}-01*.nc"))[0]
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
            ds3hr = mon3hfiles.make_3h_daily_file( ds_fxd ) #, directory_3hr=path_out_3hr)
        elif int(24/ts_per_day)==3 :  # if we already have 3hourly data, just aggregate to monthly?
            print(f" input data already has 3hr timestep!")
            ds3hr = ds_fxd   #???  aggregate

        # ____________ save corrected  _____________
        if save_3hr_month:  # make optional?
            # save 3hr dataset to disk:
            file_out_3hr  = f"{path_out_3hr}/{model}_{scenario}/3hr/icar_3hr_{model}_{scenario.split('_')[0]}_{year}-{str(m).zfill(2)}.nc"
            print(f"\n   **********************************************")
            print( '   writing 3hfile to ', file_out_3hr )

            if not os.path.exists(f"{path_out_3hr}/{model}_{scenario}/3hr"):
                os.makedirs(f"{path_out_3hr}/{model}_{scenario}/3hr")

            ds3hr.to_netcdf(file_out_3hr, encoding={'time':{'units':"days since 1900-01-01"},
                                                    # 'precipitation':{"dtype": "float32"},
                                                    # 'graupel':{"dtype": "float32"},
                                                    # 'snowfall':{"dtype": "float32"}
                                                    })


        # _________  remove cp  ____________
        #   check if GCM on ICAR grid /3hr
        #   check if no missing timesteps
        print(f"\n   **********************************************")
        print( f'   removing GCM cp  {year}-{str(m).zfill(2)}')
        ds_nocp = cp.remove_3hr_cp( ds_in       = ds3hr,
                                    m           = m,
                                    year        = year,
                                    model       = model,
                                    scen        =scenario.split('_')[0],
                                    GCM_path    ='/glade/scratch/bkruyt/CMIP6/GCM_Igrid_3hr',
                                    drop_vars   =  True
                                    )

        # _________ save 3h nocp file ________
        file_out_3hr_nocp  = f"{path_out_3hr_nocp}/{model}_{scenario}/3hr/icar_3hr_{model}_{scenario.split('_')[0]}{year}-{str(m).zfill(2)}*.nc"
        print(f"\n   **********************************************")
        print( '   writing 3h_nocp file to ', file_out_3hr_nocp )

        if not os.path.exists(f"{path_out_3hr_nocp}/{model}_{scenario}/3hr"):
            os.makedirs(f"{path_out_3hr_nocp}/{model}_{scenario}/3hr")

        ds_nocp.to_netcdf( file_out_3hr_nocp,
                          encoding={'time':{'units':"days since 1900-01-01"}}
                          )


        print(f"\n   - - - - -     {year} {m} done   - - - - - ")

    print(f"\n------------------------------------------------------ ")
    print(f"     {model} {scenario.split('_')[0]} {year} done ")
    print(f"------------------------------------------------------ \n ")



#################################
#           Main
#################################
if __name__ == '__main__':


    # process command line
    args = process_command_line()  # dt should be argument!!
    path_in  = args.path_in
    path_out_3hr =args.path_out
    path_out_3hr_nocp =args.path_out_nocp
    model    = args.model
    scenario = args.scenario
    # scen    = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    year     = int(args.year)

    # save monthly 3hr files with pcp fixed but GCM cp still included?
    save_3hr_month = False  # for now.

    ########          correct negative variables          ########
    vars_to_correct_3hr = {'precipitation'   : 'precip_dt',
                            'snowfall'        : 'snowfall_dt',
                            'cu_precipitation': 'cu_precip_dt',
                            'graupel'         : 'graupel_dt'
                            }

    vars_to_correct_24hr = {'precipitation'   : 'precip_dt' }


    print(f"\n#######################################  ")
    print(f"   {model}   {scenario}   {year}   ")
    print(f"#######################################  \n")

    # determine timestep (nr of timesteps per day):
    ts_per_day = check.determine_time_step(f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(10).zfill(2)}*.nc")
    # print(f"  input timestep is {int(24/ts_per_day)} hr")

    correct_to_monthly_3hr_files( path_in, path_out_3hr, model, scenario, year,
                                 # settings for no_cp:
                                  GCM_path='/glade/scratch/bkruyt/CMIP6/GCM_Igrid_3hr',
                                  drop_vars=True
                                )





