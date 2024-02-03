#!usr/bin/env python
################################################################################
#
# This script is used to check the final results of the procedure, by:
#   - Looping through the subdirs of path_out (these should be [model]_[scen] )
#   - 3hr subdirs: loop through years, then months
#   - 24hr subdirs: loop through years
#   - Check:
#      - nr of timesteps (completeness)
#      - nr of  variables
#      - nr of dims, coordinates
#
# To be run after main_Xhr.py /sumit_postprocess_{X}hinput.sh
#
# Usage:
#   - $ python check_results.py [path_out OPTIONAL] [model OPTIONAL] [scenario OPTIONAL]
#   - BUT: either give no arguments, only path_out, or all 3 !
#   - when no arguments are given, all subdirs in the default
#       path_out=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_nocp_full will be checked.
#
# Author:
#   - Bert Kruyt, NCAR RAL 2024
################################################################################

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




#################################
#       FUNCTIONS
#################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='remove GCM convective precip, add noise')
    parser.add_argument('path_out', nargs='?',   help='path to input files (should have [model_scen] as subdirs)')
    # parser.add_argument('path_out',   help='path to write to')
    # parser.add_argument('year',      help='year to process')
    parser.add_argument('model',    nargs='?',  help='model')
    parser.add_argument('scenario',  nargs='?', help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    # parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")

    return parser.parse_args()

def determine_time_step(path_to_files, print_results=True):
    """returns nr of timesteps per day (integer) """
    try:
        ds=xr.open_mfdataset(path_to_files)
        # ds=xr.open_mfdataset(f"{path}/icar_*.nc")
    except OSError:
        print(f"   cannot open {path_to_files}")
        sys.exit()
    # except HDFError:
    #     print(f"   cannot open ")

    # Determine which month, timestep (hour or 3hr or...)
    timestep =  (ds.time.diff(dim='time')).mean().values
    timestep_h = int(timestep/60/60/1000/1000/1000)
    ts_p_day = int(24/timestep_h)  # nr of timesteps per day

    try:
        calendar=ds.time.dt.calendar
    except:
        try:
            calendar=ds.time.encoding['calendar']
        except:
            calendar="could not determine calendar"

    if print_results:
        print(f" Input timestep is {timestep}, or {timestep_h}hr")
        print(f" calendar is: {calendar}")    # or {ds.time.encoding['calendar']}

    return ts_p_day


###################################
#
###################################
def check_year(path_to_files,
                # m,
                year,
                ts_p_day    = 1,
                n_dims      = 3,
                n_coords    = 3,
                n_data_vars = 4
                ):
    """ Check yearly 24hr files for correct nr of dims, coords, data vars and timesteps."""

    try:
        ds=xr.open_mfdataset(path_to_files)

        # if len(ds.time) < 365*ts_p_day :
        if len(ds.time) < 365*ts_p_day and not (year in [2005, 2050, 2099]):
            print(f"  {year} is short")
            err=True
        if len(ds.data_vars) < n_data_vars :
            print(f"  {year} has less than 21 data vars")
            err=True
        elif len(list(ds.coords)) < n_coords :
            print(f"  {year} has less than 3 coordinates")
            err=True
        elif len(list(ds.dims)) < n_dims:
            print(f"  {year} has less than 4 dims")
            err=True
    except:
        print(f"  {year} is wrong")
        err=True


###################################
#
###################################
def check_month(path_to_files,
                m,
                year,
                ts_p_day    = 24,
                n_dims      = 4,
                n_coords    = 3,
                n_data_vars = 21
                ):
    """check files in path_to_files for correct nr of timesteps, dims, coordinates and variables"""

    err=None
    try:
        ds=xr.open_mfdataset(path_to_files)
        # ds=xr.open_mfdataset(f"{path}/icar_*.nc")
    except OSError:
        print(f"   cannot open {path_to_files}")
        return
    # except HDFError:
    #     print(f"   cannot open ")


    # Check length (time)
    if m in [1,3,5,7,8,10,12] and len(ds.time) < 31*ts_p_day: # January, March, May, July, August, October, and December
        print(f"   {year} month {str(m).zfill(2)} is short")
        err=True
    elif m in [4,6,9,11] and len(ds.time) < 30*ts_p_day:
        print(f"   {year} month {str(m).zfill(2)} is short")
        err=True
    elif m==2 and len(ds.time) < 28*ts_p_day:
        print(f"   {year} month {str(m).zfill(2)} is short")
        err=True
    # CHeck nr of data_vars, coords and dims:
    if len(ds.data_vars) < n_data_vars :
        print(f"   {year} month {str(m).zfill(2)} has less than 21 data vars")
        err=True
    elif len(list(ds.coords)) < n_coords :
        print(f"   {year} month {str(m).zfill(2)} has less than 3 coordinates")
        err=True
    elif len(list(ds.dims)) < n_dims:
        print(f"   {year} month {str(m).zfill(2)} has less than 4 dims")
        err=True

    # if not err: print(f"   no errors found in month {m}")


#################################
#           Main
#################################
if __name__ == '__main__':

    # process command line: arguments path_out, model , scenario are optional
    args = process_command_line()
    if len(sys.argv) > 1:
        path_out  = args.path_out
        if len(sys.argv) > 2:
            model    = args.model
            scenario = args.scenario
            # print(f" sysargv: {len(sys.argv)} ; {model} {scenario}")
        else:
            model     = None
            scenario  = None
    else:
        path_out  = "/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_nocp_full"
        # could also set default in argparse.process_comand_line()

    if model is not None:
        dirs=[f"{path_out}/{model}_{scenario}"]
    else:
        # get a list of all model_scenario directories in path:
        dirs = sorted(glob.glob(f"{path_out}/*"))


    # _________  Print general info: __________
    print(f"\n#######################################################################")
    print(f"#   checking results in {path_out} ")
    print(f"#\n#   for model/scen: ")
    for ms in dirs: print(f"#      {ms.split('/')[-1]} ")
    print(f"#\n#######################################################################\n")


    # _________  check monthly 3hr results: __________
    print(f"\n**********   checking 3hr monthly results  ***************  ")
    for modscen in dirs:

        print("\n", modscen.split('/')[-1])

        if modscen[-5:] == "_hist":
            y1=1950; y2=2005
        elif modscen[-5:] == "_2004":
            y1=2005; y2=2050
        elif modscen[-5:] == "_2049":
            y1=2050; y2=2099


        # # #   file characteristics to check for (timestep is derived from data)
        n_dims =3 ; n_coords=3 ;n_data_vars=21-14 # default-len(vars_to_drop) (in remove_cp.py)

        # determine timestep (nr of timesteps per day): (could just harcode?)
        ts_per_day = determine_time_step(f"{modscen}/3hr/icar_*_{y1}-{str(10).zfill(2)}*.nc",
                                         print_results=False
                                         )
        if not ts_per_day==8: print(f"\n ! ! ! ts_per_day={ts_per_day} ! ! !\n")

        for year in range(y1, y2+1):
            # set start and end month to check based on year and scenario:
            m_start=1 ; m_end=13
            if modscen[-5:] == "_hist" and year==2005:
                m_end=10
            elif modscen[-5:] == "_2004" and year==2005:
                m_start=10
            elif modscen[-5:] == "_2004" and year==2050:
                m_end=10
            elif modscen[-5:] == "_2049" and year==2050:
                m_start=10
            elif modscen[-5:] == "_2049" and year==2099:
                m_end=11  # 2099-12 often misses last day.
            else:
                m_start=1 ; m_end=13

            for m in range(m_start,m_end):
                path_m = f"{modscen}/3hr/icar_*_{year}-{str(m).zfill(2)}*.nc"

                check_month(path_m, m, year,
                            ts_p_day    = ts_per_day,
                            n_dims      = n_dims,
                            n_coords    = n_coords,
                            n_data_vars = n_data_vars
                            )


    # _________  check Yearly 24hr results: __________
    print(f"\n**********   checking 24hr yearly results  ***************")
    for modscen in dirs:

        print("\n", modscen.split('/')[-1])

        if modscen[-5:] == "_hist":
            y1=1950; y2=2005
        elif modscen[-5:] == "_2004":
            y1=2005; y2=2050
        elif modscen[-5:] == "_2049":
            y1=2050; y2=2099
        elif modscen[-5:] == "2005_2050":
            y1=2005; y2=2050
        elif modscen[-5:] == "2050_2099":
            y1=2050; y2=2099


        # # #   file characteristics to check for (timestep is derived from data)
        n_dims =3 ; n_coords=3 ;n_data_vars=4 # default-len(vars_to_drop) (in remove_cp.py)

        # determine timestep (nr of timesteps per day): (could just harcode?)
        if len(glob.glob(f"{modscen}/daily/icar_*_{y1}.nc"))>0:
            file_ts=f"{modscen}/daily/icar_*_{y1}.nc"
        elif len(glob.glob(f"{modscen}/daily/ICAR_*_{y1}.nc"))>0:
            file_ts=f"{modscen}/daily/ICAR_*_{y1}.nc"

        ts_per_day = determine_time_step(file_ts,
                                         print_results=False
                                         )
        if not ts_per_day==1: print(f"\n ! ! ! ts_per_day={ts_per_day} ! ! !\n")

        for year in range(y1, y2+1):
            # print(f"- - -  {year}  - - -")
            path_y = f"{modscen}/daily/ICAR_*_{year}.nc"

            check_year( path_y, #glob.glob(path_y)[0],
                        year,
                        ts_p_day    = ts_per_day,
                        n_dims      = n_dims,
                        n_coords    = n_coords,
                        n_data_vars = n_data_vars
                        )

##################### old  ########################


    # scen    = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    # model    = args.model
    # scenario = args.scenario
    # year     = int(args.year)


    # if scenario.split('_')[0]=="hist":
    #     y1=1950; y2=2005
    # elif scenario.split('_')[-1]=="2004":
    #     y1=2005; y2=2050
    # elif scenario.split('_')[-1]=="2049":
    #     y1=2050; y2=2099

    # # # #   file characteristics to check for (timestep is derived from data)
    # n_dims =3 ; n_coords=3 ;n_data_vars=21-14 # default-len(vars_to_drop) (in remove_cp.py)

    # # determine timestep (nr of timesteps per day):
    # ts_per_day = determine_time_step(f"{path_out}/{model}_{scenario}/3hr/icar_*_{y1}-{str(10).zfill(2)}*.nc")

    # # check monthly 3hr results:
    # print(f"\n**********   checking monthly results  ***************\n")
    # for year in range(y1, y2+1):
    #     print(f"- - -  {year}  - - -")
    #     for m in range(1,13):
    #         path_m = f"{path_out}/{model}_{scenario}/3hr/icar_*_{year}-{str(m).zfill(2)}*.nc"
    #         # print(path_m)
    #         check_month(path_m, m,
    #                     ts_p_day    = ts_per_day,
    #                     n_dims      = n_dims,
    #                     n_coords    = n_coords,
    #                     n_data_vars = n_data_vars
    #                     )