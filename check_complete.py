#!usr/bin/env python
################################################################################
#
# Check icar output files for:
#   - missing time (completeness)
#   - missing variables
#   - missing dims, coordinates
#
#
#
#  Bert Kruyt NCAR RAL 2024
################################################################################

import pandas as pd
from datetime import datetime, timedelta
import xarray as xr
import numpy as np
# import matplotlib.pyplot as plt
# import matplotlib.animation as animation
# import matplotlib
# import matplotlib.pylab as pylab
# import matplotlib.patches as patches
# import seaborn as sns
import glob
import os
import multiprocessing as mp
import sys
import cftime
import argparse

# params = {
#     "legend.fontsize": "x-large",  # ‘xx-small’, ‘x-small’, ‘small’, ‘medium’, ‘large’, ‘x-large’, ‘xx-large’
#     #           'figure.figsize': (15, 5),
#     "axes.labelsize": "x-large",
#     "axes.titlesize": 18,  # 'x-large', #(=14)
#     "figure.titlesize":22,
#     "xtick.labelsize": "x-large",
#     "ytick.labelsize": "x-large",
# }
# matplotlib.pylab.rcParams.update(params)
# pylab.rcParams["pcolor.shading"] = "auto"
# import warnings
# # warnings.filterwarnings( "ignore", module = "matplotlib\..*" )
# warnings.filterwarnings("ignore")
# # adding scripts to the system path
# sys.path.insert(0, '../scripts/')
# pylab.rcParams["animation.embed_limit"] = 128*8
# import itertools


#################################
#       FUNCTIONS
#################################

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='remove GCM convective precip, add noise')
    parser.add_argument('path_in',   help='path input files (should have years as subdirs)')
    parser.add_argument('path_out',   help='path to write to')
    parser.add_argument('year',      help='year to process')
    parser.add_argument('model',     help='model')
    parser.add_argument('scenario',  help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    # parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")

    return parser.parse_args()

def determine_time_step(path_to_files):
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

    print(f" Input timestep is {timestep}, or {timestep_h}hr")
    try:
        calendar=ds.time.dt.calendar
    except:
        try:
            calendar=ds.time.encoding['calendar']
        except:
            calendar="could not determine calendar"
    print(f" calendar is: {calendar}")    # or {ds.time.encoding['calendar']}
    return ts_p_day


def check_month(path_to_files,
                m,
                ts_p_day    = 24,
                n_dims      = 4,
                n_coords    = 3,
                n_data_vars = 21
                ):
    """check files in path_to_files for correct nr of timesteps, dims, coordinates and variables"""

    err=None
    # take ds or path_to_files as input:
    if isinstance(path_to_files, str ) :
        # print(f'   loading: {path_to_files}')
        try:
            ds = xr.open_mfdataset( path_to_files)
        except OSError:
            print(f"   cannot open {path_to_files}")
            return
    elif isinstance( path_to_files, xr.core.dataset.Dataset):
        # print(f"   input is ds with {(path_to_files.time.shape)} timesteps")
        ds = path_to_files
    # try:
    #     ds=xr.open_mfdataset(path_to_files)
    #     # ds=xr.open_mfdataset(f"{path}/icar_*.nc")
    # except OSError:
    #     print(f"   cannot open {path_to_files}")
    #     return



    # Check length (time)
    if m in [1,3,5,7,8,10,12] and len(ds.time) < 31*ts_p_day: # January, March, May, July, August, October, and December
        print(f"  month {str(m).zfill(2)} is short")
        err=True
    elif m in [4,6,9,11] and len(ds.time) < 30*ts_p_day:
        print(f"   month {str(m).zfill(2)} is short")
        err=True
    elif m==2 and len(ds.time) < 28*ts_p_day:
        print(f"   month {str(m).zfill(2)} is short")
        err=True
    # CHeck nr of data_vars, coords and dims:
    if len(ds.data_vars) < n_data_vars :
        print(f"   month {str(m).zfill(2)} has less than 21 data vars")
        err=True
    elif len(list(ds.coords)) < n_coords :
        print(f"   month {str(m).zfill(2)} has less than 3 coordinates")
        err=True
    elif len(list(ds.dims)) < n_dims:
        print(f"   month {str(m).zfill(2)} has less than 4 dims")
        err=True

    if not err: print(f"   no errors found in month {m}")


#################################
#           Main
#################################
if __name__ == '__main__':
# if __name__=="main":

    # process command line
    args = process_command_line()  # dt should be argument!!
    path_in  = args.path_in
    # scen    = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    model    = args.model
    scenario = args.scenario
    year     = int(args.year)

    # # #   file characteristics to check for (timestep is derived from data)
    n_dims =4 ; n_coords=3 ;n_data_vars=21 # default

    # determine timestep (nr of timesteps per day):
    ts_per_day = determine_time_step(f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(10).zfill(2)}*.nc")


    for m in range(1,13):
        path_m = f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(m).zfill(2)}*.nc"
        # print(path_m)

        check_month(path_m)