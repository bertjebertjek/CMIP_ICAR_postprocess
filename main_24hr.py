#!usr/bin/env python

#####################################################################################
#
# Aggregated ICAR postprocessing
#
# takes 1h / 3h ICAR output files and:
#   - check incomplete
#       - stop or interpolate / fill? (Wishlist)
#   - corrects neg pcp
#   - aggregates to monthly (3hr) files / yearly (24hr) files
#   - (Optional) removes GCM cp (requires GCM cp on ICAR grid AND missing timesteps in ICAR to be filled in )
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
    parser.add_argument('path_out',         help='path to write to')
    # parser.add_argument('path_out_nocp',    help='path to write to')  # obsolete
    parser.add_argument('year',             help='year to process')
    parser.add_argument('model',            help='model')
    parser.add_argument('scenario',         help='scenario to process; one of hist, sspXXX_2004, sspXXX_2049')
    parser.add_argument('remove_cp',        help='remove GCM cp from ICAR data, requires GCM_cp_path') # bool
    parser.add_argument('GCM_cp_path',      help='path with the GCM cp on ICAR grid')
    # parser.add_argument('dt',        help="time step of input ICAR data, either 'daily' or '3hr' ")

    return parser.parse_args()


######################  24hr files are corrected by year   #########################
#
###################################################################################
def correct_to_yearly_24hr_files( path_in, path_out, model, scenario, year,
                                 GCM_path  = '/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid',
                                 drop_vars = False
                                ):
    '''Post process hourly ICAR output to yearly files with 24hr timestep'''
    t00=time.time()

    # __________  check files for completeness  ______
    print(f"\n**********************************************")
    for m in range(1,13):
        path_m = f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(m).zfill(2)}*.nc"

        print(f"   checking {year}-{str(m).zfill(2)}")
        check_result = check.check_month( path_to_files=path_m, m=m, ts_p_day=ts_per_day )

        # ________ interpolate / fill missing timesteps  __________
        # if check_result == not None:
            #   ... call interpolation routine?
            # .......

    # # # #    END MONTH LOOP   # # # # #


    # ____________       corr neg pcp      _____________
    #
    #     make timestep precip vars  &  fix neg precip (should be separate functions, so we can turn them on/off indiv.)
    # __________________________________________________
    if cor_neg_pcp:
        print(f"\n   **********************************************")
        print(f"   fixing neg pcp  for {year} ")
        t0 = time.time()

        # find next month's file (needed to calculate timestep pcp (diff))
        try:
                nextmonth_file_in = sorted(glob.glob(f"{path_in}/{model}/{scenario}/{str(int(year)+1)}/icar_out_{str(int(year)+1)}-01*.nc"))[0]
        except:
                nextmonth_file_in=None  # should catch all fringe cases, ie 2005 in hist, 2050 in sspXXX_2004
        print( "   nextmonth_file_in ", nextmonth_file_in )

        # call the correction functions
        path_y = f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-*.nc"
        ds_fxd = fix.open_and_remove_neg_pcp(path_y,
                                            nextmonth_file_in,
                                            vars_to_correct=vars_to_correct_24hr
                                            )
        print(f"\n   correcting  neg pcp took: {np.round(time.time()-t0,1)} sec")
    else:
         # in this case we should still disaggregate pcp!!
         print("   NOT IMPLEMENTED YET, Stopping")
         sys.exit()
        #  ds_fxd = xr.open_mfdataset( path_y )   # ???? Not tested (or recommended)

    # # # # SAVE TEMP w cp:
    # print(f"\n   **********************************************")
    # path_tmp="/glade/derecho/scratch/bkruyt/CMIP6/tmp_w_cp-1"
    # file_out_tmp=f"{path_tmp}/{model}_{scenario}/daily/icar_daily_{model}_{scenario.split('_')[0]}_{year}.nc"
    # if not os.path.exists(f"{path_tmp}/{model}_{scenario}/daily"):
    #     os.makedirs(f"{path_tmp}/{model}_{scenario}/daily")
    # print( '   writing tmp 24hfile to ', file_out_tmp )
    # ds_fxd.to_netcdf(file_out_tmp, encoding={'time':{'units':"days since 1900-01-01"},
    #                                         #   'Prec':{'dtype':"float32"}  # leads to overflow error?
    #                                           }
    #                                           )

    # ____________ aggregate to 24hr yearly files __________
    print(f"\n   **********************************************")
    print(f"   aggregating to yearly 24hr files: {year}")
    t0 =time.time()
    # precip is now dt, correct attrs etc...
    ds24hr = change_temporal_res.make_yearly_24h_file( ds_fxd ) #, directory_3hr=path_out)
    print(f"\n   aggregating to 24hr took: {np.round(time.time()-t0,1)} sec")



    # _________  remove cp  ____________
    if remove_cp:
        print(f"\n   **********************************************")
        print( f'   removing GCM cp  {year}-{str(m).zfill(2)}')
        t0 =time.time()
        ds24hr = cp.remove_24hr_cp( ds_in       = ds24hr,
                                    m           = m,
                                    year        = year,
                                    model       = model,
                                    scen        = scenario.split('_')[0],
                                    GCM_path    = GCM_path,
                                    noise_path  = noise_path, #'/pscratch/sd/b/bkruyt/CMIP/uniform_noise_480_480.nc',
                                    drop_vars   =  drop_vars
                                    )
        print(f"\n   removing cp took: {np.round(time.time()-t0,1)} sec")


    # ____________ save output _____________

    # save 24hr dataset to disk:
    file_out_24hr  = f"{path_out}/{model}_{scenario}/daily/icar_daily_{model}_{scenario.split('_')[0]}_{year}.nc"
    print(f"\n   **********************************************")
    print( '   writing 24hfile to ', file_out_24hr )

    if not os.path.exists(f"{path_out}/{model}_{scenario}/daily"):
        os.makedirs(f"{path_out}/{model}_{scenario}/daily")

    ds24hr.to_netcdf(file_out_24hr, encoding={'time':{'units':"days since 1900-01-01"},
                                            #   'Prec':{'dtype':"float32"}  # leads to overflow error?
                                              }
                                              )


    print(f"\n   - - - - -     {year}  done   - - - - - ")

    print(f"\n------------------------------------------------------ ")
    print(f"     {model} {scenario.split('_')[0]} {year} done in {np.round((time.time()-t00)/60,1)} min ")
    print(f"------------------------------------------------------ \n ")



#################################
#           Main
#################################
if __name__ == '__main__':


    # process command line
    args = process_command_line()  # dt should be argument!!
    path_in         = args.path_in
    path_out        = args.path_out
    model           = args.model
    scenario        = args.scenario
    # scen    = args.scenario.split('_')[0]  # drop the year from sspXXX_year
    year            = int(args.year)
    remove_cp       = True if args.remove_cp=="True" else False
    GCM_path        = args.GCM_cp_path


    # # #  Additional settings (might become arguments )  # # #
    # noise_path  = '/pscratch/sd/b/bkruyt/CMIP/uniform_noise_480_480.nc'
    noise_path   = '/glade/derecho/scratch/bkruyt/CMIP6/uniform_noise_480_480.nc'
    drop_vars    = False
    cor_neg_pcp  = True # also does the pcp_cum -> pcp_dt, so keep set at True (for now)


    ########          correct negative variables          ########
    vars_to_correct_3hr = {'precipitation'   : 'precip_dt',
                            'snowfall'        : 'snowfall_dt',
                            'cu_precipitation': 'cu_precip_dt',
                            'graupel'         : 'graupel_dt'
                            }

    vars_to_correct_24hr = {'precipitation'   : 'precip_dt' }


    print(f"\n#######################################  ")
    print(f"#   Making daily corrected ICAR files for: " )
    print(f"#      {model}   {scenario}   {year}      \n")
    print(f"#   remove GCM cp:           {remove_cp}    ")
    # print(f"#   drop unwanted variables: {drop_vars}    ")
    print(f"#######################################  \n")

    # determine timestep (nr of timesteps per day):
    try:
        files_oct = glob.glob(f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(10).zfill(2)}*.nc")
        ts_per_day = check.determine_time_step(files_oct[0])
    except:
        files = glob.glob(f"{path_in}/{model}/{scenario}/{year}/icar_out_{year}-{str(1).zfill(2)}*.nc")
        ts_per_day = check.determine_time_step(files[0])

    correct_to_yearly_24hr_files( path_in, path_out, model, scenario, year,
                                 GCM_path  = GCM_path,
                                 drop_vars = drop_vars
                                )




