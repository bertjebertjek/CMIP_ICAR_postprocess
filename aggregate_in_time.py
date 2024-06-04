#!/usr/bin/env python
# coding: utf-8
########################################################################################################
#
#  Create monthly 3h icar files from hourly ones, that are stored in a folder per year.
#
#   IN:  - hourly icar_out files, one file per day , 1h temporal resolution.
#   OUT: - 3h icar_3h files, one file per month, 3h temporal resolution.
#
########################################################################################################


import xarray as xr
import numpy as np
import glob
import os
import datetime
import pandas as pd
import cftime
import multiprocessing as mp
import time, sys


#####################
#   FUNCTIONS
#####################

def sum_var_3hr(ds3hr, ds1, varname, varname_dt):
    """ Sum a 1h variable over 3 hours. ds3hr is the 3hourly dataset, ds1 the original hourly."""

    # sum over 3 hours:
    ds3hr[varname_dt]    = ds1[varname_dt].resample(time='3H').sum().astype('float64') # 64 b/c of overflow??
     # write attrs:
    ds3hr[varname_dt].attrs['processing_note2'] = f'summed the hourly {varname} amount over 3hours'
    ds3hr[varname_dt].attrs['units']           = 'kg m-2'
    ds3hr[varname_dt].attrs['standard_name']   = f'{varname}_amount_dt'
    ds3hr[varname_dt].attrs['long_name']       = f'timestep {varname} amount '

    return ds3hr


# # # not used  # # # #
def sum_var_24hr(ds24hr, ds1, varname, varname_dt):
    """ Sum a 1h variable over 24 hours. ds24hr is the 3hourly dataset, ds1 the original hourly."""

    # sum over 24 hours:
    ds24hr[varname_dt]    = ds1[varname_dt].resample(time='24H').sum().astype('float32')
    # write attrs:
    ds24hr[varname_dt].attrs['processing_note2'] = f'summed the hourly {varname} amount over 24hours'
    ds24hr[varname_dt].attrs['units']           = 'kg m-2'
    ds24hr[varname_dt].attrs['standard_name']   = f'{varname}_amount_dt'
    ds24hr[varname_dt].attrs['long_name']       = f'timestep {varname} amount '

    return ds24hr


#####################
#  3h function
#####################
def make_3h_monthly_file( ds_in  ): # file2load
    # """ make a 3hourly and a daily file from 1h input file. filename is destilled from input filename,  """
    """ make a 3hourly file from hourly dataset ds1 (can also be path to file(s)) """
    print("   swe and precip are float64")
    # ________ Open 1h file  ____________
    # take ds or path_to_files as input:
    if isinstance( ds_in, str ) :
        try:
            ds1 = xr.open_mfdataset(ds_in, parallel=True)
        except OSError:
            print( '\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(   '   !!! OSError in file ',ds_in,' !!!')
            print(   '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
            return
        except ValueError:
            print( '\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(   '   !!! ValueError in file ',ds_in,' !!! ')
            print(   '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
            return
    elif isinstance( ds_in, xr.core.dataset.Dataset):
        print(f"   input is ds with {(ds_in.time.shape)} timesteps")
        ds1 = ds_in

    # Errror dect;
    # all_variables = ['precipitation','cu_precipitation','snowfall','graupel','rsds','rlds',
    #              'swe','soil_water_content','hfls','u10m','v10m','ta2m','hus2m','psfc',
    #              'runoff_surface','runoff_subsurface','soil_column_total_water',
    #              'ivt','iwv','iwl','iwi']

    # varsInFile=list(ds1.keys()) # list of variables that exist in files
    # varsInFile=list(ds1.data_vars) # list of variables that exist in files  Better???

    print(f"    input vars {list(ds1.data_vars)}")
    all_variables = list(ds1.data_vars)


    ############################################################
    # # # # # # #              3 hourly              # # # # # #
    ############################################################

    ds3hr       = ds1.resample(time='3H').mean().astype('float32')
    ds3hr.attrs = ds1.attrs

    variables = all_variables.copy()
    for var in variables:

        ds3hr[var].attrs = ds1[var].attrs
        ds3hr[var].attrs['processing_note2'] = 'computed the average over three hour from hourly output. Average include time stamp plus next two time steps.'


    vars_to_sum={'precipitation'   : 'precip_dt',
                 'snowfall'        : 'snowfall_dt',
                 'cu_precipitation': 'cu_precip_dt',
                 'graupel'         : 'graupel_dt'
                }
    # # # # Keep for attrs history?
    # ds3hr.drop_vars(vars_to_sum.values) # drop the 3h mean variables, will be replaces with sum.

    for varname, varname_dt in vars_to_sum.items():
        print(f'   calculating 3hr {varname} sum' )
        ds3hr = sum_var_3hr(ds3hr, ds1, varname, varname_dt)


    # SWE: resample nearest iso mean? (Abby's code)
    ds3hr['swe']              = ds1['swe'].resample(time='3H').nearest().astype('float64')
    ds3hr['swe'].attrs['processing_note'] = 'took the instantaneous value from hourly output to three hourly data'

    ds3hr.attrs['history'] = ds3hr.attrs['history'] + ', modified to 3 hourly data (instantaneous) on '+str(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

    #_______ return 3hr file ________
    return ds3hr










#####################
#   24h version
#####################
def make_yearly_24h_file( ds_in ):

    """ make one file per year with 24h resolution. Input is dataset with 1h (or other) temporal resolution"""

    err_file=None

    # ________ Open 1h file  ____________
    # take ds or path_to_files as input:
    if isinstance( ds_in, str ) :
        try:
            ds1 = xr.open_mfdataset(ds_in, parallel=True)
        except OSError:
            print( '\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(   '   !!! OSError in file ',ds_in,' !!!')
            print(   '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
            return
        except ValueError:
            print( '\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(   '   !!! ValueError in file ',ds_in,' !!! ')
            print(   '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
            return
    elif isinstance( ds_in, xr.core.dataset.Dataset):
        print(f"   input is ds with {(ds_in.time.shape)} timesteps")
        ds1 = ds_in


    print(f"    input vars {list(ds1.data_vars)}")
    all_variables = list(ds1.data_vars)

    # ______ get precip _______
    if 'precip_dt' in ds_in.data_vars:
        precip_var='precip_dt'
    elif 'precipitation' in ds_in.data_vars:
        precip_var='precipitation'
    elif 'Prec' in ds_in.data_vars:
        precip_var='Prec'
    else:
        print( f"! ! !  ERROR:  define precip variable in ICAR  dataset")
        sys.exit()

    # or
    # if precip_var=='Prec': return ds1

    ############################################################
    # # # # # # #              24 hourly              # # # # # #
    ############################################################
    if 'ta2m' in ds1.data_vars:
        # Tmax, Tmin, Wind Speed, Daily Precipitation
        t_max          = ds1['ta2m'].resample(time='D').max(dim='time').to_dataset(name='Tmax').astype('float32')
        t_min          = ds1['ta2m'].resample(time='D').min(dim='time').to_dataset(name='Tmin').astype('float32')
    if 'wind_speed' not in ds1.data_vars:
        wind_speed     = np.abs((np.sqrt(ds1['u10m']**2+ds1['v10m']**2))).resample(time='D').mean(dim='time').to_dataset(name='Wind').astype('float32')
    # precDaily = ds1[pcp_var].diff(dim='time', label='lower').resample(time='D').sum(dim='time').to_dataset(name='Prec')
    if 'Prec' not in ds1.data_vars:
        precDaily = ds1[precip_var].resample(time='D').sum(dim='time').to_dataset(name='Prec').astype('float32')


    # combine into new dataset
    ds_daily=xr.merge([precDaily,t_max,t_min,wind_speed])

    # Add Attributes
    ds_daily.attrs=ds1.attrs


    ds_daily['Prec'].attrs  = ds1[precip_var].attrs
    # print( "   ds1[precip_var].attrs : ", ds1[precip_var].attrs)
    # print( "   ds_daily['Prec'].attrs : ", ds_daily['Prec'].attrs)
    ds_daily['Prec'].attrs['units'] = 'kg m-2 d-1'
    ds_daily['Prec'].attrs['standard_name'] = 'precipitation_flux'
    ds_daily['Prec'].attrs['long_name'] = 'precipitation flux per day'
    ds_daily['Prec'].attrs['processing_note2'] = 'calculated from timestep precipitation by summation - precipitation equals the total amount of precipitation that occured throughout the day. e.g. If time stamp is 1950-01-01 00:00:00 then it is the precipitation that occured between 1950-01-01 00:00 and 1950-01-02 00:00'

    ds_daily['Tmax'].attrs = ds1['ta2m'].attrs
    ds_daily['Tmax'].attrs['non-standard_name'] = 'maximum_daily_air_temperature'
    ds_daily['Tmax'].attrs['long_name'] = 'Bulk maximum daily air temperature at 2m'

    ds_daily['Tmin'].attrs = ds1['ta2m'].attrs
    ds_daily['Tmin'].attrs['non-standard_name'] = 'minimum_daily_air_temperature'
    ds_daily['Tmin'].attrs['long_name'] = 'Bulk minimum daily air temperature at 2m'

    ds_daily['Wind'].attrs = ds1['u10m'].attrs
    ds_daily['Wind'].attrs['standard_name'] = 'wind_speed'
    ds_daily['Wind'].attrs['long_name'] = '10-m wind speed independent of direction calculated as abs(sqrt(u10m^2 + v10m^2)): Daily Average'

    ds_daily.attrs['history'] = ds_daily.attrs['history'] + ', modified to daily data on '+str(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))


    #_______ return 24hr file ________
    return ds_daily



    # ------------- --------------- --------------- ---------



##################################################################################################
########################################   BELOW IS OLD   ########################################

    # # _______ next file:  ___________
    # # we also need next year's file to calc daily prec
    # # if yr1 != "2099":
    # nextyearsfiles = sorted(glob.glob(f'{directory_base}/{int(yr1)+1}/icar_out_*'))
    # try:
    #     file2load_next = nextyearsfiles[0]
    #     print('   next file: ' ,file2load_next )
    #     has_nextyearfiles=True
    #     try:
    #         ds2 = xr.open_dataset(file2load_next)
    #     except OSError:
    #         err_file.append(file2load_next.split('icar_out_')[-1])
    #         print('\n   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    #         print(  '   !!! OSError in file ',file2load_next.split('icar_out_')[-1],' !!!')
    #         print(  '   !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  \n')
    #         print(f'\n   !!! no next year file for year {int(yr1)+1} \n')
    #         has_nextyearfiles=False ; ds2=None
    # except IndexError:
    #     print(f'\n   !!! no next year file for year {int(yr1)+1} \n')
    #     has_nextyearfiles=False ; ds2=None



#     # Not used, should we?
#     all_variables = ['precipitation','cu_precipitation','snowfall','graupel','rsds','rlds',
#                  'swe','soil_water_content','hfls','u10m','v10m','ta2m','hus2m','psfc',
#                  'runoff_surface','runoff_subsurface','soil_column_total_water',
#                  'ivt','iwv','iwl','iwi']

#     varsInFile=list(ds1.keys()) # list of variables that exist in files

#     #### Fill missing data with NaN's
#     if len(varsInFile) < len(all_variables): # generally do not have to do this but if a file was not properly aggregated then this is necessary - fill with NaN's
#         # list of all variables that should exist
#         variable_list = all_variables.copy()

#         # add the file to errlist, deal with it later:
#         # err_file.append(ftime)
#         err_file = ftime
#         print( '   !!! var Error in file ',ftime,' !!!')
# #         # Get list of variables 2 add based on what already exists
# #         for nav in varsInFile:
# #             if nav in variable_list: variable_list.remove(nav)
# #         variables2fill=variable_list

# #         # Open previous file - assuming this has all the files we need to add to serve as a template to make a NaN array
# #         dsPrev=xr.open_dataset(directory_base+'icar_out_'+str(days_t_hr[i-1].year)+'-'+str(days_t_hr[i-1].month).zfill(2)+'-'+str(days_t_hr[i-1].day).zfill(2)+'_00-00-00.nc')

# #         for variable in variables2fill:
# #             nanArray = np.empty(dsPrev[variable].shape)
# #             nanArray[:] = np.nan
# #             dsPrev[variable].values=nanArray
# #             array_save=dsPrev[variable].copy()
# #             array_save['time']=ds1['time']
# #             ds1[variable]=array_save

# #         # often if all the files did print all the variables the time is also messed up - make sure the time stamps are apporpriate
# #         ds1['time']=xr.cftime_range(str(days_t_hr[i].year)+'-'+str(days_t_hr[i].month).zfill(2)+'-'+str(days_t_hr[i].day).zfill(2)+'T00:00',                 str(days_t_hr[i].year)+'-'+str(days_t_hr[i].month).zfill(2)+'-'+str(days_t_hr[i].day).zfill(2)+'T23:00',calendar=calendar,freq='H')

# #         # Store a list of missing files and dates for when this happened.
# #         with open(directory_3hr+'/'+'missing_dates.txt', 'a') as f:
# #             f.write('Missing: '+str(days_t_hr[i-1].year)+'-'+str(days_t_hr[i-1].month).zfill(2)+'-'+str(days_t_hr[i-1].day).zfill(2)+'\n'+'\n'.join(variables2fill))


#     ### Actual Processing


#     ############################################################
#     # # # # # # #                  Daily             # # # # # #
#     ############################################################

    # # Tmax, Tmin, Wind Speed, Daily Precipitation
    # t_max          = ds1['ta2m'].resample(time='D').max(dim='time').to_dataset(name='Tmax').astype('float32')
    # t_min          = ds1['ta2m'].resample(time='D').min(dim='time').to_dataset(name='Tmin').astype('float32')
    # wind_speed     = np.abs((np.sqrt(ds1['u10m']**2+ds1['v10m']**2))).resample(time='D').mean(dim='time').to_dataset(name='Wind').astype('float32')
    # # precDaily      = (ds2['precipitation'][0]-ds1['precipitation'][0]).to_dataset(name='Prec')
    # # precDaily      = precDaily.expand_dims(time=t_max.time).astype('float32')

#     # add last timestep of next file so we can calculate the difference:
#     # if yr1 != "2099":
#     if has_nextyearfiles:
#         pcp = xr.concat([ds1['precipitation'], ds2['precipitation'][0]], dim='time')
#     else: # year=2099 or 2049 without 2050
#         pcp = ds1['precipitation']

#     # pcp_dt = pcp.diff(dim='time', label='lower')  # # make hourly precip
#     # pcp_day = pcp_dt.resample(time='D').sum(dim='time')  # # sum to daily precip:
#     # precDaily = pcp_day.to_dataset(name='Prec_day')

#     # all in one:  diff, resample, sum to daily, & make dataset
#     precDaily = pcp.diff(dim='time', label='lower').resample(time='D').sum(dim='time').to_dataset(name='Prec')


#     """CHeck if prec is ok for monthly files!!"""

#     dsDayMetForcing=xr.merge([precDaily,t_max,t_min,wind_speed])

#     # Add Attributes
#     dsDayMetForcing.attrs=ds1.attrs

#     dsDayMetForcing['Prec'].attrs  = ds1['precipitation'].attrs
#     dsDayMetForcing['Prec'].attrs['units'] = 'kg m-2 d-1'
#     dsDayMetForcing['Prec'].attrs['standard_name'] = 'precipitation_flux'
#     dsDayMetForcing['Prec'].attrs['long_name'] = 'precipitation flux per day'
#     dsDayMetForcing['Prec'].attrs['description'] = 'calculated from hourly output of cumulative data - precipitation equals the total amount of precipitation that occured throughout the day. e.g. If time stamp is 1950-01-01 00:00:00 then it is the precipitation that occured between 1950-01-01 00:00 and 1950-01-02 00:00'

#     dsDayMetForcing['Tmax'].attrs = ds1['ta2m'].attrs
#     dsDayMetForcing['Tmax'].attrs['non-standard_name'] = 'maximum_daily_air_temperature'
#     dsDayMetForcing['Tmax'].attrs['long_name'] = 'Bulk maximum daily air temperature at 2m'

#     dsDayMetForcing['Tmin'].attrs = ds1['ta2m'].attrs
#     dsDayMetForcing['Tmin'].attrs['non-standard_name'] = 'minimum_daily_air_temperature'
#     dsDayMetForcing['Tmin'].attrs['long_name'] = 'Bulk minimum daily air temperature at 2m'

#     dsDayMetForcing['Wind'].attrs = ds1['u10m'].attrs
#     dsDayMetForcing['Wind'].attrs['standard_name'] = 'wind_speed'
#     dsDayMetForcing['Wind'].attrs['long_name'] = '10-m wind speed independent of direction calculated as abs(sqrt(u10m^2 + v10m^2)): Daily Average'

#     dsDayMetForcing.attrs['history'] = dsDayMetForcing.attrs['history'] + ', modified to daily data on '+str(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

#     # Write Output - Daily
#     print( '   writing daily file to ', file_out_daily , '\n')
#     # file_out_daily=directory_daily+'/icar_daily_'+model+'_'+scen.split('/')[0]+'_'+str(days_daily[i].year)+'-'+str(days_daily[i].month).zfill(2)+'-'+str(days_daily[i].day).zfill(2)+'_00-00-00.nc'
#     encoding = {}
#     if model == 'HadGEM2-ES':
#         encoding['time'] = {'units': 'days since 1850-01-01'}
#     else:
#         encoding['time'] = {'units': 'days since 1900-01-01'}

#     dsDayMetForcing.to_netcdf(file_out_daily,encoding=encoding)
#     # dsDayMetForcing.to_netcdf(file_out_daily)
#     # if not err_file==None: return err_file




# -------- OLD ----------

# #____________________________________
# ds3hr['precip_dt']    = ds1['precip_dt'].resample(time='3H').sum().astype('float32')
# ds3hr['precip_dt'].attrs['processing_note'] = 'Summed the hourly precipititation value over 3 hours.'
# ds3hr['precip_dt'].attrs['units']           = 'kg m-2'
# ds3hr['precip_dt'].attrs['standard_name']   = 'timestep (3hr) precipitation_amount'
# ds3hr['precip_dt'].attrs['long_name']       = 'timestep (3hr) precipitation amount'

# # ds3hr['precipitation']    = ds1['precipitation'].resample(time='3H').nearest().astype('float64')
# # ds3hr['precipitation'].attrs['processing_note'] = 'Took the instantaneous value every 3 hours from hourly output of cumulative precipitation. Cumulative precipitation from model start date.'
# # ds3hr['precipitation'].attrs['units']           = 'kg m-2'
# # ds3hr['precipitation'].attrs['standard_name']   = 'precipitation_amount'
# # ds3hr['precipitation'].attrs['long_name']     = 'precipitation amount since model start date'

# ds3hr['cu_precip_dt'] = ds1['cu_precip_dt'].resample(time='3H').sum().astype('float32')
# # ds3hr['cu_precip_dt'] = ds1['cu_precip_dt'].resample(time='3H').nearest().astype('float64')
# ds3hr['cu_precip_dt'].attrs['processing_note'] = 'Summed the hourly cu precipititation value over 3 hours.'#'Took the instantaneous value every 3 hours from hourly output of cumulative precipitation. Cumulative convective precipitation from model start date.'
# ds3hr['cu_precip_dt'].attrs['units']           = 'kg m-2'
# ds3hr['cu_precip_dt'].attrs['standard_name']   = 'convective_precipitation_amount'
# ds3hr['cu_precip_dt'].attrs['long_name']       = 'timestep (3hr) convective precipitation '


# # ds3hr['snowfall']         = ds1['snowfall'].resample(time='3H').nearest().astype('float64')
# ds3hr['snowfall_dt'].attrs['processing_note'] = 'Summed the hourly value over 3 hours of cumulative snowfall.'
# ds3hr['snowfall_dt'].attrs['units']           = 'kg m-2'
# ds3hr['snowfall_dt'].attrs['standard_name']   = 'snowfall_amount'
# ds3hr['snowfall_dt'].attrs['long_name']       = 'timestep (3hr) snowfall (liquid equivalent) '

# # ds3hr['graupel']          = ds1['graupel'].resample(time='3H').nearest().astype('float64')
# ds3hr['graupel'].attrs['processing_note'] = 'Summed the hourlyvalue over 3 hours from hourly output of graupel.'
# ds3hr['graupel'].attrs['units']           = 'kg m-2'
# ds3hr['graupel'].attrs['standard_name']   = 'graupel_fall_amount'
# ds3hr['graupel'].attrs['long_name']       = 'timestep (3hr) graupel (liquid equivalent) amount'


# # Write Output - Three Hourly
# print( '   writing 3hfile to ', file_out_t_hr )
# # encode pcp as float32!!
# ds3hr.to_netcdf(file_out_t_hr, encoding={'time':{'units':"days since 1900-01-01"}})
