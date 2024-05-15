#!/bin/bash
#PBS -l select=1:ncpus=1:mem=30GB
#PBS -l walltime=12:00:00
#PBS -A P48500028
#PBS -q casper
#PBS -N PP_Array
#PBS -J 0-50:1
#PBS -o job_output/array_3h.out
#PBS -j oe

##############################################################################
#
#  Job submission script to postprocess raw ICAR output.
#   - check for missing timesteps/variables
#   - disaggregate cumulative precip vars into timestep (1h/3h) precipitation ('precip dt')
#   - correct negative precip values
#   - aggregate to 3hr (24hr) timestep & monthly (yearly) files
#   - remove GCM cp (optional, set flag to true)
#   - drop unwanted variables from dataset (drop_vars flag in main.py, default=True)
#
# (runtime ca 1.5 hr per scen-year (24hr + 3hr) - incorrect?)
# when running 3h and daily seprately: 3hr requires more mem (150GB?) daily : 50GB - 3hours per mod-scen?
#
# Bert Kruyt NCAR RAL 2024
##############################################################################


JOBID=`echo ${PBS_JOBID} | cut -d'[' -f1`
# echo " JOBID: $JOBID"

# ____________
module load conda
conda activate npl

# ____________   Set arguments: (year = PBS_array_index) -_______________
CMIP=CMIP5   # CMIP5 or CMIP6

# Remove GCM cp from ICAR data? Requires GCM cp regdridded to ICAR grid.
remove_cp=True                    # in case ICAR was run with rain_var=cp !

dt=daily   # "daily" or "3hr"

if [[ "${CMIP}" == "CMIP5" ]]; then

    path_in=/glade/campaign/ral/hap/currierw/icar/output
    path_day_in=/glade/campaign/ral/hap/currierw/icar/output
    # path_day_in=/glade/campaign/ral/hap/bert/${CMIP}/WUS_icar_day
    path_out=/glade/campaign/ral/hap/bert/${CMIP}/WUS_icar_nocp_full # rerun 2024-04-22 (no noise@3hr)
    GCM_cp_path=/glade/derecho/scratch/bkruyt/${CMIP}/GCM_Igrid_3hr # 3hr # 24h path is hardcoded in remove_cp.py

    #- - - -  set model and scenarios  - - - - -
    # allModels=( CanESM2 CCSM4 CMCC-CM CNRM-CM5 GFDL-CM3 MIROC5 MRI-CGCM3)   #  CCSM4 CMCC-CM CNRM-CM5  GFDL-CM3 MIROC5 MRI-CGCM3
    # allModels=(  GFDL-CM3 MIROC5 MRI-CGCM3 )  # CanESM2 ) #
    allModels=(   CNRM-CM5 )
    # allScens=( historical )
    # allScens=(  rcp45_2005_2050 ) # rcp85_2050_2100 ) #rcp45_2050_2100 rcp85_2050_2100 )
    allScens=(rcp85_2005_2050 rcp85_2050_2100    ) #  rcp45_2050_2100 rcp85_2005_2050 ) #rcp45_2050_2100 rcp85_2005_2050 rcp85_2050_2100 )
    # allScens=( rcp45_2005_2050 rcp45_2050_2100  historical)

elif [[ "${CMIP}" == "CMIP6" ]]; then

    # path_in=/glade/campaign/ral/hap/bert/${CMIP}/WUS_icar_3hr
    path_in=/glade/campaign/ral/hap/bert/${CMIP}/WUS_icar_3hr_month
    path_day_in=/glade/campaign/ral/hap/bert/${CMIP}/WUS_icar_day
    path_out=/glade/campaign/ral/hap/bert/${CMIP}/WUS_icar_nocp_full2
    GCM_cp_path=/glade/derecho/scratch/bkruyt/${CMIP}/GCM_Igrid # -> move glade

    allModels=( CanESM5 )
    # allModels=( CMCC-CM2-SR5 )
    # allModels=( MIROC-ES2L )
    # allModels=(MPI-M.MPI-ESM1-2-LR )
    # allModels=(  NorESM2-MM )
    # allModels=( CMCC-CM2-SR5 MIROC-ES2L  MPI-M.MPI-ESM1-2-LR NorESM2-MM )

    # # #   hist needs 55 jobs, sspXX_2004 needs 0-46, sspXX_2049 needs 0-50!  # # #
    # allScens=( ssp245_2049 ) #ssp245_2004 ssp245_2049 ssp370_2004 ssp370_2049 ssp585_2004 ssp585_2049 )
    allScens=( hist ssp245_2004 ssp245_2049 ssp370_2004 ssp370_2049 ssp585_2004 ssp585_2049 )
    # allScens=(hist ssp245_2004 ssp245_2049  )
    # allScens=( ssp370_2004 ssp370_2049 ssp245_2004 ssp245_2049 )
fi

#_______________ launch the python script _____________
for model in ${allModels[@]}; do
for scen in ${allScens[@]}; do
    # determine start year from scenario parameter:
    if [[ "${scen:0:4}" == "hist" ]]; then
        start_year=1950
    elif [[ "${scen:6:5}" == "_2004" ]]; then
        start_year=2005
    elif [[ "${scen:6:5}" == "_2049" ]]; then
        start_year=2050
    elif [[ "${scen:5:10}" == "_2005_2050" ]]; then
        start_year=2005
    elif [[ "${scen:5:10}" == "_2050_2100" ]]; then
        start_year=2050
    else
        echo " "
        echo " ! ! !    start year unclear, exiting.   ! ! !"
        exit 1
    fi

    # set the year from array idx:
    year=$(( $PBS_ARRAY_INDEX + $start_year  ))

    # launch script:
    echo " "
    echo "Launching yearly 24hr & 3hr file correction for $model $scen ${year}"
    echo " "

    # (un)comment the 24h / 3h code below as desired

    # # # # # Launch the 24h file script: (not tested yet for 3h inputs)
    if [[ "${dt}" == "daily" ]]; then
        mkdir -p job_output_daily/${model}_${scen}
        python -u main_24hr_from3hinput.py $path_in $path_day_in $path_out $year $model  $scen $remove_cp $GCM_cp_path >& job_output_daily/${model}_${scen}/${year}

    # # # # # # Launch the 3h file script:
    elif [[ "${dt}" == "3hr" ]]; then
        mkdir -p job_output_3hr/${model}_${scen}
        python -u main_3hr_from3hinput.py  $path_in $path_out $year $model  $scen $remove_cp $GCM_cp_path $CMIP >& job_output_3hr/${model}_${scen}/${year}
    fi

    echo " "
    echo " - - -    Done processing daily & 3hr files for $model $scen  - - - - -"

done
done
echo " "
echo " - - -    $model  Done    - - - - -"

