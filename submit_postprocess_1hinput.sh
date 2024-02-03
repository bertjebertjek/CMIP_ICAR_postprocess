#!/bin/bash
#PBS -l select=1:ncpus=1:mem=50GB
#PBS -l walltime=04:00:00
#PBS -A P48500028
#PBS -q casper
#PBS -N PP_Array
#PBS -J 0-55:1
#PBS -o job_output/array_1h.out
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
# (runtime ca 1.5 hr per scen-year)
#
# Bert Kruyt NCAR RAL 2024
##############################################################################


JOBID=`echo ${PBS_JOBID} | cut -d'[' -f1`
# echo " JOBID: $JOBID"

# ____________
module load conda
conda activate npl

# ____________   Set arguments: (year = PBS_array_index) -_______________
path_in=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_out

# path_out=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3h_test
path_out=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_nocp_full
# Remove GCM cp from ICAR data? Requires GCM cp regdridded to ICAR grid.
remove_cp=True                     # in case ICAR was run with rain_var=cp !
GCM_cp_path=/glade/derecho/scratch/bkruyt/CMIP6/GCM_Igrid


# model=CanESM5
model=CMCC-CM2-SR5
# model=MIROC-ES2L
# model=MPI-M.MPI-ESM1-2-LR
# model=NorESM2-MM

# # #   hist needs 55 jobs, sspXX_2004 needs 0-46, sspXX_2049 needs 0-50!  # # #
# allScens=(hist ssp245_2004 ssp245_2049 ssp370_2004 ssp370_2049 ssp585_2004 ssp585_2049 )
# allScens=(hist ssp370_2004 ssp370_2049 ssp245_2004 ssp245_2049 )
allScens=( ssp245_2004 ssp245_2049 )
# allScens=(ssp245_2049 )
# allScens=(ssp585_2049 )
# allScens=(ssp370_2049)



#_______________ launch the python script _____________
for scen in ${allScens[@]}; do
    # determine start year from scenario parameter:
    if [[ "${scen:0:4}" == "hist" ]]; then
        start_year=1950
    elif [[ "${scen:6:5}" == "_2004" ]]; then
        start_year=2005
    elif [[ "${scen:6:5}" == "_2049" ]]; then
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

    # # # # Launch the 24h file script:
    # mkdir -p job_output_daily/${model}_${scen} #_${JOBID}
    # python -u main_24hr.py $path_in $path_out $year $model  $scen $remove_cp $GCM_cp_path >& job_output_daily/${model}_${scen}/${year}

    # # # Launch the 3h file script:
    mkdir -p job_output_3hr/${model}_${scen} #_${JOBID}
    python -u main_3hr.py  $path_in $path_out $year $model $scen $remove_cp $GCM_cp_path #>& job_output_3hr/__${model}_${scen}/${year}


    echo " "
    echo " - - -    Done processing daily & 3hr files for $model $scen  - - - - -"

done
echo " "
echo " - - -    $model  Done    - - - - -"

