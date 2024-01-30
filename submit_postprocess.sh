#!/bin/bash
#PBS -l select=1:ncpus=1:mem=50GB
#PBS -l walltime=03:00:00
#PBS -A P48500028
#PBS -q casper
#PBS -N negPCP_Array
#PBS -J 47-50:1
#PBS -o job_output/array.out
#PBS -j oe

JOBID=`echo ${PBS_JOBID} | cut -d'[' -f1`
# echo " JOBID: $JOBID"

# ____________
module load conda
conda activate npl

# ____________   Set arguments: (year = PBS_array_index) -_______________
path_out=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3h_test
path_out_nocp=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3h_test/nocp
# path_out=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_pcpfix  # for version 2, all values at erroneous timesteps

# 1.____ input from Bert's CAMPAIGN _____
path_in=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_out
# path_in=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_3hr_month


# model=CanESM5
model=CMCC-CM2-SR5
# model=MIROC-ES2L
# model=MPI-M.MPI-ESM1-2-LR
# model=NorESM2-MM

# # #   hist needs 55 jobs, sspXX_2004 needs 0-46, sspXX_2049 needs 0-50!  # # #
# allScens=(hist ssp245_2004 ssp245_2049 ssp370_2004 ssp370_2049 ssp585_2004 ssp585_2049 )
# allScens=(hist ssp370_2004 ssp370_2049 ssp245_2004 ssp245_2049 )
# allScens=(ssp245_2004 ) #ssp245_2049 )
allScens=(ssp245_2049 )
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

    # make directory for output:
    # mkdir -p job_output_daily/${model}_${scen}_${JOBID}
    # mkdir -p job_output_3hr/${model}_${scen}_${JOBID}

    mkdir -p job_output/${model}_${scen}_${JOBID}

    # launch script:
    echo " "
    echo "Launching yearly 24hr & 3hr file correction for $model $scen ${year}"
    echo " "

    # # Launch the 24h file script:
    python -u main_24hr.py $path_in $path_out $path_out_nocp ${year} $model  $scen >& job_output_daily/${model}_${scen}_$JOBID/${year}

    # # Launch the 3h file script:
    python -u main_3hr.py $path_in $path_out $path_out_nocp ${year} $model  $scen >& job_output_3hr/${model}_${scen}_$JOBID/${year}  # XGB / year?

    echo " "
    echo " - - -    Done processing daily & 3hr files for $model $scen  - - - - -"

done
echo " "
echo " - - -    $model  Done    - - - - -"

