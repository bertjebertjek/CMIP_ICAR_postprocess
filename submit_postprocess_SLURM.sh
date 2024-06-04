#!/bin/bash
#SBATCH --job-name="PostPrcs"
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --constraint=cpu
#SBATCH --time=06:00:00
#SBATCH --array=5-50
#SBATCH --account=m4062
#SBATCH --output=job_output/log_%x.%j.out


echo "SLURM_ARRAY_TASK_ID: $SLURM_ARRAY_TASK_ID"
echo "SLURM_JOB_ID: $SLURM_JOB_ID"  # different for every task member...
JOBID={$SLURM_JOB_ID:0:8}
echo "MY_JOB_ID: $JOB_ID"

# ____________
# module load conda
conda activate myenv


# 1.____ input from NERSC project _____
# path_in=/glade/campaign/ral/hap/bert/CMIP6/WUS_icar_out
# path_in=/global/cfs/cdirs/m4062/icar_output/CMIP6/great_lakes
path_in=/global/cfs/cdirs/m4062/icar_output/CMIP6/GL_1h_raw

path_out=/global/cfs/cdirs/m4062/icar_output/CMIP6/GL_3h_fix_nocp


# Remove GCM cp from ICAR data? Requires GCM cp regdridded to ICAR grid.
remove_cp=True                     # in case no rain_var during run!
GCM_cp_path=/pscratch/sd/b/bkruyt/CMIP/greatlakes/GCM_Igrid_GL
dt=3hr

# model=CanESM5
# model=CMCC-CM2-SR5
model=MIROC-ES2L  # hist till 2014!! has GCM cp!
# model=MPI-M.MPI-ESM1-2-LR
# model=NorESM2-MM # rem_cp=F

# # #   hist needs 55 jobs, sspXX_2004 needs 0-46, sspXX_2049 needs 0-50!  # # #

# allScens=(historical  ssp370_2004 ssp370_2049 ssp585_2004 ssp585_2049 )
# allScens=(hist ssp370_2004 ssp370_2049 )
# allScens=( historical ssp585_2004  ssp585_2049 )
# allScens=(ssp370_2049)
allScens=( ssp370_2004 )



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
    year=$(( $SLURM_ARRAY_TASK_ID + $start_year  ))

    # make directory for output:
    mkdir -p job_output_daily/${model}_${scen}    #_${SLURM_JOB_ID}
    mkdir -p job_output_3hr/${model}_${scen}     #_${SLURM_JOB_ID}

    # launch script:
    echo " "
    echo "Launching yearly 24hr & 3hr file correction for $model $scen ${year}"
    echo "  remove cp is $remove_cp "
    echo " "

    # # # # # Launch the 24h file script: (not tested yet for 3h inputs)
    if [[ "${dt}" == "daily" ]]; then
        python -u main_24hr.py $path_in $path_out $year $model  $scen $remove_cp $GCM_cp_path >& job_output_daily/${model}_${scen}/${year}

    # # # # # # Launch the 3h file script:
    elif [[ "${dt}" == "3hr" ]]; then
        python -u main_3hr.py  $path_in $path_out $year $model  $scen $remove_cp $GCM_cp_path >& job_output_3hr/${model}_${scen}/${year}

    fi

    echo " "
    echo " - - -    Done processing daily & 3hr files for $model $scen  - - - - -"

done
echo " "
echo " - - -    $model  Done    - - - - -"

