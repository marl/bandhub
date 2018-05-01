#!/bin/bash

#SBATCH --job-name=DL
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=20GB
#SBATCH --time=48:00:00
#SBATCH --output=slurm-%j.out

module purge

module load python3/intel/3.5.3


SRCDIR=$HOME
RUNDIR=$SCRATCH/run-${SLURM_JOB_ID/.*}
mkdir -p $RUNDIR


source activate bandhub

cd $RUNDIR

chmod +x $SRCDIR/BandhubDownload.py
python3 $SRCDIR/BandhubDownload.py '/home/gjr286/BandhubDatasetTemp.h5' '/scratch/gjr286/TestDownload' 'Tracks' 0 10 True