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

chmod +x $SRCDIR/BandhubAudioDownload.py
python3 $SRCDIR/BandhubAudioDownload.py '/home/gjr286/BandhubDatasetTemp.h5' '/scratch/gjr286/TestUnprocessed' '/scratch/gjr286/TestProcessed' '/scratch/gjr286/tempFiles' 0 100 True