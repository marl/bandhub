#!/bin/bash

#SBATCH --job-name=CreateDataset
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=20GB
#SBATCH --time=48:00:00
#SBATCH --output=slurm_%j.out

module purge

module load python3/intel/3.5.3

SRCDIR=$HOME
RUNDIR=$SCRATCH/run-${SLURM_JOB_ID/.*}
mkdir -p $RUNDIR


source activate bandhub
module load mongodb/3.4.10
mongod --dbpath /scratch/gjr286/mongodb --fork --logpath mongo.log --port 27017

cd $RUNDIR

chmod +x $SRCDIR/BandhubAppend.py
python3 $SRCDIR/BandhubAppend.py '/home/gjr286/t.h5' 27017 'b=bandhub' 'bandhubExtraVols.h5' 0 30000