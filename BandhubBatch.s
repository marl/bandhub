#!/bin/bash

#SBATCH --job-name=DatasetFileCreate
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=30GB
#SBATCH --time=24:00:00
#SBATCH --output=slurm_%j.out

module purge

module load python3/intel/3.5.3


SRCDIR=$HOME
RUNDIR=$SCRATCH/run-${SLURM_JOB_ID/.*}
mkdir -p $RUNDIR


source activate bandhub
module load mongodb/3.4.10
mongod --dbpath /scratch/gjr286/mongodb --fork --logpath mongo.log --port 32768

cd $RUNDIR

chmod +x $SRCDIR/BandhubFileCreation.py
python3 $SRCDIR/BandhubFileCreation.py 32768 'b=bandhub' 'bandhub.h5' 0