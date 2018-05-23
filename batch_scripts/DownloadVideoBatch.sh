#!/bin/bash

#SBATCH --job-name=VideoDL
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=40GB
#SBATCH --time=48:00:00
#SBATCH --output=slurm-%j.out

module purge

module load python3/intel/3.5.3

SRCDIR=$HOME

source activate bandhub

chmod +x $SRCDIR/BandhubVideoDownload.py
python3 $SRCDIR/BandhubVideoDownload.py '/scratch/work/marl/bandhub/BandhubDataset.h5' '/scratch/work/marl/bandhub/video' 0 300000 True