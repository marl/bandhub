#!/bin/bash

#SBATCH --job-name=Transfer
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=20GB
#SBATCH --time=48:00:00
#SBATCH --output=slurm-%j.out

rsync -rthv --progress /scratch/gjr286/UnprocessedAudio/ /scratch/work/marl/bandhub/unprocessedAudio 