#!/bin/bash
#SBATCH -J python
#SBATCH -p first
#SBATCH -w tms07
#SBATCH -N 1
#SBATCH -n 48
#SBATCH --ntasks-per-node=48
##SBATCH --mem=100G
#SBATCH -o python.o-out
#SBATCH -e python.e-out
#SBATCH --exclusive
#SBATCH -t 7-00:00

ulimit -s unlimited

python /tera04/zhwei/xionghui/bedrock/job.py
