#!/bin/bash -e

#SBATCH -J echo
#SBATCH -A mccullocha        # Project Account
#SBATCH --time=240:00:00            # Walltime
#SBATCH --ntasks=1                 # number of parallel processes
#SBATCH --ntasks-per-socket=1      # number of processes allowed on a socket
#SBATCH --cpus-per-task=1          #number of threads per process
#SBATCH --hint=nomultithread         # enable hyperthreading
#SBATCH --mem-per-cpu=1G
#SBATCH --partition=inv-blade-g8     # Use nodes in any partition
#SBATCH --array=1-1%50          # Iterate 1 to N, but only run up to 50 concurrent runs at once
#SBATCH --error=/bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_6Kbqb1/run-%A_%a.stderr
#SBATCH --output=/bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_6Kbqb1/run-%A_%a.stdout

srun /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_6Kbqb1/slurm_array_shim.sh ${SLURM_ARRAY_TASK_ID}
