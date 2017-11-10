#!/bin/sh

#export PYTHONPATH=/dataset/bioinformatics_dev/active/tardis/tardis

rm -f results.out.gz

#tardis.py -w -d .  -hpctype local ls      # current version on system 
#../tardis.py -w -d .  -hpctype local ls      # new version  
#../tardis.py -w -d . -c 2  -hpctype local blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out       
#../tardis.py -w -d . -c 2  -hpctype local blastn -query _condition_fastq2fasta_input_test.fastq -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out       

#../tardis.py -dryrun -hpctype slurm  echo hello world \> /home/mccullocha/hello_from_slurm.txt       
#../tardis.py -dryrun  -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
../tardis.py -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 

