#!/bin/sh

export PYTHONPATH=/dataset/bioinformatics_dev/active/tardis/tardis
export PATH="/dataset/bioinformatics_dev/active/tardis/tardis:$PATH"

BWA_REF=/dataset/bioinformatics_dev/active/tardis/tardis/test/mRNAs.fa
DATADIR=/dataset/bioinformatics_dev/active/tardis/tardis/test
BWA_REF=mRNAs.fa 


rm -f results.out.gz R1R2_vs_mRNAs.bam /home/mccullocha/hello_from_slurm.txt .tardishrc test_add_path.txt

function test_legacy() {
   hpctype=$1
   /usr/local/agr-scripts/tardis.py -w -d .  -hpctype $hpctype echo "hello world"      # current version on system 
}

function test_hello_world() {
   hpctype=$1
   tardis.py -q  -hpctype $1 echo \"hello world\"     
}

function test_blastn() {
   hpctype=$1
   tardis.py -q -c 2  -hpctype $1 -dryrun blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna
}


#tardis.py -dryrun -hpctype slurm  echo hello world \> /home/mccullocha/hello_from_slurm.txt       
#tardis.py -dryrun  -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#tardis.py -dryrun -shell-include-file test_include.txt  -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#tardis.py -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#tardis.py -shell-include-file test_include.txt  -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#tardis.py -shell-include-file test_include.txt -job-file array_job1.txt  -c 2  -hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 

##################################################
function test_bwa() {
tardis.py -w -c 5  -hpctype slurm bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R1.fastq \> _condition_throughput_R1_v_$BWA_REF.sai \;  bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_throughput_R2_v_$BWA_REF.sai \; bwa sampe $DATADIR/$BWA_REF _condition_throughput_R1_v_$BWA_REF.sai _condition_throughput_R2_v_$BWA_REF.sai _condition_paired_fastq_input_$DATADIR/R1.fastq _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_sam_output_R1R2_vs_mRNAs.bam
}

##################################################
function test_add_path() {
echo "
source activate bifo-essential
export PATH="\$PATH:/dataset/bioinformatics_dev/active/tardis/conda/bin"
" > test_add_path.txt
echo "[tardish]

[tardis_engine]
runtimeconfigsourcefile=test_add_path.txt
" > ./.tardishrc
tardis.py -dryrun -hpctype slurm  echo hello world \> /home/mccullocha/hello_from_slurm.txt
}
##################################################
function test_add_path2() {
echo "
source activate bifo-essential
export PATH="\$PATH:/dataset/bioinformatics_dev/active/tardis/conda/bin"
" > test_add_path.txt
echo "[tardish]

[tardis_engine]
runtimeconfigsourcefile=test_add_path.txt
" > ./.tardishrc
tardis.py -w -c 5  -hpctype slurm bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R1.fastq \> _condition_throughput_R1_v_$BWA_REF.sai \;  bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_throughput_R2_v_$BWA_REF.sai \; bwa sampe $DATADIR/$BWA_REF _condition_throughput_R1_v_$BWA_REF.sai _condition_throughput_R2_v_$BWA_REF.sai _condition_paired_fastq_input_$DATADIR/R1.fastq _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_sam_output_R1R2_vs_mRNAs.bam
}

set -x
#test_hello_world slurm
test_blastn slurm

#test_bwa
#test_add_path
#test_add_path2
set +x
