#!/bin/sh

export PYTHONPATH=/dataset/bioinformatics_dev/active/tardis/tardis
export PATH="/dataset/bioinformatics_dev/active/tardis/tardis:$PATH"

BWA_REF=/dataset/bioinformatics_dev/active/tardis/tardis/test/mRNAs.fa
DATADIR=/dataset/bioinformatics_dev/active/tardis/tardis/test
BWA_REF=mRNAs.fa 


rm -f demo.inc baton.tmp results.out.gz R1R2_vs_mRNAs.bam /home/mccullocha/hello_from_slurm.txt .tardishrc test_add_path.txt

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
   #tardis.py -q -c 2  -hpctype $1 -dryrun blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna
   #tardis.py -q -hpctype $1 blastn -query test.fa -db /dataset/blastdata/active/mirror/rat.fna
   #tardis.py -c 1 -hpctype $1 blastn -query  _condition_fasta_input_test.fa -db nt
   tardis.py -hpctype $1 blastn -query _condition_fasta_input_test.fa -task blastn -num_threads 2 -db nt -max_target_seqs 1 -outfmt  \'7 qseqid sseqid pident evalue staxids sscinames scomnames sskingdoms stitle\'


}

function test_baton_passing() {
   hpctype=$1
   tardis.py -hpctype $1 -batonfile baton.tmp blastn -query test.fa -db /dataset/blastdata/active/mirror/rat.fna
}

function test_include() {
   hpctype=$1
   echo 'eval `modulecmd sh load samtools/1.1`' >  demo.inc
   tardis.py -q -hpctype $1 -shell-include-file demo.inc samtools
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

#test_hello_world slurm
#test_hello_world condor 
#test_hello_world local 
#test_blastn slurm
#test_blastn condor 

#test_bwa
#test_add_path
#test_add_path2

#test_baton_passing condor
#test_baton_passing local

#test_include condor
test_include local
