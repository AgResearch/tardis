#!/bin/sh

#export PYTHONPATH=/dataset/bioinformatics_dev/active/tardis/tardis
#export PATH="/dataset/bioinformatics_dev/active/tardis/tardis:$PATH"

BWA_REF=/dataset/bioinformatics_dev/active/tardis/tardis/test/mRNAs.fa
DATADIR=/dataset/bioinformatics_dev/active/tardis/tardis/test
BWA_REF=mRNAs.fa 


###### these settings for testing the dev branch that is checked out######### 
TARDIS="run.py"
export PATH=/dataset/bioinformatics_dev/active/tardis/tardis/tardis:/stash/miniconda3/envs/universal2/bin:$PATH 
export PYTHONPATH=/dataset/bioinformatics_dev/active/tardis/tardis
#############################################################################


rm -f demo.inc baton.tmp results.out.gz R1R2_vs_mRNAs.bam /home/mccullocha/hello_from_slurm.txt .tardishrc test_add_path.txt

function test_legacy() {
   hpctype=$1
   /usr/local/agr-scripts/$TARDIS -w -d .  --hpctype $hpctype echo "hello world"      # current version on system 
}

function test_hello_world() {
   hpctype=$1
   $TARDIS -q  --hpctype $1 echo \"hello world\"     
}

function test_blastn() {
   hpctype=$1
   #$TARDIS -q -c 2  --hpctype $1 -dryrun blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna
   #$TARDIS -q --hpctype $1 blastn -query test.fa -db /dataset/blastdata/active/mirror/rat.fna
   #$TARDIS -c 1 --hpctype $1 blastn -query  _condition_fasta_input_test.fa -db nt
   $TARDIS --hpctype $1 blastn -query _condition_fasta_input_test.fa -task blastn -num_threads 2 -db nt -max_target_seqs 1 -outfmt  \'7 qseqid sseqid pident evalue staxids sscinames scomnames sskingdoms stitle\'


}

function test_baton_passing() {
   hpctype=$1
   $TARDIS --hpctype $1 -batonfile baton.tmp blastn -query test.fa -db /dataset/blastdata/active/mirror/rat.fna
}

function test_include() {
   hpctype=$1
   echo 'eval `modulecmd sh load samtools/1.1`' >  demo.inc
   $TARDIS -q --hpctype $1 -shell-include-file demo.inc samtools
}


function test_kmers() {
# seq_prisms/kmer_prism.sh -n -p "-k 3" -a fastq -O /dataset/miseq/scratch/postprocessing/gtseq/180426_M02412_0076_000000000-G1TCL/kmers /dataset/miseq/active/180426_M02412_0076_000000000-G1TCL/Data/Intensities/BaseCalls/*.fastq.gz
# inscrutable$ cat /dataset/miseq/scratch/postprocessing/gtseq/180426_M02412_0076_000000000-G1TCL/kmers/tardis.toml
#max_tasks = 1
#min_sample_size = 0
#inscrutable$
#inscrutable$ cat /dataset/miseq/scratch/postprocessing/gtseq/180426_M02412_0076_000000000-G1TCL/kmers/BBG74173_S161_L001_R1_001.fastq.gz.fastq.k3.sh
#!/bin/bash
#         $TARDIS -q --hpctype slurm -d  . --shell-include-file configure_biopython_env.src cat  _condition_fastq2fasta_input_/dataset/miseq/active/180426_M02412_0076_000000000-G1TCL/Data/Intensities/BaseCalls/BBG74173_S161_L001_R1_001.fastq.gz  \> _condition_uncompressedtext_output_BBG74173_S161_L001_R1_001.fastq.gz.k3.1
          $TARDIS --hpctype slurm -d  .  --shell-include-file configure_biopython_env.src kmer_prism.py -f fasta -k 3 -o BBG74173_S161_L001_R1_001.fastq.gz.frequency.txt  BBG74173_S161_L001_R1_001.fastq.gz.k3.1  
#        if [ 0 == 0 ]; then
#           rm /dataset/miseq/scratch/postprocessing/gtseq/180426_M02412_0076_000000000-G1TCL/kmers/BBG74173_S161_L001_R1_001.fastq.gz.BBG74173_S161_L001_R1_001.fastq.gz.fastq.k3.1
#           rm /dataset/miseq/scratch/postprocessing/gtseq/180426_M02412_0076_000000000-G1TCL/kmers/BBG74173_S161_L001_R1_001.fastq.gz.frequency.txt
#        fi
}




#$TARDIS -dryrun --hpctype slurm  echo hello world \> /home/mccullocha/hello_from_slurm.txt       
#$TARDIS -dryrun  -c 2  --hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#$TARDIS -dryrun -shell-include-file test_include.txt  -c 2  --hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#$TARDIS -c 2  --hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#$TARDIS -shell-include-file test_include.txt  -c 2  --hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 
#$TARDIS -shell-include-file test_include.txt -job-file array_job1.txt  -c 2  --hpctype slurm blastn -query _condition_fasta_input_test.fa -db /dataset/blastdata/active/mirror/rat.fna -out _condition_text_output_results.out 

##################################################
function test_bwa() {
    $TARDIS  --hpctype slurm bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R1.fastq \> _condition_throughput_R1_v_$BWA_REF.sai \;  bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_throughput_R2_v_$BWA_REF.sai \; bwa sampe $DATADIR/$BWA_REF _condition_throughput_R1_v_$BWA_REF.sai _condition_throughput_R2_v_$BWA_REF.sai _condition_paired_fastq_input_$DATADIR/R1.fastq _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_sam_output_R1R2_vs_mRNAs.bam
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
$TARDIS -dryrun --hpctype slurm  echo hello world \> /home/mccullocha/hello_from_slurm.txt
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
$TARDIS -w -c 5  --hpctype slurm bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R1.fastq \> _condition_throughput_R1_v_$BWA_REF.sai \;  bwa aln $DATADIR/$BWA_REF _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_throughput_R2_v_$BWA_REF.sai \; bwa sampe $DATADIR/$BWA_REF _condition_throughput_R1_v_$BWA_REF.sai _condition_throughput_R2_v_$BWA_REF.sai _condition_paired_fastq_input_$DATADIR/R1.fastq _condition_paired_fastq_input_$DATADIR/R2.fastq \> _condition_sam_output_R1R2_vs_mRNAs.bam
}

#test_hello_world slurm
#test_hello_world condor 
#test_hello_world local 
#test_blastn slurm
#test_blastn condor 

#test_bwa
test_kmers
#test_add_path
#test_add_path2

#test_baton_passing condor
#test_baton_passing local

#test_include condor
#test_include local
