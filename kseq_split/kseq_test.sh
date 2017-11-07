#!/bin/sh

#./kseq_test /dataset/KCCG/archive/dx/160906_FR07934513/inputFastq/H52J5ALXX_3_160906_FR07934513_Other__R_160818_SHACLA_DNA_M001_R1.fastq.gz
#./kseq_test /home/mccullocha/isgcandrepbase2.fa
#rm test.*.fastq; ./kseq_split test.fastq  4 test.%05d.fastq test.fastq.chunkstats
#rm test.*.fastq; ./kseq_split -f test.fastq.chunkstats test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fastq 
#rm test.*.fastq; ./kseq_split -f test.fastq.chunkstats -s 0.04 test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fastq 
#rm test.*.fastq; ./kseq_split -f test.fastq.chunkstats -s 0.2 test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fastq 
#rm test.*.fastq; ./kseq_split -v -f test.fastq.chunkstats -s 0.2 test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fastq 
#rm test.*.fastq; ./kseq_split -f test.fastq.chunkstats -s 0.2 -o fasta test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fastq 
#rm test.*.fastq; rm test.*.fasta; ./kseq_split -f test.fastq.chunkstats -s 0.2 -o fasta test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fasta 
#rm test.*.fastq; ./kseq_split -f test.fastq.chunkstats -s 0.2 -o fastq  test.fastq  4 /home/mccullocha/tardis/tardis/test.%05d.fastq 
#rm test.*.fastq; time ./kseq_split -f chunkstats.txt -s 0.01 -o fasta /bifo/active/Shi_Rumen_Microbiome/fastq/sratrim/Fasta/HighCH4/SRR1206671_noShort.fa 999999999 /dataset/Shi_Rumen_Microbiome/scratch/test/SRR1206671_noShort.%05d.fasta 
#rm test.*.fastq; time kseq_split -s 0.01 -o fastq /bifo/active/Shi_Rumen_Microbiome/fastq/SRR1206671_1.fastq 999999999 /dataset/Shi_Rumen_Microbiome/scratch/test/SRR1206671_1_sample.%05d.fastq 
rm test.*.fastq; time kseq_split -s 0.01 -o fasta /dataset/hiseq/scratch/postprocessing/170609_D00390_0306_ACA93DANXX.processed/bcl2fastq/SQ2627/SQ2627_S3_L006_R1_001.fastq.gz  1000000 /dataset/hiseq/scratch/postprocessing/test/SQ2627_S3_L006_R1_001.%05d.fasta  
