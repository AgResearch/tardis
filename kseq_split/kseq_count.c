/*
* This software is based on Heng Li's kseq parser
* http://lh3lh3.users.sourceforge.net/kseq.shtml
* AMcC
* 
* kseq parser Contact: Heng Li <lh3@sanger.ac.uk> 
* 
* The MIT License included with kseq is reproduced below
*/  
/* The MIT License 
 
   Copyright (c) 2008 Genome Research Ltd (GRL). 
 
   Permission is hereby granted, free of charge, to any person obtaining 
   a copy of this software and associated documentation files (the 
   "Software"), to deal in the Software without restriction, including 
   without limitation the rights to use, copy, modify, merge, publish, 
   distribute, sublicense, and/or sell copies of the Software, and to 
   permit persons to whom the Software is furnished to do so, subject to 
   the following conditions: 
 
   The above copyright notice and this permission notice shall be 
   included in all copies or substantial portions of the Software. 
 
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS 
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN 
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
   SOFTWARE. 
*/  
#include <zlib.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include "kseq.h"
KSEQ_INIT(gzFile, gzread)


/*
* This program counts the number of logical recrods in  a fastq or fasta, and prints the count to stdout
*/


typedef struct kseqcount_opts {
	char* input_filename;
	int approximate;
        	
} t_kseqcount_opts;

typedef struct kseqcount_tempfile {
	char* temp_filename;
	gzFile temp_file;
        	
} t_kseqcount_tempfile;


int get_kseqcount_opts(int argc, char **argv, t_kseqcount_opts *kseqcount_opts)
{
	char* usage="Usage: %s [ -h ] filename \n";
	int c;

	kseqcount_opts->approximate = 0; 

	while ((c = getopt (argc, argv, "ha")) != -1) {
		switch (c) {
			case 'h':
				fprintf(stderr, usage, argv[0]);
				return 2;
			case 'a':
				kseqcount_opts-> approximate = 1;
				break;
			case '?':
				fprintf (stderr,"Unknown option character `\\x%x'.\n",optopt);
				return 1;
			default:
				abort ();
		} // switch
   	} // getopt loop


  
	// assign non-option args 
	if( argc - optind != 1 ) {
		fprintf(stderr, usage, argv[0]);
		return 1;
	}
	kseqcount_opts->input_filename = argv[optind];    
  	return 0;
}

void kseq_count_write( kseq_t *seq, gzFile fp) {
	if ( ! seq->qual.l ) {
		// assume fasta
		if ( seq->comment.l ) {
			gzprintf(fp, ">%s %s\n", seq->name.s, seq->comment.s);
		}
		else {
			gzprintf(fp, ">%s\n", seq->name.s);
		}
		gzprintf(fp, "%s\n", seq->seq.s);
	}
	else {
		// assume fastq		
        	if ( seq->comment.l ) {
			gzprintf(fp, "@%s %s\n", seq->name.s, seq->comment.s);
                }
		else {
			gzprintf(fp, "@%s\n", seq->name.s);
		}
		gzprintf(fp, "%s\n+\n%s\n", seq->seq.s, seq->qual.s);
	}

	return;
}

double get_filesize(char *filename) {
	struct stat stbuf;
	stat(filename, &stbuf);
	return (double) stbuf.st_size;
}

void get_tempfile(t_kseqcount_tempfile *tempfile) {
	int fp;
        gzFile zp;

        char *template = "/tmp/kseq_countXXXXXX";
	tempfile->temp_filename = (char *) malloc(1+strlen(template));
        tempfile->temp_filename = strcpy(tempfile->temp_filename, template);
   	fp=mkstemp(tempfile->temp_filename);
	tempfile->temp_file=gzdopen(fp,"w");
	return;
}


int main(int argc, char *argv[])
{
	gzFile fp,zp;
	kseq_t *seq;
	int l;
        int record_count=0;
        int kseqcount_opts_result = 0;

        const int sample_size = 20000;

	t_kseqcount_opts kseqcount_opts;
	t_kseqcount_tempfile tempfile;
        
        kseqcount_opts_result = get_kseqcount_opts(argc, argv, &kseqcount_opts);
 	if ( kseqcount_opts_result != 0 ){
		return kseqcount_opts_result;
 	} 

	// process the file 
	fp = gzopen(kseqcount_opts.input_filename, "r");

	if(kseqcount_opts.approximate == 0) {
		seq = kseq_init(fp);
		while ((l = kseq_read(seq)) >= 0) {
	           record_count += 1;
        	}
	} // not approximating
	else {
		// read and write sample_size records , then count = big_size / small_size * sample_size
		get_tempfile(&tempfile);
		seq = kseq_init(fp);
		while((l = kseq_read(seq)) >= 0) {
			kseq_count_write( seq, tempfile.temp_file);
			record_count++;
			if(record_count == sample_size) {
				break;
			}
		}
		//kseq_read(seq); 


		//gzprintf(tempfile.temp_file, "Hello World\n");
		
		gzclose(tempfile.temp_file);
		record_count = (int)( 0.5 + sample_size * get_filesize( kseqcount_opts.input_filename ) / get_filesize(tempfile.temp_filename) );
		//record_count = 0;

	} // approximating

	// clean up and return
	kseq_destroy(seq);
	gzclose(fp);
        printf ("%d\n", record_count);

	return 0;
}
