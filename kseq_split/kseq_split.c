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
#include "kseq.h"
KSEQ_INIT(gzFile, gzread)


/*
* This program splits a fastq or fasta file into chunks (for example for 
* submission to a compute cluster), optionally subsampling the file. Input 
* files may be compressed or uncompressed.  It is compatible for use with a 
* client that submits each chunk to processing as it becomes available, with 
* an "interim" filename used to write data, and then this is renamed to the actual target 
* chunkname only when complete. Thus a client can poll for chunk files,
* with a guarantee that any found are completed.
* 
* 
*/
#define MAX_CHUNK_NUMBER_LENGTH 30


/*
* Next two methods : given the current chunk number, close the current in-progress chunk file (if any),
* rename it according to the template to be used for the final chunks,  and 
* open a new "in-progress" chunk file, returning the new file pointer. 
* Chunk file names are made using a template. "In progress" chunk file names are made by 
* appending the final name with an underscore.
* For example a template SQ0352_S1_L001_R1_001.%05d.fastq.
*
* will yield SQ0352_S1_L001_R1_001.00001.fastq, SQ0352_S1_L001_R1_001.00002.fastq etc 
* (with "in-progress" names SQ0352_S1_L001_R1_001.00001.fastq_, SQ0352_S1_L001_R2_001.00001.fastq_ etc )
* etc 
*/
void finalise_chunk( FILE *current_fp, char *chunk_name_buffer) { 
	char *final_name_buffer;
	final_name_buffer = (char *) malloc(strlen(chunk_name_buffer));
        final_name_buffer = strncpy(final_name_buffer, chunk_name_buffer, strlen(chunk_name_buffer)-1);
	final_name_buffer[strlen(chunk_name_buffer)-1] = '\0'; 
      
	fclose( current_fp );	
	rename(chunk_name_buffer, final_name_buffer);
	free(final_name_buffer);
	return;
}
FILE *advance_chunk( char *filename_template, int current_chunk_number, FILE *current_fp, char *chunk_name_buffer) {       
	FILE *next_fp;

	if ( current_chunk_number > 0 ) {
		finalise_chunk(current_fp, chunk_name_buffer);
	}

	// construct name of next in-progress chunk file - i.e. chunk file name appended with _
        sprintf((char *) chunk_name_buffer, filename_template, 1+current_chunk_number);
	chunk_name_buffer = strcat(chunk_name_buffer,"_");
	next_fp = fopen(chunk_name_buffer, "w");	
        return next_fp;
}


/* 
* write back out a kseq_t record to a fastq file. Example kseq_t record : 
* name: ST-E00118:256:H52J5ALXX:3:1101:2270:1309
* comment: 1:N:0:0
* seq: NGAGTTTGCTCAAATTCATGTCCATTGAGTCGGTGATGCTAGACAGTCTGAATACATGCTAGTTGGTGATGAGGCAAATAGCCTTAGCATTTCTCAGATGAAGAGTTCAAGATTCAGCACACTTGAATGGCTTTCTCAGAGTAATTCAGT
* qual: #AA<AFJJJFJJFFJJJJJJJJJJJJJJJJJJJJJFJJJFFJJJFJFFJJFJJJJJJJJFFJJJJFJJJJJJJJJJJFJJJJJJJJJJJJJJJJJJJFJJAFFJJFFJJJJJJFJFJJJJJJJJJJAFFFJ<AJJAJJFJFFJA7FFFFA
* 
* source record which we want to round-trip back out: 
* @ST-E00118:256:H52J5ALXX:3:1101:2270:1309 1:N:0:0
* NGAGTTTGCTCAAATTCATGTCCATTGAGTCGGTGATGCTAGACAGTCTGAATACATGCTAGTTGGTGATGAGGCAAATAGCCTTAGCATTTCTCAGATGAAGAGTTCAAGATTCAGCACACTTGAATGGCTTTCTCAGAGTAATTCAGT
* +
* #AA<AFJJJFJJFFJJJJJJJJJJJJJJJJJJJJJFJJJFFJJJFJFFJJFJJJJJJJJFFJJJJFJJJJJJJJJJJFJJJJJJJJJJJJJJJJJJJFJJAFFJJFFJJJJJJFJFJJJJJJJJJJAFFFJ<AJJAJJFJFFJA7FFFFA
* 
*/

typedef struct kseqsplit_opts {
	char* input_filename;	
	int chunksize;
        char *filename_template;
	char* stats_filename;
	char* output_format;
	float sampling_proportion;
} t_kseqsplit_opts;


void kseq_split_write( kseq_t *seq, FILE *fp_chunk, t_kseqsplit_opts *kseqsplit_opts) {
	if ( (! seq->qual.l ) || strcmp(kseqsplit_opts->output_format,"fasta") == 0) {
		// assume fasta
		if ( seq->comment.l ) {
			fprintf(fp_chunk, ">%s %s\n", seq->name.s, seq->comment.s);
		}
		else {
			fprintf(fp_chunk, ">%s\n", seq->name.s);
		}
		fprintf(fp_chunk, "%s\n", seq->seq.s);
	}
	else {
		// assume fastq		
        	if ( seq->comment.l ) {
			fprintf(fp_chunk, "@%s %s\n", seq->name.s, seq->comment.s);
                }
		else {
			fprintf(fp_chunk, "@%s\n", seq->name.s);
		}
		fprintf(fp_chunk, "%s\n+\n%s\n", seq->seq.s, seq->qual.s);
	}

	return;
}


int get_sample_bool(float sampling_proportion) {
	float univariate;

	univariate = rand() / (float) RAND_MAX;

	if(univariate <= sampling_proportion ) {
		return 1;
	}
	else {
		return 0;
	}
}   


int get_kseqsplit_opts(int argc, char **argv, t_kseqsplit_opts *kseqsplit_opts)
{
	char* usage="Usage: %s [-f stats_filename (optional, only useful if part of pipeline)] [ -s sampling_proportion ] [ -h ] [ -v ] -o output_format_required (fasta|fastq) <input filename (input maybe fasta or fastq optionally compressed> <chunksize (before sampling)> <output_filenames_template>\n";
 	int index;
	int c;
	int iresult;
	int validate_only = 0;

	kseqsplit_opts->stats_filename = "";
	kseqsplit_opts->output_format = "";
	kseqsplit_opts->sampling_proportion = -1.0 ; 


	opterr = 0;

	while ((c = getopt (argc, argv, "hvs:f:o:")) != -1) {
		switch (c) {
			case 'h':
				fprintf(stderr, usage, argv[0]);
				return 2;
			case 'v':
				validate_only = 1;
				break;
 			case 's':
				// parse sampling proportion
				iresult = sscanf(optarg,"%f", &(kseqsplit_opts->sampling_proportion) );
				if(iresult != 1) {
					fprintf (stderr, "Unable to parse sampling proportion from %s \n", optarg);
	   				return 1;
				}
				if( kseqsplit_opts->sampling_proportion < 0.0 || kseqsplit_opts->sampling_proportion > 1.0 ) {
					fprintf (stderr, "Sampling proportion ( %f ) should be greater than 0 and less than 1\n", kseqsplit_opts->sampling_proportion);
					return 1;
				}
				// initialise random number generator
				srand(time(NULL));   // should only be called once
				break;
			case 'f':
				kseqsplit_opts->stats_filename = optarg;
				break;
			case 'o':
				kseqsplit_opts->output_format = optarg;
				break;
			case '?':
				if (optopt == 's')
					fprintf (stderr, "Option -%c requires an argument.\n", optopt);
				else if (optopt == 'f')
					fprintf (stderr, "Option -%c requires an argument.\n", optopt);
				else if (isprint (optopt))
					fprintf (stderr, "Unknown option `-%c'.\n", optopt);
				else
					fprintf (stderr,"Unknown option character `\\x%x'.\n",optopt);
					return 1;
			default:
				abort ();
		} // switch
   	} // getopt loop


  
	// assign non-option args 
	if( argc - optind != 3 ) {
		fprintf(stderr, usage, argv[0]);
		return 1;
	}
	kseqsplit_opts->input_filename = argv[optind];    
	iresult = sscanf(argv[optind+1],"%d", &(kseqsplit_opts->chunksize));
	if( iresult != 1) {
		fprintf(stderr, "Unable to parse chunksize from %s\n", argv[optind+1]);
		return 1;
	}
	if ( kseqsplit_opts->sampling_proportion > 0 ){
		kseqsplit_opts->chunksize = (int) 0.5 + kseqsplit_opts->sampling_proportion * kseqsplit_opts->chunksize; 
                if ( kseqsplit_opts->chunksize < 1 ) {
			kseqsplit_opts->chunksize = 1;
                }
	}



	kseqsplit_opts->filename_template = argv[optind+2];

	// do some checks 
	if ( strcmp(kseqsplit_opts->output_format, "fasta") != 0 && strcmp(kseqsplit_opts->output_format , "fastq") != 0  ) {
		fprintf(stderr, "must specify output format fasta or fastq\n");
		return 1;		
	}

  
	if (validate_only) {
		printf ("kseq_split options:\n stats_filename = %s\n sampling_proportion = %f\n input_filename=%s\n chunksize=%d\n filename_template=%s output_format=%s\n",\
        	kseqsplit_opts->stats_filename,  kseqsplit_opts->sampling_proportion,\
        	kseqsplit_opts->input_filename, kseqsplit_opts->chunksize, \
        	kseqsplit_opts->filename_template, kseqsplit_opts->output_format);
		return 2;
	}
	else {
  		return 0;
	}
}


int main(int argc, char *argv[])
{
	
	gzFile fp;
	FILE *fp_chunk, *fp_stats;
	kseq_t *seq;
	int l;
        int record_count=0;
        int chunk_number=0;
        int kseqsplit_opts_result = 0;
        int sample_bool = 1;

	t_kseqsplit_opts kseqsplit_opts;
        kseqsplit_opts_result = get_kseqsplit_opts(argc, argv, &kseqsplit_opts);
 	if ( kseqsplit_opts_result != 0 ){
		// usage message on help is not an error
		return kseqsplit_opts_result == 2 ? 0 : kseqsplit_opts_result;
 	} 
      
	char* chunk_name_buffer;

	// set up buffer for creating chunk filenames
	chunk_name_buffer = (char *) malloc(MAX_CHUNK_NUMBER_LENGTH + strlen(kseqsplit_opts.filename_template)); 


	// process the file 
	fp = gzopen(kseqsplit_opts.input_filename, "r");
	seq = kseq_init(fp);
	while ((l = kseq_read(seq)) >= 0) {
		if ( kseqsplit_opts.sampling_proportion > 0 ) {
			sample_bool = get_sample_bool( kseqsplit_opts.sampling_proportion );
		}

		if(sample_bool) {		
			record_count++;
			if (record_count%kseqsplit_opts.chunksize == 1 || kseqsplit_opts.chunksize == 1) {
				fp_chunk = advance_chunk(kseqsplit_opts.filename_template, chunk_number, fp_chunk, chunk_name_buffer);
				chunk_number++;
                	}
			kseq_split_write( seq, fp_chunk, &kseqsplit_opts);
		}
	}

	// clean up and return
	kseq_destroy(seq);
	gzclose(fp);

	if(record_count > 0) {
		finalise_chunk(fp_chunk, chunk_name_buffer);
	}
	free(chunk_name_buffer);

	// if requested, write stats
	if (strlen(kseqsplit_opts.stats_filename) >  0) {
		fp_stats = fopen(kseqsplit_opts.stats_filename, "w");
		fprintf(fp_stats, "chunk_number=%d\n", chunk_number);
		fclose(fp_stats);
        }

	if ( l == -1 ) {
		return 0;  // end of file
	}
	else {
		return l;  // parsing error 
	}
}
