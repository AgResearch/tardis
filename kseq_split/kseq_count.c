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
#include <math.h>
#include "kseq.h"
KSEQ_INIT(gzFile, gzread)


/*
* This program counts the number of logical records in  a fastq or fasta file , and prints the count to stdout.
* The input file may be compressed or uncompressed. 
* It has an optional "approximate mode" (-a) , which estimates the number of records by 
* reading and writing a small preview , of n records, and then estimating N = empirical_adjustment_function( n * original file size / preview filesize)
* (if the original is compressed then so is the preview). The empirical adjustment is determined by fitting a model to 
* a test dataset of Y = actual/raw_approximation , in terms of X1=filesize in bytes, X2=compression type.
*
* bugs / limitations associated with the -a option : 
* 
*    1. accuracy in general on compressed data is somewhat poor as compression size is probably nonlinear w.r.t file size
*    2. accuracy will be poor if the preview seq lengths are unrepresentative
*    3. will not recognise .zz compression
*    4. for all compression types other than gzip, the -a option approximation may be poor (because the compressed preview is always gzip )
*    5. not extensively tested on formats other than gzip, and uncompressed
*    6. there are big-endian/little-endian variations on the compression magic bytes that are not yet supported. (One of these,for gzip, is supported)
*       - if compression is not detected, -a option will be "way out"
*    7. not tested on variant fastq and fasta 
*    8. The empirical adjustment is based on a fairly small test dataset and could be improved with more data and a better model
* 
*/

#define DEBUG 0


typedef struct kseqcount_opts {
	char* input_filename;
	int approximate;
        	
} t_kseqcount_opts;

typedef struct kseqcount_tempfile {
	char* temp_filename;
	gzFile ztemp_file;
	FILE *temp_file;        	
} t_kseqcount_tempfile;


int file_exist (char *filename) {
  	struct stat   stbuff;   
  	return (stat (filename, &stbuff) == 0);
}


int get_kseqcount_opts(int argc, char **argv, t_kseqcount_opts *kseqcount_opts)
{
	char* usage="Usage: %s [ -h ] [-a] filename \n";
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

	if ( ! file_exist( kseqcount_opts->input_filename )) {
		fprintf(stderr, "file not found : %s\n", kseqcount_opts->input_filename);
		return 1;
	}
		
  
  	return 0;
}

void kseq_count_zwrite( kseq_t *seq, gzFile fp) {
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

void kseq_count_write( kseq_t *seq, FILE *fp) {
	if ( ! seq->qual.l ) {
		// assume fasta
		if ( seq->comment.l ) {
			fprintf(fp, ">%s %s\n", seq->name.s, seq->comment.s);
		}
		else {
			fprintf(fp, ">%s\n", seq->name.s);
		}
		fprintf(fp, "%s\n", seq->seq.s);
	}
	else {
		// assume fastq		
        	if ( seq->comment.l ) {
			fprintf(fp, "@%s %s\n", seq->name.s, seq->comment.s);
                }
		else {
			fprintf(fp, "@%s\n", seq->name.s);
		}
		fprintf(fp, "%s\n+\n%s\n", seq->seq.s, seq->qual.s);
	}

	return;
}


double get_filesize(char *filename) {
	struct stat stbuf;
	stat(filename, &stbuf);
	return (double) stbuf.st_size;
}

void get_tempfile(t_kseqcount_tempfile *tempfile, int compression_type) {
	// set up either a compressed or standard temp filestream
	int fp;
        gzFile zp;

        char *template = "/tmp/kseq_countXXXXXX";
	tempfile->temp_filename = (char *) malloc(1+strlen(template));
        tempfile->temp_filename = strcpy(tempfile->temp_filename, template);
   	fp=mkstemp(tempfile->temp_filename);

	if(compression_type != -1) {
		tempfile->ztemp_file=gzdopen(fp,"w");
	}
	else{
		tempfile->temp_file=fdopen(fp,"w");
	}
	return;
}

int get_compression_type(char *filename) {
	// ref https://stackoverflow.com/questions/19120676/how-to-detect-type-of-compression-used-on-the-file-if-no-file-extension-is-spe
	//const char 
	//Zip (.zip) format description, starts with 0x50, 0x4b, 0x03, 0x04 (unless empty — then the last two are 0x05, 0x06 or 0x06, 0x06)
	//Gzip (.gz) format description, starts with 0x1f, 0x8b, 0x08
	//xz (.xz) format description, starts with 0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00

	//zlib (.zz) format description, starts with two bytes (in bits) 0aaa1000 bbbccccc, where ccccc is chosen so that the 
        // first byte viewed as a int16 times 256 plus the second byte viewed as a int16 is a multiple of 31. 
	// e.g: 01111000(bits) = 120(int16), 10011100(bits) = 156(int16), 120 * 256 + 156 = 30876 which is a multiple of 31
	//compress (.Z) starts with 0x1f, 0x9d
	//bzip2 (.bz2) starts with 0x42, 0x5a, 0x68

	// note there are big-endian /little-endian variations to the above - e.g. from the wild : 
        //illustrious$ od -x /dataset/hiseq/scratch/postprocessing/180222_D00390_0347_ACC8WAANXX.processed/bcl2fastq/SQ0634/SQ0634_S8_L008_R1_001.fastq.gz | head
        //0000000 8b1f 0408 0000 0000 ff00 0006 4342 0002


        const unsigned char gzip[] = { 0x1f, 0x8b,'\0' };
        const unsigned char gzip_little_endian[] = { 0x8b,0x1f, '\0' };
	const unsigned char zip[] = { 0x50, 0x4b, 0x03, 0x04 ,'\0'};
	const unsigned char zip_empty_a[] = { 0x50, 0x4b, 0x05, 0x06, '\0'};
	const unsigned char zip_empty_b[] = { 0x50, 0x4b, 0x06, 0x06, '\0'};
	const unsigned char compress[] = { 0x1f, 0x9d, '\0'};
	const unsigned char bzip2[] = { 0x42, 0x5a, 0x68, '\0'};
	const unsigned char xz[] = {  0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00, '\0' };


        const unsigned char* fingerprints[] = { gzip , gzip_little_endian ,zip, zip_empty_a, zip_empty_b, compress, bzip2, xz};

        const int finger_sizes[] = { sizeof(gzip)/sizeof(gzip[0])-1, sizeof(gzip_little_endian)/sizeof(gzip_little_endian[0])-1, \
                                     sizeof(zip)/sizeof(zip[0])-1, sizeof(zip_empty_a)/sizeof(zip_empty_a[0])-1,\
                                     sizeof(zip_empty_b)/sizeof(zip_empty_b[0])-1, sizeof(compress)/sizeof(compress[0])-1,\
                                     sizeof(bzip2)/sizeof(bzip2[0])-1, sizeof(xz)/sizeof(xz[0])-1 };
	int num_fingerprints;
	const int BUF_SIZE=7;
	unsigned char fingerprint_buffer[BUF_SIZE];
	
	FILE *instream;
	int num_read=0; 
	int i;

        int compression_type = -1;

        num_fingerprints = sizeof(fingerprints)/ sizeof(fingerprints[0]);

  
	instream = fopen(filename,"rb");
	num_read = fread(fingerprint_buffer, sizeof(fingerprint_buffer[0]), BUF_SIZE, instream);
	fclose(instream);

	if(DEBUG) {
		printf("buf length=%d\nsize=%d\nnum read=%d\n%02x\n%02x\n%02x\n\n\n", BUF_SIZE, sizeof(fingerprint_buffer[0]), num_read,
                      (unsigned int) fingerprint_buffer[0], (unsigned int) fingerprint_buffer[1], (unsigned int) fingerprint_buffer[2] );
	}


	for(i=0; i< num_fingerprints; i++) {
		if (DEBUG) {
                	printf("window: %d\nbuffer: \n%02x\n%02x\n%02x\n fingerprint: \n%02x\n%02x\n", finger_sizes[i],\
                      	(unsigned int) fingerprint_buffer[0], (unsigned int) fingerprint_buffer[1], (unsigned int) fingerprint_buffer[2],\
                       	fingerprints[i][0],fingerprints[i][1] );
		}

           
	   	if(strncmp( fingerprint_buffer, fingerprints[i], finger_sizes[i]) == 0) { 
	      		compression_type = i;
	      		break;
	   	}
        }
	return(compression_type); 

}



int estimate_count(int preview_record_count, int compression_type, double file_size , double preview_size) {
	double empirical_adjustment = 1.0;
	double raw_estimate;
	double x;

	raw_estimate = preview_record_count * file_size / preview_size ; 

	if ( file_size < 100000000 ) {
		return raw_estimate;
	}

	if (compression_type < 0 ) {
		return raw_estimate; 
	}
	else {
		x = file_size / 1000000000;

		empirical_adjustment = .001 * pow(x,3) - .0256 * pow(x,2) + .1717 * x + 0.6475;

		if (DEBUG) {
			printf("empirical adj = %f\n", empirical_adjustment );
		}

		if ( empirical_adjustment > 1.0 ) {
			empirical_adjustment = 1.0;
		}
		else if (empirical_adjustment < 0.8 ) {
			empirical_adjustment = 0.8;
		}
	
		return ( int ) ( 0.5 + empirical_adjustment * raw_estimate ) ;
	}
}



int main(int argc, char *argv[])
{
	gzFile fp,zp;
	kseq_t *seq;
	int l;
        int record_count=0;
        int kseqcount_opts_result = 0;
	enum compression_types { gzip , gzip2, gzip3, zip, zip_empty_a, zip_empty_b, compress, bzip2, xz };
	int compression_type = -1;

        const int sample_size = 20000;

	t_kseqcount_opts kseqcount_opts;
	t_kseqcount_tempfile tempfile;
        
        kseqcount_opts_result = get_kseqcount_opts(argc, argv, &kseqcount_opts);
 	if ( kseqcount_opts_result != 0 ){
		// usage message on help is not an error
		return kseqcount_opts_result == 2 ? 0 : kseqcount_opts_result;
 	} 

	// check compression type of input file. Note that we only need this for the approximate 
	// option, in order to decide whether to write out a compressed or uncompressed 
	// preview file. (zlib can handle either compressed or uncompressed *input* transparently)
        compression_type = get_compression_type( kseqcount_opts.input_filename) ;

	if(DEBUG) 
		printf("DEBUG : compression type = %d\n",compression_type);
	

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
		// ( - we need to know whether to write compressed or uncompressed records)
		get_tempfile(&tempfile, compression_type);
		seq = kseq_init(fp);
		while((l = kseq_read(seq)) >= 0) {
			if(compression_type != -1 ) { 
				kseq_count_zwrite( seq, tempfile.ztemp_file);
			}
			else {
				kseq_count_write( seq, tempfile.temp_file);
			}
			
			record_count++;
			if(record_count == sample_size) {
				break;
			}
		}
		//kseq_read(seq); 


		//gzprintf(tempfile.temp_file, "Hello World\n");
		
		if(compression_type != -1) {
			gzclose(tempfile.ztemp_file);	
		}
		else {
			fclose(tempfile.temp_file);
		}
		record_count = estimate_count(record_count, compression_type, get_filesize( kseqcount_opts.input_filename ),  get_filesize(tempfile.temp_filename) );
		//record_count = 0;

	} // approximating

	// clean up and return
	kseq_destroy(seq);
	gzclose(fp);
	free(tempfile.temp_filename);
        printf ("%d\n", record_count);

	return 0;
}
