all: kseq_test kseq_split


kseq_test:kseq.h kseq_test.c 
		$(CC) -g -O2 kseq_test.c -o kseq_test -lz
kseq_split:kseq.h kseq_split.c 
		$(CC) -g -O2 kseq_split.c -o kseq_split -lz

clean:
		rm -f *.o kseq_test kseq_split
