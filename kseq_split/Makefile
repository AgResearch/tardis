all: kseq_test kseq_split

LDLIBS += -lz

kseq_test: kseq.h kseq_test.c

kseq_split: kseq.h kseq_split.c

kseq_count: kseq.h kseq_count.c
	$(CC) $^ $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LDLIBS) -lm $(TARGET_ARCH) -o $@

clean:
		rm -f *.o kseq_test kseq_split kseq_count
