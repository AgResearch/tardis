all: kseq_test kseq_split kseq_count

LDLIBS += -lz

kseq_test: kseq_test.c kseq.h
	$(CC) $< $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LDLIBS) $(TARGET_ARCH) -o $@

kseq_split: kseq_split.c kseq.h
	$(CC) $< $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LDLIBS) $(TARGET_ARCH) -o $@

kseq_count: kseq_count.c kseq.h
	$(CC) $< $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LDLIBS) -lm $(TARGET_ARCH) -o $@

clean:
		rm -f *.o kseq_test kseq_split kseq_count
