import re, os , time, subprocess, sys, itertools, gzip

global MAX_DIMENSION
MAX_DIMENSION = 999999 # the maximum number of chunks we will allow  - prevent incoherent chunking options generating huge numbers of fragments


import tardis.conditioner.data as data
import tardis.tutils.tutils as tutils



class textDataConditioner(data.dataConditioner):
    my_directives = ["_condition_text_input_(\S+)", "_condition_compressedtext_input_(\S+)",  "_condition_text_output_(\S+)",\
                     "_condition_uncompressedtext_output_(\S+)"    , "_condition_text_product_(\S+)", \
                     "_condition_uncompressedtext_product_(\S+)" ]                     
    data.dataConditioner.all_directives = data.dataConditioner.all_directives + my_directives
    data.dataConditioner.published_directives = data.dataConditioner.published_directives + my_directives
    (input_directive_pattern, compressedinput_directive_pattern,  output_directive_pattern, uncompressedoutput_directive_pattern,\
     product_directive_pattern, uncompressedproduct_directive_pattern) = my_directives
     
    inFormat="text"
    outFormat="text"
    pairBond = lambda cls, x, y : True  # a function which is passed records from paired-up files that should match in some way - return
                                # True if they match in that way. The default (for matching generic text files) is that
                                # records should always match - i.e. no checking is done. Subclasses such as
                                # the fastq data conditioner redefine this so as to extract and compare the part of the names of paired-end
                                # reads that should match in some way
                                # (the cls arg is needed because this lambda gets handled as a bound method and so expects a cls first arg)
                                
    # symbolic names for file compression types 
    GZIP=0
    ZIP=1
    NO_COMPRESSION = 2


    def __init__(self,inputFileName = None, outputFileName = None, commandConditioning = True, isPaired = False, \
                 conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        super(textDataConditioner, self).__init__(inputFileName = inputFileName, outputFileName = outputFileName, commandConditioning = commandConditioning , \
                                                  isPaired = isPaired, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)        
        self.compressionConditioning = compressionConditioning


    @classmethod
    def getFileCompressionType(cls, filename):
        match = re.search("(.*)\.gz$", filename)
        if match != None:
            return cls.GZIP

        match = re.search("(.*)\.zip$", filename)
        if match != None:
            return cls.ZIP

        return cls.NO_COMPRESSION

    @classmethod
    def getUncompressedFilestream(cls, filename):
        """
        work out the compression, and return a tuple consisting of
        a file over uncompressed data, and the uncompressed filename
        """
        if cls.getFileCompressionType(filename) == cls.GZIP:
            return ( gzip.open(filename, 'rb') , cls.getUncompressedBaseName(filename))
        elif cls.getFileCompressionType(filename) == cls.ZIP:
            return ( zipfile.ZipFile(filename, "r"), cls.getUncompressedBaseName(filename))
        else:
            return (open(filename,"r"), filename)

    @classmethod
    def getLogicalRecordCount(cls, arg_filename):
        """
        get an approximate logical record count for a file 
        """
        filenames = [arg_filename]
        if cls.isListFile(arg_filename):
            with open(arg_filename, "r") as file_list:
                filenames = [record.strip()  for record in file_list]

        record_count = 0
        for filename in filenames: 
            if cls.getFileCompressionType(filename) == cls.GZIP:
                f = gzip.open(filename, 'rb')
                record_count += reduce(lambda x,y:1+x, f, 0)
                f.close()
            elif cls.getFileCompressionType(filename) == cls.ZIP:
                f = zipfile.ZipFile(filename, "r")
                record_count += reduce(lambda x,y:1+x, f, 0)
                f.close()
            else:
                with open(filename,"r") as f:
                    record_count += reduce(lambda x,y:1+x, f, 0)

        return record_count
        

    @classmethod
    def getFileCompressionCommand(cls, filename):
        return ["gzip", filename]


    @classmethod
    def getUncompressedBaseName(cls, filename):
        if cls.getFileCompressionType(filename) == cls.GZIP:
            return os.path.basename(re.search("(.*)\.gz$", filename).groups()[0])
        elif cls.getFileCompressionType(filename) == cls.ZIP:
            return os.path.basename(re.search("(.*)\.zip$", filename).groups()[0])
        else:
            return filename        


    @classmethod
    def addInputConditionerDEPRECATED(cls, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=textDataConditioner(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord , compressionConditioning = compressionConditioning)
        dataConditioner.inputConditioners.append(dc)
        return dc

    @classmethod
    def addInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord , compressionConditioning = compressionConditioning)
        prototype.inputConditioners.append(dc)
        prototype.induct(dc)
        return dc

    @classmethod
    def addOutputUnconditioner(cls, prototype, outputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(outputFileName = outputFileName, commandConditioning = commandConditioning, \
                               conditioningPattern = conditioningPattern, conditioningWord = conditioningWord ,compressionConditioning = compressionConditioning)
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc

    def getConditionedInputGenerator(self):
        """
        This method returns a generator of conditioned input, based on the input conditioners that have
        been created. If paired input files have been requested, the generator does strictly paired conditioning
        - i.e. chunks are generated in matching pairs, and an exception is raised it non-matching sequence names are
        encountered.

        Input conditioners are distinct, if they refer to different input files. If they refer to the same
        input file, then they are not regarded as distinct. Two input conditioners that refer to the
        same input file will received the same conditioned input.

        "list files" are supported. A list file contains a list of filenames. The content of all of the listed files is
        treated as a single stream from the point of view of conditioning. An input is recognised as a list file if
        it has the suffix "list", and if each non-trivial record in the file contains the name of a file that
        exists on the system. This handles the common use-case of a number of (often compressed) files that need to be
        processed essentially as a single stream, and avoids alot of I/O and scratch space spent processing them either
        singly, or else joining them together
        """

        igList = []

        listProcessing = False

        # check to see whether we have a list file (or files)
        if self.isPaired:
            if self.isListFile(self.inputFileName) and self.isListFile(self.pairPartner.inputFileName):
                listProcessing = True
        else:
            if self.isListFile(self.inputFileName):
                listProcessing = True


        self.logWriter.info("(listProcessing = %s)"%listProcessing)


        # check to see whether record length bounds are defined. 
        lengthBounds  = ( self.options.get("seqlength_min", None), self.options.get("seqlength_max", None) )
        self.logWriter.info("(length_bounds = %s)"%str(lengthBounds))

        # warn if from_record or to_record are specified and we are list processing
        if (self.options["from_record"] is not None or self.options["to_record"] is not None ) and listProcessing:
            self.logWriter.info("warning - input slicing request ignored as we are processing a listfile")

        # if chunksize is -1, estimate it from the record count of the file
        if self.options["chunksize"] < 0:
            self.logWriter.info("calculating chunk size to yield %d tasks from %s"%(self.options["max_tasks"], self.inputFileName))
            record_count = self.getLogicalRecordCount(self.inputFileName)
            self.options["chunksize"] = max(1, int( .5 + record_count / float( self.options["max_tasks"] )))
            if self.options["samplerate"] is not None:

                # adjust sample rate upwards if it looks like will obtain less than min_sample_size records
                if self.options["min_sample_size"] is not None:
                    if float(record_count) * self.options["samplerate"] < self.options["min_sample_size"]:
                        samplerate0 = self.options["samplerate"]
                        self.options["samplerate"] = min(1.0, float(self.options["min_sample_size"]) / float(record_count))
                        self.logWriter.info("text data conditioner is adjusting the sample rate up from %f to %f to meet minimum sample size parameter of %d"%( samplerate0,\
                                                                                 self.options["samplerate"], self.options["min_sample_size"]))               
                self.options["chunksize"] = max(self.options["chunksize"], int(0.5 + 1.0/self.options["samplerate"]))
                self.logWriter.info("(chunksize allow for sampling rate)")                
            self.logWriter.info("using chunk size of %d"%self.options["chunksize"])
            
                              
        if not listProcessing:
            if self.isPaired:
                #print "DEBUG : paired"
                if self.pairMaster:
                    #print "DEBUG : master"
                    igList.append( getConditionedFilenames(self,self.inputFileName, self.options["chunksize"], self.workingRoot, informat = self.inFormat, outformat = self.outFormat, \
                                                    samplerate = self.options["samplerate"] ,filename2 = self.pairPartner.inputFileName,\
                                                           pairBond = self.pairBond, length_bounds = lengthBounds ,\
                                                           record_filter_func=self.options.get("record_filter_func",None),\
                                                           from_record=self.options["from_record"], to_record=self.options["to_record"]))
            else:        
                igList.append( getConditionedFilenames(self,self.inputFileName, self.options["chunksize"], self.workingRoot, informat = self.inFormat, outformat = self.outFormat, \
                                                samplerate = self.options["samplerate"], length_bounds = lengthBounds, record_filter_func=self.options.get("record_filter_func",None),\
                                                       from_record=self.options["from_record"], to_record=self.options["to_record"]))
        else:
            if self.isPaired:
                if self.pairMaster:
                    for (listedpath1,listedpath2) in itertools.izip((self.getListedFilePath(record, self.inputFileName) for record in open(self.inputFileName,"r")) , \
                                                                    (self.getListedFilePath(record, self.pairPartner.inputFileName) for record in open(self.pairPartner.inputFileName,"r"))):
                        #print "DEBUG : %s %s"%(listedpath1,listedpath2)
                        igList.append(getConditionedFilenames(self,listedpath1, self.options["chunksize"], self.workingRoot, informat = self.inFormat, outformat = self.outFormat, \
                                                    samplerate = self.options["samplerate"] ,filename2 = listedpath2,  pairBond = self.pairBond,\
                                                              listfilename1 = self.inputFileName, listfilename2 = self.pairPartner.inputFileName, length_bounds = lengthBounds,\
                                                              record_filter_func=self.options.get("record_filter_func",None)))
            else:
                for listedpath in (self.getListedFilePath(record, self.inputFileName) for record in open(self.inputFileName,"r")):
                    igList.append (getConditionedFilenames(self,listedpath, self.options["chunksize"], self.workingRoot, informat = self.inFormat, outformat = self.outFormat, \
                                                samplerate = self.options["samplerate"], listfilename1 = self.inputFileName, length_bounds = lengthBounds,\
                                                           record_filter_func=self.options.get("record_filter_func",None)))

        # if we have more than one generator, chain them together
        if len(igList) == 0:
            ig = None
        elif len(igList) == 1:
            ig = igList[0]
        else:
            self.logWriter.info("chaining together %d conditioned input generators"%len(igList))
            ig = itertools.chain(*igList)

        
        return ig

                    

    def nextConditionedInputWord(self):
        """
        return the next conditioned input word
        """
        word = ""

        if self.commandConditioning:
        
            if self.options["chunksize"] == 0 and (self.getFileCompressionType(self.inputFileName) == self.NO_COMPRESSION or not self.compressionConditioning) and  self.options["samplerate"] is None:        
                self.logWriter.info("textDataConditioner.nextConditionedInputWord - chunksize 0, do-not-do-compression-conditioning was requested, no sampling, doing generic conditioning")
                word = super(textDataConditioner, self).nextConditionedInputWord(self.conditioningWord)                        
            else:
                self.logWriter.info("DEBUG : %s is splicing %s into %s using pattern %s"%(self, self.conditionedInputFileNames[-1], self.conditioningWord, self.conditioningPattern))                    
                word = re.sub(self.conditioningPattern, self.conditionedInputFileNames[-1], self.conditioningWord) 

        return word
        

    def unconditionOutput(self):
        """
        this class method coordinates unconditioning - i.e. "joining back together"
        output text files (many files can be handled simply as text files - e.g.
        fasta, fastq etc)
        """
        filesToProcess = self.getExpectedUnconditionedOutputFiles()
        if len(filesToProcess) == 0:
            self.logWriter.info("unconditionOutput : no files to uncondition")
            return 
            
        self.logWriter.info("""
        textDataConditioner.unconditionOutput : unconditioning the following conditioned files
        %s
        to
        %s"""%(str(filesToProcess), self.outputFileName))

        fileout = open( self.outputFileName, "w" )
        for file_to_process in filesToProcess:
            with open(file_to_process,"r") as in_stream:
                fileout.writelines(in_stream)
        if fileout != None:
            fileout.close()
            
        if self.compressionConditioning:
            compressionCommand = self.getFileCompressionCommand(self.outputFileName)
            self.logWriter.info("executing %s"%str(compressionCommand))
            proc = subprocess.Popen(compressionCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = proc.communicate()
            self.logWriter.info("textDataConditioner : file compression returned  ( return code %s ) - here is its output "%proc.returncode)
            self.logWriter.info("stdout : \n%s"%stdout)
            self.logWriter.info("stderr : \n%s"%stderr)

            if proc.returncode != 0:
                self.error("compression of concatenated files appears to have failed - setting error state")
            
    

        return




def getConditionedFilenames(caller, filename1, argchunksize, outdir, informat = "text", outformat = "text", samplerate = None ,filename2=None,  pairBond = None,\
                            listfilename1 = None, listfilename2 = None, length_bounds = (None,None), record_filter_func=None, from_record = None, to_record = None):
    """
    return a generator of filenames. 
    """
    
    if caller.options["fast_sequence_input_conditioning"] and informat in ("fastq", "fasta"):
        if record_filter_func is None and from_record is None and to_record is None and length_bounds == (None,None) and \
                    not(filename2 is not None and samplerate is not None)  :
            caller.logWriter.info("*** getConditionedFilenames: using fast input conditioning. Note: pairBond ignored***")
            return _fast_get_conditioned_filenames(caller, filename1, argchunksize, outdir, informat, outformat, samplerate,filename2,\
                            listfilename1, listfilename2, length_bounds, from_record, to_record)
        else:
            caller.logWriter.info("""
*** getConditionedFilenames: fast input conditioning requested but either record_filter_func, from_record, to_record or length_bounds specified, or paired sampling requested, so using standard conditioning ***
""")
            return _slow_get_conditioned_filenames(caller, filename1, argchunksize, outdir, informat, outformat, samplerate,filename2,  pairBond,\
                            listfilename1, listfilename2, length_bounds, record_filter_func, from_record, to_record)
    

    return _slow_get_conditioned_filenames(caller, filename1, argchunksize, outdir, informat, outformat, samplerate,filename2,  pairBond,\
                            listfilename1, listfilename2, length_bounds, record_filter_func, from_record, to_record)
                 
        
def _fast_get_conditioned_filenames(caller, filename1, argchunksize, outdir, informat, outformat, samplerate,filename2,\
                            listfilename1 = None, listfilename2 = None, length_bounds = (None, None) , from_record = None, to_record = None):
    """
    A generator.

    See also below, _slow_get_conditioned_filenames. This was the original version. Cloned and hacked to
    make this version. 

    This method forks and calls one or subprocesses to do the actual split - the parent
    polls for the split files.
    
    This means that e.g. record_filter_func is not supported as this method does not have access to sequence
    objects.
    
    Other record oriented filters (e.g. by length , from - to, samplerate) are planned to be supported but are not yet
    """    
    
    #named indexes
    LOWER=0
    UPPER=1

    # if chunksize zero yield empty chunknames and stop
    if argchunksize == 0:
        yield ((filename1, filename2), (None, None))
        raise StopIteration

    caller.logWriter.info("_fast_get_conditioned_filenames : conditioning %s to %s chunksize %d informat %s outformat %s samplerate %s from %s to %s file2 %s"%(filename1, outdir, \
                                                                                            argchunksize , informat, outformat, samplerate, from_record, to_record , filename2))  

    chunknames1 = []
    chunknames2 = []

    # (we don't adjust chunksize if we are sampling as kseq does it)
    chunksize = argchunksize

    # set various filenames that will be needed
    uncompressedName1 = textDataConditioner.getUncompressedBaseName(filename1)
    batonfile1 = os.path.join(outdir, "%s.chunk_stats$"%os.path.basename(uncompressedName1))
    chunkbase1 = os.path.basename(uncompressedName1)
    name_parts = os.path.splitext(chunkbase1)
    chunktemplate1 =  os.path.join(outdir, name_parts[0] + ".%05d" + name_parts[1])

    uncompressedName2 = None
    batonfile2 = None
    chunkbase2 = None
    chunktemplate2 = None
    if filename2 is not  None:
        uncompressedName2 = textDataConditioner.getUncompressedBaseName(filename2)
        batonfile2 = os.path.join(outdir, "%s.chunk_stats$"%os.path.basename(uncompressedName2))
        chunkbase2 = os.path.basename(uncompressedName2)
        name_parts = os.path.splitext(chunkbase2)
        chunktemplate2 =  os.path.join(outdir, name_parts[0] + ".%05d" + name_parts[1])

    split_logfile = os.path.join(outdir, "split_processing.log")

    # fork a process to kick off split of file 1
    try:
        split_command = ["kseq_split", "-f" ,  batonfile1, "-o", outformat, filename1, str(chunksize), chunktemplate1]
        if samplerate is not None:
            split_command = ["kseq_split", "-f" ,  batonfile1, "-o", outformat, "-s" , str(samplerate), filename1, str(chunksize), chunktemplate1]
            
            
        caller.logWriter.info("fast input conditioner forking split process : %s"%" ".join(split_command))
        me = os.fork()
        if me == 0:   # child
            mypid = os.getpid()

            # kick off the splitting process and wait for it to finish
            proc = subprocess.Popen(split_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = proc.communicate()

            with open(split_logfile,"a") as l:
                print >> l, "child process %d started split subprocess %d"%(mypid, proc.pid) 
                print >> l, "split subprocess stdout : \n%s"%stdout
                print >> l, "split subprocess stderr : \n%s"%stderr
                print >> l, "split subprocess %d terminated with return value %d"%(proc.pid, proc.returncode)
                print >> l, "child process %d exiting with status %s"%(mypid, proc.returncode)
            sys.exit(proc.returncode)
    except OSError,e:
        caller.logWriter.info("fast input conditioner : error - fork of %s failed with OSError : %s"%(" ".join(split_command), e))
        raise tutils.tardisException("fast input conditioner : error - fork of %s failed with OSError : %s"%(" ".join(split_command), e))

    # parent
    if filename2 != None:
        try:
            split_command = ["kseq_split", "-f" , batonfile2, "-o", outformat, filename2, str(chunksize), chunktemplate2 ]
            if samplerate is not None:
                raise tutils.tardisException("this input conditioner does not support paired random sampling ! - should not be executing this code block !?")
            
            caller.logWriter.info("fast input conditioner forking split process : %s"%" ".join(split_command))
            me = os.fork()
            if me == 0:   # child
                mypid = os.getpid()

                # kick off the splitting process and wait for it to finish
                proc = subprocess.Popen(split_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (stdout, stderr) = proc.communicate()

                with open(split_logfile,"a") as l:
                    print >> l, "child process %d started split subprocess %d"%(mypid, proc.pid) 
                    print >> l, "split subprocess stdout : \n%s"%stdout
                    print >> l, "split subprocess stderr : \n%s"%stderr
                    print >> l, "split subprocess %d terminated with return value %d"%(proc.pid, proc.returncode)
                    print >> l, "child process %d exiting with status %s"%(mypid, proc.returncode)
                sys.exit(proc.returncode)
        except OSError,e:
            caller.logWriter.info("fast input conditioner : error - fork of %s failed with OSError : %s"%(" ".join(split_command), e))
            raise tutils.tardisException("fast input conditioner : error - fork of %s failed with OSError : %s"%(" ".join(split_command), e))

    # parent

    # we have kicked off subprocesses to (asynchronously) split the input files.
    # now poll for the chunk files they generate. We use the following embedded method

    
    # this method polls for each chunk being generated by the child processes
    def advance_chunk(caller, chunk, total_chunks_in, batonfile1, batonfile2,chunkbase1, chunkbase2, outdir, chunk_info):
        # if total_chunks has been set, and we are being asked
        # for a chunk number greater than this, raise StopIteration
        total_chunks = total_chunks_in
        if total_chunks is not None:
            if chunk > total_chunks:
                raise StopIteration
        
        
        # in a wait loop, poll
        wait_duration=0
        exception_count = 0
        while True:
            # if we find either baton file , try reading the total number of chunks from it if we haven't already obtained this
            if total_chunks is None:
                if os.path.isfile(batonfile1):
                    try:
                        with open(batonfile1,"r") as bf:
                            for record in bf:
                                total_chunks = int(re.split("=", record.strip())[1])
                                caller.logWriter.info("%d chunks in total were written (according to %s)"%(total_chunks, batonfile1))
                                break
                    except Exception, e :
                        # this could happen if we try to read the baton file at the same time as
                        # the chunk-writer writes it. No action needed - will get it
                        # next pass. But if we fail more than 50 times give up.
                        caller.logWriter.info("warning - exception (%s) reading batonfile %s"%(str(e), batonfile1))
                        caller.logWriter.info("error - too many failed attempts to parse batonfile %s - giving up"%batonfile1)
                        exception_count += 1
                        if exception_count >= 50:
                            raise tutils.tardisException("error - too many failed attempts to parse batonfile %s - giving up"%batonfile1)
                        
                        

            if total_chunks is None and batonfile2 is not None:
                if os.path.isfile(batonfile2):
                    try:
                        with open(batonfile2,"r") as bf:
                            for record in bf:
                                total_chunks = int(re.split("=", record.strip())[1])
                                caller.logWriter.info("%d chunks in total were written (according to %s)"%(total_chunks, batonfile2))
                                break
                    except Exception, e :
                        # this could happen if we try to read the baton file at the same time as
                        # the chunk-writer writes it. No action needed - will get it
                        # next pass. But if we fail more than 50 times give up.
                        caller.logWriter.info("warning - exception (%s) reading batonfile %s"%(str(e), batonfile1))
                        caller.logWriter.info("error - too many failed attempts to parse batonfile %s - giving up"%batonfile1)
                        exception_count += 1
                        if exception_count >= 50:
                            raise tutils.tardisException("error - too many failed attempts to parse batonfile %s - giving up"%batonfile1)

            # if we just obtained number of chunks, and we are being asked for a chunk more than this, raise StopIteration
            if total_chunks is not None:
                if chunk > total_chunks:
                    raise StopIteration
                                              

            # test existence of chunkfilename1 (and chunkfilename2 if applicable), corresponding to chunk
            # note that it is possible we are testing for a chunkfile past the last one , if we haven't yet
            # picked up total_chunks - however this will be picked up in a subsequent pass
            if chunk_info[0] is None:
                name_parts = os.path.splitext(chunkbase1)
                chunkfilename1 =  os.path.join(outdir, "%s.%05d%s"%(name_parts[0], chunk, name_parts[1]))
                if os.path.isfile(chunkfilename1):
                    chunk_info[0] = chunkfilename1
                    
            if filename2 is not None and chunk_info[1] is None:
                name_parts = os.path.splitext(chunkbase2)
                chunkfilename2 =  os.path.join(outdir, "%s.%05d%s"%(name_parts[0], chunk, name_parts[1]))
                if os.path.isfile(chunkfilename2):
                    chunk_info[1] = chunkfilename2
                
            # if we are done (i.e. have the chunk filenames we need), break
            if filename2 is None and chunk_info[0] is not None:
                return total_chunks
            elif chunk_info[0] is not None and chunk_info[1] is not None:
                return total_chunks

            # brief sleep
            time.sleep(caller.POLL_INTERVAL)
            wait_duration += caller.POLL_INTERVAL
            if wait_duration > 60 * 60:   # one hour
                caller.logWriter.info("warning - polling for chunk %d is taking a long time ! "%chunk)
            elif wait_duration > caller.POLL_DURATION:
                caller.logWriter.info("error - polling for chunk %d has exceeded POLL_DURATION (24 hours) ! (not exiting but this doesn't look good)"%chunk)
                
    # end of advance_chunk method
    
    # loop getting chunks
    chunk = 1
    chunksYieldedCount = 0
    chunk_info = [None, None]
    total_chunks = None
    while True:

        total_chunks = advance_chunk(caller, chunk, total_chunks, batonfile1, batonfile2,chunkbase1, chunkbase2, outdir, chunk_info)
        chunk += 1
        
        if listfilename1 is not None and listfilename2 is not None:
            yield ((listfilename1, listfilename2),chunk_info)
        elif listfilename1 is not None:
            yield ((listfilename1, filename2),chunk_info)
        elif listfilename2 is not None:
            yield ((filename1, listfilename2),chunk_info)
        else:
            yield ((filename1, filename2),chunk_info)

        chunksYieldedCount += 1
            
        if chunksYieldedCount > MAX_DIMENSION:
            #raise tardisException("error - too many chunks - please adjust chunksize to yield no more than %d chunks"%MAX_DIMENSION)
            caller.error("error - too many chunks - please adjust chunksize to yield no more than %d chunks"%MAX_DIMENSION)
            raise tutils.tardisException("error - too many chunks - please adjust chunksize to yield no more than %d chunks"%MAX_DIMENSION)

        chunk_info = [None, None]
                    

def _slow_get_conditioned_filenames(caller, filename1, argchunksize, outdir, informat = "text", outformat = "text", samplerate = None ,filename2=None,  pairBond = None,\
                            listfilename1 = None, listfilename2 = None, length_bounds = (None,None), record_filter_func=None, from_record = None, to_record = None):
    """
    A generator.
    Split up a generic or structured text file, and return fragments as they become available, via yield.
    Structures supported are fasta, fastq (and text). Based on an original stand-alone
    script "slice_fastq.py" - updated to support paired fastq files and brought "in-house" to tardis, and
    implemented as a generator so that we can get chunknames and launch jobs , as the chunks become
    available. 
    Returns a tuple : ((inputfilename1, inputfilename2), (fragmentname1, fragmentname2))
    (The input names are returned as well, so that consumers of this generator know which
    original name each fragment relates to)
    The first element of the sub-tuples contains original / fragment-filenames obtained by (optionally uncompressing and)
    splitting up the first file.
    The second element of the sub-tuple contains either None, if there was only one file to process, or corresponding fragment
    filenames obtained by splitting up the second file in synch with the first file.
    
    The pairBond argument is a function (usually a lambda), which is applied to each pair of records
    from filename1 and filename2 , when processing two files. It tests whether they are in synch. For example
    for paired fastq files, this function could be "lambda x,y: x.name == y.name". The function should return True
    if a pair of records are in synch, or False if not. If the function returns False, then an exception is raised
    as this is unrecoverable - it indicates the pair of files are incompatible (e.g. - may indicate an upstream
    error in trimming of paired read files )
    """

    #named indexes
    LOWER=0
    UPPER=1



    # if chunksize zero yield empty chunknames and stop
    if argchunksize == 0:
        yield ((filename1, filename2), (None, None))
        raise StopIteration

    # some arg checks
    if filename2 is None and pairBond != None:
        caller.logWriter.info("getConditionedFilenames : warning pairBond function ignored, no second file")
        

    caller.logWriter.info("getConditionedFilenames : conditioning %s to %s chunksize %d informat %s outformat %s samplerate %s from %s to %s file2 %s"%(filename1, outdir, \
                                                                                            argchunksize , informat, outformat, samplerate, from_record, to_record , filename2))  

    chunknames1 = []
    chunknames2 = []

    # adjust chunksize if we are sampling
    chunksize = argchunksize
    if samplerate != None:
        chunksize = int(.5 + samplerate * argchunksize)
        if argchunksize > 0 and chunksize == 0:
            caller.error("error - chunksize was rounded to zero after adjusting for sampling - please specify a chunksize which ignores your sampling rate (it will be adjusted later)")
            raise StopIteration
    
    # open infiles    
    (infile1, uncompressedName1) = textDataConditioner.getUncompressedFilestream(filename1)
    infile2 = None

    if filename2 != None:
        (infile2 , uncompressedName2) = textDataConditioner.getUncompressedFilestream(filename2)

    #if chunksize != 0:
    chunk = 1
    chunksYieldedCount = 0
        
    chunkname1 = os.path.basename(uncompressedName1)
    if filename2 != None:
        chunkname2 = os.path.basename(uncompressedName2)

    #print "DEBUG %s %s"%(uncompressedName1, chunkname1)
        
    # set up iterators over structured input records
    iter1 = None
    iter2 = None
    if informat in ("fastq", "fasta"):
        from Bio import SeqIO
        iter1 = SeqIO.parse(infile1, informat)
        if infile2 != None:
            iter2 = SeqIO.parse(infile2, informat)
    elif informat == "text":
        iter1 = infile1
        if infile2 != None:
            iter2 = infile2
    else:
        caller.error("unsupported input file format : %s"%informat)
        caller.logWriter.info("unsupported input file format : %s"%informat)
        raise StopIteration
        #raise tardisException("unsupported input file format : %s"%informat)

    # if we have a record filter, make iterators to apply this
    # ( currently only support a single filter - i.e. can't specify a different one for each pair)
    if record_filter_func is not None:
        caller.logWriter.info("inserting filter function")
        iter1 = (record_filter_func(unfiltered) for unfiltered in iter1)
        if iter2 is not None:
            iter2 = (record_filter_func(unfiltered) for unfiltered in iter2)

            
    # if there are two iterators zip them up to make an iterator over paired input. Else
    # make a paired iterator with the second iterator being a dummy repeat returning None
    piter = iter1
    if iter1 != None and iter2 != None:        
        piter = itertools.izip(iter1, iter2)
    else:
        piter = itertools.izip(iter1, itertools.repeat(None))
        
                     
    # output  ! 
    output_count = 0
    input_count = 0
    outfile1 = None
    outfile2 = None
    record1 = None
    record2 = None
    try:
        for (record1, record2) in piter :
            input_count += 1

            # will sample the output if needed
            sampleBool = tutils.getSampleBool(samplerate) # 1 or 0 (always 1 if not sampling)

            # will length-filter the output if needed.
            if length_bounds != (None, None):            
                for check_record in (record1,record2):
                    if check_record is not None:
                        if length_bounds[LOWER] is not None:
                            if len(check_record) < length_bounds[LOWER]:
                                sampleBool = 0
                        if length_bounds[UPPER] is not None:
                            if len(check_record) > length_bounds[UPPER]:
                                sampleBool = 0


            # will slice the file(s) if required
            if from_record is not None:
                if input_count < from_record:
                    sampleBool = 0

            if to_record is not None:
                if input_count > to_record:
                    sampleBool = 0
                    

            if sampleBool != 1:
                continue

            output_count += sampleBool 

            if chunksize > 0:
                mychunk = 1+int(output_count / (1.0*chunksize))
            else:
                mychunk = chunk


            # open a chunkfile if we need one
            if outfile1 is None:
                #outfilename1 =  os.path.join(outdir, "%s.%05d.%s"%(chunkname1, chunk, outformat))
                #outfilename1 =  os.path.join(outdir, "%s.%05d"%(chunkname1, chunk))
                name_parts = os.path.splitext(chunkname1)
                outfilename1 =  os.path.join(outdir, "%s.%05d%s"%(name_parts[0], chunk, name_parts[1]))
                
                #print "DEBUG : %s %s"%(outdir, outfilename1)
                if os.path.exists(outfilename1):
                    #raise tardisException("getConditionedFilenames : error - %s already exists"% outfilename1)
                    caller.error("getConditionedFilenames : error - %s already exists"% outfilename1)
                    caller.logWriter.info("the last sequences encountered before the error were : %s, %s"%(record1, record2))
                    raise StopIteration

                outfile1 = open(outfilename1, "w")
                chunknames1.append(outfilename1)
                
                
                if filename2 != None:
                    #outfilename2 =  os.path.join(outdir, "%s.%05d.%s"%(chunkname2, chunk, outformat))
                    #outfilename2 =  os.path.join(outdir, "%s.%05d"%(chunkname2, chunk))
                    name_parts = os.path.splitext(chunkname2)
                    outfilename2 =  os.path.join(outdir, "%s.%05d%s"%(name_parts[0], chunk, name_parts[1]))
                    if os.path.exists(outfilename2):
                        #raise tardisException("getConditionedFilenames : error - %s already exists"% outfilename2)
                        caller.error("getConditionedFilenames : error - %s already exists"% outfilename2)
                        caller.logWriter.info("the last sequences encountered before the error were : %s, %s"%(record1, record2))
                        raise StopIteration

                    outfile2 = open(outfilename2, "w")
                    chunknames2.append(outfilename2)
        

            # if two files, check pair-bonding and if OK output both records
            if outfile1 != None and outfile2 != None and pairBond != None:
                if not pairBond(record1 , record2):
                    #raise tardisException("pair bonding error - %s does not bond with %s"%(str(record1), str(record2)))
                    caller.error("pair bonding error - %s does not bond with %s"%(str(record1), str(record2)))
                    caller.logWriter.info("the last sequences encountered before the error were : %s, %s"%(record1, record2))
                    raise StopIteration

                
                if outformat in ("fasta","fastq"):
                    outfile1.write(record1.format(outformat))
                    outfile2.write(record2.format(outformat))
                else:
                    outfile1.write(record1)
                    outfile2.write(record2)
           
            elif outfile1 != None:                    
                if outformat in ("fasta","fastq"):
                    outfile1.write(record1.format(outformat))
                else:
                    outfile1.write(record1)


            # if need a new chunk, close and yield the old one (if there is one)
            if mychunk > chunk:
                chunkInfo = [None, None]
                if outfile1 != None:
                    outfile1.close()
                    outfile1 = None
                    chunkInfo[0] = outfilename1
                if outfile2 != None:
                    outfile2.close()
                    outfile2 = None
                    chunkInfo[1] = outfilename2

                if listfilename1 is not None and listfilename2 is not None:
                    yield ((listfilename1, listfilename2),chunkInfo)
                elif listfilename1 is not None:
                    yield ((listfilename1, filename2),chunkInfo)
                elif listfilename2 is not None:
                    yield ((filename1, listfilename2),chunkInfo)
                else:
                    yield ((filename1, filename2),chunkInfo)

                chunksYieldedCount += 1
                    
                #yield ((filename1, filename2),chunkInfo)
                
                chunk = mychunk

                if chunk > MAX_DIMENSION:
                    #raise tardisException("error - too many chunks - please adjust chunksize to yield no more than %d chunks"%MAX_DIMENSION)
                    caller.error("error - too many chunks - please adjust chunksize to yield no more than %d chunks"%MAX_DIMENSION)
                    caller.logWriter.info("the last sequences encountered before the error were : %s, %s"%(record1, record2))
                    raise tutils.tardisException("error - too many chunks - please adjust chunksize to yield no more than %d chunks"%MAX_DIMENSION)
                
    # handle exceptions that relate to problems with the data so we can report
    # where we are, then re-raise so we bail out.
    except ValueError, e:
        caller.error(e)
        caller.logWriter.info("the last sequences encountered before the error were : %s, %s"%(record1, record2))
        #
        #raise e
        raise StopIteration

                
    chunkInfo = [None, None]
    if outfile1 != None:
        outfile1.close()
        chunkInfo[0] = outfilename1
    if outfile2 != None:
        outfile2.close()
        chunkInfo[1] = outfilename2

    if chunkInfo != [None, None] :
        if listfilename1 is not None and listfilename2 is not None:
            yield ((listfilename1, listfilename2),chunkInfo)
        elif listfilename1 is not None:
            yield ((listfilename1, filename2),chunkInfo)
        elif listfilename2 is not None:
            yield ((filename1, listfilename2),chunkInfo)
        else:
            yield ((filename1, filename2),chunkInfo)
        chunksYieldedCount += 1

         
    if samplerate is None:
        caller.logWriter.info("*** getConditionedFilenames : processed %s of %s records ***\n"%(output_count, input_count))
    else:
        caller.logWriter.info("*** getConditionedFilenames : sampled %s of %s records***\n"%(output_count, input_count))

    caller.logWriter.info("*** getConditionedFilenames wrote %d chunks ***\n"%chunksYieldedCount)

    raise StopIteration
    


