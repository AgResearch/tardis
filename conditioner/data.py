import string, os, re, itertools

class dataConditioner(object):
    """
    This is the base-class for a number of application-specific data conditioners (
    e.g. fastqDataConditioner etc)

    Input conditioners split inputs and return an iterator over the splits

    Output conditioners join output fragments together

    Conditioners are created by a class method, which stores references to each conditioner
    created at the class level. This ensures that conditioner instances can be coordinated.

    """
    POLL_INTERVAL = 0.5 # seconds
    POLL_DURATION = 1 * 24 * 60 * 60 # one day

    OK = 0
    ERROR = 1
    state = OK
    stateDescription = ""

    my_directives = ["_condition_input_(\S+)","_condition_compressed_input_(\S+)", "_condition_throughput_(\S+)",\
                      "_condition_output_(\S+)", "_condition_product_(\S+)"]
    all_directives = my_directives
    published_directives = []
    
    (input_directive_pattern, compressedinput_directive_pattern, throughput_directive_pattern,\
         output_directive_pattern, product_directive_pattern) = my_directives

    @classmethod
    def stateAND(cls, state1, state2):
        """
        this method can be used to calculate the consensus state of a set of conditioner states 
        (without needing to know the encoding of the state)
        e.g.
        consensus = reduce(lambda x,y:dataConditioner.stateAND(x,y), [list of states])
        """
        if state1 == cls.OK and state2 == cls.OK:
            return cls.OK
        else:
            return cls.ERROR
    

                            
    def __init__(self, inputFileName = None, outputFileName = None, commandConditioning = True, isPaired = False, conditioningPattern = None, conditioningWord = None):
        super(dataConditioner, self).__init__()
        self.inputFileName = inputFileName
        self.outputFileName = outputFileName
        self.commandConditioning = commandConditioning
        self.isPaired = isPaired
        self.conditioningPattern = conditioningPattern
        self.conditioningWord = conditioningWord
        self.pairMaster = False
        self.pairPartner = None
        self.conditionedWordCount = 0
        self.expectedOutputCardinality = 0
        self.conditionedInputFileNames = []
        self.conditionedOutputFileNames =[]
        self.conditionedProductPatterns=[]
        self.conditionedOutputManifests = []    # this is not actually used by the unconditioning process
        self.conditionedProductManifests = []   # this *is* used by the unconditioning process
        self.outputCollector = self.conditionedOutputCollector() # this is a co-routine that is sent output by the hpcConditioner
        self.productCollector = self.conditionedProductCollector() # this is a co-routine that is sent product by the hpcConditioner
        self.outputCollector.send(None) # initialise the co-routine generators
        self.productCollector.send(None) 
        self.options = None # options from configuration 


        # the following lists of all conditioners (and the job controller) will be inherited via prototyping
        # (see the induct method) from a master conditioner that creates all of the
        # others. i.e. in all but the master, the following new arrays will be replaced
        # by the array of the parent via the induct method
        self.inputConditioners = []
        self.pairedInputConditioners = []
        self.outputUnconditioners = []
        self.throughputConditioners = []
        self.jobcontroller = None



    def induct(self,other):
        """
        this allows a data conditioner to spawn other data conditioners , using a add*Conditioner method,
        but without having to pass into the constructor a number of house-keeping arguments, such as the
        workingRoot, logger , and the arrays containing lists of all of the conditioners that
        all the conditioners ned to have. The "induct" method is called inside an add*Conditoner method,
        called by the parent, after the child  conditioner has been created and added to the
        parent's own conditioner list array
        - e.g. self.induct(other) - i.e. this method "inducts" the new conditioner into the
        community of conditioners. 
        (the add*Conditioner methods were formerly class methods - however this broke down
        when this module was imported by a client which then did repeated calls to
        handle a number of different jobs - at that point, all of the house keeping needs
        to be handled inside object instances, rather than in a class)
        """
        other.inputConditioners = self.inputConditioners
        other.pairedInputConditioners = self.pairedInputConditioners
        other.outputUnconditioners = self.outputUnconditioners
        other.throughputConditioners = self.throughputConditioners
        other.logWriter = self.logWriter
        other.workingRoot = self.workingRoot
        other.jobcontroller = self.jobcontroller
        other.options = self.options
        other.logWriter.info("dataConditioner created with inputFileName = %s outputFileName = %s isPaired = %s"%(other.inputFileName, other.outputFileName, other.isPaired))
        

    def error(self,errorMessage):
        self.logWriter.info("dataConditioner setting error state, message = %s"%errorMessage)
        self.state = dataConditioner.ERROR
        self.stateDescription += " *** error : %s *** "%errorMessage

    def warn(self,warnMessage):
        self.logWriter.info("dataConditioner warning , message = %s"%warnMessage)
        self.stateDescription += " *** warning : %s *** "%warnMessage

    def getDataResultState(self):
        
        return reduce(lambda x,y:self.stateAND(x,y), [dc.state for dc in self.getDistinctInputConditioners() +\
                                                                          self.outputUnconditioners + \
                                                                          self.throughputConditioners], dataConditioner.OK)

    def getDataResultStateDescription(self):
        
        return string.join([dc.stateDescription for dc in self.getDistinctInputConditioners() + \
                                                                          self.outputUnconditioners + \
                                                                          self.throughputConditioners if len(dc.stateDescription) > 0],';')
    
    
        
    def getInputConditionerByFile(self, inputFileName, includePairedConditioners = True):
        """
        given a filename, this method will look (optionally through just the single file input
        conditioners, or through both single and paired conditioners (default both)) for an existing
        input conditioner instance which was created for the same input filename, and if found will
        return the same instance. If not found, return None. If found more then one, return None
        """

        if includePairedConditioners :
            found_dcs = [ dc for dc in self.inputConditioners + reduce(lambda x,y : x+y, self.pairedInputConditioners,[]) if self.inputFileName == inputFileName]
        else:
            found_dcs = [ dc for dc in self.inputConditioners if dc.inputFileName == inputFileName]

        if len(found_dcs) == 0:
            self.logWriter.info("getInputConditionerByFile : found no existing data conditioners for %s"%inputFileName)
            return None
        elif len(found_dcs) >  1:
            self.logWriter.info("getInputConditionerByFile : found %s data conditioners for %s, returning the first one"%(len(found_dcs), inputFileName))
            return found_dcs[0]
        else:
            self.logWriter.info("getInputConditionerByFile : found one existing data conditioner for %s"%inputFileName)
            return found_dcs[0]

    def getPairedInputConditionerByFile(self, inputFileName):
        """
        given a filename, this method will look (optionally through just the single file input
        conditioners, or through both single and paired conditioners (default both)) for an existing
        input conditioner instance which was created for the same input filename, and if found will
        return the same instance. If not found, return None. If found more then one, return None
        """

        found_dcs = [ dc for dc  in reduce(lambda x,y : x+y, self.pairedInputConditioners,[]) if dc.inputFileName == inputFileName] 
        
        if len(found_dcs) != 1:
            self.logWriter.info("getPairedInputConditionerByFile : found none or > 1 data conditioner for %s"%inputFileName)
            return None
        else:
            self.logWriter.info("getPairedInputConditionerByFile : found existing data conditioner for %s"%inputFileName)
            return found_dcs[0]

    def getDistinctInputConditioners(self):
        """
        return an array of input conditioners, with any duplicates (i.e. conditioners that condition the
        same files) removed
        """
        distinctConditioners = []
        for c in self.inputConditioners + [pair[0] for pair in self.pairedInputConditioners] + [pair[1] for pair in self.pairedInputConditioners] :
            if c.inputFileName  not in [ dc.inputFileName for dc in distinctConditioners ]:
                distinctConditioners.append(c)

        return distinctConditioners

    def getDistinctOutputBaseConditioners(self):
        """
        return an array of data conditioners that have type of the base class. These do not 
        uncondition data, and are used to condition arguments to commands that specify a 
        basename for a set of output files.
        """
        distinctConditioners = []
        for c in self.outputUnconditioners:
            if type(c) == dataConditioner and c.outputFileName not in [ dc.outputFileName for dc in distinctConditioners ]:
                self.logWriter.info("DEBUG : getDistinctOutputBaseConditioners adding an OutputBaseConditioner")
                distinctConditioners.append(c)

        return distinctConditioners
    
    def getConditionedInputGenerators(self):
        """
        This method returns a generator of conditioned input, based on the input conditioners that have
        been created. The generator is a (itertoools.)zipped combination of generators, one for each
        distinct input data conditioner (except that for "paired" input conditioners - e.g. paired
        fastq files - a single generator handles the pair)

        Input conditioners are "distinct", if they refer to different input files. If they refer to the same
        input file, then for this method they are not regarded as distinct. If several conditioners refer to the 
        same input file, then only one of them (the first one found), is included in the composite 
        generator created below - the conditioned file names are distributed to all of the conditioners.

        (if we find that there are no input generators, then
        we return a generator that will yield a single value "None". (For example if a command
        does not include any input conditioning, this will be the case)
        """    

        inputGenerators = [dc.getConditionedInputGenerator() for dc in self.getDistinctInputConditioners()]
        inputGenerators = [ig for ig in inputGenerators if ig != None]

        if len(inputGenerators) > 0:
        
            self.logWriter.info("dataConditioner : making zipped input generator from %s"%str(inputGenerators))
            input_iter = itertools.izip(*inputGenerators)
            self.logWriter.info("DEBUG : zipped input generator is %s"%str(input_iter))
            self.jobcontroller.options["input_conditioning"] = True
        else:
            return [((None,None),[None,None])]
        
        return input_iter

    def parseExpectedUnconditionedOutputFiles(self, file_list):
        # we will look for files that match the patterns. There is probably (and currently) actually
        # only one pattern - sniff the first eleemnt to get this
        expectedFilesToUncondition = []
        if len(file_list) > 0 and len( self.conditionedProductPatterns ) > 0:
            self.logWriter.info("parseExpectedUnconditionedOutputFiles: received file list %s"%str(file_list))
            #manifest  = set(reduce(lambda x,y:x+y, file_list))
            pattern = self.conditionedProductPatterns[0][1]
            self.logWriter.info("parseExpectedUnconditionedOutputFiles looking for products using regexp=%s"%pattern)
            expectedFilesToUncondition = [os.path.join(self.workingRoot, filename) for filename in file_list if re.search(pattern, filename ) is not None]
            self.logWriter.info("parseExpectedUnconditionedOutputFiles found %d files to uncondition"%len(expectedFilesToUncondition))
        return expectedFilesToUncondition


    def getExpectedUnconditionedOutputFiles(self):
        expectedFilesToUncondition = [filename for filename in self.conditionedOutputFileNames if filename != None]
        if len(self.conditionedProductPatterns) > 0:
            expectedFilesToUncondition = sorted(self.conditionedProductManifests)
        return expectedFilesToUncondition

    # 
    # always create a new conditioner - because even if it references the same file 
    # as another conditioner, the context is probably different - i.e. different
    # conditioning word, so can't use the same conditioner instance, to handle
    # two different directives referring to the same file. 
    # We do still need to make sure that in that case the underlying file is only 
    # processed once, and the chunk names distributed to all conditioner instances that 
    # reference that file. This is handled, by only pulling out distinct conditioners 
    # (i.e. referring to different files), to generate the conditioned file names, and 
    # then these are distributed to all of the conditioners.
    #
    @classmethod
    def addInputConditionerDEPRECATED(cls, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        dc=dataConditioner(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        dataConditioner.inputConditioners.append(dc)
        return dc

    #def addInputConditioner(self, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
    #    dc=dataConditioner(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
    #   self.inputConditioners.append(dc)
    #    self.induct(dc)
    #    return dc

    @classmethod
    def addInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        dc=cls(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        prototype.inputConditioners.append(dc)
        prototype.induct(dc)
        return dc

    @classmethod
    def addPairedInputConditionerDEPRECATED(cls, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        dc=dataConditioner(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        dataConditioner.appendPairedInputConditioner(dc)

    #def addPairedInputConditioner(self, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
    #    dc=dataConditioner(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
    #    self.appendPairedInputConditioner(dc)
    #    self.induct(dc)
    @classmethod    
    def addPairedInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        dc=cls(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        prototype.appendPairedInputConditioner(dc)
        prototype.induct(dc)
        return dc
    
    @classmethod
    def appendPairedInputConditionerDEPRECATED(cls, dc):

        if len(dataConditioner.pairedInputConditioners)== 0:
            dataConditioner.pairedInputConditioners.append([dc])
            dc.pairMaster = True                                              # the first member of the pair is the master and coordinates conditioning
        elif len(dataConditioner.pairedInputConditioners[-1]) == 1:
            dataConditioner.pairedInputConditioners[-1].append(dc)
            dataConditioner.pairedInputConditioners[-1][0].pairPartner = dc   # each member of the pair stores state about which is its partner
            dc.pairPartner = dataConditioner.pairedInputConditioners[-1][0]
        elif len(dataConditioner.pairedInputConditioners[-1]) == 2:            
            dataConditioner.pairedInputConditioners.append([dc])
            dc.pairMaster = True
        else:
            raise tardisException("addPairedInputConditioner : unsupported contents of pairedInputConditioners : %s"%str(dataConditioner.pairedInputConditioners))            
        return dc

    def appendPairedInputConditioner(self, dc):
        if len(self.pairedInputConditioners)== 0:
            self.pairedInputConditioners.append([dc])
            dc.pairMaster = True                                              # the first member of the pair is the master and coordinates conditioning
        elif len(self.pairedInputConditioners[-1]) == 1:
            self.pairedInputConditioners[-1].append(dc)
            self.pairedInputConditioners[-1][0].pairPartner = dc   # each member of the pair stores state about which is its partner
            dc.pairPartner = self.pairedInputConditioners[-1][0]
        elif len(self.pairedInputConditioners[-1]) == 2:            
            self.pairedInputConditioners.append([dc])
            dc.pairMaster = True
        else:
            raise tardisException("addPairedInputConditioner : unsupported contents of pairedInputConditioners : %s"%str(dataConditioner.pairedInputConditioners))            
        return dc
    

    
    @classmethod         
    def addOutputUnconditionerDEPRECATED(cls, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        """
        base-class output conditioners do not know how to uncondition data but can be useful to generate 
        conditioned output file basenames wich can be used by product conditioners.
        """
        dc=dataConditioner(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord )
        dataConditioner.outputUnconditioners.append(dc)
        return dc

    #def addOutputUnconditioner(self, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
    #    """
    #    base-class output conditioners do not know how to uncondition data but can be useful to generate 
    #    conditioned output file basenames wich can be used by product conditioners.
    #    """
    #    dc=dataConditioner(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord )
    #    self.outputUnconditioners.append(dc)
    #    self.induct(dc)
    #    return dc

    @classmethod         
    def addOutputUnconditioner(cls, prototype, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        """
        base-class output conditioners do not know how to uncondition data but can be useful to generate 
        conditioned output file basenames wich can be used by product conditioners.
        """
        dc=cls(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord )
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc
    
    

    @classmethod         
    def addThroughputConditionerDEPRECATED(cls, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        dc=dataConditioner(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        dataConditioner.throughputConditioners.append(dc)
        return dc

    #def addThroughputConditioner(cls, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
    #    dc=dataConditioner(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
    #    self.throughputConditioners.append(dc)
    #    self.induct(dc)
    #    return dc

    @classmethod         
    def addThroughputConditioner(cls, prototype, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None):
        dc=cls(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        prototype.throughputConditioners.append(dc)
        prototype.induct(dc)
        return dc

    @staticmethod
    def isListFile(filename):
        """
        tardis supports specifying input via a list file. A list file is recognised as such if
        * it has the suffix .list (not case sensitive)
        * all non-trivial records in the file correspond to files that exist on the system, relative to
        either filesystem root or the path to the list file. (i.e. the filenames in the
        listfile may contain either absolute paths, or just names of files in the
        same folder as the list file)
        """
        result = True
        
        if not os.path.exists(filename):
            raise tardisException("%s does not exist"%filename)

        listPath = os.path.dirname(filename)

        if re.search("(.*)\.list$", filename, re.IGNORECASE) is None:
            result = False
        else:

            l = open(filename,"r")
            for record in l:
                if len(record.strip()) == 0:
                    continue
                #print "DEBUG : looking for %s"%record.strip()
                if not os.path.isfile(record.strip()):      # test absolute path
                    #print "DEBUG : looking for %s"%os.path.join(listPath, record.strip())
                    if not os.path.isfile(os.path.join(listPath, record.strip())):    # test relative path
                        result = False
                        break
            l.close()


            #print "DEBUG : %s"%result

        return result


    @staticmethod
    def getListedFilePath(listedFileName, listFileName):
        """
        list files may contain filenames with either absolute or relative paths. Return
        an absolute path to a listed file (if possible)
        """
        result = listedFileName.strip()    # had read these from a file - strip any trailing CR/LF
        
        if not os.path.exists(result):
            # try relative path (relative to the list file name)
            result =  os.path.join(os.path.dirname(listFileName), os.path.basename(result))
            if not os.path.exists(result):
                raise tardisException("could not find path for listed file %s (in list file %s)"%(listedFileName, listFileName))


        return result
            

    # a wrapper to get around the problem of creating circular symlinks when the 
    # source is not provided with a full filename
    def symlink(self, source, linkname):
        if os.path.basename(source) == source:
            self.logWriter.info("dataConditioner.symlink : will link to %s"%os.path.join(os.getcwd(), source))
            os.symlink(os.path.join(os.getcwd(), source), linkname)
        else:
            os.symlink(source, linkname)

    
    def distributeAvailableInputs(self, conditionedInputs):
        """
        as each chunk of input data becomes available, each of
        the data conditioners which is conditioning input of which that is a fragment,
        is updated with the filename, so that when "next" is next called on the conditioners,
        they return a conditioned word for this chunk

        Each of the output conditioners increments its expected output cardinality counter

        The argument conditionedInputs consists of
symlink
        ((inputfilename1, inputfilename2), (fragmentname1, fragmentname2))

        - i.e. first the unconditioned input filename(s) and then the conditioned input filenames

        each input conditioner will see if the conditionedInput is of interest to it,
        by looking at the unconditioned file name

        
        """
        INFILES=0
        FRAGMENTS=1
        self.logWriter.info("distributeAvailableInput : distributing %s"%str(conditionedInputs))
        for conditionedInput in conditionedInputs:
            for dc in self.inputConditioners + [pair[0] for pair in self.pairedInputConditioners] + [pair[1] for pair in self.pairedInputConditioners]:
                if dc.inputFileName == conditionedInput[INFILES][0] and conditionedInput[FRAGMENTS][0] != None:
                    dc.conditionedInputFileNames.append(conditionedInput[FRAGMENTS][0])
                elif dc.inputFileName == conditionedInput[INFILES][1] and conditionedInput[FRAGMENTS][1] != None:
                    dc.conditionedInputFileNames.append(conditionedInput[FRAGMENTS][1])

        # if the conditioned input filename is the same as the original input filename,
        # - e.g. chunksize 0 - then make a shortcut from the working folder to the original file.
        # e.g. the conditioned output names are based on the conditioned input names so we need
        # to at least have the conditioned input name living in the working folder
        for dc in self.inputConditioners + [pair[0] for pair in self.pairedInputConditioners] + [pair[1] for pair in self.pairedInputConditioners]:
            if len(dc.conditionedInputFileNames) == 0 and dc.inputFileName != None:
                dc.conditionedInputFileNames = [os.path.join(dc.workingRoot, os.path.basename(dc.inputFileName))]
                self.logWriter.info("dataConditioner : symlinking %s ---> %s "%(dc.conditionedInputFileNames[0] , dc.inputFileName))
                self.symlink(dc.inputFileName, dc.conditionedInputFileNames[0])
                # check it is there
                if not os.path.exists(dc.conditionedInputFileNames[0]):
                    dc.logWriter.info("error failed symlinking %s ---> %s "%(dc.conditionedInputFileNames[0] , dc.inputFileName))
                    raise tardisException("error failed symlinking %s ---> %s"%(dc.conditionedInputFileNames[0] , dc.inputFileName))


        for dc in self.outputUnconditioners + self.throughputConditioners :
            dc.expectedOutputCardinality += 1


    def __iter__(self):
        """
        when conditioning commands, a dataConditioner is an iterator. Each conditioner iterates conditioned command words- e.g.
        chunk file names
        """
        return self

    def next(self):
        """
        when acting as an iterator through conditioned command words, will return the next command word.
        """
        conditionedWord = ""
        
        if self.inputFileName != None:
            conditionedWord = self.nextConditionedInputWord()
        elif self.outputFileName != None:
            conditionedWord = self.nextConditionedOutputWord()

        self.conditionedWordCount += 1

        return conditionedWord
        

    def getConditionedInputGenerator(self):
        """
        base class returns in the input file name
        """
        self.logWriter.info("base class conditionedInputGenerator - will generate input file name")
        return [(self.inputFileName, None), (self.inputFileName, None)]
                           
        
    def nextConditionedInputWord(self):
        """
        return the next conditioned input word
        """
        
        # if we have already generated condtioned command words for all of our
        # conditioned inputs , then we should not be here - thats a bug, raise an
        # exception
        if self.conditionedWordCount >= len(self.conditionedInputFileNames):
            raise tardisException("nextConditionedInputWord : error - have already used all of %s conditioned filenames without being reset, no more available !"%len(self.conditionedInputFileNames))


                    
        # splice the input file into the input word that appears on the command-line
        if self.commandConditioning:
            self.logWriter.info("dataConditioner : conditioning  %s using pattern %s and input filename %s"%(self.conditioningWord, self.conditioningPattern, self.conditionedInputFileNames[-1])) 
            inputWord = re.sub(self.conditioningPattern, self.conditionedInputFileNames[-1], self.conditioningWord)
        else:
            inputWord = "" 

        return inputWord


    def removeConditionedInput(self):
        """
        remove the conditioned input
        """
        for fileName in self.conditionedInputFileNames:
            # these filenames should already include the full path to the working root - however
            # to be extra sure we do not run amok and remove files outside there, strip the 
            # path and add it back on....
            safeFileName = os.path.join(self.workingRoot, os.path.basename(fileName))
            # sometimes the same conditioner instance occurs more than once
            # (e.g. throughput conditioning) so the file may already have gone
            if os.path.exists(safeFileName):  
                self.logWriter.info("dataConditioner : removing %s"%safeFileName)
                os.remove(safeFileName)
            else:
                self.logWriter.info("dataConditioner : skipping %s , not there (already removed?) "%safeFileName )

    def removeConditionedOutput(self):
        """
        remove all conditioned output. Should  normally only do this all the
        states are OK however leave it to the caller to check this.
        """
        for fileName in self.conditionedOutputFileNames:
            # these filenames should already include the full path to the working root - however
            # to be extra sure we do not run amok and remove files outside there, strip the 
            # path and add it back on....
            safeFileName = os.path.join(self.workingRoot, os.path.basename(fileName))
            # sometimes output files have not been generated as expected. If this should be an error
            # it is flagged elsewhere - so don't fail here
            if os.path.isfile(safeFileName):              
                self.logWriter.info("removeConditionedOutput : removing %s"%safeFileName)
                os.remove(safeFileName)
            elif os.path.isdir(safeFileName):
                self.logWriter.info("removeConditionedOutput : skipping %s as it is a directory"%safeFileName )
            else:
                self.logWriter.info("removeConditionedOutput : skipping %s , not there "%safeFileName )


    def nextConditionedOutputWord(self):
        """
        return the next conditioned input word

        If the conditioner is handling by-product of a tool command - i.e. output that is not specified on the
        command-line (i.e. self.conditioningCommmands is False), then the outputWord that is passed in  - i.e.
        the product conditioning token entered on the command-line - consists of either 

        1.  _product_conditioning_directive_productsuffix. For example _condition_fastq_product_.trimmed 
            _product_conditioning_directive_sourcesuffix,productsuffix. For example _condition_fastq_product_.bwa.gz,bam 
        2. (may add other patterns for specifying how the unconditioner finds the products

        if no input conditioners have been specified, then we return a path name, 
        of the working directory joined to the output file base-name
        """
        
        
        if self.commandConditioning:

            
            self.logWriter.info("nextConditionedOutputWord : conditioning outputs")
            if self.outputFileName is None:
                raise tardisException("nextConditionedOutputWord : error - command conditioning has been requested but outputfilename is None")
            
            self.logWriter.info("nextConditionedOutputWord : cardinality = %s"%self.expectedOutputCardinality)
            if self.expectedOutputCardinality == 0:
                self.conditionedOutputFileNames = [os.path.join(self.workingRoot, os.path.basename(self.outputFileName))]
            else:
                #self.conditionedOutputFileNames.append("%s.%s"%(os.path.join(self.workingRoot, os.path.basename(self.outputFileName)), 1 + self.conditionedWordCount)) 
                #self.conditionedOutputFileNames.append("%s.%05d"%(os.path.join(self.workingRoot, os.path.basename(self.outputFileName)), 1 + self.conditionedWordCount))
                name_parts=os.path.splitext(os.path.basename(self.outputFileName))
                self.conditionedOutputFileNames.append(os.path.join(self.workingRoot, "%s.%05d%s"%(name_parts[0],1 + self.conditionedWordCount, name_parts[1])))
                

            # splice the output file into the output word that appears on the command-line
            self.logWriter.info("nextConditionedOutputWord : conditioned output filename : %s"%self.conditionedOutputFileNames[-1])

            self.logWriter.info("nextConditionedOutputWord : splicing %s into %s using %s"%(self.conditionedOutputFileNames[-1], self.conditioningWord, self.conditioningPattern))
            outputWord = re.sub(self.conditioningPattern, self.conditionedOutputFileNames[-1], self.conditioningWord)
        
               
        else:
            self.logWriter.info("nextConditionedOutputWord : conditioning product")

            # we need to generate conditioned filenames, of files which will contain by-products of the
            # computation. These filenames are not passed to the application - the application
            # just generates these, with naming based on the input file. (An example is
            # DynamicTrim.pl, which does not accept output file arguments - all of its output neeeds
            # to be handled as a by-product)

            # A rule is required that can be used to derive the product names for each chunk, from the
            # input file name for that chunk. In the case of multiple input files we use a heuristic approach to
            # figuring out which input and conditioned input word to use to condition the product name - based on
            # either the first inputConditioner or (if none of those) the first pairedInputConditioner.
            #
            # Rules can be specified using markup as below.
            conditionedInputName = None
            if len(self.inputConditioners) > 0 :
                conditionedInputName = self.inputConditioners[0].conditionedInputFileNames[-1]
                inputName = self.inputConditioners[0].inputFileName
            elif len(self.pairedInputConditioners) > 0 :
                conditionedInputName = self.pairedInputConditioners[0][0].conditionedInputFileNames[-1]
                inputName = self.pairedInputConditioners[0][0].inputFileName                
            else:
                raise tardisException("error - unable to find any input conditioners on which to base product conditioning !?")

            self.logWriter.info("product may be conditioned based on a conditioned input of %s, and an input of %s"%(conditionedInputName, inputName))            

            self.logWriter.info("checking for output basename conditioning")
            conditionedBaseName = None
            outputBaseConditioners = self.getDistinctOutputBaseConditioners() 
            if len(outputBaseConditioners) > 0:
                conditionedBaseName = outputBaseConditioners[0].conditionedOutputFileNames[-1]
                self.logWriter.info("product may be conditioned based on a conditioned output base name of %s"%(conditionedBaseName))            
            else:
                self.logWriter.info("(no output basename conditioning found)")
            
            if self.conditioningPattern is None:
                pattern = self.__class__.product_directive_pattern
            else:
                pattern = self.conditioningPattern
            
            match = re.search(pattern, self.conditioningWord)

            if match is None:
                self.logWriter.info("warning - unable to parse the output product suffix from %s - no unconditioning of products will be done"%self.conditioningWord)
                if self.expectedOutputCardinality == 0:
                    self.conditionedOutputFileNames = [None]
                else:
                    self.conditionedOutputFileNames.append(None)
            else:
                productSuffix = match.groups()[0]

                # this is either like
                # 1  .suffix - the product is expected to be input.suffix, and we leave it like that
                # 2  .suffix,.new_suffix - the product is expected to be input.suffix, send output to input.newsuffix
                # 3  .suffix,filename - the product is expected to be input.suffix, send output to filename
                # 4  _suffix,filename - the product is expected to be input_suffix, send output to filename
                # 4.1  -suffix,filename - the product is expected to be input-suffix, send output to filename                
                # 5  {}.suffix,filename - the product is expected to be conditionedoutputbasename.suffix, send output to filename
                # 6  text-and-chunknumber-reference,filename
                #
                
                
                self.logWriter.info("found product suffix %s"%productSuffix)
                
                # look for form 2 - i.e. the lexical form sourcesuffix,productsuffix, which can be used for a slightly more flexible conditioning expression. For example 
                # ".bwa.gz,.bam" - this indicates that the product filename is 
                # obtained by stripping back the sourcesuffix - e.g. from myfile.bwa.gz , and adding on the product suffix, so that the output would be
                # myfile.bam
                match = None
                while True:
                    # try form 2
                    match = re.search("^(\.\S+?)\,(\.\S+)$", productSuffix)
                    if match != None:
                        (inputSuffix, productSuffix) = (match.groups()[0], match.groups()[1])
                        self.logWriter.info("found input and product suffices %s and %s"%(inputSuffix, productSuffix))
                        self.logWriter.info("output conditioner : will condition outputs assuming a product suffix of %s and a source suffix of %s"%(productSuffix, inputSuffix)) 
                        self.conditionedOutputFileNames.append(string.replace(conditionedInputName, inputSuffix, productSuffix))
                        self.outputFileName = string.replace(inputName, inputSuffix, productSuffix)
                        break

                    # try form 3   
                    match = re.search("^(\.\S+?)\,(\S+)$", productSuffix)
                    if match != None:
                        (productSuffix, outputFileName) = (match.groups()[0], match.groups()[1])
                        self.logWriter.info("found product suffix and output filename %s and %s"%(productSuffix, outputFileName))
                        self.logWriter.info("output conditioner : will condition outputs assuming a product suffix of %s and an output filename of %s"%(productSuffix, outputFileName)) 
                        self.conditionedOutputFileNames.append("%s%s"%(conditionedInputName, productSuffix))
                        self.outputFileName = outputFileName
                        break

                    # try form 1
                    match = re.search("^(\.\S+?)$", productSuffix)
                    if match != None:                    
                        self.logWriter.info("output conditioner : will condition outputs assuming a product suffix of %s"%productSuffix)
                        self.conditionedOutputFileNames.append("%s%s"%(conditionedInputName, productSuffix) )
                        self.outputFileName = "%s%s"%(textDataConditioner.getUncompressedBaseName(inputName) , productSuffix)
                        break

                    # try form 4
                    match = re.search("^(\_\S+?)\,(\S+)$", productSuffix)
                    if match != None:
                        (productSuffix, outputFileName) = (match.groups()[0], match.groups()[1])
                        self.logWriter.info("found product suffix and output filename %s and %s"%(productSuffix, outputFileName))
                        self.logWriter.info("output conditioner : will condition outputs assuming a product suffixof %s and an output filename of %s"%(productSuffix, outputFileName))
                        self.conditionedOutputFileNames.append("%s%s"%(conditionedInputName, productSuffix))
                        self.outputFileName = outputFileName
                        break

                    # try form 4.1
                    match = re.search("^(\-\S+?)\,(\S+)$", productSuffix)
                    if match != None:
                        (productSuffix, outputFileName) = (match.groups()[0], match.groups()[1])
                        self.logWriter.info("found product suffix and output filename %s and %s"%(productSuffix, outputFileName))
                        self.logWriter.info("output conditioner : will condition outputs assuming a product suffixof %s and an output filename of %s"%(productSuffix, outputFileName))
                        self.conditionedOutputFileNames.append("%s%s"%(conditionedInputName, productSuffix))
                        self.outputFileName = outputFileName
                        break
                    
                    # try form 5 - output filename is derived from a data-less output conditioner - it generates conditioned labels 
                    match = re.search("^\{\}(\S+?)\,(\S+)$", productSuffix)
                    if match != None:
                        (productSuffix, outputFileName) = (match.groups()[0], match.groups()[1])
                        self.logWriter.info("found labelled product suffix and output filename %s and %s"%(productSuffix, outputFileName))
                        self.logWriter.info("output conditioner : will condition outputs assuming a product suffixof %s and an output filename of %s"%(productSuffix, outputFileName))
                        self.logWriter.info("labelled output conditoner will look for product name %s"%("%s%s"%(conditionedBaseName, productSuffix)))
                        self.conditionedOutputFileNames.append("%s%s"%(conditionedBaseName, productSuffix))
                        self.outputFileName = outputFileName
                        break

                    # try form 6 (regexp)
                    #
                    match = re.search("^(\S+?)\,(\S+)$", productSuffix)
                    if match != None:
                        (productRegexp, outputFileName) = (match.groups()[0], match.groups()[1])
                        self.logWriter.info("found product regexp and output filename %s and %s"%(productRegexp, outputFileName))
                        self.logWriter.info("output conditioner : will condition outputs assuming a product regexp of %s and an output filename of %s"%(productRegexp, outputFileName))
                        self.logWriter.info("output conditoner will look for product regexp %s"%productRegexp)
                        self.conditionedProductPatterns.append((conditionedBaseName, productRegexp))
                        self.outputFileName = outputFileName
                        break                    
                    

                    if match is None:
                        raise tardisException("unsupported product conditioning directive suffix : %s"%productSuffix)

            # (for product conditioning , we needed the details of the conditioned output products to look for but don't care about
            # conditioning a command word)
            outputWord = "" 

        return outputWord


    def conditionedOutputCollector(self):
        """
        this is a coroutine - an initialised instance of this is passed to somebody else 
        who will send us the output product packages 
 
        reference : http://dabeaz.com/coroutines/cofollow.py
                    http://dabeaz.com/coroutines/coroutine.py

        the co-routine is initialised when the owning object is created

        Currently this does not really need to be a coroutine, as this method does not
        currently need to maintain any state between "sends" of data. However there may be
        some need for this at some stage.

        Also - currently we don't do anything with this manifest
        """
        while True:
            output = (yield)
            if output != None:
                self.conditionedOutputManifests.append( output )
                self.logWriter.info("getconditionedOutput : have received %d of %d job products"%\
                                    ( len(self.conditionedOutputManifests), len(self.jobcontroller.jobList) ))


    def conditionedProductCollector(self):
        """
        similar coroutine as above
        """
        while True:
            output = (yield)
            #self.logWriter.info("conditionedProductCollector yielded : %s"%str(output))
            if output != None:
                self.conditionedProductManifests = list(set(self.conditionedProductManifests).union(set(self.parseExpectedUnconditionedOutputFiles(output))))
                self.logWriter.info("getconditionedProduct : have received %d of %d job product packages"%\
                                    ( len(self.conditionedProductManifests), len(self.jobcontroller.jobList) ))
          

    def unconditionOutput(self):
        """
        this class method coordinates unconditioning - i.e. "joining back together"
        the output of each of the conditioned computations. 
        """
        return 

