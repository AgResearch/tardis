import tardis.conditioner.data as data 

class bamDataConditioner(data.dataConditioner):
    my_directives = ["_condition_bam_output_(\S+)", "_condition_bam_product_(\S+)"] 
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (output_directive_pattern, product_directive_pattern)  = my_directives
    
    @classmethod
    def addInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord, \
                                  compressionConditioning = compressionConditioning)
        prototype.inputConditioners.append(dc)
        prototype.induct(dc)
        return dc


    @classmethod
    def addOutputUnconditioner(cls, prototype, outputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,\
                              compressionConditioning = compressionConditioning)
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc

    def __init__(self, inputFileName = None, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        super(bamDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        self.compressionConditioning = compressionConditioning


    def unconditionOutput(self):
        """
        this class method coordinates unconditioning - i.e. "joining back together"
        output bam files. 
        """

        #expectedFilesToProcess = [filename for filename in self.conditionedOutputFileNames if filename != None]
        expectedFilesToProcess = self.getExpectedUnconditionedOutputFiles()


        # tolerate missing bams but warn - set error state so don't do the next step
        # (which would be removing the conditioned files)
        filesToProcess = [filename for filename in expectedFilesToProcess if os.path.isfile(filename)]
        if len(filesToProcess) < len(expectedFilesToProcess):
            self.warn("""warning : not all of the expected bam files were found - the following were not found :
            %s
            """%string.join([filename for filename in expectedFilesToProcess if filename not in filesToProcess],","))
            

        if len(filesToProcess) == 0:
            self.logWriter.info("unconditionOutput : no files to uncondition")

        self.logWriter.info("""
        bamDataConditioner.unconditionOutput : unconditioning the following conditioned files
        %s
        to
        %s"""%(str(filesToProcess), self.outputFileName))

        #Usage:   samtools merge [-nr] [-h inh.sam] <out.bam> <in1.bam> <in2.bam> [...]
        #Options: -n       sort by read names
        # -r       attach RG tag (inferred from file names)
        # -u       uncompressed BAM output
        # -f       overwrite the output BAM if exist
        # -1       compress level 1
        # -R STR   merge file in the specified region STR [all]
        # -h FILE  copy the header in FILE to <out.bam> [in1.bam]        
           
        #dataGluingCommand = ["samtools", "merge", "-n", self.outputFileName ] + filesToProcess 
        #read-order use-case not so common as expected - change to default sort ordering
        dataGluingCommand = ["samtools", "merge", self.outputFileName ] + filesToProcess 
        self.logWriter.info("executing %s"%str(dataGluingCommand))
        proc = subprocess.Popen(dataGluingCommand,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        self.logWriter.info("bamDataConditioner : samtools glue of bam files returned  ( return code %s ) - here is its output "%proc.returncode)
        self.logWriter.info("stdout : \n%s"%stdout)
        self.logWriter.info("stderr : \n%s"%stderr)

        if proc.returncode != 0:
            self.error("merge of bam files appears to have failed - setting error state")

        return


