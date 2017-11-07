import conditioner.data as data 

class pdfDataConditioner(data.dataConditioner):
    #output_directive_pattern = "_condition_pdf_output_(\S+)"
    #uncompressedoutput_directive_pattern = "_condition_uncompressedpdf_output_(\S+)"    
    #product_directive_pattern = "_condition_pdf_product_(\S+)"
    #uncompressedproduct_directive_pattern = "_condition_uncompressedpdf_product_(\S+)"

    my_directives = ["_condition_pdf_output_(\S+)", "_condition_uncompressedpdf_output_(\S+)",\
                     "_condition_pdf_product_(\S+)", "_condition_uncompressedpdf_product_(\S+)"]
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (output_directive_pattern, uncompressedoutput_directive_pattern, product_directive_pattern, uncompressedproduct_directive_pattern)  = my_directives
    


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
        super(pdfDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        self.compressionConditioning = compressionConditioning


    def unconditionOutput(self):
        """
        this class method coordinates unconditioning - i.e. "joining back together"
        output pdf files. The end results may not be that great even if valid pdf !  
        """

        #expectedFilesToProcess = [filename for filename in self.conditionedOutputFileNames if filename != None]
        expectedFilesToProcess = self.getExpectedUnconditionedOutputFiles()


        # its not uncommon for some expected pdf outputs to be not-there. (For example - DynamicTrim tries to use the
        # current working folder for an R temp file, which means that all of the concurrent instances
        # try to use the same file , and only one succeeds !) Tolerate this but set error state so don't do the next step
        # (which would be removing the conditioned files)
        filesToProcess = [filename for filename in expectedFilesToProcess if os.path.isfile(filename)]
        if len(filesToProcess) < len(expectedFilesToProcess):
            self.warn("""warning : not all of the expected pdf files were found - the following were not found :
            %s
            """%string.join([filename for filename in expectedFilesToProcess if filename not in filesToProcess],","))
            

        if len(filesToProcess) == 0:
            self.logWriter.info("unconditionOutput : no files to uncondition")

        self.logWriter.info("""
        pdfDataConditioner.unconditionOutput : unconditioning the following conditioned files
        %s
        to
        %s"""%(str(filesToProcess), self.outputFileName))
   
        # example of ghostscript command for gluing back together
        # gs -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -sOutputFile=finished.pdf /dataset/reseq_wf_dev/scratch/temp/inbfop03.agresearch.co.nz/tardis_rbB6Qe/test.fastq.trimmed_segments.hist.pdf 
        #                                                                     /dataset/reseq_wf_dev/scratch/temp/inbfop03.agresearch.co.nz/tardis_E0PG9W/test.fastq.trimmed_segments.hist.pdf 
        
        dataGluingCommand = ["gs", "-dBATCH", "-dNOPAUSE", "-q", "-sDEVICE=pdfwrite", "-sOutputFile=%s"%self.outputFileName ] + filesToProcess 
        self.logWriter.info("executing %s"%str(dataGluingCommand))
        proc = subprocess.Popen(dataGluingCommand,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        self.logWriter.info("pdfDataConditioner : ghostscript glue of pdf files returned  ( return code %s ) - here is its output "%proc.returncode)
        self.logWriter.info("stdout : \n%s"%stdout)
        self.logWriter.info("stderr : \n%s"%stderr)

        if self.compressionConditioning:
            # if ok so far - compress the output, else set a class level error flag
            if proc.returncode == 0:
                compressionCommand = textDataConditioner.getFileCompressionCommand(self.outputFileName)
                self.logWriter.info("executing %s"%str(compressionCommand))
                proc = subprocess.Popen(compressionCommand,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                (stdout, stderr) = proc.communicate()
                self.logWriter.info("pdfDataConditioner : file compression returned  ( return code %s ) - here is its output "%proc.returncode)
                self.logWriter.info("stdout : \n%s"%stdout)
                self.logWriter.info("stderr : \n%s"%stderr)

                if proc.returncode != 0:
                    self.error("compression of concatenated pdf files appears to have failed - setting error state")
            else:
                self.error("concatenation of pdf files appears to have failed - skipped compression and setting error state")
        else:
            if proc.returncode != 0:
                self.error("concatenation of pdf files appears to have failed - setting error state")
            
        return 


