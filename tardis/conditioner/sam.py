import os, subprocess

import tardis.conditioner.data as data
import tardis.tutils.tutils as tutils

class samDataConditioner(data.dataConditioner):
    #output_directive_pattern = "_condition_sam_output_(\S+)"   # generates BAM output 
    #uncompressedoutput_directive_pattern = "_condition_uncompressedsam_output_(\S+)"    # generates SAM output
    #headlessoutput_directive_pattern = "_condition_headlesssam_output_(\S+)" # generates headless SAM output
    #product_directive_pattern = "_condition_sam_product_(\S+)"
    #uncompressedproduct_directive_pattern = "_condition_uncompressedsam_product_(\S+)"    
    
    my_directives = ["_condition_sam_output_(\S+)", "_condition_uncompressedsam_output_(\S+)",\
                     "_condition_headlesssam_output_(\S+)", "_condition_sam_product_(\S+)", "_condition_uncompressedsam_product_(\S+)"]
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (output_directive_pattern, uncompressedoutput_directive_pattern, headlessoutput_directive_pattern, \
     product_directive_pattern, uncompressedproduct_directive_pattern)  = my_directives


    def __init__(self,inputFileName = None, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        # nothing is done with the compressionConditioning arg at the moment - left there for consistency with 
        # the other sibling classes
        super(samDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        self.compressionConditioning = compressionConditioning
        

    @classmethod
    def addOutputUnconditioner(cls, prototype, outputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,\
                              compressionConditioning = compressionConditioning)
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc

    def unconditionOutput(self):
        """
        this class method coordinates unconditioning SAM output, - i.e. "joining back together"
        SAM output, and optionally compressing as BAM. 
        """
        #filesToProcess = [filename for filename in self.conditionedOutputFileNames if filename != None]
        filesToProcess = self.getExpectedUnconditionedOutputFiles()
        if len(filesToProcess) == 0:
            self.logWriter.info("unconditionOutput : no files to uncondition")
            return
        
        self.logWriter.info("""
        samDataConditioner.unconditionOutput : unconditioning the following conditioned files
        %s
        to
        %s"""%(str(filesToProcess), self.outputFileName))

        # open the output file
        if os.path.exists(self.outputFileName):
            self.logWriter.info("warning- %s already exists will overwrite"%self.outputFileName) # file will already exist if running in Galaxy
        #samfile = open(self.outputFileName, "w")

        samHeader = None
        
        for fileName in filesToProcess:
            # sam "merge" step - concatenate all of the SAM files. Note that we could do this more efficiently
            # via launching a single concatenate command of some kind - however this is not scalable to a combination
            # of many files , and long paths , as it may overflow the command buffer. Hence the
            # less efficient approach of concatenate one at a time, in a loop  

            # get the header
            headerCommand = ["tardis.py" , "-d", self.options["rootdir"], "-q", "-hpctype", self.options["hpctype"], "samtools",  "view" , "-H", "-S" , fileName]
            self.logWriter.info("starting (recursive tardis) %s"%headerCommand)
            hproc = subprocess.Popen(headerCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            (hstdout, hstderr) = hproc.communicate()
            self.logWriter.info("header stderr : \n%s"%hstderr)
            self.logWriter.info("header stdout length : \n%s"%len(hstdout))
            

            if hproc.returncode != 0:
                self.error("header listing of %s appears to have failed"%fileName)
                break

            # if have no header cached , cache this, and write to the output file ( - unless we are doing headless conditioning,
            # then we do not add to output)
            if samHeader is None:
                samHeader = hstdout
                if self.conditioningPattern != self.headlessoutput_directive_pattern:
                    with open(self.outputFileName, "w") as samfile:
                        samfile.write(samHeader)
                    
                #samfile.close()
                #samfile = open(self.outputFileName, "a")
                
            # else check this header is the same as the cached one - if not fail
            else:
                if not tutils.SAMHeadersEqual(samHeader,hstdout):
                    self.error("error - inconsistent headers encoutered this header : %s  \n\n --- ....versus this header ....--- \n\n%s"%(samHeader, hstdout))
                    break

            # view the file (without header), stdout = output file
            #viewCommand = ["tardis.py", "-d", self.options["rootdir"], "-q", "-hpctype", self.options["hpctype"], "samtools",  "view" , "-S", fileName]
            viewCommand = ["tardis.py", "-d", self.options["rootdir"], "-q", "-hpctype", self.options["hpctype"], "samtools",  "view" , "-S", fileName, ">>" , self.outputFileName]

            self.logWriter.info("starting (recursive tardis) %s "%str(viewCommand))
            #vproc = subprocess.Popen(viewCommand, stdout=samfile, stderr=subprocess.PIPE)
            vproc = subprocess.Popen(viewCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            
            (vstdout, vstderr) = vproc.communicate()
            self.logWriter.info("view stdout : \n%s"%vstdout)
            self.logWriter.info("view stderr : \n%s"%vstderr)

            #samfile.close()
            #samfile = open(self.outputFileName, "a")            

            if vproc.returncode != 0:
                self.error("SAM listing of %s appears to have failed, exit code %d, error text as above"%(fileName, vproc.returncode))
                break

        self.logWriter.info("unconditionOutput : finished SAM merge step")
        #samfile.close()

        if self.compressionConditioning:
            if data.dataConditioner.state == data.dataConditioner.OK:
                self.logWriter.info("compressing sam to sorted bam (compressionConditioning = True)")

                bamCommand = ["tardis.py", "-d", self.options["rootdir"], "-q", "-hpctype", self.options["hpctype"], "samtools","view","-h","-S","-b",self.outputFileName, "|",\
                              "samtools","sort","-", "-T" , "_tardis_sam_sort_tmp" , "-o" , self.outputFileName] # will write outfilebase.bam

                self.logWriter.info("starting (recursive tardis) %s "%str(bamCommand))
                bproc = subprocess.Popen(bamCommand, stdout = subprocess.PIPE, stderr=subprocess.PIPE)

                (bstdout, bstderr) = bproc.communicate()
                self.logWriter.info("bam stdout : \n%s"%bstdout)
                self.logWriter.info("bam stderr : \n%s"%bstderr)

                if bproc.returncode != 0:
                    self.error("bam compression or sort appears to have failed")
                # this was how things went with an earlier verson of samtools
                #else:
                #    self.logWriter.info("unconditionOutput : finished sam to bam compression and sort - removing sam file %s"%self.outputFileName)
                #    # remove the sam file
                #    os.remove(self.outputFileName)
            
            else:
                self.logWriter.info("(skipped bam compression and sort as previous steps failed)")
        return



