import os

import tardis.conditioner.data as data

class waitDataConditioner(data.dataConditioner):
    #output_directive_pattern = "_condition_bam_output_(\S+)"
    #product_directive_pattern = "_condition_bam_product_(\S+)"

    my_directives = ["_condition_wait_output_(\S+)", "_condition_wait_product_(\S+)"] 
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (output_directive_pattern, product_directive_pattern)  = my_directives
    

    @classmethod
    def addOutputUnconditioner(cls, prototype, outputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(outputFileName = outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,\
                              compressionConditioning = compressionConditioning)
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc

    def __init__(self, inputFileName = None, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        super(waitDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
        self.compressionConditioning = compressionConditioning


    def unconditionOutput(self):
        """
        the wait conditioner just waits for the given unconditioned output file and does not do anything more
        """

        if not os.path.exists(self.outputFileName):
            self.error("output file/folder %s does not exist  - setting error state"%self.outputFileName)
        else:
            self.logWriter.info("wait conditioner found output file/folder %s"%self.outputFileName)
            
        return

    def removeConditionedOutput(self):
        """
        the wait data condtioner does not remove conditioned output 
        """
        return


    def nextConditionedOutputWord(self):
        """
        the wait data conditioner just returns the output file name, or nothing 
        """
        if self.commandConditioning:
            return self.outputFileName
        else:
            return ""
        



