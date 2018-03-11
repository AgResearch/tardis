import subprocess

import tardis.conditioner.data as data
import tardis.conditioner.text as text

import tardis.tutils.tutils as tutils

class fastqDataConditioner(text.textDataConditioner):
    #input_directive_pattern = "_condition_fastq_input_(\S+)"
    #paired_input_directive_pattern = "_condition_paired_fastq_input_(\S+)"
    #output_directive_pattern = "_condition_fastq_output_(\S+)"
    #uncompressedoutput_directive_pattern = "_condition_uncompressedfastq_output_(\S+)"    
    #product_directive_pattern = "_condition_fastq_product_(\S+)"
    #uncompressedproduct_directive_pattern = "_condition_uncompressedfastq_product_(\S+)"

    my_directives = ["_condition_fastq_input_(\S+)", "_condition_paired_fastq_input_(\S+)", "_condition_fastq_output_(\S+)",\
                     "_condition_uncompressedfastq_output_(\S+)" , "_condition_fastq_product_(\S+)", "_condition_uncompressedfastq_product_(\S+)"]
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (input_directive_pattern, paired_input_directive_pattern, output_directive_pattern, uncompressedoutput_directive_pattern,\
     product_directive_pattern, uncompressedproduct_directive_pattern) = my_directives
    
    
    inFormat="fastq"
    outFormat="fastq"
    pairBond = lambda cls,x,y: fastqPairedNamesEqual(x.name, y.name) # x, y is a biopython seqrecord object 
    

    
    """
    references : http://www.biopython.org/DIST/docs/api/Bio.SeqIO.QualityIO-pysrc.html
    to test this conditioner, run (e.g.)

     ./tardis.py ls -l conditionfastqinput_YOURFASTQFILE
     e.g.
     ./tardis.py ls -l conditionfastqinput_/dataset/reseq_wf_dev/active/afm/afm_medium1/s_1_1_sequence_MEDIUM.fastq
    
    """
 
    supportedfastqFormats = {
        "qual" : "means simple quality files using PHRED scores (e.g. from Roche 454)", 
        "fastq" : """means Sanger style FASTQ files using PHRED scores and an ASCII
            offset of 33 (e.g. from the NCBI Short Read Archive and Illumina 1.8+). 
             These can potentially hold PHRED scores from 0 to 93""",
        "fastq-sanger" : """is an alias for "fastq" """ ,
        "fastq-solexa" : """means old Solexa (and also very early Illumina) style FASTQ 
          files, using Solexa scores with an ASCII offset 64. These can hold Solexa 
          scores from -5 to 62.""",
        "fastq-illumina" : """
           means newer Illumina 1.3 to 1.7 style FASTQ files, using 
           PHRED scores but with an ASCII offset 64, allowing PHRED scores from 0 to 62"""
    }

    @classmethod
    def addInputConditionerDEPRECATED(cls, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):

        dc = cls.getInputConditionerByFile(inputFileName)
        if dc is None:
            dataConditioner.logWriter.info("addInputConditioner : (did not find existing conditioner for %s)"%inputFileName)
            dataConditioner.logWriter.info("DEBUG addInputConditioner adding fastqDataConditioner")
            dc=fastqDataConditioner(inputFileName, commandConditioning=commandConditioning, isPaired = False, \
                                    conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,compressionConditioning=compressionConditioning)
        dataConditioner.inputConditioners.append(dc)
        return dc


    @classmethod
    def addPairedInputConditionerDEPRECATED(cls, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):

        dc = cls.getPairedInputConditionerByFile(inputFileName)
        if dc is None:
            dataConditioner.logWriter.info("addPairedInputConditioner : (did not find existing conditioner for %s)"%inputFileName)
            dc=fastqDataConditioner(inputFileName, commandConditioning=commandConditioning, isPaired = True, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,\
                                    compressionConditioning=compressionConditioning)
        dataConditioner.appendPairedInputConditioner(dc)
        return dc

    @classmethod
    def addInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):

        dc=cls(inputFileName, commandConditioning=commandConditioning, isPaired = False, \
                                    conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,compressionConditioning=compressionConditioning)
        prototype.inputConditioners.append(dc)
        prototype.induct(dc)
        return dc


    @classmethod
    def addPairedInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):

        dc=cls(inputFileName, commandConditioning=commandConditioning, isPaired = True, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord,\
                                    compressionConditioning=compressionConditioning)
        prototype.appendPairedInputConditioner(dc)
        prototype.induct(dc)
        return dc



    @classmethod
    def addOutputUnconditioner(cls, prototype, outputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(outputFileName = outputFileName, commandConditioning=commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord ,\
                                compressionConditioning=compressionConditioning)
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc

    def getLogicalRecordCount(self, arg_filename):
        """
        get an approximate logical record count for a fastq file 
        """
        filenames = [arg_filename]
        if self.isListFile(arg_filename):
            with open(arg_filename, "r") as file_list:
                filenames = [record.strip()  for record in file_list]

        record_count = 0
        for filename in filenames:
            try:
                count_command = ["kseq_count", "-a" , filename]
                self.logWriter.info("getLogicalRecordCount executing: %s"%" ".join(count_command))
                proc = subprocess.Popen(count_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (stdout, stderr) = proc.communicate()
                record_count += int(stdout)
            except Exception as e:
                self.logWriter.info("getLogicalRecordCount -failed getting logical record count : %s"% str(e))
                raise tutils.tardisException( "getLogicalRecordCount -failed getting logical record count : %s"% str(e) )

        self.logWriter.info("getLogicalRecordCount estimates there are %d records in %s"%(record_count, arg_filename))
        return record_count

    
    
    def __init__(self,inputFileName = None, outputFileName = None, commandConditioning = True, isPaired = False, \
                 conditioningPattern = None, conditioningWord = None, compressionConditioning = True, fastqFormat = "fastq-illumina"):
        super(fastqDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, isPaired = isPaired, \
                                                   conditioningPattern = conditioningPattern, conditioningWord = conditioningWord, compressionConditioning = compressionConditioning)
        self.fastqFormat = fastqFormat

