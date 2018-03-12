import itertools, re, gzip, subprocess
import tardis.conditioner.data as data
import tardis.conditioner.text as text

import tardis.tutils.tutils as tutils


class fastaDataConditioner(text.textDataConditioner):
    #input_directive_pattern = "_condition_fasta_input_(\S+)"
    #output_directive_pattern = "_condition_fasta_output_(\S+)"
    #product_directive_pattern = "_condition_fasta_product_(\S+)"
    #uncompressedoutput_directive_pattern = "_condition_uncompressedfasta_output_(\S+)"
    #uncompressedproduct_directive_pattern = "_condition_uncompressedfasta_product_(\S+)"

    my_directives = ["_condition_fasta_input_(\S+)", "_condition_fasta_output_(\S+)",\
                     "_condition_uncompressedfasta_output_(\S+)" , "_condition_fasta_product_(\S+)", "_condition_uncompressedfasta_product_(\S+)"]
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (input_directive_pattern, output_directive_pattern, uncompressedoutput_directive_pattern,\
     product_directive_pattern, uncompressedproduct_directive_pattern) = my_directives


    inFormat="fasta"
    outFormat="fasta"
    pairBond = lambda cls,x,y : fastqPairedNamesEqual(x.name, y.name)  # x, y biopython seqrecord objects 
    
    

    @classmethod
    def addInputConditioner(cls, prototype, inputFileName, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        dc=cls(inputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, \
                                    conditioningWord = conditioningWord,compressionConditioning = compressionConditioning)
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

    @classmethod
    def deprecated_getLogicalRecordCount(cls, arg_filename):
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
                record_count += reduce(lambda x,y:1+x, itertools.ifilter(lambda record: re.search("^\s*>",record) is not None,f), 0)
                f.close()
                
            elif cls.getFileCompressionType(filename) == cls.ZIP:
                f = zipfile.ZipFile(filename, "r")
                record_count += reduce(lambda x,y:1+x, itertools.ifilter(lambda record: re.search("^\s*>",record) is not None,f), 0)
                f.close()
            else:
                with open(filename,"r") as f:
                    record_count += reduce(lambda x,y:1+x, itertools.ifilter(lambda record: re.search("^\s*>",record) is not None,f), 0)

        return record_count

    def getLogicalRecordCount(self, arg_filename):
        """
        get an approximate logical record count for a fasta file 
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
                 conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        super(fastaDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, isPaired = isPaired, \
                                                   conditioningPattern = conditioningPattern, conditioningWord = conditioningWord, \
                                                   compressionConditioning = compressionConditioning)

