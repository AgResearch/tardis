import conditioner.data as data
import conditioner.text as text

class fastq2fastaDataConditioner(text.textDataConditioner):
    #input_directive_pattern = "_condition_fastq2fasta_input_(\S+)"
    #output_directive_pattern = "_condition_fastq2fasta_output_(\S+)"
    #product_directive_pattern = "_condition_fastq2fasta_product_(\S+)"

    my_directives = ["_condition_fastq2fasta_input_(\S+)", "_condition_fastq2fasta_output_(\S+)",\
                     "_condition_fastq2fasta_product_(\S+)"]
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (input_directive_pattern, output_directive_pattern, product_directive_pattern)  = my_directives
    
    inFormat="fastq"
    outFormat="fasta"
    pairBond = lambda cls,x,y: fastqPairedNamesEqual(x.name, y.name)  # x , y is a biopython seqrecord object 
    

    @classmethod
    def addInputConditioner(cls, prototype, inputFileName, commandConditioning = True, isPaired = False, conditioningPattern = None, conditioningWord = None, compressionConditioning = True ):
        dc=cls(inputFileName, commandConditioning = commandConditioning, isPaired = isPaired, \
                                          conditioningPattern = conditioningPattern, conditioningWord = conditioningWord, compressionConditioning = compressionConditioning)
        prototype.inputConditioners.append(dc)
        prototype.induct(dc)
        return dc


    @classmethod
    def addOutputUnconditioner(cls, prototype, outputFileName, commandConditioning = True, isPaired = False, compressionConditioning = True):
        dc=fastq2fastaDataConditioner(outputFileName = outputFileName, commandConditioning = commandConditioning, isPaired = isPaired, \
                                      conditioningPattern = conditioningPattern, conditioningWord = conditioningWord, compressionConditioning = compressionConditioning)
        prototype.outputUnconditioners.append(dc)
        prototype.induct(dc)
        return dc

    def __init__(self,inputFileName = None, outputFileName = None, commandConditioning = True, isPaired = False, \
                 conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        super(fastq2fastaDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, isPaired = isPaired, \
                                                         conditioningPattern = conditioningPattern, conditioningWord = conditioningWord, compressionConditioning = compressionConditioning)



             
