import conditioner.text as text
import conditioner.data as data

class blastxmlDataConditioner(text.textDataConditioner):
    #output_directive_pattern = "_condition_blastxml_output_(\S+)"
    #product_directive_pattern = "_condition_blastxml_product_(\S+)"

    my_directives = ["_condition_blastxml_output_(\S+)", "_condition_blastxml_product_(\S+)"]
    data.dataConditioner.all_directives += my_directives
    data.dataConditioner.published_directives += my_directives
    (output_directive_pattern, product_directive_pattern)  = my_directives
    
    

    def __init__(self,inputFileName = None, outputFileName = None, commandConditioning = True, conditioningPattern = None, conditioningWord = None, compressionConditioning = True):
        super(blastxmlDataConditioner, self).__init__(inputFileName, outputFileName, commandConditioning = commandConditioning, conditioningPattern = conditioningPattern, conditioningWord = conditioningWord)
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
        this class method coordinates unconditioning - i.e. "joining back together"
        blast xml output . There are some applications ( such as MEGAN ), which require a
        single merged XML output , that is valid XML (i.e. simple text concatenation is 
        not enough) 
    
        acknowledgement : 
        ### this method is based on galaxy code
        ### https://bitbucket.org/peterjc/galaxy-central/src/5cefd5d5536e/tools/ncbi_blast_plus/blast.py
        # see also lib/galaxy/datatypes/xml.py
        """
        
        #filesToProcess = [filename for filename in self.conditionedOutputFileNames if filename != None]
        filesToProcess = self.getExpectedUnconditionedOutputFiles()
        
        if len(filesToProcess) == 0:
            self.logWriter.info("unconditionOutput : no files to uncondition")
        
        self.logWriter.info("""
        blastxmlDataConditioner.unconditionOutput : unconditioning the following conditioned files
        %s
        to
        %s"""%(str(filesToProcess), self.outputFileName))


        split_files = filesToProcess
        output_file = self.outputFileName
        self.logWriter.info("starting galaxy based blast xml merge courtesy of https://bitbucket.org/peterjc/galaxy-central/src/5cefd5d5536e/tools/ncbi_blast_plus/blast.py")
        
        ##################################  begin Galaxy code snippet #######################################
	"""Merging multiple XML files is non-trivial and must be done in subclasses."""
        #print "DEBUG1"
	#if len(split_files) == 1:
	    #For one file only, use base class method (move/copy)
	    #return Text.merge(split_files, output_file)
	out = open(output_file, "w")
	h = None
	for f in split_files:
            print "processing %s"%f
	    h = open(f)
	    body = False
	    header = h.readline()
	    if not header:
		out.close()
		h.close()
		#raise ValueError("BLAST XML file %s was empty" % f)
		self.error("BLAST XML file %s was empty" % f)
		continue
	    if header.strip() != '<?xml version="1.0"?>':
		out.write(header) #for diagnosis
		out.close()
		h.close()
		#raise ValueError("%s is not an XML file!" % f)
		self.error("BLAST XML file %s was empty" % f)
		continue
	    line = h.readline()
	    header += line
	    if line.strip() not in ['<!DOCTYPE BlastOutput PUBLIC "-//NCBI//NCBI BlastOutput/EN" "http://www.ncbi.nlm.nih.gov/dtd/NCBI_BlastOutput.dtd">',
				    '<!DOCTYPE BlastOutput PUBLIC "-//NCBI//NCBI BlastOutput/EN" "NCBI_BlastOutput.dtd">']:
		out.write(header) #for diagnosis
		out.close()
		h.close()
		#raise ValueError("%s is not a BLAST XML file!" % f)
		self.error("%s is not a BLAST XML file!" % f)
		continue
	    while True:
		line = h.readline()
		if not line:
		    out.write(header) #for diagnosis
		    out.close()
		    h.close()
		    #raise ValueError("BLAST XML file %s ended prematurely" % f)
                    self.error("BLAST XML file %s ended prematurely" % f)
                    break
		    
		header += line
		if "<Iteration>" in line:
		    break
		if len(header) > 10000:
		    #Something has gone wrong, don't load too much into memory!
		    #Write what we have to the merged file for diagnostics
		    out.write(header)
		    out.close()
		    h.close()
		    #raise ValueError("BLAST XML file %s has too long a header!" % f)
                    self.error("BLAST XML file %s has too long a header!" % f)
                    break
		
	    if "<BlastOutput>" not in header:
		out.close()
		h.close()
		#raise ValueError("%s is not a BLAST XML file:\n%s\n..." % (f, header))
                self.error("%s is not a BLAST XML file:\n%s\n..." % (f, header))
                break
		
	    if f == split_files[0]:
		out.write(header)
		old_header = header
	    elif old_header[:300] != header[:300]:
		#Enough to check <BlastOutput_program> and <BlastOutput_version> match
		out.close()
		h.close()
		#raise ValueError("BLAST XML headers don't match for %s and %s - have:\n%s\n...\n\nAnd:\n%s\n...\n" \
		#		 % (split_files[0], f, old_header[:300], header[:300]))
                self.error("BLAST XML headers don't match for %s and %s - have:\n%s\n...\n\nAnd:\n%s\n...\n" \
				 % (split_files[0], f, old_header[:300], header[:300]))
                break
	    else:
		out.write("    <Iteration>\n")
	    for line in h:
		if "</BlastOutput_iterations>" in line:
		    break
		#TODO - Increment <Iteration_iter-num> and if required automatic query names
		#like <Iteration_query-ID>Query_3</Iteration_query-ID> to be increasing?
		out.write(line)
	    h.close()
	out.write("  </BlastOutput_iterations>\n")
	out.write("</BlastOutput>\n")
	out.close()
        ##################################  end Galaxy code snippet #######################################
	self.logWriter.info("finished galaxy based blast xml merge courtesy of https://bitbucket.org/peterjc/galaxy-central/src/5cefd5d5536e/tools/ncbi_blast_plus/blast.py")


        compressionCommand = self.getFileCompressionCommand(self.outputFileName)
        self.logWriter.info("executing %s"%str(compressionCommand))
        proc = subprocess.Popen(compressionCommand,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        self.logWriter.info("blastxmlDataConditioner : file compression returned  ( return code %s ) - here is its output "%proc.returncode)
        self.logWriter.info("stdout : \n%s"%stdout)
        self.logWriter.info("stderr : \n%s"%stderr)

        if proc.returncode != 0:
            self.error("compression of concatenated files appears to have failed - setting error state")



