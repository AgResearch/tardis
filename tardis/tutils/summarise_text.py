#!/usr/bin/env python
#########################################################################
# this script can be used to summarised concatenated output from tardis
#########################################################################
import re
import os
import string
import zipfile
import gzip
import sys
import subprocess
import time
import random
import argparse



class unconditionedTextOuput(object):
    GZIP=0
    ZIP=1
    NO_COMPRESSION = 2
    
    def __init__(self, filename,pattern):
        super(unconditionedTextOuput, self).__init__()

        self.filename = filename
        self.pattern = pattern
    

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
    def getUncompressedBaseName(cls, filename):
        if cls.getFileCompressionType(filename) == cls.GZIP:
            return os.path.basename(re.search("(.*)\.gz$", filename).groups()[0])
        elif cls.getFileCompressionType(filename) == cls.ZIP:
            return os.path.basename(re.search("(.*)\.zip$", filename).groups()[0])
        else:
            return filename        
        

    def print_matches(self):
        (mystream,name) = self.getUncompressedFilestream(self.filename)
        for record in mystream:
            match = re.search(self.pattern, record)
            if match is not None:
                sys.stdout.write(record)
                if len(match.groups()) > 0:
                    print "====Values Found to Summarise===> %s"%str(match.groups())


    def analyse_file(self):
        (mystream,name) = self.getUncompressedFilestream(self.filename)
        summary = None
        for record in mystream:
            match = re.search(self.pattern, record)
            if match is not None:
                if len(match.groups()) > 0:
                    if summary is None:
                        summary = len(match.groups()) * [0]
                    summary = map(lambda x,y:x+float(y),summary,match.groups())

        return summary
                    
                    
            
    

def get_options():
    description = """
summarise a text file which is a concatenation of identical fragments 
    """
    long_description = """
Examples

summarise_text.py  -l -p '^Reads Used:\s+(\d+)\s+' /dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/bbmap_mappings/LDH_genes/results/Rank6_clean.fasta.log.gz
summarise_text.py  -p '^Reads Used:\s+(\d+)\s+' /dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/bbmap_mappings/LDH_genes/results/Rank6_clean.fasta.log.gz
summarise_text.py  -l -p '^mapped.*?\s+(\d+)\s+.*?\s+(\d+)$' /dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/bbmap_mappings/LDH_genes/results/Rank6_clean.fasta.log.gz
summarise_text.py  -p '^mapped.*?\s+(\d+)\s+.*?\s+(\d+)$' /dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/bbmap_mappings/LDH_genes/results/Rank6_clean.fasta.log.gz
summarise_text.py  -l -p '^unambiguous.*?\s+(\d+)\s+.*?\s+(\d+)$' /dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/bbmap_mappings/LDH_genes/results/Rank6_clean.fasta.log.gz
summarise_text.py  -p '^unambiguous.*?\s+(\d+)\s+.*?\s+(\d+)$' /dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/bbmap_mappings/LDH_genes/results/Rank6_clean.fasta.log.gz
    """
    parser = argparse.ArgumentParser(description=description, epilog=long_description, formatter_class = argparse.RawDescriptionHelpFormatter)
    parser.add_argument('filename', type=str, nargs=1,help='name of file to be summarised')
    parser.add_argument('-p','--pattern', required = True , help='regular expression used to find records and fields to summarise')
    parser.add_argument('-t','--summary_operation', dest='summary_operation', choices=["add"], default="add", help="The operation used to summarise (e.g. add)")
    parser.add_argument('-l','--list',  action='store_true', default=False, help='list the records that match and the values that will be summarised')

    
    args = vars(parser.parse_args())

    return args

            

def main():
    options = get_options()
    print "using %s"%str(options)

    myfile=unconditionedTextOuput(options["filename"][0], options["pattern"])

    if options["list"]:
        myfile.print_matches()
    else:
        summary = [str(item) for item in myfile.analyse_file()]
        print "Summary of %s:%s (%s) = %s"%(options["filename"][0],options["pattern"], options["summary_operation"], string.join(summary, ",")) 


if __name__=='__main__':
    sys.exit(main())    

    

        

