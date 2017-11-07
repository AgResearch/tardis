#!/usr/bin/env python

#tardis_analyze.py - analyse a tardis log folder and optionally generate status information, statistics and job repair information
#
# Copyright 2014 AgResearch. See the COPYING file at the top-level directory of this distribution.
"""
tardis_analyze.py

(see the "usage" text below and associated docs for more information
"""


import sys
import tempfile
import logging
from string import Template
import subprocess
import re
import os
import stat
import string
import exceptions
import itertools
import time
import zipfile
import gzip
import io

from tardis import hpcConditioner,hpcJob, condorhpcJob, tardisLogger


###############################################################################################
# globals                                                                                     #
###############################################################################################


class Usage(Exception):
    def __init__(self, msg):
        super(Usage, self).__init__(msg)


def getOptions(argv):
    usage = """

   tardis_analyze is a script which analyses a tardis run folder and optionally reports statistics and 
   job repair information.

   Usage : 

   tardis_analyze.py [-l path_to_log_folder ] [-x repair|statistics]  

   example : 

   ./tardis_analyze.py -l /dataset/reseq_wf_dev/scratch/temp/inbfop03.agresearch.co.nz/tardis_xtlzI2 -x repair

    """
    #
    # get and parse args - can't use standard OptionParser due to the way we mix args to this script, and the commands passed in
    if len(argv) < 2:
        raise Usage("please supply the path to a tardis log folder (type tardis_analyze.py -h for usage")

    args =  argv[1:]
    options = {
       "logfolder" : None,
       "action" : "repair" 
    }
    while len(args) > 0:
        arg = args.pop(0)
        
        if arg == "-l" : 
            folder = args.pop(0)
            if not os.path.isfile(os.path.join(folder, "tardis.log")):
                raise Usage("either this is not a tardis log folder, or you don't have permission to access it (can't see %s)"%os.path.join(folder, "tardis.log"))
            options["logfolder"] = folder 
        elif arg == "-x" : 
            options["action"] = args.pop(0)
        elif arg == "-h" : 
            raise Usage(usage)
        else:
            raise Usage("unknown argument %s (type tardis_analyze -h for usage"%arg)

    return (options, args)


def getRunTime(logFolder):
    logFile = os.path.join(logFolder,"tardis.log")

    runTimeDict = {
        'logfolder' : logFolder,
        'rootdir' : None,
        'in_workflow' : None,
        'chunksize' : None,
        'dry_run' : None,
        'hpctype' : None,
        'samplerate' : None,
        'KEEP_CONDITIONED_DATA' : None,
        'jobtemplatefile' : None,
        'toolargs' : None
    }

    for record in open(logFile,"r"):

        if runTimeDict['rootdir'] is not None and runTimeDict['toolargs'] is not None:
            break

        if runTimeDict['rootdir'] is None:
            match = re.search("INFO using (.*)$", record.strip())
            if match != None:
                runTimeDict.update(eval(match.groups()[0]))

        if runTimeDict['toolargs'] is None:
            match = re.search("INFO tool args = (.*)$", record.strip())
            if match != None:
                runTimeDict['toolargs'] =  eval(match.groups()[0])

        # get tardis opts from 
        #2014-01-27 16:03:18,433 INFO using {'rootdir': '/dataset/reseq_wf_dev/scratch/temp/inbfop03.agresearch.co.nz', 'in_workflow': True, 'chunksize': 500, 'dry_run': False, 'hpctype': 'condor', 'samplerate': None, 'KEEP_CONDITIONED_DATA': False, 'jobtemplatefile': None}

        # get tool args from 
        # 2014-01-27 16:03:18,434 INFO tool args = ['blastx', '-query', '_condition_fasta_input_/dataset/JHI_High_Low_Sequencing_Data/scratch/Janine/all_ranks_metagenome_orfs_jgi.fa', '-num_threads', '10', '-db', '/dataset/datacache/scratch/kegg/genes.pep', '-evalue', '1.0e-3', '-max_target_seqs', '1', '-outfmt', '6', '-lcase_masking', '-out', '_condition_text_output_Kegg_search_allorfs_metagenome_tabular.tardis.out']

        # get log files from the run folder
        if runTimeDict['hpctype'] == 'condor' or runTimeDict['hpctype'] is None:
            runTimeDict['logfiles'] = hpcConditioner(tardisLogger(), logFolder).gethpcJob([],"condor").getManifest("([\d]\.log$)",sensitivity=1)

    return runTimeDict
    

def getRepair(runTimeConfig, stats_only = False):

    repairInfo = io.StringIO()

    if runTimeConfig["hpctype"] == "condor" or runTimeConfig["hpctype"] is None:
        returncodeStats = {}
        repairList = []
        returnCodeList = []

        # for each log file
        for logfilename in runTimeConfig["logfiles"]:
            #print "getting repair for %s"%logfilename

            job = None

            if runTimeConfig["hpctype"] == "condor":
                job = hpcConditioner(tardisLogger(), runTimeConfig['logfolder']).gethpcJob([],"condor") 
                job.logname = os.path.join(runTimeConfig['logfolder'], logfilename) 
                job.getExitFootprint()

                returncodeStats[job.returncode] = returncodeStats.setdefault(job.returncode, 0) + 1

                if job.returncode != 0:
                    repairList.append(logfilename)
                    returnCodeList.append(job.returncode)
         

        # generate text repair info

        repairInfo.write(u"""
        Analyzed %s log files.

        %s jobs need repair 

        Summary of jobs by status (reported by condor): 

        %s

        """%( len(runTimeConfig["logfiles"]), len(repairList), str(returncodeStats)) )


        if not stats_only:
            # if any repairs needed, write them out
            if len(repairList) > 0:
                repairInfo.write(u"""
            
            Suggested repair code is below, split into (up to ) 10 batches. You can for example write this 
            to 10 batch-files, then execute all of them concurrently (assuming you have capacity).
     
            (also included are commented out commands that will append a normal termination code to each 
            log file - if your tardis master job is still running, this will then pickup the repaired output
            (it is polling for the termination line in each log file), and if all outputs are picked up,
            will complete the job and merge the output.
            These commands can be uncommented and executed after checking that the repairs ran without error, and completed)
            """)

            batchnumber = 1
            for i in range(0, len(repairList)):
                if i%(1 + int(len(repairList) / 10.0) ) == 0:
                    repairInfo.write(u"""
                    **** Batch %s ****
                    """%batchnumber)
                    batchnumber += 1

                repairInfo.write(u"""
                %s > %s 2>&1   # (return code = %s)
                #echo "JOB REPAIR RESULTS : Normal termination (return value 0)" >> %s
                """%(re.sub("\.log$", ".sh", os.path.join(runTimeConfig['logfolder'], repairList[i]))\
                                           , re.sub("\.log$", ".repairlog", os.path.join(runTimeConfig['logfolder'], repairList[i]))\
                                           , returnCodeList[i]\
                                           , os.path.join(runTimeConfig['logfolder'], repairList[i]) ))
                

        runTimeConfig["repair_info"] = repairInfo.getvalue()

    else :
        repairInfo.write(u"""
        unsupported hpctype for tardis_analyze.py : %s
        """%runTimeConfig["hpctype"])
        runTimeConfig["repair_info"] = repairInfo.getvalue()
    repairInfo.close()


def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        (options,toolargs) = getOptions(argv)
    except Usage, msg:
        print >> sys.stderr, msg
        return 2

    condorhpcJob.workingRoot = options['logfolder']

    print "tardis_analyze.py  using %s"%str(options)

    # get misc runtime details
    runTimeDict = getRunTime(options['logfolder'])

    if options["action"] == "repair" :
        getRepair(runTimeDict)
    elif options["action"] == "statistics" :
        getRepair(runTimeDict, True)

    print runTimeDict["repair_info"]


if __name__=='__main__':
    sys.exit(main())

