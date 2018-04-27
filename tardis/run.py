#!/usr/bin/env python
import argparse, re, sys, time, string

import tardis.tutils.tutils as tutils
import tardis.conditioner.factory as factory
import tardis.conditioner.data as data
import tardis.job.local as local
import tardis.job.hpc as hpc
from tardis.tutils.tutils import tardisException

def run(toolargs, options, stdout = sys.stdout, stderr=sys.stderr, checkCommandIsValid = True):
    # some merging / prioritisation of options is needed in some cases.
    msg_for_log=None # we don't have a logger yet - will log this later when we do
    if options.get("job_template_name",None) is not None and options.get("jobtemplatefile",None) is not None:
        msg_for_log= "warning - a job template filename was specified (%s) - this overrides the job template name specified (%s)"%(options.get("jobtemplatefile",None), options.get("job_template_name",None))
        del options["job_template_name"]


    # if we check the command is supported on this tardis
    if checkCommandIsValid:
        if not isCommandValid(toolargs, options["valid_command_patterns"]):
            print >> stderr, "%s is not supported by this tardis engine"%toolargs[0]            
            return 2


    #print "using %s"%str(options)
    if not options["quiet"]:
        print "tool args = %s"%str(toolargs)


    # set up logging and working folder for this run
    (l,workingRoot) = factory.hpcConditioner.getLogger(options)
    logger = tutils.tardisLogger(l)

    # log msg_for_log if we have one 
    if msg_for_log is not None:
        logger.info(msg_for_log)


    if not options["quiet"]:                                                     
        print "tardis.py : logging this session to %s"%workingRoot

    logger.info("tardis options : " + str(options))
    
    c = factory.hpcConditioner(logger,workingRoot,options,toolargs)
    c.options = options
    c.logWriter.info("tardis.py : logging this session to %s"%workingRoot)        
    #c.logWriter.info("using %s"%str(options))
    c.logWriter.info("tool args = %s"%str(toolargs))
    

    # create a prototype data conditioner. This won't actually do any data conditioning,
    # but will be used to induct subsequent conditioners, by passing on shared
    # shared objects
    dcPrototype=data.dataConditioner()
    dcPrototype.logWriter = logger 
    dcPrototype.workingRoot = workingRoot
    dcPrototype.jobcontroller = c    
    dcPrototype.logWriter.info("prototype dataConditioner created")
    dcPrototype.options = options

    #hpcConditioner.logWriter.info("main : requesting conditioned commands")
    conditionedCommandIter = c.getConditionedCommandGenerator(dcPrototype)
    conditionedInputGenerators = dcPrototype.getConditionedInputGenerators()    
    for conditionedInputs in conditionedInputGenerators:
        dcPrototype.distributeAvailableInputs(conditionedInputs)
        cmd = conditionedCommandIter.next()

        c.logWriter.info("setting up job for conditioned command : %s"%str(cmd)) 
        job = c.gethpcJob(cmd)
        job.runCommand()


        # check for partially submitted jobs here in case we are rate limited - otherwise we will have to wait until all chunks
        # have been written. This will also do a wait on the jobs that are running
        if c.hpcClass == local.localhpcJob:
            c.logWriter.info("(running jobs locally and there are %d partially submitted jobs)"%len(c.getUnsubmittedJobs()))
            if len(c.getUnsubmittedJobs()) > 0:
                c.retryJobSubmission(maxRetries = 1, retryPause = 1)

    # for some hpc types (e.g. slurm array jobs) , runCommand does not actually run the command, it
    # just sets up the comamnd. Thse are then all batch submitted here : 
    c.launchArrayJobs()
    
                    

    # if in a workflow, or conditioning output, and not a dry run , poll for results
    if (options["in_workflow"] or len(dcPrototype.outputUnconditioners) > 0) and not options["dry_run"] :
        c.logWriter.info("tardis.py : done setting up jobs - polling for results (and submitting any queued jobs)")
        for dc in dcPrototype.outputUnconditioners:    # (if in a workflow and no unconditioners were specified, then a default one
                                                           # will have been created)

            # results are sent to each output unconditioner
            # clear sent flag
            for job in c.jobList:
                job.sent = False

            poll_count = 0
            while True:
                poll_count +=1
                if poll_count * hpc.hpcJob.POLL_INTERVAL > hpc.hpcJob.POLL_DURATION:
                    raise tardisException("error in tardis.py session - bailing out as we have been hanging around waiting for output for far too long ! ")
                
                
                unsentJobs = [ job for job in c.jobList if not job.sent ]
                if len(unsentJobs) == 0:
                    break

                # retry jobs here in case we are rate limited
                if len(c.getUnsubmittedJobs()) > 0:
                    c.logWriter.info("(there are %d partially submitted jobs)"%len(c.getUnsubmittedJobs()))
                    c.retryJobSubmission(maxRetries = hpc.hpcJob.SUBMIT_RETRIES, retryPause = hpc.hpcJob.SUBMIT_RETRY_PAUSE)

                sent_count = 0 # count how many jobs just finished 
                for unsentJob in unsentJobs:
                    unsentJob.sendAvailableOutput(dc.outputCollector, dc.productCollector)
                    if unsentJob.sent:
                        sent_count += 1

                # if no jobs just finished , wait for awhile , otherwise go back for more output immediately
                if sent_count == 0:
                    time.sleep(hpc.hpcJob.POLL_INTERVAL)
                    



        c.logWriter.info("%s output unconditioners are unconditioning"%len(dcPrototype.outputUnconditioners))
        # uncondition all output 
        for dc in dcPrototype.outputUnconditioners:
            dc.unconditionOutput()

            
        # only remove the conditioned output if we are in a workflow and we are not sampling  and no error state was set , and KEEP_CONDITIONED_DATA is
        # not set
        if options["in_workflow"] and options["samplerate"] is None and dcPrototype.getDataResultState() == data.dataConditioner.OK and \
                   c.getJobResultState()  == hpc.hpcJob.OK and not options["keep_conditioned_data"]:
            for dc in dcPrototype.outputUnconditioners:            
                dc.logWriter.info("removing conditioned output") 
                dc.removeConditionedOutput()
        else:
            c.logWriter.info("either not in workflow or sampling , or error state set , not removing conditioned output")


        # stream the output from all jobs to stdout of this job
        c.unconditionJobStreams(stdout,stderr)

                                               
        # do not uncondition input if sampling , or if options["keep_conditioned_data"] is set, or if an error state has been set
        if options["samplerate"] is None and  dcPrototype.getDataResultState() == data.dataConditioner.OK \
                   and c.getJobResultState()  == hpc.hpcJob.OK and not options["keep_conditioned_data"]:

            c.logWriter.info("%s input conditioners are unconditioning the following files : %s"%\
                                           (len(dcPrototype.getDistinctInputConditioners()),\
                                            string.join([dc.inputFileName for dc in dcPrototype.getDistinctInputConditioners()]," , ")\
                                            )\
                                           )
            c.logWriter.info("unconditioning input")
            
            for dc in dcPrototype.getDistinctInputConditioners():
                dc.removeConditionedInput()
        else:
            c.logWriter.info("not unconditioning input as either sampling was specified, or keep conditioned input was set, or error state is set due to a previous error")


    else:
        c.logWriter.info("tardis.py : not in a workflow and no output unconditioners (or this is a dry run) - exiting")


    if dcPrototype.getDataResultState() == data.dataConditioner.OK and c.getJobResultState()  == hpc.hpcJob.OK :
        c.logWriter.info("tardis.py : done logging this session to %s , no errors detected"%workingRoot)
        if not options["quiet"]:
            print "tardis.py : done logging this session to %s , no errors detected"%workingRoot
        if len(c.getJobResultStateDescription()) > 0:
            c.logWriter.info(c.getJobResultStateDescription())
            if not options["quiet"]:
                print c.getJobResultStateDescription()
                print >> stderr, c.getJobResultStateDescription()        
        if len(dcPrototype.getDataResultStateDescription()) > 0:
            c.logWriter.info(dcPrototype.getDataResultStateDescription())
            if not options["quiet"]:
                print dcPrototype.getDataResultStateDescription()
                print >> stderr, dcPrototype.getDataResultStateDescription()
        return (0,c)
    else:
        c.logWriter.info("tardis.py : done logging this session to %s. NOTE : some errors were logged"%workingRoot)
        if not options["quiet"]:
            print "tardis.py : done logging this session to %s. NOTE : some errors were logged"%workingRoot
        if len(c.getJobResultStateDescription()) > 0:
            c.logWriter.info(c.getJobResultStateDescription())
            if not options["quiet"]:
                print c.getJobResultStateDescription()
                print >> stderr, c.getJobResultStateDescription()        
        if len(dcPrototype.getDataResultStateDescription()) > 0:
            c.logWriter.info(dcPrototype.getDataResultStateDescription())
            if not options["quiet"]:
                print dcPrototype.getDataResultStateDescription()        
                print >> stderr, dcPrototype.getDataResultStateDescription()
            
        return (2,c)


        
def tardis_main():
    parser = argparse.ArgumentParser(description='Condition a command for execution on a cluster.')
    parser.add_argument('-w', '--in-workflow', dest='in_workflow', action='store_const', const=True, help='Run the command as part of a workflow. After launching all of the jobs, tardis waits for all outputs, which are then collated and merged into a single output file, as specified by the output file path in the original command; all of the temporary input files (for example chunks of uncompressed fastq) are deleted provided all prior steps completed without error (if there was an error they are left there to assist with debugging). Without this option, the program exits immediately after launching all of the jobs, and output is left un-collated in the scratch folder created by this script, and no cleanup is done.')
    parser.add_argument('-c', '--chunksize', dest='chunksize', type=int, metavar='N', help='When conditioning the input file(s), split into files each containing N logical records. (A logical record for a sequence file is a complete sequence. For a text file it is a line of text). (If the -s option is used to sample the inputs, the chunksize relates to the full un-sampled file . so the same chunk-size can be used whether random sampling or not. For example a chunksize of 1,000,000 is specified in combination with a sampling rate of .0001, then each chunk would contain 100 sequences . i.e. you should not adjust the chunk-size, for the sampling rate. Note that to avoid a race-condition that could be caused by a very small chunk-size resulting in launching a very large number of jobs, tardis will throw an exception if the chunk-size used would result in launching more than MAX_DIMENSION jobs (currently 5000) )')
    parser.add_argument('--from', '--from-record', dest='from_record', type=int, metavar='N', help='When conditioning the input file(s), only use records from the input file after or including N (where that is logical record number . e.g. in a fastq file, start from record number N means start from sequence N). By combining this option with -to, you can process slices of a file. Note that this option has no affect when processing a list-file.')
    parser.add_argument('--to', '--to-record', dest='to_record', type=int, metavar='N', help='When conditioning the input file(s), only use records up to and including the record N (where that is logical record number . e.g. in a fastq file, process up to record number N means process up to and including sequence N). By combining this option with    -from, you can process slices of a file. Note that this option has no affect when processing a list-file.')
    parser.add_argument('-s', dest='samplerate', type=float, metavar='RATE', help='Rather than process the entire input file(s), a random sample of the records is processed. RATE is the probability that a given record will be sampled. For example -s .001 will result in roughly 1 in every 1000 logical records being sampled.  When the -s option is specified, tardis does not clean up the conditioned input and output . e.g. all of the uncompressed fastq sample fragments would be retained. These are retained to assist with the Q/C work that is normally associated with a sampled run. Paired fastq input files are sampled in lock-step, provided the paired fastq conditioning directive is used for both files.')
    parser.add_argument('-d', '--rootdir', dest='rootdir', type=str, metavar='DIR', help='create the tardis working folder under DIR. If no working root is specified, a default location is used.')
    parser.add_argument('--dry-run', dest='dry_run', action='store_const', const=True, help='validate the run by doing a dry run. This means that the chunks, job scripts and job files etc. are all generated but the jobs are not launched. The user can start then kill (CTRL-C) the run, inspect the script and job files that were generated to check that their command has been conditioned as envisaged.')
    parser.add_argument('-k', '--keep-conditioned-data', dest='keep_conditioned_data', action='store_const', const=True, help='keep the conditioned input and output - i.e. the input and output fragments. Normally in workflow mode these are deleted after the output is successfully "unconditioned" - i.e. joined back together')
    parser.add_argument('--job-file', dest='jobtemplatefile', type=str, metavar='FILE', help='optionally supply a job template - tardis will read the contents of FILE and use this as the job template.')
    parser.add_argument('--templatedir', dest='templatedir', type=str, metavar='DIR', help='template directory')
    parser.add_argument('--job-template-name', dest='job_template_name', type=str, metavar='NAME', help='job template name, resolved in template directory')
    parser.add_argument('--hpctype', dest='hpctype', type=str, help='indicate the hpc environment. Currently the only supported values are: condor which results in tardis attempting to set up and launch condor jobs; local which results in each job being launched by tardis itself on the local machine, using the native python sub-process API. The maximum number of processes it will run at a time is controlled by a global variable in the script MAX_PROCESSES, which is initially 10; slurm which results in tardis attempting to set up and launch slurm jobs.')
    parser.add_argument('--batonfile', dest='batonfile', type=str, metavar='FILE', help='if you supply a "baton file" FILE, tardis will write the process exit code to this file after all processing has completed. This can be useful to preserve synchronous execution of a workflow, even if tardis is started in the background - the workflow can test the existence of the batonfile - if it exists then the corresponding tardis processing step has completed (i.e. another way of each step in a workflow "passing the baton" to the next step)')
    parser.add_argument('--shell-include-file', '--runtimeconfigsourcefile', dest='runtimeconfigsourcefile', type=str, metavar='FILE', help='shell script fragment included in jobs')
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_const', const=True, help='run quietly')
    parser.add_argument('--config', dest='config', metavar='FILE', help='configuration file')
    parser.add_argument('command', help='command to run')
    parser.add_argument('arg', nargs='*', help='command arguments')

    args = parser.parse_args()
    options = dict((k,v) for k,v in vars(args).iteritems() if v is not None and k != 'command')

    # filter command args
    command_args = [args.command] + args.arg
    for arg in command_args:
        if re.search("[\!\&]|(?<!\w)rm(?!\w)|(?<!\w)mv(?!\w)|(?<!\w)cp(?!\w)", arg) != None: # do not allow irrelevant/dangerous shell chars
            raise tardisException("error : dangerous argument to shell ( %s ) - will not run this"%arg)
            #args.remove(arg)
    if len(args.command) < 1:
        raise tardisException("please supply a valid command to condition and run (type tardis -h for usage")

    try:
        options = tutils.mergeOptionsWithConfig(options)
    except tutils.tardisException, msg:

        print >> sys.stderr, msg
        return 2

    (exit_code, factory) = run(command_args, options, checkCommandIsValid = False)

    if options["batonfile"] is not None:
        tutils.pass_the_baton(options["batonfile"], exit_code, factory.logWriter)

    return exit_code


if __name__=='__main__':
    sys.exit(tardis_main())

