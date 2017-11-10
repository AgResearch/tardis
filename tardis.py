#!/usr/bin/env python
import sys, time, string 

import tutils.tutils as tutils
import conditioner.factory as factory
import conditioner.data as data
import job.local as local
import job.hpc as hpc


def run(toolargs, client_options, stdout = sys.stdout, stderr=sys.stderr, checkCommandIsValid = True):

    # get the options to use, incorporating this server's options and what we have from the client
    options = tutils.getOptions(None, client_options)

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
    print "tool args = %s"%str(toolargs)


    # set up logging and working folder for this run
    (l,workingRoot) = factory.hpcConditioner.getLogger(options)
    logger = tutils.tardisLogger(l)

    # log msg_for_log if we have one 
    if msg_for_log is not None:
        logger.info(msg_for_log)

                                                              
    print "tardis.py : logging this session to %s"%workingRoot
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
    # just sets up the comamnd.
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
                    c.retryJobSubmission(maxRetries = SUBMIT_RETRIES, retryPause = SUBMIT_RETRY_PAUSE)

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
        print "tardis.py : done logging this session to %s , no errors detected"%workingRoot
        if len(c.getJobResultStateDescription()) > 0:
            print c.getJobResultStateDescription()
            print >> stderr, c.getJobResultStateDescription()        
        if len(dcPrototype.getDataResultStateDescription()) > 0:
            print dcPrototype.getDataResultStateDescription()
            print >> stderr, dcPrototype.getDataResultStateDescription()
        return 0
    else:
        print "tardis.py : done logging this session to %s. NOTE : some errors were logged"%workingRoot
        if len(c.getJobResultStateDescription()) > 0:
            print c.getJobResultStateDescription()        
            print >> stderr, c.getJobResultStateDescription()        
        if len(dcPrototype.getDataResultStateDescription()) > 0:
            print dcPrototype.getDataResultStateDescription()        
            print >> stderr, dcPrototype.getDataResultStateDescription()
            
        return 2


        
def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        (options,toolargs) = tutils.getOptions(argv)
    except tutils.Usage, msg:
        print >> sys.stderr, msg
        return 2
    #except tutils.tardisException, msg:
    except tutils.tardisException, msg:

        print >> sys.stderr, msg
        return 2
        
    

    exit_code = run(toolargs,options, checkCommandIsValid = False)

    if options["batonfile"] is not None:
        pass_the_baton(options["batonfile"], exit_code)

    return exit_code 
    
        
if __name__=='__main__':
    sys.exit(main())

