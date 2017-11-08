import string, os, stat, subprocess, sys, re

import job.hpc as hpc

class localhpcJob(hpc.hpcJob):
    #HPC_ROOT=HPC_CONDITIONER_ROOT
    #MAX_PROCESSES = 20  # need to provide a way to set this         
    def __init__(self, controller, command = []):
        super(localhpcJob, self).__init__(controller, command)
            
        self.workerList  = {} # this will be overwritten by a shared worker list when the new object is inducted

        self.get_templates(None, "local_shell")

    def induct(self,other):
        super(localhpcJob,self).induct(other)
        other.workerList = self.workerList
        return

    @classmethod
    def getUnsubmittedJobs(cls, jobList):
        """
        get a list of jobs which look unsubmitted, according to jobHeld is True
        """
        retryJobs = [job for job in jobList if job.jobHeld]
        return retryJobs

    def waitOnChildren(self):
        """
        the tardis process has long running children when using this HPC class. We need to wait on this
        at various points to ensure we track the status of these jobs
        """
        for (workerpid, workerResult) in self.workerList.items():
            if workerResult == (0,0):
                self.logWriter.info("waiting on %d"%workerpid)
                try:
                    self.workerList[workerpid] = os.waitpid(workerpid, os.WNOHANG)
                    self.logWriter.info("wait returned %s"%str(self.workerList[workerpid]))
                except OSError as inst:
                    if inst.errno == errno.ECHILD :
                        self.logWriter.info("(no child processes)")
                    else:
                        self.logWriter.info("(unhandled OSError - re-raising)")
                        raise inst
        
        return 
    

    def getManifest(self, manifestFilter = None, isExcludeFilter = False, sensitivity = 0):
        """
        get output files , optionally matching a filter expression. Sensitivity
        controls how many of the files we return

        0 = none, just a message
        1 = all 
        """
        if sensitivity == 0:
            manifest = ["(not listing files for output manifest)"]
        else:
            manifest = os.listdir(self.workingRoot)
            if manifestFilter is not None:
                if not isExcludeFilter:
                    manifest = [item for item in manifest if re.search(manifestFilter, item)]
                else:
                    manifest = [item for item in manifest if re.search(manifestFilter, item) is None]

        return manifest

    
    
    def getRunningProcesses(self):
        """
        get a list of jobs which are running 
        """
        if len(self.workerList) == 0:
            return []

        #
        # 5/2014 bug - the call to ps can fail with 
        # OSError: [Errno 11] Resource temporarily unavailable
        # - unsure if process table full, or command line too long (many pids)
        # in any case unnecessary as the number of jobs should be the number of workers we are waiting on 
        #, it is unnecessary to invoke ps  
        # Deprecated code : 
        #ps_command = ["ps"] + [str(pid) for pid in cls.workerList]
        #proc = subprocess.Popen(ps_command,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #(stdout, stderr) = proc.communicate()
        #pidList = [record for record in re.split("\n", stdout) if re.search("^PID", record) == None and re.search("defunct", record) is None]
        #pidList = [re.split("\s+", record)[0] for record in shell_script_templatepidList]
        #pidList = [item for item in pidList if len(item) > 0]
        #return pidList


        return [item[0] for item in self.workerList.items() if item[1] == (0,0)]
        

    def runCommand(self, argCommand=None):
        command = argCommand
        if argCommand is None:
            command = self.command
        
        if len(command) > 0:
            self.logWriter.info("localhpcJob : running %s"%str(command))


            # set up the shell scriptfile(s) (one per chunk) (unless this is a rerun in which case its already been done)
            if self.submitCount == 0:
                shellcode = self.shell_script_template.safe_substitute(hpcdir=self.workingRoot,command=string.join(self.command," "))
                self.scriptfilename = os.path.join(self.workingRoot, "run%d.sh"%self.jobNumber)
                if os.path.isfile(self.scriptfilename):
                    raise tardisException("error %s already exists"%self.scriptfilename)
                f=open(self.scriptfilename,"w")
                self.logWriter.info("localhpcJob : local shell script wrapper is %s"%self.scriptfilename)
                f.writelines(shellcode)
                f.close()
                os.chmod(self.scriptfilename, stat.S_IRWXU | stat.S_IRGRP |  stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

                self.stdoutfilename = "%s.stdout"%self.scriptfilename
                self.stderrfilename = "%s.stderr"%self.scriptfilename
                self.stdoutnamepattern = os.path.basename(self.stdoutfilename)
                self.stderrnamepattern = os.path.basename(self.stderrfilename)

                self.logname = "%s.log"%self.scriptfilename
                self.submitCount += 1

            # launch the job if we can.
            # we can launch the job if jobs running < max_processes
            # first update process statuses
            self.waitOnChildren()
            
            running_processes = self.getRunningProcesses()
            #self.logWriter.info("running jobs : %s"%str(running_processes))
            #jobs_running = hpcConditioner.getJobSubmittedCount() - hpcConditioner.getResultsSentCount()
            if len(running_processes)  < self.controller.options["max_processes"]:
                self.logWriter.info("localhpcJob : launching %s"%self.scriptfilename)
                self.jobHeld = False
            else:
                self.logWriter.info("localhpcJob : not launching %s (jobs_running = %s)"%(self.scriptfilename, str(running_processes)))
                #self.logWriter.info("DEBUGx : job list is %s"%str(hpcConditioner.jobList))
                #self.logWriter.info("DEBUGx : job submit counts are  %s"%str([j.submitCount for j in hpcConditioner.jobList]))
                #self.logWriter.info("DEBUGx : jobs sent are  %d"%hpcConditioner.getResultsSentCount())
                self.jobHeld = True
                return

            local_submit = [self.scriptfilename]        
            if self.controller.options["dry_run"] :
                self.logWriter.info("localhpcJob : this is a dry run - not launching the job")
            else:
                self.logWriter.info("localhpcJob : forking to execute %s"%str(local_submit))
                self.jobHeld = False

                # now fork - if we are the parent, return, if we are the child execute the job and the exit
                # before forking, however, do an asynchronous waitpid to clean up defunct processes
                # (currently we don't do anything with these results - we just want to
                # do a wait so that the child processes can be removed from the process table
                #self.logWriter.info("localhpcJob : checking waits")
                #try:
                #    pidresults = os.waitpid(0, os.WNOHANG)
                #    self.logWriter.info("wait returned : %s"%str(pidresults))  # returns [(pid, status), (pid,status),...]
                #    pidresultsDict = dict([(pidresult[0], pidresult) for pidresult in [pidresults]])
                #    self.workerList.update(pidresultsDict)
                #except OSError as inst:
                #    if inst.errno == 10:
                #        self.logWriter.info("(no child processes)")
                #    else:
                #        self.logWriter.info("(unknown OSError - re-raising)")
                #        raise inst

                try:
                    me = os.fork()
                    if me == 0:
                        mypid = os.getpid()
                        with open(self.logname,"w") as l:
                            print >> l, "job starting pid %d"%mypid 

                        fstdout=open(self.stdoutfilename,"w")
                        fstderr=open(self.stderrfilename,"w")

                        self.proc = subprocess.Popen(local_submit,stdout=fstdout, stderr=fstderr)
                        self.proc.communicate()
                        fstdout.close()
                        fstderr.close()                    
                        self.submitreturncode = self.proc.returncode                
                        self.logWriter.info("localhpcJob : %s (pid %d) has returned (status %s)"%(str(local_submit), os.getpid(), self.submitreturncode))
                        self.logWriter.info("localhpcJob : stdout was written to %s"%self.stdoutfilename)
                        self.logWriter.info("localhpcJob : stderr was written to %s"%self.stderrfilename)
                        self.logWriter.info("localhpcJob : child %d exiting"%os.getpid())
                        with open(self.logname,"a") as l:
                            print >> l, "pid %d job terminated return value %d"%(os.getpid(), self.submitreturncode)
                        sys.exit(0)
                    else:
                        self.workerList[me] = (0,0)
                        self.submitCount += 1
                        self.submitreturncode = 0
                        self.logWriter.info("localhpcJob : parent returning")
                        return
                except OSError,e:
                    self.logWriter.info("localhpcJob : warning - fork of %s failed with OSError : %s"%(self.scriptfilename, e))
                    self.logWriter.info("localhpcJob : job %s held "%self.scriptfilename)
                    self.jobHeld = True

        else:
	    self.logWriter.info("hpcJob : nothing to do")

    def getExitFootprint(self):
        """
        get the exit footprint of a job - i.e. at least the
        return code.

        read the log file to see if we are done. We are looking for (e.g.)
        
         005 (23334.000.000) 04/20 12:36:19 Job terminated.
         (1) Normal termination (return value 0)
         - when we find some output , send this to the co-routine
        
        """

        # no point looking if we haven't started
        if self.jobHeld :
            return

        r=open(self.logname)
        matches=[None, None, None]

        # index names
        TERMINATED = 0
        RETURNED = 1
        EVICTED = 2
        
        for record in r:
            if matches[TERMINATED] is None and matches[EVICTED] is None:
                matches[TERMINATED] = re.search("job terminated", record, re.IGNORECASE)
                matches[EVICTED] = re.search("job evicted", record, re.IGNORECASE)

            if not matches[EVICTED] is None:
                self.error("one or more jobs were evicted - setting error")
                self.returncode = 999
                break

            if not matches[TERMINATED] is None:    
                matches[RETURNED] = re.search("return value (\d+)", record, re.IGNORECASE)

                if not matches[RETURNED] is None:
                    self.logWriter.info("hpcJob : this job (%d) looks finished"%self.jobNumber)
                    self.returncode = int(matches[1].groups()[0])
                    if self.returncode != 0:
                        self.error("job number %d returned %d - setting error"%(self.jobNumber, self.returncode))
                        
                    break
        r.close()
        return
        
    
    def sendAvailableOutput(self, outputCollector, productCollector):
        if self.sent:
            return

        # don't look any further if we haven't started - instead, update the job status information
        # (potentially allowing held jobs to be started, when the outer loop that calls this , goes on
        # to call retry
        if self.jobHeld:
            self.waitOnChildren()
            return

        self.pollCount += 1

        # if we haven't started, wait and then hand back control to the caller who may then 
        # be able to start the job
        if self.submitCount == 0:
            time.sleep(self.POLL_INTERVAL)
            return

        # protect against race condition , accessing log file before ready
        if not os.path.isfile(self.logname):   # job might not have quite got going yet - wait , then try again
            time.sleep(self.POLL_INTERVAL)
            if not os.path.isfile(self.logname):
                self.logWriter.info("sendAvailableOutput : so far failed to find log file %s"%self.logname)
                return


        #self.logWriter.info("DEBUG1 job %s opening %s pollCount %d to check for finished"%(self.jobNumber, self.logname, self.pollCount))
        self.getExitFootprint()
        

        if not self.returncode is None:
            #manifest = os.listdir(self.workingRoot)
            #manifest = [item for item in manifest if re.search("(\.log$)|(\.sh\.)|(\.sh$)|(\.job$)", item)]
            manifest = self.getManifest("(\.log$)|(\.sh\.)|(\.sh$)|(\.job$)")
            outputCollector.send(manifest)
            product_manifest = self.getManifest("(\.log$)|(\.sh\.)|(\.sh$)|(\.job$)", True, 1)
            productCollector.send(product_manifest)
            

            # get standard output and error filenames from the job
            stdoutlist = [item for item in os.listdir(self.workingRoot) if re.search(self.stdoutnamepattern, item) != None]
            if len(stdoutlist) != 1:
                self.logWriter.info("hpcJob : warning could not find unique match for stdout file using %s, in the manifest ( %s )"%(self.stdoutnamepattern, str(os.listdir(self.workingRoot))))

            stderrlist = [item for item in os.listdir(self.workingRoot) if re.search(self.stderrnamepattern, item) != None]
            if len(stderrlist) != 1:
                self.logWriter.info("hpcJob : warning could not find unique match for stderr file using %s, in the manifest ( %s )"%(self.stderrnamepattern, str(os.listdir(self.workingRoot))))
                
            self.sent = True
        
        #if not self.sent:
        #    #self.logWriter.info("DEBUG1 job %d not finished, sleeping before continuing"%self.jobNumber)
        #    time.sleep(self.POLL_INTERVAL)
            
        # sanity check
        #if self.pollCount * self.POLL_INTERVAL > self.POLL_DURATION:
        #    raise tardisException("error in tardis.py session - bailing out as we have been hanging around waiting for output for far too long ! ")


