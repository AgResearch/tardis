import string, os, stat, subprocess, sys, re
from job import hpc

class slurmhpcJob(hpc.hpcJob):
    def __init__(self, controller, command = [],job_template= None, shell_script_template = None):
        super(slurmhpcJob, self).__init__(controller,command)

        (self.job_template, self.shell_script_template) = self.get_templates("slurm_array_job", "slurm_shell")

        

    @classmethod
    def getUnsubmittedJobs(cls, jobList):
        """
        get a list of jobs which look unsubmitted, according to
            a) the return code from the submit call is != None and != 0
            b) the returncode for the job itself is None
            c) results-sent is False
        """
        retryJobs = [job for job in jobList if job.returncode is None and job.submitreturncode != None and job.submitreturncode != 0 and not job.sent]
        return retryJobs

    def waitOnChildren(self):
        """
        the tardis process does not normally have any children when using this HPC class (apart from
        briefly while queuing a job on the cluster, and the API used there involves a synchonous
        wait on the process)
        """
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
        #self.logWriter.info("DEBUG: slurmhpcJob.getManifest returning listing : %s"%str(manifest))
        return manifest
    

    def runCommand(self, argCommand=None):
        command = argCommand
        if argCommand is None:
            command = self.command
        
        if len(command) > 0:
            self.logWriter.info("hpcJob : running %s"%str(command))

            # set up the shell scriptfile(s) (one per chunk) (unless this is a rerun in which case its already been done)
            if self.submitCount == 0:
                shellcode = self.shell_script_template.safe_substitute(hpcdir=self.workingRoot,command=string.join(self.command," "))
                
                self.scriptfilename = os.path.join(self.workingRoot, "run%d.sh"%self.jobNumber)
                if os.path.isfile(self.scriptfilename):
                    raise tardisException("error %s already exists"%self.scriptfilename)
                f=open(self.scriptfilename,"w")
                self.logWriter.info("hpcJob : slurm shell script wrapper is %s"%self.scriptfilename)
                f.writelines(shellcode)
                f.close()
                os.chmod(self.scriptfilename, stat.S_IRWXU | stat.S_IRGRP |  stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )


            # set up the slurm jobfile  (one per chunk) (unless already done)
            if self.submitCount == 0:
                self.logname=re.sub("\.sh$",".log",self.scriptfilename)
                self.stderrnamepattern = "%s\.err\.\S+$"%re.escape(os.path.basename(self.scriptfilename))
                self.stdoutnamepattern = "%s\.out\.\S+$"%re.escape(os.path.basename(self.scriptfilename))
                self.jobfilename=re.sub("\.sh$",".job",self.scriptfilename)
                jobcode = self.job_template.safe_substitute(script=self.scriptfilename,log=self.logname,rundir=self.workingRoot)
                self.logWriter.info("hpcJob : slurm job file is %s"%self.jobfilename)
                f=open(self.jobfilename,"w")
                f.writelines(jobcode)
                f.close()

            # submit the slurm job 
            slurm_submit = ["slurm_submit", self.jobfilename]
            if self.controller.options["dry_run"] :
                self.logWriter.info("hpcJob : this is a dry run - not launching the job")
            else:
                self.logWriter.info("hpcJob : launching using %s"%str(slurm_submit))
                self.proc = subprocess.Popen(slurm_submit,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.submitCount += 1
                (self.stdout, self.stderr) = self.proc.communicate()
                self.submitreturncode = self.proc.returncode                
                self.logWriter.info("hpcJob : %s has returned (status %s) - here is its output (but now we wait for the real output !)"%(str(slurm_submit), self.submitreturncode))
                self.logWriter.info("hpcJob : stdout : \n%s"%self.stdout)
                self.logWriter.info("hpcJob : stderr : \n%s"%self.stderr)
                    
        else:
	    self.logWriter.info("hpcJob : nothing to do")


    def getExitFootprint(self):
        """
        get the exit footprint of a job - i.e. at least the
        return code.

        - basically read the log file to see if we are done. We are looking for (e.g.)
         005 (23334.000.000) 04/20 12:36:19 Job terminated.
         (1) Normal termination (return value 0)
         - when we find some output , send this to the co-routine

        """
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

    
        
        

    def sendAvailableOutput(self, outputCollector, productCollector):
        if self.sent:
            return

        self.pollCount += 1
        if not os.path.isfile(self.logname):   # job might not have started yet - wait , then try again
            time.sleep(self.POLL_INTERVAL)
            if not os.path.isfile(self.logname):
                self.logWriter.info("sendAvailableOutput : so far failed to find log file %s"%self.logname)
                return

        #self.logWriter.info("DEBUG1 job %s opening %s pollCount %d to check for finished"%(self.jobNumber, self.logname, self.pollCount))
        self.getExitFootprint()

        if not self.returncode is None:
            #manifest = os.listdir(self.workingRoot)
            #manifest = [item for item in manifest if re.search("(\.log$)|(\.sh\.)|(\.sh$)|(\.job$)", item)]
            manifest = self.getManifest("(\.log$)|(\.sh\.)|(\.sh$)|(\.job$)")    # default - i.e. don't actually get real manifest as not used 
            outputCollector.send(manifest)
            self.logWriter.info("slurmhpcJob.sendAvailableOutput : getting a product manifest") 
            product_manifest = self.getManifest("(\.log$)|(\.sh\.)|(\.sh$)|(\.job$)", True,1)   # do get specific files for this job
            #self.logWriter.info("slurmhpcJob.sendAvailableOutput : got %s"%str(product_manifest))                                
            productCollector.send(product_manifest)

            # get standard output and error filenames from the job
            stdoutlist = [item for item in os.listdir(self.workingRoot) if re.search(self.stdoutnamepattern, item) != None]
            if len(stdoutlist) != 1:
                self.logWriter.info("hpcJob : warning could not find unique match for stdout file using %s, in the manifest ( %s )"%(self.stdoutnamepattern, str(os.listdir(self.workingRoot))))
            else:
                self.stdoutfilename =  os.path.join(self.workingRoot,stdoutlist[0])

            stderrlist = [item for item in os.listdir(self.workingRoot) if re.search(self.stderrnamepattern, item) != None]
            if len(stderrlist) != 1:
                self.logWriter.info("hpcJob : warning could not find unique match for stderr file using %s, in the manifest ( %s )"%(self.stderrnamepattern, str(os.listdir(self.workingRoot))))
            else:
                self.stderrfilename =  os.path.join(self.workingRoot,stderrlist[0])
                
            self.sent = True

            
        
        #if not self.sent:
        #    #self.logWriter.info("DEBUG1 job %d not finished, sleeping before continuing"%self.jobNumber)
        #    time.sleep(self.POLL_INTERVAL)
            
        # sanity check
        #if self.pollCount * self.POLL_INTERVAL > self.POLL_DURATION:
        #    raise tardisException("error in tardis.py session - bailing out as we have been hanging around waiting for output for far too long ! ")



