import string, os, stat, subprocess, sys, re, time
from job import hpc

class slurmhpcJob(hpc.hpcJob):
    def __init__(self, controller, command = [],job_template= None, shell_script_template = None):
        super(slurmhpcJob, self).__init__(controller,command)
        
        (junk, self.shell_script_template, self.runtime_config_template) = self.get_templates(None, "slurm_shell", "basic_slurm_runtime_environment")


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
            self.logWriter.info("slurmhpcJob : running %s"%str(command))

            # set up the shell scriptfile(s) (one per chunk) (unless this is a rerun in which case its already been done)
            if self.submitCount == 0:
                self.scriptfilename = os.path.join(self.workingRoot, "run%d.sh"%self.jobNumber)
                if os.path.isfile(self.scriptfilename):
                    raise tardisException("error %s already exists"%self.scriptfilename)
                
                runtime_environmentcode = self.runtime_config_template.safe_substitute() # currently no templating actually done here
                self.logname=re.sub("\.sh$",".tlog",self.scriptfilename)
                shellcode = self.shell_script_template.safe_substitute(configure_runtime_environment=runtime_environmentcode, \
                                                                       hpcdir=self.workingRoot,\
                                                                       command=string.join(self.command," "),\
                                                                       tlog=self.logname, startdir=self.controller.options["startdir"])
                
                f=open(self.scriptfilename,"w")
                self.logWriter.info("slurmJob : slurm shell script wrapper is %s"%self.scriptfilename)
                f.writelines(shellcode)
                f.close()
                os.chmod(self.scriptfilename, stat.S_IRWXU | stat.S_IRGRP |  stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )


                self.stderrnamepattern = "^run-\d+_%s.stderr$"%self.jobNumber
                self.stdoutnamepattern = "^run-\d+_%s.stdout$"%self.jobNumber
                
        else:
	    self.logWriter.info("slurmJob : nothing to do")


    def getExitFootprint(self):
        """
        get the exit footprint of a job - i.e. at least the
        return code.

        - basically read the log file to see if we are done. We are looking for (e.g.)
         005 (23334.000.000) 04/20 12:36:19 Job terminated.
         (1) Normal termination (return value 0)
         - when we find some output , send this to the co-routine

        """
        with open(self.logname,"r") as r:
            footprint = {}
            footprint.update( ( re.split("=", record.strip()) for record in r if len(re.split("=", record)) == 2))

            if "job_ended" in footprint:
                self.logWriter.info("slurmhpcJob : this job (%d) looks finished"%self.jobNumber)
                self.returncode = int(footprint["job_exit_code"])
                if self.returncode != 0:
                    self.error("job number %d returned %d - setting error"%(self.jobNumber, self.returncode))
          

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
            manifest = self.getManifest("(\.tlog$)|(\.sh\.)|(\.sh$)|(\.job$)")    # default - i.e. don't actually get real manifest as not used 
            outputCollector.send(manifest)
            self.logWriter.info("slurmhpcJob.sendAvailableOutput : getting a product manifest") 
            product_manifest = self.getManifest("(\.tlog$)|(\.log$)|(\.sh\.)|(\.sh$)", True,1)   # do get specific files for this job
            #self.logWriter.info("slurmhpcJob.sendAvailableOutput : got %s"%str(product_manifest))                                
            productCollector.send(product_manifest)

            # get standard output and error filenames from the job
            stdoutlist = [item for item in os.listdir(self.workingRoot) if re.search(self.stdoutnamepattern, item) != None]
            if len(stdoutlist) != 1:
                self.logWriter.info("slurmhpcJob : warning could not find unique match for stdout file using %s, in the manifest ( %s )"%(self.stdoutnamepattern, str(os.listdir(self.workingRoot))))
            else:
                self.stdoutfilename =  os.path.join(self.workingRoot,stdoutlist[0])

            stderrlist = [item for item in os.listdir(self.workingRoot) if re.search(self.stderrnamepattern, item) != None]
            if len(stderrlist) != 1:
                self.logWriter.info("slurmhpcJob : warning could not find unique match for stderr file using %s, in the manifest ( %s )"%(self.stderrnamepattern, str(os.listdir(self.workingRoot))))
            else:
                self.stderrfilename =  os.path.join(self.workingRoot,stderrlist[0])
                
            self.sent = True

            


