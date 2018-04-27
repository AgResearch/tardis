import string
import tardis.tutils.tutils as tutils

class hpcJob(object):
    """
    this class encapsulates an HPC oommand that we run, its state, output 
    and resources for starting it. It is the base class for
    localhpcJob, condorhpcJob and other job classes which
    add the detail needed for specific HPC environments
    
    references : 
        /home/galaxy/galaxy/database/pbs/*.sh   (for potential environment setup requirements for submitting jobs)
    """
    OK = 0
    ERROR = 1
    state = OK
    stateDescription = ""
    
    POLL_INTERVAL = 5 # seconds
    POLL_DURATION = 14 * 24 * 60 * 60 # two weeks

    MAX_DIMENSION = 999999 # the maximum number of chunks we will allow  - prevent incoherent chunking options generating huge numbers of fragments
    SUBMIT_RETRIES = 2
    SUBMIT_RETRY_PAUSE = 30

    @classmethod
    def stateAND(cls, state1, state2):
        """
        this method can be used to calculate the consensus state of a set of job states
        (without needing to know the encoding of the state)
        e.g.
        consensus = reduce(lambda x,y:hpcJob.stateAND(x,y), [list of states])
        """
        if state1 == cls.OK and state2 == cls.OK:
            return cls.OK
        else:
            return cls.ERROR

        
    def __init__(self, controller, command = []):
        super(hpcJob, self).__init__()
        self.controller = controller
        self.command = command
        self.scriptfilename = None
        self.jobfilename = None              
        self.logname = None
        self.stdoutnamepattern = None
        self.stderrnamepattern = None
        self.stdoutfilename = None
        self.stderrfilename = None
        self.sent = False   # whether we have sent our output
        self.submitreturncode = None
        self.submitCount = 0
        self.returncode = None
        self.pollCount = 0
        self.jobNumber = controller.hpcJobNumber
        self.logWriter = controller.logWriter
        self.workingRoot = controller.workingRoot
        self.jobHeld = False
        self.shell_script_template = None

    def get_templates(self,default_job_template_name, default_shell_template_name, default_runtime_config_template_name):
        """
        this method examines the run-time arguments supplied to tardis, and from these figures
        out a job template, shell template and runtime config template.
        The job template is used to create a job file for the scheduler (e.g. slurm) , for each job to be launched
        The shell template is used to create a wrapper shell (i.e. run1.sh, run2.sh etc) , for each task
        The runtime config template is used to generate source to be included in the wrapper shell  - i.e. in run1.sh, run2.sh etc.

        The run time arguments examined by this method specify whether the user wants to
        
        a) use one of the named, hard-coded (in tutils.py) templates
        b) supply the name of a file containing templating
        c) don't supply either a or b , in which case a default is used.

        (if they specify both a and b, this is an error)
        """
        (job_template, shell_script_template, runtime_config_template) = (None, None, None)
        
        if self.controller.options is not None:

            # figure out a job template from the options. (You can specify one of the inbuilt templates by name, or
            # supply a file containing a custom template)
            job_template_name = self.controller.options.get("job_template_name",None)
            job_template_filename = self.controller.options.get("jobtemplatefile",None)        
            
            # use default if there is one and its needed 
            if default_job_template_name is not None:
                if job_template_name is None and job_template_filename is None:
                    #use the default job template
                    job_template_name = default_job_template_name

            # check we have at least named template or template file but not both     
            if job_template_name is not None and job_template_filename is not None:
                raise tutils.tardisException("error both job_template_name (%s) and job_template_filename (%s) defined - only define one of these"%(job_template_name,job_template_filename) )
            elif job_template_name is None and job_template_filename is None:
                raise tutils.tardisException("error neither  job_template_name nor job_template_filename are defined (and no default available")

            if job_template_name is not None:
                job_template = tutils.getTemplateContent(self.controller.options, job_template_name, logWriter=self.logWriter)
            else:
                if not os.path.isfile(job_template_filename):
                    raise tutils.tardisException("error job template file %s not found"%job_template_filename )    
                job_template = string.join(file(job_template_filename,"r"),"")
                
            if job_template is None:
                raise tutils.tardisException("hpcJob: Error job template is null after templating")
            job_template = string.Template(job_template)



            # figure out a shell template from the options. (You can specify one of the inbuilt templates by name, or
            # supply a file containing a custom template)
            shell_template_name = self.controller.options.get("shell_template_name",None)
            shell_template_filename = self.controller.options.get("shelltemplatefile",None)        
            if shell_template_name is None and shell_template_filename is None:
                #use the default local shell template
                shell_template_name = default_shell_template_name
            if shell_template_name is not None and shell_template_filename is not None:
                raise tutils.tardisException("error both shell_template_name (%s) and shell_template_filename (%s) defined - only define one of these"%(shell_template_name,shell_template_filename) )

            if shell_template_name is not None:
                shell_script_template = tutils.getTemplateContent(self.controller.options, shell_template_name, logWriter=self.logWriter)
            else:
                shell_script_template = string.join(file(shell_template_filename,"r"),"")
                
            if shell_script_template is None:
                raise tutils.tardisException("hpcJob : Error shell template is null after templating")
            shell_script_template = string.Template(shell_script_template)


            # figure out run-time configuration code (You can specify one of the inbuilt configs by name, or
            # supply a file containing a custom config)
            runtime_config_template_name = self.controller.options.get("runtime_config_name",None)
            runtime_config_template_filename = self.controller.options.get("runtimeconfigsourcefile",None)

            # use default if available and needed. Note this logic means that if you supply a run-time config, then the
            # default will not be used - so if for example the default loads a base env, thne if you supply your own ,
            # you will need to explicitly load the base before doing your own
            # this is based on the assumption that its easier to do than to undo 
            if default_runtime_config_template_name is not None:
                if runtime_config_template_name is None and runtime_config_template_filename is None:
                    #use the default - for example this might load a deafult conda env or load a default module (site dependent)
                    runtime_config_template_name = default_runtime_config_template_name

            # don't want both named,  and a file 
            if runtime_config_template_name is not None and runtime_config_template_filename is not None:
                raise tutils.tardisException("error both runtime_config_template_name (%s) and runtime_config_template_filename (%s) defined - only define one of these"%(runtime_config_template_name,runtime_config_template_filename) )

            if runtime_config_template_name is not None:
                runtime_config_template = tutils.getTemplateContent(self.controller.options, runtime_config_template_name, logWriter=self.logWriter)
            else:
                runtime_config_template = string.join(file(runtime_config_template_filename,"r"),"")
                
            if runtime_config_template is None:
                raise tutils.tardisException("hpcJob : Error config template is null after templating")
            
            runtime_config_template = string.Template(runtime_config_template)


        return (job_template, shell_script_template, runtime_config_template)

        
    def error(self,errorMessage):
        self.logWriter.info("hpcJob setting error state, message = %s"%errorMessage)
        self.state = hpcJob.ERROR
        self.stateDescription += " *** error : %s *** "%errorMessage

    def induct(self,other):
        """
        this supports prototyping and also data sharing  - after the first hpcjob is created
        every new one is inducted by the last one. E.G. for localhpcjobs the induction will also copy
        the workerlist to the new job, so that all jobs have access to this. For other types
        of hpcjob, there is no induction required
        """
        return


    @classmethod
    def getUnsubmittedJobs(cls, jobList):
        return []

    def waitOnChildren(self):
        return 
    
    def getExitFootprint(self):
        return []
    
    def runCommand(self, argCommand=None):
        self.logWriter.info("hpcJob : runCommand is not implemented in the hpcJob base class")
        return

    def sendAvailableOutput(self, outputCollector, productCollector):
        self.logWriter.info("hpcJob : sendAvailableOutput is not implemented in the hpcJob base class")
        return


