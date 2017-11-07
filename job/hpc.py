import string

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

        # sometimes we create a skeleton hpcJob just to access various methods - so options may not be set
        if self.controller.options is not None:
            shell_template_name = self.controller.options.get("shell_template_name",None)
            if shell_template_name is None:
                raise tardisException("error mandatory option shell_template_name not found")
            shell_template = self.controller.options.get(shell_template_name, None)
            if shell_template is None:
                raise tardisException("Error shell template %s not found in options"%shell_template_name)
            self.shell_script_template = string.Template(shell_template)
        
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


