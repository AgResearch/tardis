import sys, exceptions, os, re, ConfigParser, logging, tempfile

class tardisException(exceptions.Exception):
    def __init__(self,args=None):
        super(tardisException, self).__init__(args)

class tardisLogger(object):
    def __init__(self, logger = None):
        super(tardisLogger,self).__init__()
        self.loggerInstance = logger
    
    def info(self, text):
        if self.loggerInstance is not None:
            self.loggerInstance.info(text)



def getSampleBool(samplerate):
   # sample from bernoulli p=1/samplerate  if we are sampling, or if not return 1
   if samplerate is None:
      return 1
   elif samplerate <= 0:
      return 1
   elif samplerate >= 1.0:
      return 1
   else:
      uniform = random.random()
      if uniform <= samplerate:
         return 1
      else:
         return 0

def SAMHeadersEqual(h1, h2, linesep = "\n"):
    """
    return True if they are the "same", False if they are "different".
    "Same" means all lines are the same, except that the @PG line
    is allowed to differ.
    """
    list1 = [line for line in re.split(linesep, h1) if re.search("^\@PG" , line) == None and len(line) > 0]    
    list2 = [line for line in re.split(linesep, h2) if re.search("^\@PG" , line) == None and len(line) > 0]
    #print "DEBUG SAMHeaderEqual will compare these lines in list 1 : %s"%str(list1)
    #print "DEBUG SAMHeaderEqual will compare these lines in list 2 : %s"%str(list2)
    if list1 == list2 :
        return True
    else:
        return False

def fastqPairedNamesEqual(name1, name2):
    """
    return True if two fastq sequence names  are the "same", False if they are "different".
    "Same" means either exact match, or permit a match  where the
    read number ends up part of the name - e.g.
    M02810:22:000000000-A856J:1:1101:13622:1113/1
    M02810:22:000000000-A856J:1:1101:13622:1113/2
    """
    if name1 == name2 :
        return True
    else:
        # try to match the above example
        match1 = re.search("(\S+)\/\d+$", name1)
        match2 = re.search("(\S+)\/\d+$", name2)
        if match1 is not None and match2 is not None:
            if len(match1.groups()) == 1 and len(match2.groups()) == 1:
                if match1.groups()[0] == match2.groups()[0]:
                    return True

        return False
    

class Usage(Exception):
    def __init__(self, msg):
        super(Usage, self).__init__(msg)

def getDefaultEngineOptions():
    """
    attempt get options from a config file - or supply
    defaults if no config available. (The results of this will
    be merged with options set from the command line, or received from
    a client calling the engine)
    """
    defaults =  {
       "rootdir" : os.getcwd(),
       "startdir" : os.getcwd(),
       "workdir_is_rootdir" : False,
       "tardish_rc_found" : False,
       "input_conditioning" : False,
       "in_workflow" : True, 
       "chunksize" : -1,   # -1 means it will be calculated to yield <= max_tasks
       "samplerate" : None,
       "from_record" : None,
       "to_record" : None,
       "dry_run" : False,
       "jobtemplatefile" : None,
       "shelltemplatefile" : None,
       "runtimeconfigsourcefile" : None,
       "keep_conditioned_data" : False,
       "quiet" : False,
       "max_processes" : 20,
       "max_tasks" : 300,
       "min_sample_size" : 500,
       "hpctype" : "slurm",
       "batonfile" : None,
       "valid_command_patterns" : "cat awk [t]*blast[nxp] bwa bowtie flexbar",  
       "shell_template_name" : None,
       "job_template_name" : None,
       "runtime_config_name" : None,
       "use_session_conda_config" : True,
       "session_conda_config_source" : None,
       "fast_sequence_input_conditioning" : True,
       "condor_job" : """
Executable     = $script
Universe       = vanilla
error          = $script.err.$(Cluster).$(Process)
output         = $script.out.$(Cluster).$(Process)
log            = $log
# request a sensible amount of RAM for a typical bioinformatics job - 2GB. Also
# request_cpus 4 is intended to stop over-subscription on a typical cluster
request_memory = 2000
request_cpus = 4
Queue
    """,
       "condor_send_env_job" : """
Executable     = $script
Universe       = vanilla
error          = $script.err.$(Cluster).$(Process)
output         = $script.out.$(Cluster).$(Process)
log            = $log
request_cpus   = 4
# request a sensible amount of RAM for a typical bioinformatics job - 2GB
request_memory = 2000   
getenv         = True
Queue
       """,
       "slurm_array_job_old" : """#!/bin/bash -e

#SBATCH -J $tardis_job_moniker
#SBATCH -A $tardis_account_moniker        # Project Account
#SBATCH --time=240:00:00            # Walltime
#SBATCH --ntasks=1                 # number of parallel processes
#SBATCH --ntasks-per-socket=1      # number of processes allowed on a socket
#SBATCH --cpus-per-task=4          #number of threads per process
#SBATCH --hint=multithread         # enable hyperthreading
#SBATCH --mem-per-cpu=1G
#SBATCH --partition=inv-iranui     # Use nodes in the IRANUI partition
#SBATCH --array=$array_start-$array_stop%50          # Iterate 1 to N, but only run up to 50 concurrent runs at once
#SBATCH --error=$hpcdir/$tardis_job_moniker-%A_%a.err
#SBATCH --output=$hpcdir/$tardis_job_moniker-%A_%a.out

srun --cpu_bind=v,threads $hpcdir/slurm_array_shim.sh ${SLURM_ARRAY_TASK_ID}
       """,
       "default_slurm_array_job" : """#!/bin/bash -e

#SBATCH -J $tardis_job_moniker
#SBATCH -A $tardis_account_moniker        # Project Account
#SBATCH --time=240:00:00            # Walltime
#SBATCH --ntasks=1                 # number of parallel processes
#SBATCH --ntasks-per-socket=1      # number of processes allowed on a socket
#SBATCH --cpus-per-task=4          #number of threads per process
#SBATCH --hint=nomultithread         # enable hyperthreading
#SBATCH --mem-per-cpu=4G
#SBATCH --partition=inv-iranui     # Use nodes in the IRANUI partition
#SBATCH --array=$array_start-$array_stop%50          # Iterate 1 to N, but only run up to 50 concurrent runs at once
#SBATCH --error=$hpcdir/run-%A_%a.stderr
#SBATCH --output=$hpcdir/run-%A_%a.stdout

srun $hpcdir/slurm_array_shim.sh ${SLURM_ARRAY_TASK_ID}
       """,       
       "slurm_array_shim" : """#!/bin/bash
array_index=$1
cd $hpcdir
./run${array_index}.sh
    """,
       
       "condor_shell" : """#!/bin/bash
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir
startdir=$startdir
input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

# configure environment - e.g. activate conda packages, load moules
# or other
_LMFILES_=/usr/share/Modules/modulefiles/null
MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/stash/modulefiles
LOADEDMODULES=null
MODULESHOME=/usr/share/Modules
export _LMFILES_ MODULEPATH LOADEDMODULES MODULESHOME

$configure_runtime_environment

$command
       """,
       "condor_sh_shell" : """#!/bin/sh
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir
startdir=$startdir

input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

# configure environment - e.g. activate conda packages, load moules
# or other
_LMFILES_=/usr/share/Modules/modulefiles/null
MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/stash/modulefiles
LOADEDMODULES=null
MODULESHOME=/usr/share/Modules
export _LMFILES_ MODULEPATH LOADEDMODULES MODULESHOME
$configure_runtime_environment

$command
       """,
       "local_shell" : """#!/bin/bash
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir
startdir=$startdir

input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi


# configure environment - e.g. activate conda packages, load moules
# or other

$configure_runtime_environment

$command
       """,
       "local_sh_shell" : """#!/bin/sh
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir
startdir=$startdir

input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

# configure environment - e.g. activate conda packages, load moules
# or other 
$configure_runtime_environment

$command
       """,
       "qiime_shell" : """#!/bin/sh
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir
startdir=$startdir
input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

eval `modulecmd sh load qiime`

# configure environment - e.g. activate conda packages, load moules
# or other 
$configure_runtime_environment

$command
       """,
       "condor_qiime_shell" : """#!/bin/sh
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir

input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

# note that the following env settings are probably not needed if
# you launch your condor job from an environment that knows about
# module,. and you use the condor job template which sends the local 
# environment to the condor process
_LMFILES_=/usr/share/Modules/modulefiles/null
MODULEPATH=/usr/share/Modules/modulefiles:/etc/modulefiles:/stash/modulefiles
LOADEDMODULES=null
MODULESHOME=/usr/share/Modules

export _LMFILES_ MODULEPATH LOADEDMODULES MODULESHOME

eval `modulecmd sh load qiime`
cd $startdir > /dev/null 2>&1

# configure environment - e.g. activate conda packages, load moules
# or other 
$configure_runtime_environment

$command
""",
    "slurm_shell" : """#!/bin/sh
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=$hpcdir
startdir=$startdir

input_conditioning=$input_conditioning

if [ $input_conditioning != True ]; then
   cd $startdir > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd $hpcdir > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

job_started=`date`
echo "job_started=$$job_started" >> $tlog
echo "
---- begin environment listing (pre config)----" >> $tlog
env >> $tlog
echo "
---- end environment listing (pre config)----" >> $tlog


# configure environment - e.g. activate conda packages, load moules
# or other 
export PATH="/stash/miniconda3/bin:$PATH"  # ensure we know how to use conda even if launched from vanilla node
$configure_runtime_environment

echo "
---- begin environment listing (post config)----" >> $tlog
env >> $tlog
echo "
---- end environment listing (post config)----" >> $tlog

# run the command
$command

# write datestamep and exit code to tardis log
job_exit_code=$$?
job_ended=`date`

echo "job_exit_code=$$job_exit_code" >> $tlog
echo "job_ended=$$job_ended" >> $tlog   #this must go last, after exit code (to avoid race condition as poll is for job_ended)

# exit with the exit code we received from the command
exit $$job_exit_code
""",
   "basic_slurm_runtime_environment" : """
source activate bifo-essential
""",
   "basic_condor_runtime_environment" : """
""",
   "basic_local_runtime_environment" : """
"""

    }

    # set values usinf checkAndSetOption e.g. to correctly type some
    # options
    for (key,value) in  defaults.items():
        checkAndSetOption(defaults,key,value)

    
    config  = ConfigParser.RawConfigParser()
    fileList = [filename for filename in ["./.tardishrc", "/etc/tardis/.tardisrc"] if filename is not None] 
    config.read(fileList)

    if not config.has_section("tardis_engine"):
        #print >> sys.stderr, "warning could not find valid config file  (.tardishrc) - using defaults"
        pass
    else:    
        defaults["tardish_rc_found"] = True 
        #print >> sys.stderr, "updating defaults from config %s"% dict(config.items("tardis_engine"))
        configDict = dict(config.items("tardis_engine"))

        # check for function definitions - if any found get the code. Note however only those
        # explicitly defined in checkAndSetOption will actuall be compiled.
        funcDict = {}
        for key in configDict:
            match = re.search("^def\s+(\S+?)\(", key)
            if match is not None:
                func_name = match.groups()[0]
                funcDict[func_name] = getPythonCodeFromConfig(fileList, func_name)
                defaults[func_name] = None
                
        configDict.update(funcDict)
                        
        for (key,value) in  configDict.items():
            checkAndSetOption(defaults,key,value)

    return defaults

def getPythonCodeFromConfig(fileList, func_name):  
    """
    work-around for limitations of standard config parser - we can't read in
    the value of an item that is python code, because the default parser
    nukes the indenting whitespace. 
    """
    configIters = [open(configFile,"r") for configFile in fileList if os.path.isfile(configFile)]
    configIter = itertools.chain(*configIters)
    code = StringIO.StringIO()
    code_block = False
    for record in configIter:
        if code_block:
            if re.search("^\S+", record) is not None: # indent has finished so we are out of the code block
                break
        else:
            if re.search("^def\s+%s\("%func_name, record) is not None:  # we found the config section containing the code
                code_block = True

        if code_block:
            code.write(record)

    if not code_block:
        return None
    else:
        return code.getvalue()       
        
    test_data = """
def record_filter_func(seqrecord):
   # need to filter id like @M02810:27:000000000-AAJE2:1:1101:9707:1735/2
   # to
   # id like                @M02810:27:000000000-AAJE2:1:1101:9707:1735 2:N:0:21
   # so that these are parsed OK by bwa paired alignment
   # and both names to @M02810:27:000000000-AAJE2:1:1101:9707:1735
   import re
   seqrecord.name = re.sub("\/1$","",seqrecord.name)
   seqrecord.name = re.sub("\/2$","",seqrecord.name)
   seqrecord.id = re.sub("\/2$", " 2:N:0:21",seqrecord.id)
   seqrecord.id = re.sub("\/1$", " 1:N:0:21",seqrecord.id)
   return seqrecord
"""
    
    

def checkAndSetOption(options, name, value):
    options[name] = value

    if value is None:
        return
    
    if name == "chunksize" : 
        options[name] = int(value)
    elif name == "quiet" : 
        options[name] = value
    elif name == "samplerate": 
        options[name] = float(value)
    elif name == "from_record" : 
        options[name] = int(value)
    elif name == "to_record" : 
        options[name] = int(value)        
    elif name == "seqlength_min":
        options[name] = int(value)
    elif name == "seqlength_max":
        options[name] = int(value)
    elif name == "max_processes":
        options[name] = int(value)
    elif name == "max_tasks":
        options[name] = int(value)
    elif name == "min_sample_size":
        options[name] = int(value)        
    elif name == "valid_command_patterns":
        options[name] = re.split("\s+",value)
    elif name == "fast_sequence_input_conditioning":
        options[name] = eval(str(value))
    elif name == "use_session_conda_config":
        options[name] = eval(str(value))        
    elif name == "record_filter_func":
        # the value should be code like this
        #def record_filter_func(my_arg):
        #   import re
        #   newname = re.sub("foo","bar",my_arg)
        #   return newname
        compileGlobals = {}
        f=compile(value,"","exec")
        eval(f,compileGlobals)
        options[name] = compileGlobals[name]
        print "DEBUG", len(options)
        print "DEBUG", len(options)
        # all going well options["record_filter_func"] is now
        #a function object , which can be inserted to filter input records
    elif name == "workdir_is_rootdir":
        options[name] = eval(str(value))
    elif name == "rootdir":
        options[name] = os.path.abspath(value)
    else:
        options[name] = value
      
        

def getWorkDir(options):
    if not options['workdir_is_rootdir']:
        return tempfile.mkdtemp(prefix="tardis_", dir=options["rootdir"])
    else:
        return options["rootdir"]


def getOptions(argv = None, client_options = None):
    """
    This method
    * calls the getDefauiltEngineOptions to retrieve options set by
    a configurations file
    * parses any command line options given (if the engine is
    being run stand-alone)
    * merges the options supplied from either (but not both) of client / stand-alone command-line, with the
    configuration file options, into a final set of options to be used
    """
    usage = """

   The tardis library implements "conditioning" a command for execution on a cluster, 
   launching of conditioned commands as a series of jobs on either a remote cluster or
   natively on localhost, and collects and collates the output.

   It can be run stand-alone as below, or via the tardis shell (tardish).


   Stand-alone usage : 

tardis.py [-w] [-c Chunksize] [-s SampleRate] [-from record_number] [-to record_number] [-d workingRootPath] [-k] [-v] any command (with optional conditioning directives)
-w                      Run the command as part of a workflow. After launching all of the jobs, tardis waits for all outputs, which are then collated and merged into a single output file, as specified by the output file path in the original command; all of the temporary input files (for example chunks of uncompressed fastq) are deleted provided all prior steps completed without error (if there was an error they are left there to assist with debugging). Without this option, the program exits immediately after launching all of the jobs, and output is left un-collated in the scratch folder created by this script, and no cleanup is done.
-c ChunkSize            When conditioning the input file(s), split into files each containing Chunksize logical records. (A logical record for a sequence file is a complete sequence. For a text file it is a line of text). (If the -s option is used to sample the inputs, the chunksize relates to the full un-sampled file . so the same chunk-size can be used whether random sampling or not. For example a chunksize of 1,000,000 is specified in combination with a sampling rate of .0001, then each chunk would contain 100 sequences . i.e. you should not adjust the chunk-size, for the sampling rate. Note that to avoid a race-condition that could be caused by a very small chunk-size resulting in launching a very large number of jobs, tardis will throw an exception if the chunk-size used would result in launching more than MAX_DIMENSION jobs (currently 5000) )
-from record_number     When conditioning the input file(s), only use records from the input file after or including record_number (where that is logical record number . e.g. in a fastq file, start from record number N means start from sequence N). By combining this option with -to, you can process slices of a file. Note that this option has no affect when processing a list-file.
-to record_number       When conditioning the input file(s), only use records up to and including the record record_number (where that is logical record number . e.g. in a fastq file, process up to record number N means process up to and including sequence N). By combining this option with    -from, you can process slices of a file. Note that this option has no affect when processing a list-file.
-s SampleRate           Rather than process the entire input file(s), a random sample of the records is processed. SampleRate is the probability that a given record will be sampled. For example -s .001 will result in roughly 1 in every 1000 logical records being sampled.  When the -s option is specified, tardis does not clean up the conditioned input and output . e.g. all of the uncompressed fastq sample fragments would be retained. These are retained to assist with the Q/C work that is normally associated with a sampled run. Paired fastq input files are sampled in lock-step, provided the paired fastq conditioning directive is used for both files.
-d workingRootPath      create the tardis working folder under workingRootPath. If no working root is specified, a default location is used.
-v                      validate the run by doing a dry run. This means that the chunks, job scripts and job files etc. are all generated but the jobs are not launched. The user can start then kill (CTRL-C) the run, inspect the script and job files that were generated to check that their command has been conditioned as envisaged.
-k                      keep the conditioned input and output - i.e. the input and output fragments. Normally in workflow mode these are deleted after the output is successfully "unconditioned" - i.e. joined back together
-t filename             optionally supply a condor job template - tardis will read the contents of filename and use this as the job template.
-hpctype value          optionally indicate the hpc environment. Currently the only supported values are :
default which results in tardis attempting to set up and launch condor jobs;
local which results in each job being launched by tardis itself on the local machine, using the native python sub-process API. The maximum number of processes it will run at a time is controlled by a global variable in the script MAX_PROCESSES, which is initially 10.
                        If the -hpctype option is not used, tardis assumes default and will try to set up and launch condor jobs.

-batonfile value        if you supply a "baton file" name, tardis will write the process exit code to this file after all processing has completed. This can be useful to preserve synchronous execution of a workflow, even if tardis is started in the background - the workflow can test the existence of the batonfile - if it exists then the corresponding tardis processing step has completed
                        (i.e. another way of each step in a workflow "passing the baton" to the next step)
                        
-h                      print usage and exit.


   see the tardis documentation for more information on the conditioning directives that are supported, and
   examples of conditioned commands.

   (see the tardish documentation for more information on using the tardish command interpreter to
   condition and run commands)

   """
    # get options from config 
    engine_options = getDefaultEngineOptions()
    options = {}
    options.update(engine_options)

    if client_options is not None and argv is not None:
        raise tardisException("error - can't configure using both command line args and library client args")

    # merge with "client options" - i.e. if we are being called from a tardish session
    elif client_options is not None:
        # client options update engine options except in some cases
        options.update(client_options)

        # exceptions
        for override in ["valid_command_patterns"]:
            options[override] = engine_options[override]
            
        return options
    
    elif argv is None:
        raise tardisException("error - need to configure either from command line args or library client args")
    
        
    
    if len(argv) < 2:
        raise Usage("please supply a valid command to condition and run (type tardis -h for usage")

    # get and parse command line args , and use these to update  options from the config file i.e.
    # if we are being run stand-alone  (didn't orignally use the standard OptionParser
    # due to the way we mix args to this script, and the commands passed in -
    # however it looks like the argparse can actually handle that use-case so this code should be replaced with
    # standard argparse parsing at some stage)
    args =  argv[1:]
    while True:
        arg = args.pop(0)
        
        if arg == "-w" : 
            checkAndSetOption(options, "in_workflow", True)
        elif arg == "-dryrun" : 
            checkAndSetOption(options,"dry_run",True)
        elif arg == "-k" : 
            checkAndSetOption(options,"keep_conditioned_data", True )
        elif arg == "-c" : 
            checkAndSetOption(options,"chunksize", args.pop(0))
        elif arg == "-s" : 
            checkAndSetOption(options,"samplerate", args.pop(0))
        elif arg == "-from" : 
            checkAndSetOption(options,"from_record", args.pop(0))
        elif arg == "-to" : 
            checkAndSetOption(options,"to_record", args.pop(0))            
        elif arg == "-job-file" : 
            checkAndSetOption(options,"jobtemplatefile", args.pop(0))
        elif arg == "-job-template-name" : 
            checkAndSetOption(options,"job_template_name", args.pop(0))
        elif arg == "-shell-include-file" : 
            checkAndSetOption(options,"runtimeconfigsourcefile", args.pop(0))            
        elif arg == "-d" : 
            checkAndSetOption(options,"rootdir",args.pop(0))
        elif arg == "-q" : 
            checkAndSetOption(options,"quiet", True)            
        elif arg == "-hpctype" :
            checkAndSetOption(options,"hpctype",args.pop(0))
        elif arg == "-batonfile" :
            checkAndSetOption(options,"batonfile",args.pop(0))
        elif arg == "-h":
            raise Usage(usage)
        elif " " in arg:
            args.insert('"%s"' % arg) # ref /home/galaxy/galaxy/tools/ncbi_blast_plus/hide_stderr.py
        else:
             args.insert(0,arg) 
             break

    # filter args 
    for arg in args:
        if re.search("[\!\&]|\srm\s|\smv\s|\scp\s", arg) != None: # do not allow irrelevant/dangerous shell chars    
            raise tardisException("error : dangerous argument to shell ( %s ) - will not run this"%arg)
            #args.remove(arg)
    if len(args) < 1:
        raise Usage("please supply a valid command to condition and run (type tardis -h for usage")

    return (options, args)


def isCommandValid(toolargs, patternList):
    print "checking for valid commands using %s"%patternList
    return reduce(lambda x,y:x or y, [re.search(pattern,toolargs[0]) is not None for pattern in patternList] , False)



def pass_the_baton(batonfile, message, logger):
    if batonfile is None:
        return

    if os.path.exists(batonfile):
        logger.info("tutils:pass_the_baton  - batonfile %s already exists - baton may have been grabbed too early !"%batonfile)
        raise tardisException("error in pass_the_baton - batonfile %s already exists - baton may have been grabbed too early !"%batonfile)

    logger.info("passing the baton ( %s in %s )"%(str(message),batonfile))
    baton = open(batonfile, "w")
    print >> baton, str(message)
    baton.close()
    return 
