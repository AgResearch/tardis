import sys, errno, exceptions, os, os.path, re, ConfigParser, logging, string, tempfile
import pytoml as toml

class tardisException(exceptions.Exception):
    def __init__(self,args=None):
        super(tardisException, self).__init__(args)

class tardisLogger(object):
    def __init__(self, logger = None):
        super(tardisLogger,self).__init__()
        self.loggerInstance = logger

    def debug(self, text):
        if self.loggerInstance is not None:
            self.loggerInstance.debug(text)

    def info(self, text):
        if self.loggerInstance is not None:
            self.loggerInstance.info(text)

    def warning(self, text):
        if self.loggerInstance is not None:
            self.loggerInstance.warning(text)

    def error(self, text):
        if self.loggerInstance is not None:
            self.loggerInstance.error(text)

    def critical(self, text):
        if self.loggerInstance is not None:
            self.loggerInstance.critical(text)

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


def readConfigFiles(client_options):
    """
    attempt get options from a config file - or supply
    defaults if no config available. (The results of this will
    be merged with options set from the command line, or received from
    a client calling the engine)
    """
    options = {}
    # later files in the list override earlier ones
    configfiles = []
    # we always start with the system config file, unless disabled by client option
    if not client_options.get('no_sysconfig'):
        configfiles.append('/etc/tardis/tardis.toml')
    client_configfile = client_options.get('userconfig')
    if client_configfile:
        # user configfile passed on the command line, so use it and ignore the others
        configfiles.append(client_configfile)
    else:
        local_configfile = 'tardis.toml'
        if os.path.exists(local_configfile):
            # configfile in current directory, so use it and ignore the others
            configfiles.append(local_configfile)
        else:
            user_configfile = os.path.expanduser('~/.tardis.toml')
            if os.path.exists(user_configfile):
                configfiles.append(user_configfile)

    for configfile in configfiles:
        try:
            with open(configfile, 'rb') as f:
                if not client_options.get("quiet", False):
                    print("reading config from %s" % configfile)
                obj = toml.load(f)
                # merge config in with existing
                #print("raw config from %s: %s" % (configfile, str(obj)))
                options.update(obj)
        except toml.TomlError as e:
            raise tardisException("syntax error at line %d of %s" % (e.line, configfile))
        except IOError as e:
            # ignore missing config file
            if e.errno != errno.ENOENT:
                raise tardisException("can't read config file %s: %s" % (configfile, e))
    return options

def validateOption(options, name, required, default, types):
    if name in options:
        value = options[name]
        if type(value) not in types:
            raise tardisException("config item %s must be one of %s, not %s" % (name, str(types), str(type(value))))
    elif required:
        raise tardisException("required config item %s missing" % name)
    else:
        value = default
        options[name] = value
    return value

def validateList(options, name, required, elementTypes):
    optionValue = validateOption(options, name, required, [], [list])
    for elementValue in optionValue:
        if type(elementValue) not in elementTypes:
            raise tardisException("config item %s must contain %s, not %s" % (name, str(elementTypes), str(value)))
    return optionValue

def validateString(options, name, required=False):
    return validateOption(options, name, required, None, [str, unicode])

def validatePath(options, name, required=False, checkIsFile=False, checkIsDir=False, checkParentIsDir=False, makeAbsolute=False):
    """Ensure path exists, and optionally turn relative path into absolute."""
    value = validateString(options, name, required)
    if value is not None:
        path = os.path.expanduser(value)
        if makeAbsolute:
            path = os.path.abspath(path)
        parentdir = os.path.dirname(path)
        if checkIsFile and not os.path.isfile(path):
            raise tardisException("config item %s path %s not a file" % (name, path))
        if checkIsDir and not os.path.isdir(path):
            raise tardisException("config item %s path %s not a directory" % (name, path))
        if checkParentIsDir and not os.path.isdir(parentdir):
            raise tardisException("config item %s directory %s not found" % (name, parentdir))
        value = path
        options[name] = value
    return value

def validateBool(options, name, required=False):
    return validateOption(options, name, required, None, [bool])

def validateInt(options, name, required=False):
    return validateOption(options, name, required, None, [int])

def validateFloat(options, name, required=False):
    return validateOption(options, name, required, None, [float])

def validateStringList(options, name, required=False):
    return validateList(options, name, required, [str, unicode])

def validatePythonCode(options, name, required=False):
    value = validateString(options, name, required)
    if value is not None:
        # the value should be code like this
        #def record_filter_func(my_arg):
        #  import re
        #  newname = re.sub("foo","bar",my_arg)
        #  return newname
        compileGlobals = {}
        try:
            f = compile(value, "", "exec")
            eval(f, compileGlobals)
        except Exception as e:
            raise tardisException("config item %s invalid Python code: %s" % (name, str(e)))
        value = compileGlobals[name]
        # all going well options["record_filter_func"] is now
        # a function object , which can be inserted to filter input records
        options[name] = value

def validateOptions(options):
    validatePath(options, "rootdir", required=True, makeAbsolute=True, checkIsDir=True)
    validatePath(options, "startdir", required=True, makeAbsolute=True, checkIsDir=True)
    validateBool(options, "workdir_is_rootdir", required=True)
    validateBool(options, "input_conditioning", required=True)
    validateBool(options, "in_workflow", required=True)
    validateInt(options, "chunksize", required=True)
    validateFloat(options, "samplerate")
    validateInt(options, "from_record")
    validateInt(options, "to_record")
    validateInt(options, "seqlength_min")
    validateInt(options, "seqlength_max")
    validateBool(options, "dry_run", required=True)
    validatePath(options, "jobtemplatefile", checkIsFile=True, makeAbsolute=True)
    validatePath(options, "shelltemplatefile", checkIsFile=True, makeAbsolute=True)
    validatePath(options, "runtimeconfigsourcefile", checkIsFile=True, makeAbsolute=True)
    validateBool(options, "keep_conditioned_data", required=True)
    validateBool(options, "quiet", required=True)
    validateInt(options, "max_processes", required=True)
    validateInt(options, "max_tasks", required=True)
    validateInt(options, "min_sample_size", required=True)
    validateString(options, "hpctype", required=True)
    validatePath(options, "batonfile", checkParentIsDir=True)
    validateStringList(options, "valid_command_patterns", required=True)
    validatePath(options, "templatedir", required=True, checkIsDir=True)   # this option does not appear to be used currently, consider removing 
    validateString(options, "shell_template_name")
    validateString(options, "job_template_name")
    validateString(options, "runtime_config_name")
    validateBool(options, "use_session_conda_config", required=True)
    validateBool(options, "fast_sequence_input_conditioning", required=True)
    validatePythonCode(options, "record_filter_func")

def getWorkDir(options):
    if not options['workdir_is_rootdir']:
        return tempfile.mkdtemp(prefix="tardis_", dir=options["rootdir"])
    else:
        return options["rootdir"]

def getTemplateContent(options, template_name, logWriter=None):
    """Resolve template name as a file relative to the option templatedir, and return content as a string, or empty if no such template."""
    try:
        templatedir = options.get("templatedir")
        content = string.join(file(os.path.join(templatedir, template_name),"r"),"")
    except IOError as e:
        msg = 'template %s not found in %s (%s), returning empty string' % (template_name, templatedir, str(e))
        if logWriter is not None:
            logWriter.warning(msg)
        else:
            print('WARNING %s' % msg)
        content = ""
    return content

def mergeOptionsWithConfig(client_options = None):
    """
    This method
    * retrieves options from configuration files
    * parses any command line options given (if the engine is
    being run stand-alone)
    * merges the options supplied from either (but not both) of client / stand-alone command-line, with the
    configuration file options, into a final set of options to be used
    """
    # get options from config
    config_options = readConfigFiles(client_options)
    options = {}
    options.update(config_options)

    if client_options is not None:
        # client options update engine options except in some cases
        options.update(client_options)

        validateOptions(options)

        # exceptions
        for override in ["valid_command_patterns"]:
            options[override] = config_options[override]

    return options

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
