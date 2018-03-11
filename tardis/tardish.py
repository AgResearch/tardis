#!/usr/bin/env python

#tardish - interactive command-line interpreter access to the tardis engine
#by Alan McCulloch (alan.mcculloch@agresearch.co.nz) 2014
#
# Copyright 2014 AgResearch. See the COPYING file at the top-level directory of this distribution.
"""
tardish

a command-line interpreter interface to the tardis engine
(see the "usage" text below and associated docs for more information
"""


import sys
import logging
import subprocess
import re
import os
import stat
import string
import exceptions
import StringIO
import cmd
import ConfigParser

import zmq
import time

import argparse
import ConfigParser
import copy


from tardis.__main__ import run
from tardis.tutils.tutils import getDefaultEngineOptions, getWorkDir, checkAndSetOption
from tardis.conditioner.data import dataConditioner


###############################################################################################
# globals                                                                                     #
###############################################################################################

class tardishException(exceptions.Exception):
    def __init__(self,args=None):
        super(tardishException, self).__init__(args)

class tardishLogger(object):
    loggerInstance = None

    @classmethod
    def info(cls, text):
        if cls.loggerInstance is not None:
            cls.loggerInstance.info(text)

DEFAULT_PORT=8273
MAX_ZBIND_ATTEMPTS=1000
MIN_RANDOM_PORT=2000
MAX_RANDOM_PORT=9000
            
###############################################################################################
# base command shell classes
# references :
#
# https://docs.python.org/2/library/cmd.html
# https://wiki.python.org/moin/CmdModule
# http://draketo.de/proj/wisp/src/99c370a68097cd69d5e20fe53d30ff42bc020356/console.py.html
# http://www.python.org/download/releases/2.3/mro/
# https://docs.python.org/2/library/configparser.html
#
# zmq:
# http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/basics.html#omq-version
# http://zeromq.github.io/pyzmq/
# http://zguide.zeromq.org/
# http://zguide.zeromq.org/py:mtserver
# http://ojs.pythonpapers.org/index.php/tpp/article/viewFile/242/212
# git clone --depth=1 git://github.com/imatix/zguide.git
#
# http://zeromq.github.io/pyzmq/serialization.html
###############################################################################################
class tardish_cmd(cmd.Cmd,object):

    def __init__(self, config, completedkey, stdin,stdout):
        super(tardish_cmd, self).__init__(completedkey, stdin,stdout)
        self.config = config
        self.client_config = None
            
    def can_exit(self):
        return True
    def onecmd(self, line):
        r = super (tardish_cmd, self).onecmd(line)
        if r and (self.can_exit() or
           raw_input('exit anyway ? (yes/no):')=='yes'):
             return True
        return False
    def do_exit(self, s):
        return True
    def help_exit(self):
        print "Exit the interpreter."
        print "You can also use the Ctrl-D shortcut."
    do_EOF = do_exit
    help_EOF= help_exit

    

###############################################################################################
# tardish command shell class (server)
###############################################################################################

class tardish_server_cmd(tardish_cmd):    
    def __init__(self, config):
        super(tardish_server_cmd, self).__init__(config, 'tab', None, None)
        self.z0bind()
        self.prompt = "tardish> "

        if self.config["lport"] is None:
            self.intro = "Welcome to tardish"
        else:
            self.intro = "Welcome to tardish (listening for clients on port %(lport)s)"%config
        
        
    ###### zeromq related methods #############################################################
    def z0bind(self, retry = True):
        self.z0context = None
        self.z0socket = None
        
        if self.config["lport"] is not None:
            self.z0context = zmq.Context()
            try:
                self.z0socket = self.z0context.socket(zmq.REP)
                #print "server trying %s"%self.config["lport"]
                self.z0socket.bind("tcp://*:%s" % self.config["lport"])
                #print "server bind OK"
            except zmq.error.ZMQError:
                self.config["lport"] = None
                if retry:
                    try:
                        #print "server trying random port"
                        self.config["lport"] = self.z0socket.bind_to_random_port("tcp://*",min_port = MIN_RANDOM_PORT, max_port = MAX_RANDOM_PORT, max_tries=MAX_ZBIND_ATTEMPTS)
                        #print "server got random port %s"%self.config["lport"]
                    except zmq.error.ZMQError, msg:
                        print msg
                        return
        return


    def z0close(self):
        if self.z0socket is not None:
            self.z0socket.close()

        if self.z0context is not None:
            self.z0context.destroy()
            
        return
    
                                
    def client_recv_loop(self):
        exit_requested = False
        
        while not exit_requested:
            (clientDict, self.client_config)  = self.z0socket.recv_pyobj()
            
            if clientDict["content_type"] == "code":
                #print "client requested: %s using %s "%(clientDict, str(self.client_config))
                exit_requested = self.onecmd(clientDict["content"])
            elif clientDict["content_type"] == "data" :
                # the content element will contain a moniker which can be used by subsequent
                # code commands to refer to the data
                None
                
            time.sleep(1)

    def send_client_response(self, msg):
        #if msg is None:
        #    sendmsg = "\n"
        #elif len(msg) == 0:
        #    sendmsg = "\n"
        #else:
        #    sendmsg = msg
        self.z0socket.send(msg)
            
        
            
    ###### standard housekeeping method overrides #############################################
    def preloop(self):
        super(tardish_server_cmd, self).preloop() 
        print "doing pre-loop"

    def postloop(self):
        super(tardish_server_cmd, self).postloop() 
        print "tardish exiting..."

    def precmd(self, line):
        super(tardish_server_cmd, self).precmd(line)
        if self.client_config is None:
            print "Error - tardis engine options not set , will not run %s"
            return ""
        return line

    def postcmd(self,stop, line):
        super(tardish_server_cmd, self).postcmd(stop,line)
        self.client_config = None
        return stop

    def emptyline(self):
        """Do nothing on empty input line"""
        pass        

    def help_introduction(self):
        print 'introduction'
        print 'a good place for a tutorial'
    
    def do_shell(self, s):
        # we shouldn't get a shell request to handle as the client intercepts and handles shell commands. But just in case
        print "Error tardish server shell access not permitted"
        self.send_client_response("Error tardish server shell access not permitted")
        return False

    def do_exit(self, s):
        self.send_client_response("server exiting...")
        super(tardish_server_cmd, self).do_exit(s)
        return True
        
        
    def help_shell(self):
        print "Error tardish server shell access not permitted"
        self.send_client_response("Error tardish server shell access not permitted")
    ###### other commands  - may involve calling the tardis engine ############################
    def default(self, s):
        """
        examples :

set samplerate=.0001
file p1 is /dataset/Kittelmann_Buccal_Ill/archive/nzgl01005/140627_M02810_0023_000000000-A856J/processed_trimmed/processed_S395Rumen-25MG_S15_L001_R1_001.fastq.trimmed
file p2 is /dataset/Kittelmann_Buccal_Ill/archive/nzgl01005/140627_M02810_0023_000000000-A856J/processed_trimmed/processed_S395Rumen-25MG_S15_L001_R2_001.fastq.trimmed
cat  _condition_paired_fastq_input_$f(p1);cat  _condition_paired_fastq_input_$f(p2)

        
        """
        #toolargs  = ['cat'] + re.split("\s+", s)
        toolargs = re.split("\s+",s)
        stdoutBuffer=StringIO.StringIO()
        stderrBuffer=StringIO.StringIO()
        run(toolargs, self.client_config, stdoutBuffer, stderrBuffer)
        self.send_client_response(stdoutBuffer.getvalue()+ stderrBuffer.getvalue())
        return False


###############################################################################################
# tardish command shell class
###############################################################################################

#class remote_tardish_cmd(shell_cmd, exit_cmd):
class tardish_client_cmd(tardish_cmd):
    shell_metachars = ["&" , ">", ";", "|" , "<"]

    def __init__(self, config, cmd_stdin = None, cmd_stdout = None):
        super(tardish_client_cmd, self).__init__(config, 'tab', cmd_stdin, cmd_stdout)
        self.z0connect()
        self.prompt = "tardish> "
        self.intro = """
        Welcome to tardish (commands will be queued to tardish server at %(rhost)s:%(rport)s)
        Use tab for tab completion of conditioning directives (tab three times to get all)
        """%config
        self.fileMonikerMapping = {} # dictionary indexed by moniker
        self.spawned_server = False


    ###### zeromq related methods #############################################################
    def z0connect(self):
        self.z0context = zmq.Context()
        self.z0socket = self.z0context.socket(zmq.REQ)
        self.z0socket.connect("tcp://%s:%s" % (self.config["rhost"], self.config["rport"]))
        #self.z0socket.bind("tcp://%s:%s" % (self.rhost, self.rport))

    def send_server_command(self, command):
        self.z0socket.send_pyobj(({"content" : command, "content_type" : "code"}, self.config))

    def send_server_data(self, command):
        # send the data and get back the remote temp name
        None

    def send_server_file(self, command):
        # send the file and get back the remote temp name
        None
        
    def recv_server_response(self):
        return self.z0socket.recv()
        
        
    ###### standard housekeeping method overrides #############################################
    def preloop(self):
        super(tardish_client_cmd, self).preloop() 
        #print "doing pre-loop"

    def postloop(self):
        super(tardish_client_cmd, self).postloop() 
        print "tardish exiting..."

    def precmd(self, line):
        super(tardish_client_cmd, self).precmd(line)
        
        # resolve any file references by interpolating entries from fileMonikerMapping
        for (moniker, filename) in self.fileMonikerMapping.items():
            #print "precmd resolving %s"%moniker
            line = re.sub("\\$f\\(%s\\)"%moniker, filename, line)
            line = re.sub("\\$F\\(%s\\)"%moniker, filename, line)


        # screen for shell metacharacter + filename combinations that will not be parsed
        # correctly by the tardis engine - pad with spaces
        for metachar in self.shell_metachars:
            line = re.sub("\\%s"%metachar, " %s "%metachar, line)

        return line

    def do_exit(self, s):
        if self.spawned_server:
            self.send_server_command("exit")
            result = self.recv_server_response()
            print result
        super(tardish_client_cmd, self).do_exit(s)
        return True
               
    def emptyline(self):
        """Do nothing on empty input line"""
        pass        

    def help_introduction(self):
        print 'introduction'
        #print 'a good place for a tutorial'

    ###### utility methods ###################################################################
    #
    ##########################################################################################
    def isComment(self,s):
        if s[0] == "#":
            return True
        else:
            return False
    
    ###### commands ##########################################################################
    # a few commands are implemented  entirely locally
    # many commands are implemented partly in the base class (for example help text),
    # partly in the client (for example command completion) and partly in the
    # server class (the actual processing  of the command instruction)
        
    def do_shell(self, s):
        #print "starting subprocss to handle shell command %s"%s
        proc = subprocess.Popen(re.split("\s+",s), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        print stdout
        print stderr
        return False
    
    def help_shell(self):
        print "this will give help on excuting shell commands"


    def do_file(self, s):
        """
        marshalls data into a file 
        """
        """
        this is used to set tardis engine server parameters 
        """
        #print "do_file handling %s"%s
        
        is_match = re.search("(\S+)\s+is\s(.+)$", s, re.IGNORECASE)
        contains_match = re.search("(\S+)\s+contains\s(.+)$", s, re.IGNORECASE)
        
        if (is_match is None and contains_match is None ) or (is_match is not None and contains_match is not None ):
            print """use
            file moniker [is|contains] [filename|value]
            """
            return False

        if contains_match:
            if len(contains_match.groups()) != 2:
                print """use
                file moniker contains value
                """
                return False
            (file_moniker, file_content) = match.groups()
            # send the data
        if is_match:
            if len(is_match.groups()) != 2:
                print """use
                file moniker is filename
                """
                return False
            (file_moniker, file_name) = is_match.groups()
            if not os.path.isfile(file_name):
                print "(warning file %s not found)"%file_name

            if self.config["copy_files"]:
                # communicate with the server, get back the temp filename and associate with the moniker
                return False
            else:
                # associate moniker with filename
                self.fileMonikerMapping[file_moniker] = file_name
                
        return False
    def help_file(self):
        print self._help_file_text

    def completefile(self):
        if re.search("\s+is", text) is None and re.search("\s+contains", text) is None:
            return ["is","contains"]

    def do_set(self, s):
        """
        this is used to set tardis engine server parameters 
        """
        settings_match = re.search("(\S+)\s*=\s*(\S+)", s)
        if settings_match is None: 
            print "use set name=value"
        else:
            if len(settings_match.groups()) != 2:
                print "use set name=value"
            else:
                (name, value) = settings_match.groups()
                if name not in self.config and name not in getDefaultEngineOptions():
                    print "unknown setting name : "
                    print self._help_set_text        
                else:
                    checkAndSetOption(self.config, name, value)
                    print self.config
                    return False          
        
    def help_set(self):
        print self._help_set_text

    def do_show(self, s):
        """
        this is used to show a specific option, or all options
        """
        if len(s) == 0:
            print "configuration:"
            print reduce(lambda x,y:x+y , ["%s=%s\n"%item for item in self.config.items()],"")
            if len(self.fileMonikerMapping) == 0:
                print """
(no active file monikers)
                """
            else:
                print "active file monikers:"
                print reduce(lambda x,y:x+y , ["%s=%s\n"%item for item in self.fileMonikerMapping.items()],"")
        elif s in self.config:
            print "%s=%s   (configuration setting)"%(s,self.config[s])
        elif s in self.fileMonikerMapping:
            print "%s is %s   (file moniker)"%(s,self.fileMonikerMapping[s])
        else:
            print "%s is not set"%s
    def help_show(self):
        print self._help_set_text
        


    ###### all commands handled by default handler and passed to server ############################
    def default(self, s):
        # ignore comments
        if self.isComment(s):
            print ""
        else:
            #self.z0socket.send(s)
            self.send_server_command(s)
            #result = self.z0socket.recv()
            result = self.recv_server_response()
            print result
        return False

    ###### command completion #################################################################
    def completedefault(self, text, line, begidx, endidx):
        #print text, line, begidx, endidx
        # search the directives

        # each directive pattern as (\S+) at the end - get rid of it
        patterns = ["%s_"%string.join(re.split("_", directive)[0:len(re.split("_", directive))-1],"_") for directive in dataConditioner.published_directives]
        #print ["====>"] + patterns
        
        matches = [directive for directive in patterns if re.search(text, directive) is not None]
        return matches
            
    



    ###### help text #######################################################################$##
    _help_set_text = """
     You can set the following tardis engine configuration values

     parameter name     example values
     rootdir                    # e.g./home/mccullocha/galaxy/hpc/dev
     in_workflow                True|False
     chunksize                  200000
     dry_run                    False|True
     hpctype                    'local'|'condor'
     samplerate                 None|.0001
     keep_conditioned_data      False|True
     jobtemplatefile            None|filename
    """

    _help_file_text = """
    the "file" command is used to associate a short name (moniker) with some data. The moniker can then
    be used to refer to the data in commands, where you would normally provide a filename. Tardish resolves the
    moniker to a filename.
    
    Examples:

    file mydata contains ">test\nCGCTGCGCGGCGCTCGGCTCG"
    blastn -query $mydata -task blastn -num_threads 2 -db plant.rna.fna  -evalue 1.0e-6 -num_descriptions 5 -num_alignments 5 -lcase_masking

    This sequence will result in the the string ">test\nCGCTGCGCGGCGCTCGGCTCG" being sent to the remote host and marshalled into
    a temporary file, whose name can subsequently be referenced using $mydata.
        
    file mydata is /dataset/something/x.fasta
    blastn -query $mydata -task blastn -num_threads 2 -db plant.rna.fna  -evalue 1.0e-6 -num_descriptions 5 -num_alignments 5 -lcase_masking

    This sequence will either
        a) result in the contents of /dataset/something/x.fasta being sent to the remote host and marshalled into
           a temporary file, whose name can subsequently be referenced using $mydata.

        b) the file will not be copied, but you can refer to it subsequently using the moniker $mydata

    if the config setting "copy_files" is set to True, then the sequence will be a), whereas if it is false then the
    sequence will be b)
    

    """


def getOptions():
    description = """
    """
    long_description = """
    """

    options = getDefaultShellOptions()

    parser = argparse.ArgumentParser(description=description, epilog=long_description, formatter_class = argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scriptfilename", default=None, nargs="?")
    parser.add_argument("-rh","--rhost", help="name of remote tardish host to connect to", default=None)
    parser.add_argument("-rp","--rport", help="name of remote tardish port to connect to", type=int, default=None)
    parser.add_argument("-lp","--lport", help="port to listen on for connetions", type=int, default=None)    
    args = vars(parser.parse_args())

    # any options set on commans line override config
    set_args = [(key, value) for (key,value) in args.items() if not value is None]
    if len(set_args) > 0:
        options.update(dict(set_args))
    
    return options

def getDefaultShellOptions():
    """
    attempt get options from a config file - or supply
    defaults if no config available
    """
    defaults =\
    {
         'rhost': None,
         'rport': None,
         'lport': None,
         'copy_files' : False,
         'scriptfilename' : None
    }    
    config  = ConfigParser.RawConfigParser()
    fileList = [filename for filename in ["./.tardishrc", os.getenv("HOME") , "/etc/tardis/.tardisrc"] if filename is not None] 
    config.read(fileList)

    if not config.has_section("tardish"):
        print "warning could not find valid config file  (.tardishrc) - using defaults"
    else:
        tardish_dict = dict(config.items("tardish"))
        # if '' or 'None' is specified for rhost and rport , set these to None
        for key in ['rhost','rport']:
            if key in tardish_dict:
                if tardish_dict[key] is not None:
                    if tardish_dict[key].lower() in ['','none']:
                        tardish_dict[key] = None
                        
        
        defaults.update(tardish_dict)

    # this checks and correctly types options.
    for key in defaults:
        checkAndSetOption(defaults, key, defaults[key])

        
    return defaults
    
    
def main(argv=None):

    # get options from config file
    config  = getOptions()

    print "tardish using  %s\n\n\n"%str(config)

    #if we have a script file then read commands from that
    shell_stdin = sys.stdin
    if config["scriptfilename"] is not None:
        #print "commands taken from %s"%config["scriptfilename"]
        shell_stdin = open(config["scriptfilename"],"r")

    
    if config["rhost"] is None and config["rport"] is None and not config["lport"] is None:
        tardis = tardish_server_cmd(config)
        tardis.client_recv_loop()        
    elif not config["rhost"] is None and not config["rport"] is None and config["lport"] is None:
        tardis = tardish_client_cmd(config, cmd_stdin=shell_stdin,cmd_stdout=sys.stdout)
        if shell_stdin != sys.stdin:
            tardis.use_rawinput = False
        tardis.cmdloop()
    elif config["rhost"] is None and config["rport"] is None and config["lport"] is None:
        print "starting a dedicated tardish server to handle this client...."
        config["lport"] = DEFAULT_PORT
        tardis = tardish_server_cmd(config)

        if tardis.config["lport"] is None:
            print "unable to start server (probably couldn't find a port to use)"
            return 2

        # we have found a port to use. We will close the socket, fork a client and server
        # and then connect them on that port. (It is possible this will fail if the port
        # is used in the meantime - just accept that)
        tardis.z0close()

        
        childpid = os.fork()
        # if I am the child reconnect the tardish server to the port we found and listen
        if childpid == 0:
            time.sleep(1)
            tardis.z0bind(retry = False)

            if tardis.config["lport"] is None:
                print "tardish server was unable to bind to the suggested port (try again - the port we were about to use was probably grabbed by somebody)"
                return 2
            print "(tardish server listening on port %d)"%tardis.config["lport"]                
            tardis.client_recv_loop()
            
        else:  #the client is the parent (so that when the user exits, so will the dedicated server). Connect to the port we discovered before the fork
            print "(server pid=%d)"%childpid
            config["rport"] = config["lport"]
            config["lport"] = None
            config["rhost"] = "localhost"            
            tardis = tardish_client_cmd(config, cmd_stdin=shell_stdin,cmd_stdout=sys.stdout)
            if shell_stdin != sys.stdin:
                tardis.use_rawinput = False
            tardis.spawned_server = True
            tardis.cmdloop()
    else:
        print "to connect to a tardis you need to specify a remote host and port (but not a local port)"

    # clean up
    if shell_stdin != sys.stdin:
        shell_stdin.close()
        
    return
        
if __name__=='__main__':
    sys.exit(main())

