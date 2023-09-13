======
tardis
======

*tardis* supports a relatively more interactive than batch style of use
of HPC compute resources, in the sense that it supports entering a
foreground shell command to execute a task in parallel on a cluster,
just as one would enter a foreground command to run an application
interactively. The details of the HPC environment (which load balancer /
scheduler etc ) do not need to be known by the end-user, these are
encapsulated by *tardis* (i.e. it acts as a simple meta-scheduler, though 
does not load-balance across different schedulers).

| To achieve this *tardis* acts as a command pre-processor. It "reconditions" a
marked-up unix shell command to generate
| a sequence of "conditioned" commands (stripped of mark-up) which it then launches on a
cluster. The mark-up may be added by the user to indicate the input(s) and
output(s) of the command. *tardis* splits input files into "conditioned"
input chunks and will "uncondition" (join together) the output chunks to
obtain the final outputs, with the sequence of conditioned commands
referring to conditioned input and output filenames. (Without mark-up, tardis simply
launches the command as entered, onto the cluster, with no conditioning of input or output)

The aspiration of *tardis* is that the user should only need to know the API
of the application they are running - i.e. what is the command needed to
start a single process on their local machine to execute their
computation: the pre-processor then handles all of the administrivia of
launching many processes on a cluster (or optionally on the local
machine) to complete the computation. Ideally the user should not even
have to know of the existence of the scheduler / load balancer (such as
slurm , condor etc).

*tardis* can also simplify pipeline development, by: encapsulating the details of launching 
jobs on the underlying cluster; ensuring that tasks launched on the cluster complete synchronously 
within the context of the pipeline; ensuring that any errors returned by subtasks (for example one chunk 
of a file), bubble up to appear as an error status for the job as a whole. 

The administrivia handled by the *tardis* pre-processor includes

-  handling compressed inputs - the user does not have to uncompress
   inputs
-  content-aware splitting of large input files. *tardis* includes a
   fast C based fasta and fastq splitter
-  handling multiple inputs which must be split in lockstep - for
   example pairs of paired-end sequence files
-  handling "file of filenames" inputs - *tardis* will treat the
   collection of files as a single input stream
-  support for random-sampling inputs for test or Q/C purposes. This
   includes support for "lock-step" random sampling of paired files
-  support for filtering inputs on the fly - for example only processing
   sequences greater than a certain length
-  job submission using condor or slurm - the user does not need to even
   know about the existence of the scheduler - they only need to know
   their own application command, and how to mark up the command
   arguments that specify input(s) and output(s)
-  optional job submission on the local machine, with *tardis* acting as
   scheduler
-  optionally waiting until all submitted jobs have completed, and then
   concatenating output chunks together to obtain final outputs
-  optionally running in the foreground - i.e. waiting until all
   launched jobs have completed before returning, as would a native
   single-process command.
-  content aware concatenation of output including pdf and XML as well
   as plain text.
-  optional automatic compression of outputs
-  automatic clean-up - deleting the input and output chunks
-  ensuring the integrity of the overall job : if one or more of the
   jobs launched returns an error code, all intermediate results are
   retained for debugging purposes (clean-up not done) and errors from
   jobs are collected and displayed to user; if any chunk output file is
   missing , the corresponding final concatenated output will not be
   generated, avoiding silently incomplete results.
-  easy restarts of failed jobs: each conditioned command is written to
   an individual shell script file, and each of these is launched on the
   cluster. This means chunks that fail on the cluster can easily be
   re-run simply by executing the corresponding shell script.


Installation
============

Dependencies
============

- kseq_split
- there are a couple of features such as filtering sequences by length that require biopython - however
  for most use-cases tardis will run fine without biopython

Configuration
=============

Tardis is configured by one or more `TOML <https://github.com/toml-lang/toml>`_
files.  The system configuration in ``/etc/tardis/tardis.toml`` is always read
first, unless disabled by the command line option ``--no-sysconfig``, in which
case it is skipped.

Values here may be overridden by command line arguments or user configuration.
The user configuration is taken from the first in the following list which
exists:

- the file specified by the command line option ``--userconfig``
- the file ``tardis.toml`` in the current directory
- the file ``~/.tardis.toml``

Only the first of these which exists is read;  the others are ignored.

Command line arguments always override values from configuration files.
