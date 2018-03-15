*tardis* supports a relatively more interactive than batch style of use
of HPC compute resources, in the sense that it supports entering a
foreground shell command to execute a task in parallel on a cluster,
just as one would enter a foreground command to run an application
interactively. The details of the HPC environment (which load balancer /
scheduler etc ) do not need to be known by the end-user, these are
encapsulated by *tardis*.

| *tardis* is a pre-processor which reconditions (precompiles) a
marked-up unix shell command to generate
| a sequence of "conditioned" commands which it then launches on a
cluster. The mark-up is added by the user to indicate the input(s) and
output(s) of the command. *tardis* splits input files into "conditioned"
input chunks and will "uncondition" (join together) the output chunks to
obtain the final outputs, with the sequence of conditioned commands
referring to conditioned input and output filenames.

The goal of *tardis* is that the user should only need to know the API
of the application they are running - i.e. what is the command needed to
start a single process on their local machine to execute their
computation: the pre-processor then handles all of the administrivia of
launching many processes on a cluster (or optionally on the local
machine) to complete the computation. Ideally the user should not even
have to know of the existence of the scheduler / load balancer (such as
slurm , condor etc).

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
   scheduler and load balancer.
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

