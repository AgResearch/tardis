#!/bin/sh
#these assignments are so that the shell has access to the symbols defined in the template
hpcdir=/bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz
startdir=/bifo/active/bioinformatics_dev/tardis/tardis/test

input_conditioning=False

if [ False != True ]; then
   cd /bifo/active/bioinformatics_dev/tardis/tardis/test > /dev/null 2>&1               # command should see the original cwd of user as its cwd
else
   cd /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz > /dev/null 2>&1                 # command should see the working folder where the conditioned input lives, as its cwd
fi

job_started=`date`
echo "job_started=$job_started" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog
echo "
---- begin environment listing (pre config)----" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog
env >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog
echo "
---- end environment listing (pre config)----" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog


# configure environment - e.g. activate conda packages, load moules
# or other
export PATH="/stash/miniconda3/bin:$PATH"  # ensure we know how to use conda even if launched from vanilla node
source /etc/profile # this will set up the runtime env  to see the same path seen by a user


echo "
---- begin environment listing (post config)----" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog
env >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog
echo "
---- end environment listing (post config)----" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog

# run the command
echo "hello world" 

# write datestamep and exit code to tardis log
job_exit_code=$?
job_ended=`date`

echo "job_exit_code=$job_exit_code" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog
echo "job_ended=$job_ended" >> /bifo/active/bioinformatics_dev/tardis/tardis/test/tardis_nxt9Pz/run1.tlog   #this must go last, after exit code (to avoid race condition as poll is for job_ended)

# exit with the exit code we received from the command
exit $job_exit_code
