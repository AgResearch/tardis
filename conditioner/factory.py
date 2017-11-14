import logging,os, itertools, re, string, stat , subprocess
import tutils.tutils as tutils
import conditioner.fastq as fastq
import conditioner.fasta as fasta
import conditioner.fastq2fasta as fastq2fasta
import conditioner.text as text
import conditioner.data as data
import conditioner.blastxml as blastxml
import conditioner.pdf as pdf
import conditioner.sam as sam
import conditioner.bam as bam
import conditioner.wait as wait
import job.local as local
import job.condor as condor
import job.slurm as slurm
import job.hpc as hpc

global SLURM_MAXARRAYSIZE
SLURM_MAXARRAYSIZE=500  # for testing - make 1000 for real


class hpcConditioner(object):
    """
    This is a controller and factory class , for orchestrating conditioning , unconditioning
    """
    #logWriter = tardisLogger
    #workingRoot = None                                 
    #jobList = [] # a list of all the conditioned commands executed - these will map 1-1 to the 
    #              # data conditioners
    def __init__(self, logWriter, workingRoot, options,toolargv = [] ):
        super(hpcConditioner, self).__init__()
        self.toolargv = toolargv  # the command that is to be condtiioned
        self.logWriter = logWriter
        self.jobList = []
        self.workingRoot = workingRoot
        self.logWriter.info("hpcConditioner base class initialised. Environment is : ")
        self.logWriter.info(os.environ)
        self.hpcClass = None
        self.hpcJobNumber = 1
        self.options = options   


        if options["hpctype"] == "condor":
            self.hpcClass = condor.condorhpcJob
        elif options["hpctype"] == "local":
            self.hpcClass = local.localhpcJob            
        elif options["hpctype"] == "slurm":
            self.hpcClass = slurm.slurmhpcJob            
        else:
            self.logWriter.info("unknown hpctype %s, will use generic class"%options["hpctype"])
            self.hpcClass = hpc.hpcJob

    def launchArrayJobs(self):
        """
        this is only applicable to slurm jobs. This is called after the job scripts have all been
        created. One or more array jobs are launched (more than one , if the number of jobs is
        > SLURM_MAXARRAYSIZE=1000. The array job looks roughly like this :
#!/bin/bash -e

#SBATCH -J $tardis_job_moniker
#SBATCH -A $tardis_account_moniker        # Project Account
#SBATCH --time=20:00:00            # Walltime
#SBATCH --ntasks=1                 # number of parallel processes
#SBATCH --ntasks-per-socket=1      # number of processes allowed on a socket
#SBATCH --cpus-per-task=4          #number of threads per process
#SBATCH --hint=multithread         # enable hyperthreading
#SBATCH --mem-per-cpu=8G
#SBATCH --partition=inv-iranui     # Use nodes in the IRANUI partition
#SBATCH --array=1-$array_size%50          # Iterate 1 to N, but only run up to 50 concurrent runs at once
#SBATCH --error=$script-%A_%a.err
#SBATCH --output=$script-%A_%a.out

srun --cpu_bind=v,threads ${SLURM_ARRAY_TASK_ID}        
        """
        # "slurm_array_job" is launched by sbatch , and internally launches a shim script, passing 
        # to it the index of the job to run. The shim then just executes run1.sh, run2.sh
        # - which are instances of "slurm_shell".
        
        if self.hpcClass != slurm.slurmhpcJob:
            return

        # write the slurm array shim to the working folder
        slurm_array_shim=string.Template(self.options.get("slurm_array_shim",None))
        shimcode  = slurm_array_shim.safe_substitute(hpcdir=self.workingRoot)
        shim_file_name = os.path.join(self.workingRoot, "slurm_array_shim.sh")
        f=open(shim_file_name,"w")
        self.logWriter.info("hpcConditioner : writing array shim")
        f.writelines(shimcode)
        f.close()
        os.chmod(shim_file_name, stat.S_IRWXU | stat.S_IRGRP |  stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

        # write one or more array job files
        # figure out a job template from the options. (You can specify one of the inbuilt templates by name, or
        # supply a file containing a custom template). If nothign supplied at all we use hard coded default 
        job_template_name = self.options.get("job_template_name",None)
        job_template_filename = self.options.get("jobtemplatefile",None)        
        if job_template_name is None and job_template_filename is None:
            #use the default condor job template
            job_template_name = "default_slurm_array_job"
            
        if job_template_name is not None and job_template_filename is not None:
            raise tutils.tardisException("error both job_template_name (%s) and job_template_filename (%s) defined - only define one of these"%(job_template_name,job_template_filename) )

        if job_template_name is not None:
            job_template = self.options.get(job_template_name, None)   
        else:
            if not os.path.isfile(job_template_filename):
                raise tutils.tardisException("error job template file %s not found"%job_template_filename )    
            job_template = string.join(file(job_template_filename,"r"),"")
            
        if job_template is None:
            raise tutils.tardisException("hpcConditioner: Error job template is null after templating")
        job_template = string.Template(job_template)        
        
        n_launched = 0
        while n_launched < len(self.jobList):
            n_launch = min(SLURM_MAXARRAYSIZE, len(self.jobList) - n_launched)
        
            arraycode  = job_template.safe_substitute(tardis_job_moniker=self.toolargv[0], tardis_account_moniker=os.environ['LOGNAME'],\
                                                                 array_start=str(n_launched+1),array_stop=str(n_launched+n_launch),\
                                                                 hpcdir=self.workingRoot)
            array_jobfile_name = os.path.join(self.workingRoot, "array_%d-%d.slurm"%(n_launched+1,n_launched+n_launch))
            f=open(array_jobfile_name,"w")
            self.logWriter.info("hpcConditioner : writing array job %s"%array_jobfile_name)
            f.writelines(arraycode)
            f.close()
            os.chmod(array_jobfile_name, stat.S_IRWXU | stat.S_IRGRP |  stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

            # launch if we need to
            if self.options["dry_run"]:
                self.logWriter.info("slurmhpcJob : this is a dry run - not launching the job")
            else:
                slurm_submit = ["sbatch", "-v", array_jobfile_name]
                self.logWriter.info("slurmhpcJob : launching using %s"%str(slurm_submit))
                proc = subprocess.Popen(slurm_submit,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (stdout, stderr) = proc.communicate()
                submitreturncode = proc.returncode
                if submitreturncode == 0:
                    self.logWriter.info("slurmhpcJob : %s has returned (status %s) - here is its output (but now we wait for the real output !)"%(str(slurm_submit), submitreturncode))
                    self.logWriter.info("slurmhpcJob : stdout : \n%s"%stdout)
                    self.logWriter.info("slurmhpcJob : stderr : \n%s"%stderr)
                else:
                    self.logWriter.info("slurmhpcJob : error %s has returned status %s !)"%(str(slurm_submit), submitreturncode))
                    self.logWriter.info("slurmhpcJob : stdout : \n%s"%stdout)
                    self.logWriter.info("slurmhpcJob : stderr : \n%s"%stderr)
                    self.logWriter.info("slurmhpcJob : giving up, the array job spec may have bugs ?")
                    raise tutils.tardisException("hpcConditioner : %s"%stderr)

            n_launched += n_launch


    def getJobResultState(self):
        return  reduce(lambda x,y:hpc.hpcJob.stateAND(x,y), [job.state for job in self.jobList], hpc.hpcJob.OK)

    def getJobResultStateDescription(self):
        return string.join([job.stateDescription for job in self.jobList if len(job.stateDescription) > 0],';')


    def getConditionedCommandGenerator(self, dcPrototype):
        """
        returns an iterator over conditioned commands 
        """
        self.logWriter.info("getConditionedCommands : conditioning%s to hpc"%str(self.toolargv))

        # set up a generator of copies of the command that is to be conditioned. First, an array of repeat iterators.
        # (Note that there is a risk here that if nothing further is done this generator
        # will yield an infinite number of commands)
        commandGen = [ itertools.repeat(token) for token in self.toolargv ]

        # the repeat-iterators of input file arguments need to be replaced by iteration over HPC conditioned
        # (i.e. split) input files - these iterators are supplied by DataConditioners. The command will contain a conditioning
        # directive for each input file - e.g. _condition_fastq_input_/dataset/reseq_wf_dev/active/afm/afm_medium1/s_1_1_sequence_MEDIUM.fastq
        # etc 

        # scan the tool command for conditioning directives, and action them by obtaining
        # an iterator across conditioned data file names. (There iterators are implemented as generator methods -
        # the body of the generator is responsible for doing the actual split of the input)
        for i in range(0,len(self.toolargv)):
            ######
            ###### look for input conditioning...
            ######  

            #fastq
            match = re.search(fastq.fastqDataConditioner.input_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fastq input conditioner")
                commandGen[i] = fastq.fastqDataConditioner.addInputConditioner(dcPrototype,match.groups()[0], conditioningPattern=fastq.fastqDataConditioner.input_directive_pattern, conditioningWord = self.toolargv[i])
                continue

            #paired fastq (this does a paired-aware split, checking that input files match up. Commands
            # using paired input can be condiioned using the unpaired fastq conditioner (however
            # this does not validate the pairing of the input files)
            match = re.search(fastq.fastqDataConditioner.paired_input_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a paired fastq input conditioner")
                commandGen[i] = fastq.fastqDataConditioner.addPairedInputConditioner(dcPrototype,match.groups()[0], conditioningPattern=fastq.fastqDataConditioner.paired_input_directive_pattern,conditioningWord = self.toolargv[i])
                continue
            

            # fasta
            match = re.search(fasta.fastaDataConditioner.input_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fasta input conditioner")
                commandGen[i] = fasta.fastaDataConditioner.addInputConditioner(dcPrototype,match.groups()[0], conditioningPattern=fasta.fastaDataConditioner.input_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # fastq2fasta
            match = re.search(fastq2fasta.fastq2fastaDataConditioner.input_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fastq2fasta input conditioner")
                commandGen[i] = fastq2fasta.fastq2fastaDataConditioner.addInputConditioner(dcPrototype,match.groups()[0], conditioningPattern=fastq2fasta.fastq2fastaDataConditioner.input_directive_pattern, conditioningWord = self.toolargv[i])
                continue

            # text
            match = re.search(text.textDataConditioner.input_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a text input conditioner")
                commandGen[i] = text.textDataConditioner.addInputConditioner(dcPrototype,match.groups()[0],conditioningPattern=text.textDataConditioner.input_directive_pattern,conditioningWord = self.toolargv[i])
                continue
            
            # text input which is compressed and should not be uncompressed
            match = re.search(text.textDataConditioner.compressedinput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a compressed text input conditioner (will not uncompress)")
                commandGen[i] = text.textDataConditioner.addInputConditioner(dcPrototype,match.groups()[0],conditioningPattern=text.textDataConditioner.compressedinput_directive_pattern,conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue

   
            ########
            ######## look for output conditioning. Output conditioners generated conditioned output words for the
            ######## conditioned command, and also "uncondition" the actual output - i.e. join it back together
            ########

            # note - for most text conditioning classes, output unconditioning simply uses the text base class method - i.e.
            # output data is simply concatenated (blastxml is an exception). So for example _condition_text_output_ and
            # _condition_fastq_output_ (currently) execute exactly the same method. However the latter is preferred as it is more
            # informative as to the meaning of the command. (Also, subsequent implementation may include a more
            # strongly typed handling of some of these formats)


            # generic (will generate conditioned output words but no unconditioning )
            match = re.search(data.dataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a generic output conditioner")
                commandGen[i] = data.dataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=data.dataConditioner.output_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            
            # fastq
            match = re.search(fastq.fastqDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fastq output conditioner")                
                commandGen[i] = fastq.fastqDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=fastq.fastqDataConditioner.output_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # fastq (do not compress output)
            match = re.search(fastq.fastqDataConditioner.uncompressedoutput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fastq uncompressed output conditioner")                
                commandGen[i] = fastq.fastqDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern = fastq.fastqDataConditioner.uncompressedoutput_directive_pattern,\
                                                                            conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue

            # fasta
            match = re.search(fasta.fastaDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fasta output conditioner")
                commandGen[i] = fasta.fastaDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=fasta.fastaDataConditioner.output_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # fasta (do not compress output)
            match = re.search(fasta.fastaDataConditioner.uncompressedoutput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fasta uncompressed output conditioner")
                commandGen[i] = fasta.fastaDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern = fasta.fastaDataConditioner.uncompressedoutput_directive_pattern,\
                                                                            conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue

            # text
            match = re.search(text.textDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a text output conditioner")
                commandGen[i] = text.textDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=text.textDataConditioner.output_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # text (do not compress output)
            match = re.search(text.textDataConditioner.uncompressedoutput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed text output conditioner")
                commandGen[i] = text.textDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern = text.textDataConditioner.uncompressedoutput_directive_pattern, \
                                                                           conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue
            

            # blastxml
            self.logWriter.info("DEBUG searching %s for %s"%(self.toolargv[i], blastxml.blastxmlDataConditioner.output_directive_pattern))
            match = re.search(blastxml.blastxmlDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a blastxml output conditioner")
                commandGen[i] = blastxml.blastxmlDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=blastxml.blastxmlDataConditioner.output_directive_pattern,\
                                                                               conditioningWord = self.toolargv[i])
                continue

            # pdf
            match = re.search(pdf.pdfDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a pdf output conditioner")
                commandGen[i] = pdf.pdfDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=pdf.pdfDataConditioner.output_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # pdf (do not compress output)
            match = re.search(pdf.pdfDataConditioner.uncompressedoutput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed pdf output conditioner")
                commandGen[i] = pdf.pdfDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=pdf.pdfDataConditioner.uncompressedoutput_directive_pattern, \
                                                                          conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue

            # bam - samtools merge of bam output
            match = re.search(bam.bamDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a bam output conditioner")
                commandGen[i] = bam.bamDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=bam.bamDataConditioner.output_directive_pattern,conditioningWord = self.toolargv[i])
                continue
            

            # SAM
            self.logWriter.info("DEBUG searching %s for %s"%(self.toolargv[i], sam.samDataConditioner.output_directive_pattern))
            match = re.search(sam.samDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a SAM output conditioner")
                commandGen[i] = sam.samDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=sam.samDataConditioner.output_directive_pattern, conditioningWord = self.toolargv[i])
                continue

            # SAM (do not compress output - i.e. final merged file is SAM , not BAM)
            self.logWriter.info("DEBUG searching %s for %s"%(self.toolargv[i], sam.samDataConditioner.uncompressedoutput_directive_pattern))
            match = re.search(sam.samDataConditioner.uncompressedoutput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a SAM output conditioner")
                commandGen[i] = sam.samDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=sam.samDataConditioner.uncompressedoutput_directive_pattern,\
                                                                          conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue


            # SAM - headless output 
            self.logWriter.info("DEBUG searching %s for %s"%(self.toolargv[i], sam.samDataConditioner.headlessoutput_directive_pattern))
            match = re.search(sam.samDataConditioner.headlessoutput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a headless SAM output conditioner")
                commandGen[i] = sam.samDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=sam.samDataConditioner.headlessoutput_directive_pattern,\
                                                                          conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue

            # wait unconditioner - this just waits for output
            self.logWriter.info("DEBUG searching %s for %s"%(self.toolargv[i], wait.waitDataConditioner.output_directive_pattern))
            match = re.search(wait.waitDataConditioner.output_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a wait output conditioner")
                commandGen[i] = wait.waitDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=wait.waitDataConditioner.output_directive_pattern,\
                                                                          conditioningWord = self.toolargv[i], compressionConditioning = False)
                continue
            
            


            #######
            ####### look for output conditioning that does not condition the command - i.e. deal with "by-products" 
            #######

            # fastq
            match = re.search(fastq.fastqDataConditioner.product_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fastq product conditioner")                
                commandGen[i] = fastq.fastqDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], commandConditioning = False, \
                                                                            conditioningPattern=fastq.fastqDataConditioner.product_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # fastq - no compression
            match = re.search(fastq.fastqDataConditioner.uncompressedproduct_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed fastq product conditioner")                
                commandGen[i] = fastq.fastqDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0],conditioningPattern = fastq.fastqDataConditioner.uncompressedproduct_directive_pattern,\
                                                conditioningWord = self.toolargv[i],commandConditioning = False, compressionConditioning = False)
                continue

            # fasta
            match = re.search(fasta.fastaDataConditioner.product_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a fasta product conditioner")
                commandGen[i] = fasta.fastaDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], commandConditioning = False, \
                                                                            conditioningPattern=fasta.fastaDataConditioner.product_directive_pattern,conditioningWord = self.toolargv[i])
                continue

            # fasta - no compression
            match = re.search(fasta.fastaDataConditioner.uncompressedproduct_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed fasta product conditioner")
                commandGen[i] = fasta.fastaDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0],conditioningPattern = fasta.fastaDataConditioner.uncompressedproduct_directive_pattern,\
                                                conditioningWord = self.toolargv[i],commandConditioning = False, compressionConditioning = False)
                continue

            # text
            match = re.search(text.textDataConditioner.product_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a text product conditioner")
                commandGen[i] = text.textDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=text.textDataConditioner.product_directive_pattern,\
                                                                           conditioningWord = self.toolargv[i],commandConditioning = False)
                continue


            # text - no compression
            match = re.search(text.textDataConditioner.uncompressedproduct_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed text product conditioner")
                commandGen[i] = text.textDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern = text.textDataConditioner.uncompressedproduct_directive_pattern,\
                                    conditioningWord = self.toolargv[i],commandConditioning = False, compressionConditioning = False)
                continue

            # SAM (do not compress output - i.e. final merged file is SAM , not BAM)
            match = re.search(sam.samDataConditioner.uncompressedproduct_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed SAM product conditioner")
                commandGen[i] = sam.samDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=sam.samDataConditioner.uncompressedproduct_directive_pattern,\
                                    conditioningWord = self.toolargv[i], commandConditioning = False, compressionConditioning = False)
                continue
            
            # BAM (do a samtools merge) 
            match = re.search(bam.bamDataConditioner.product_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a BAM product conditioner")
                commandGen[i] = bam.bamDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=bam.bamDataConditioner.product_directive_pattern,\
                                    conditioningWord = self.toolargv[i], commandConditioning = False, compressionConditioning = False)
                continue

            # pdf
            match = re.search(pdf.pdfDataConditioner.product_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a pdf product conditioner")
                commandGen[i] = pdf.pdfDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningPattern=pdf.pdfDataConditioner.product_directive_pattern,\
                                    conditioningWord = self.toolargv[i], commandConditioning = False, compressionConditioning = False)
                continue


            # pdf - no compression
            match = re.search(pdf.pdfDataConditioner.uncompressedproduct_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding an uncompressed  pdf product conditioner")
                commandGen[i] = pdf.pdfDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningWord = self.toolargv[i],\
                                                        conditioningPattern = pdfDataConditioner.uncompressedproduct_directive_pattern,\
                                                        commandConditioning = False, compressionConditioning = False)
                continue
            

            # wait conditioner 
            match = re.search(wait.waitDataConditioner.product_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a wait product conditioner")
                commandGen[i] = wait.waitDataConditioner.addOutputUnconditioner(dcPrototype,match.groups()[0], conditioningWord = self.toolargv[i],\
                                                        conditioningPattern = waitDataConditioner.product_directive_pattern,\
                                                        commandConditioning = False, compressionConditioning = False)
                continue



            ########
            ######## look for throughput conditioning. Throughput conditioners generated conditioned output words for the
            ######## conditioned command, but do not "uncondition" the output (i.e. join it back together), since
            ######## the conditioned file is an intermediate product. This supports (limited) conditioning of short
            ######## shell based pipelines - e.g. "command1; command2" - where command2 uses the output of command1
            ########

            # generic (will generate conditioned output words but no unconditioning )
            match = re.search(data.dataConditioner.throughput_directive_pattern, self.toolargv[i])
            if match != None:
                self.logWriter.info("getConditionedCommands : adding a generic throughput conditioner")
                commandGen[i] = data.dataConditioner.addThroughputConditioner(dcPrototype,match.groups()[0], conditioningPattern = data.dataConditioner.throughput_directive_pattern,\
                                                                         conditioningWord = self.toolargv[i])
                continue
            
        # if no output conditioners have been specified and we are running as part of a workflow, 
        # add a default unconditioner. If we are part of a workflow, this will result in waiting for
        # results and cleaning up conditioned input. If there are no output conditioners, we do not 
        # wait for results and conditioned input is not removed 
        # The default output conditioner will wait for the output but not do much else
        if len(dcPrototype.outputUnconditioners) == 0 and self.options["in_workflow"] :
            self.logWriter.info("hpcConditoner : adding default outputUncondtioner (in workflow and no output conditioning was specified)")
            commandGen.append(data.dataConditioner.addOutputUnconditioner(dcPrototype,commandConditioning = False, conditioningWord = ""))

        if len(dcPrototype.inputConditioners) + len(dcPrototype.pairedInputConditioners) + \
                              len(dcPrototype.outputUnconditioners) + len(dcPrototype.throughputConditioners) > 0:
            self.logWriter.info("hpcConditoner : making zipped command iterator from %s"%str(commandGen))
            command_iter = itertools.izip(*commandGen)
            self.logWriter.info("DEBUG : zipped iterator is %s"%str(command_iter))
            return command_iter 
        else:
            self.logWriter.info("(no conditioning specified - returning original command)")                
            return ([self.toolargv])


    def unconditionJobStreams(self, stdout, stderr):
        """
        write stdout and stderr from each job to stdout of this process
        """
        for j in self.jobList:
            if not j.sent:
                self.logWriter.info("unconditionJobStreams : warning job %s has not sent data (so should not be here !), ignoring"%j.jobNumber)
                continue

            if j.stdoutfilename != None:
                #stdout.write("\n--- begin stdout from job %s ---\n"%j.jobNumber)
                f=open(j.stdoutfilename,"r")
                stdout.writelines(f)
                f.close()
                #stdout.write("\n--- end stdout from job %s ---\n"%j.jobNumber)

            # output non-trivial stderr, on stderr
            if j.stderrfilename != None:
                f=open(j.stderrfilename,"r")
                e=[line for line in f.readlines() if len(line.strip()) > 0]
                if len(e) > 0:
                    #stderr.write("\n--- begin stderr from job %s ---\n"%j.jobNumber)
                    stderr.writelines(e)
                    #stderr.write("\n--- end stderr from job %s ---\n"%j.jobNumber) 
                f.close() 

    
    @staticmethod
    def getConditionerDEPRECATED(toolargv):
        """
        get a specific conditioner. If we can't make sense of the
        request, return an instance of the base class , this will
        do non-workflow conditioning  
        """
        
        hpcConditioner.logWriter.info("getting hpc conditioner to handle : %s"%str(toolargv))
        
        # we may add sub-classes of hpcConditioner  one day but not yet
        # (if so , test the toolargs here to see what sort of conditioner to create)
        return hpcConditioner(toolargv)

    def gethpcJob(self, conditioned_toolargv):
        """
        base class returns a generic hpcJob
        """
        if self.hpcClass == condor.condorhpcJob:
            self.logWriter.info("creating condorhpcJob")                    
            hpcjob = self.hpcClass(self,conditioned_toolargv)            
        elif self.hpcClass == local.localhpcJob:
            self.logWriter.info("creating localhpcJob")                
            hpcjob = self.hpcClass(self,conditioned_toolargv)
        elif self.hpcClass == slurm.slurmhpcJob:
            self.logWriter.info("creating slurmhpcJob")                
            hpcjob = self.hpcClass(self,conditioned_toolargv)            
        else:
            self.logWriter.info("creating generic hpcJob")
            hpcjob = self.hpcClass(self,conditioned_toolargv)
            
        self.jobList.append(hpcjob)
        self.hpcJobNumber += 1

        # if this is not the first hpcjob, then ask the previous
        # job to induct the one we just made. (e.g. for localhpcjobs
        # this means all the jobs have a shared copy of the workerlist. (You can't store
        # this as a class variable, for various reasons))
        if len(self.jobList) > 1:
            self.jobList[-2].induct(hpcjob)
        
        return hpcjob

    def getUnsubmittedJobs(self):
        return self.hpcClass.getUnsubmittedJobs(self.jobList)

    def getJobSubmittedCount(self):
        return reduce(lambda x,y:x+{True:1, False:0}[y > 0] , [job.submitCount for job in self.jobList],0)

    def getResultsSentCount(self):
        return reduce(lambda x,y:x+{True:1, False:0}[y] , [job.sent for job in self.jobList],0)
        
    def retryJobSubmission(self, maxRetries = 2, retryPause = 30):
        """
        retry jobs which appear to have not been submitted
        """
        retryCount = 0
        while len(self.getUnsubmittedJobs()) > 0 and retryCount < maxRetries:
            
            self.logWriter.info("retryJobSubmission : found %d jobs that appear to need re-submitting  - pausing for %d then retrying (max retries =%d)"%\
                                (len(self.getUnsubmittedJobs()), retryPause, maxRetries))
            time.sleep(retryPause)
            self.logWriter.info("done pausing, processing retries")
            for retryJob in self.getUnsubmittedJobs():
                self.logWriter.info("retrying job")
                retryJob.runCommand()
                
            retryCount += 1


        self.logWriter.info("retryCount = %d"%retryCount)
                                
        return 


    @staticmethod
    def getLogger(options):
        l = logging.getLogger('tardis')
        #d = tempfile.mkdtemp(prefix="tardis_", dir=HPC_CONDITIONER_ROOT)
        d=tutils.getWorkDir(options)
        #f = open(os.path.join(d, "conditioner.log"), "w")
        lh = logging.FileHandler(os.path.join(d,"tardis.log"))
        f = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        lh.setFormatter(f)
        l.addHandler(lh)
        l.setLevel(logging.INFO)
        return (l,d)

    
