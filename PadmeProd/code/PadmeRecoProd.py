#!/usr/bin/python

import os
import sys
import getopt
import time
import subprocess
import shutil
import shlex
import re
import daemon
import daemon.pidfile

from PadmeProdServer import PadmeProdServer
from PadmeMCDB import PadmeMCDB
from ProxyHandler import ProxyHandler

# Get location of padme-prod software from PADME_PROD env variable
# Default to ./padme-prod if not set
PADME_PROD = os.getenv('PADME_PROD',"./padme-prod")

# Create global handler to PadmeMCDB
DB = PadmeMCDB()

# Create proxy handler
PH = ProxyHandler()

# ### Define PADME grid resources ###

# SRMs to access PADME area on the LNF and CNAF storage systems
PADME_SRM_URI = {
    "LNF":  "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org",
    "CNAF": "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape"
}

PADME_ROOT_URI = {
    "LNF": "root://atlasse.lnf.infn.it:1094//vo.padme.org",
}

# List of available submission sites and corresponding default CE nodes
PADME_CE_NODE = {
    "LNF":   "atlasce1.lnf.infn.it:8443/cream-pbs-padme_c7",
    "CNAF":  "ce04-lcg.cr.cnaf.infn.it:8443/cream-lsf-padme",
    "SOFIA": "cream.grid.uni-sofia.bg:8443/cream-pbs-cms"
}

# Initialize global parameters and set some default values
PROD_RUN_NAME = ""
PROD_NAME = ""
PROD_FILES_PER_JOB = 100
PROD_FILES_PER_JOB_MAX = 1000
PROD_STORAGE_DIR = ""
PROD_DIR = ""
PROD_SCRIPT = "%s/PadmeProd/script/padmereco_prod.py"%PADME_PROD
PROD_SOURCE_URI = PADME_SRM_URI["LNF"]
PROD_CE_NODE = ""
PROD_CE_PORT = "8443"
PROD_CE_QUEUE = ""
PROD_RUN_SITE = "LNF"
PROD_STORAGE_SITE = "LNF"
PROD_RECO_VERSION = ""
PROD_PROXY_FILE = ""
PROD_YEAR = ""
PROD_DEBUG = 0
PROD_DESCRIPTION = "TEST"

def print_help():

    print "PadmeRecoProd -r <run_name> -v <version> [-y <year>] [-j <files_per_job>] [-n <prod_name>] [-s <submission_site>] [-C <CE_node> [-P <CE_port>] -Q <CE_queue>] [-d <storage_site>] [-p <proxy>] [-D <description>] [-V] [-h]"
    print "  -r <run_name>\t\tname of the run to process"
    print "  -v <version>\t\tversion of PadmeReco to use for production. Must be installed on CVMFS."
    print "  -y <year>\t\tyear of run. N.B. used only if run name is not self-documenting"
    print "  -n <prod_name>\tname for the production. Default: <run_name>_<version>"
    print "  -j <files_per_job>\tnumber of rawdata files to be reconstructed by each job. Default: %d"%PROD_FILES_PER_JOB
    print "  -s <submission_site>\tsite to be used for job submission. Allowed: %s. Default: %s"%(",".join(PADME_CE_NODE.keys()),PROD_RUN_SITE)
    print "  -S <source_uri>\tURI to use to get list of files to process"
    print "  -C <CE_node>\t\tCE node to be used for job submission. If defined, <submission_site> will not be used"
    print "  -P <CE_port>\t\tCE port. Default: %s"%PROD_CE_PORT
    print "  -Q <CE_queue>\t\tCE queue to use for submission. This parameter is mandatory if -C is specified"
    print "  -d <storage_site>\tsite where the jobs output will be stored. Allowed: %s. Default: %s"%(",".join(PADME_SRM_URI.keys()),PROD_STORAGE_SITE)
    print "  -p <proxy>\t\tLong lived proxy file to use for this production. If not defined it will be created."
    print "  -D <description>\tProduction description (to be stored in the DB). '%s' if not given."%PROD_DESCRIPTION
    print "  -V\t\t\tenable debug mode. Can be repeated to increase verbosity"

def execute_command(command):
    if PROD_DEBUG: print "> %s"%command
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()
    return (p.returncode,out,err)

def rawfile_sort_key(f):
    r = re.match("^.*_(\d\d)_(\d\d\d).root$",f)
    if r:
        index = int(r.group(2))*100+int(r.group(1))
        #print f,r.group(1),r.group(2),index
        return index
    return 0

def get_run_file_list(run):

    run_file_list = []
    run_dir = "%s/daq/%s/rawdata/%s"%(PROD_SOURCE_URI,PROD_YEAR,run)

    tries = 0
    cmd = "gfal-ls %s"%run_dir
    while True:
        (rc,out,err) = execute_command(cmd)
        if rc == 0:
            for line in iter(out.splitlines()):
                if PROD_DEBUG >= 2: print line
                run_file_list.append(line)
            break
        else:
            print "WARNING gfal-ls returned error status %d while retrieving file list from run dir %s"%(rc,run_dir)
            if PROD_DEBUG:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err
            tries += 1
            if tries >= 3:
                print "*** ERROR *** Could not retrieve list of files in %s. Tried %d times."%(run_dir,tries)
                break
            time.sleep(5)

    run_file_list.sort(key=rawfile_sort_key)
    return run_file_list

def main(argv):

    # Declare that here we can possibly modify these global variables
    global PROD_RUN_NAME
    global PROD_NAME
    global PROD_FILES_PER_JOB
    global PROD_STORAGE_DIR
    global PROD_DIR
    global PROD_SCRIPT
    global PROD_SOURCE_URI
    global PROD_CE_NODE
    global PROD_CE_PORT
    global PROD_CE_QUEUE
    global PROD_RUN_SITE
    global PROD_STORAGE_SITE
    global PROD_RECO_VERSION
    global PROD_PROXY_FILE
    global PROD_YEAR
    global PROD_DEBUG
    global PROD_DESCRIPTION

    try:
        opts,args = getopt.getopt(argv,"hVr:y:n:j:s:d:S:C:P:Q:v:p:D:",[])
    except getopt.GetoptError as e:
        print "Option error: %s"%str(e)
        print_help()
        sys.exit(2)

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit(0)
        elif opt == '-V':
            PROD_DEBUG += 1
        elif opt == '-r':
            PROD_RUN_NAME = arg
        elif opt == '-y':
            PROD_YEAR = arg
        elif opt == '-n':
            PROD_NAME = arg
        elif opt == '-v':
            PROD_RECO_VERSION = arg
        elif opt == '-S':
            PROD_SOURCE_URI = arg
        elif opt == '-C':
            PROD_CE_NODE = arg
        elif opt == '-P':
            PROD_CE_PORT = arg
        elif opt == '-Q':
            PROD_CE_QUEUE = arg
        elif opt == '-p':
            PROD_PROXY_FILE = arg
        elif opt == '-D':
            PROD_DESCRIPTION = arg
        elif opt == '-s':
            if arg in PADME_CE_NODE.keys():
                PROD_RUN_SITE = arg
            else:
                print "*** ERROR *** Invalid submission site %s. Valid: %s"%(arg,",".join(PADME_CE_NODE.keys()))
                print_help()
                sys.exit(2)
        elif opt == '-d':
            if arg in PADME_SRM_URI.keys():
                PROD_STORAGE_SITE = arg
            else:
                print "*** ERROR *** Invalid storage site %s. Valid: %s"%(arg,",".join(PADME_SRM_URI.keys()))
                print_help()
                sys.exit(2)
        elif opt == '-j':
            try:
                PROD_FILES_PER_JOB = int(arg)
            except ValueError:
                print "*** ERROR *** Invalid number of files per job: '%s'"%arg
                print_help()
                sys.exit(2)

    if not PROD_RUN_NAME:
        print "*** ERROR *** No run name specified."
        print_help()
        sys.exit(2)

    if not PROD_RECO_VERSION:
        print "*** ERROR *** No software version specified."
        print_help()
        sys.exit(2)

    # Choose submission CE
    if PROD_CE_NODE:
        # If CE was explicitly defined, use it
        if not PROD_CE_QUEUE:
            print "*** ERROR *** No queue specified for CE %s"%PROD_CE_NODE
            print_help()
            sys.exit(2)
        PROD_CE = "%s:%s/%s"%(PROD_CE_NODE,PROD_CE_PORT,PROD_CE_QUEUE)
    else:
        # If CE was not defined, get it from submission site
        PROD_CE = PADME_CE_NODE[PROD_RUN_SITE]

    # Define storage SRM URI according to chosen storage site (will add more options)
    PROD_SRM = PADME_SRM_URI[PROD_STORAGE_SITE]

    # Extract year of run from run name. If not found, use default
    r = re.match("^run_\d+_(\d\d\d\d)\d\d\d\d_\d\d\d\d\d\d$",PROD_RUN_NAME)
    if r:
        PROD_YEAR = r.group(1)
    else:
        if PROD_YEAR:
            if PROD_DEBUG: print "Run name format \'%s\' is unknown: using year %s"%(PROD_RUN_NAME,PROD_YEAR)
        else:
            print "*** ERROR *** run name format \'%s\' is unknown and no year was specified"%PROD_RUN_NAME
            print_help()
            sys.exit(2)

    if PROD_FILES_PER_JOB < 0 or PROD_FILES_PER_JOB > PROD_FILES_PER_JOB_MAX:
        print "*** ERROR *** Invalid number of files per job requested: %d - Max allowed: %d."%(PROD_FILES_PER_JOB,PROD_FILES_PER_JOB_MAX)
        print_help()
        sys.exit(2)

    if PROD_NAME == "":
        PROD_NAME = "%s_%s"%(PROD_RUN_NAME,PROD_RECO_VERSION)
        if PROD_DEBUG: print "No Production Name specified: using %s"%PROD_NAME

    # If storage directory was not specified, use default
    if PROD_STORAGE_DIR == "":
        PROD_STORAGE_DIR = "/daq/%s/recodata/%s/%s"%(PROD_YEAR,PROD_RECO_VERSION,PROD_NAME)

    # If production directory was not specified, use default
    if PROD_DIR == "":
        version_dir = "prod/%s"%PROD_RECO_VERSION
        if not os.path.exists(version_dir):
            os.mkdir(version_dir)
        elif not os.path.isdir(version_dir):
            print "*** ERROR *** '%s' exists but is not a directory"%version_dir
            sys.exit(2)
        PROD_DIR = "%s/%s"%(version_dir,PROD_NAME)

    # Show info about required production
    print "- Starting production %s"%PROD_NAME
    print "- Processing run %s"%PROD_RUN_NAME
    print "- PadmeReco version %s"%PROD_RECO_VERSION
    print "- Each job will process %d rawdata files"%PROD_FILES_PER_JOB
    print "- Submitting jobs to CE %s"%PROD_CE
    print "- Main production directory: %s"%PROD_DIR
    print "- Production script: %s"%PROD_SCRIPT
    print "- Storage SRM: %s"%PROD_SRM
    print "- Storage directory: %s"%PROD_STORAGE_DIR
    if PROD_DEBUG:
        print "- Debug level: %d"%PROD_DEBUG
        PH.debug = PROD_DEBUG

    # Check if production dir already exists
    if os.path.exists(PROD_DIR):
        print "*** ERROR *** Path %s already exists"%PROD_DIR
        sys.exit(2)

    # Check if production already exists in DB
    if (DB.is_prod_in_db(PROD_NAME)):
        print "*** ERROR *** A production named '%s' already exists in DB"%PROD_NAME
        sys.exit(2)

    # Create production directory to host support dirs for all jobs
    print "- Creating production dir %s"%PROD_DIR
    os.mkdir(PROD_DIR)

    # Check if long-lived (30 days) proxy was defined. Create it if not
    JOB_PROXY_FILE = "%s/%s.proxy"%(PROD_DIR,PROD_NAME)
    if PROD_PROXY_FILE:
        if os.path.isfile(PROD_PROXY_FILE):
            try:
                shutil.copyfile(PROD_PROXY_FILE,JOB_PROXY_FILE)
            except:
                print "*** ERROR *** Unable to copy long-lived proxy file %s to %s"%(PROD_PROXY_FILE,JOB_PROXY_FILE)
                shutil.rmtree(PROD_DIR)
                sys.exit(2)
            try:
                os.chmod(JOB_PROXY_FILE,0o600)
            except:
                print "*** ERROR *** Unable to set access permissions of long-lived proxy file %s"%JOB_PROXY_FILE
                shutil.rmtree(PROD_DIR)
                sys.exit(2)
        else:
            print "*** ERROR *** Long-lived proxy file %s was not found"%PROD_PROXY_FILE
            shutil.rmtree(PROD_DIR)
            sys.exit(2)
    else:
        print "- Creating long-lived proxy file %s"%JOB_PROXY_FILE
        proxy_cmd = "voms-proxy-init --valid 720:0 --out %s"%JOB_PROXY_FILE
        if PROD_DEBUG: print "> %s"%proxy_cmd
        if subprocess.call(shlex.split(proxy_cmd)):
            print "*** ERROR *** while generating long-lived proxy file %s"%JOB_PROXY_FILE
            shutil.rmtree(PROD_DIR)
            sys.exit(2)

    # Check if VOMS proxy exists and is valid. Renew it if not.
    # This is needed to get list of files in this run and to create the storage dir on the SRM server
    PH.renew_voms_proxy(JOB_PROXY_FILE)

    # Get list of files for run to reconstruct
    # All files are assumed to be on the LNF SE and available via the ROOTX protocol
    file_list = get_run_file_list(PROD_RUN_NAME)
    job_file_lists = []
    job_file_list = []
    for f in file_list:
        if len(job_file_list) == PROD_FILES_PER_JOB:
            job_file_lists.append(job_file_list)
            job_file_list = []
        file_url = "%s/daq/%s/rawdata/%s/%s"%(PADME_ROOT_URI["LNF"],PROD_YEAR,PROD_RUN_NAME,f)
        job_file_list.append(file_url)
    if job_file_list: job_file_lists.append(job_file_list)

    # Create new production in DB
    print "- Creating new production in DB"
    prodId = DB.create_recoprod(PROD_NAME,PROD_RUN_NAME,PROD_DESCRIPTION,PROD_CE,PROD_RECO_VERSION,PROD_DIR,PROD_SRM,PROD_STORAGE_DIR,JOB_PROXY_FILE,len(job_file_lists))

    # Create production directory in the storage SRM
    print "- Creating dir %s in %s"%(PROD_STORAGE_DIR,PROD_SRM)
    gfal_mkdir_cmd = "gfal-mkdir -p %s%s"%(PROD_SRM,PROD_STORAGE_DIR)
    if PROD_DEBUG: print ">",gfal_mkdir_cmd
    rc = subprocess.call(shlex.split(gfal_mkdir_cmd))

    # Create job structures
    print "- Creating directory structure for production jobs"
    for j in range(0,len(job_file_lists)):

        jobName = "job%05d"%j

        # Create dir to hold individual job info
        jobLocalDir = jobName
        jobDir = "%s/%s"%(PROD_DIR,jobLocalDir)
        try:
            os.mkdir(jobDir)
        except:
            print "*** ERROR *** Unable to create job directory %s"%jobDir
            sys.exit(2)

        # Copy production script to job dir
        jobScript = "%s/job.py"%jobDir
        try:
            shutil.copyfile(PROD_SCRIPT,jobScript)
        except:
            print "*** ERROR *** Unable to copy job script file %s to %s"%(PROD_SCRIPT,jobScript)
            sys.exit(2)

        # Create list with files to process
        jobListFile = "%s/job.list"%jobDir
        with open(jobListFile,"w") as jlf:
            for f in job_file_lists[j]: jlf.write("%s\n"%f)

        # Copy long-lived proxy file to job dir
        jobProxy = "%s/job.proxy"%jobDir
        try:
            shutil.copyfile(JOB_PROXY_FILE,jobProxy)
        except:
            print "*** ERROR *** Unable to copy job proxy file %s to %s"%(JOB_PROXY_FILE,jobProxy)
            sys.exit(2)
        try:
            os.chmod(jobProxy,0o600)
        except:
            print "*** ERROR *** Unable to set access permissions of job proxy file %s"%jobProxy
            sys.exit(2)

        # Create JDL file in job dir
        jobJDL = "%s/job.jdl"%jobDir
        with open(jobJDL,"w") as jf:
            jf.write("[\n")
            jf.write("Type = \"Job\";\n")
            jf.write("JobType = \"Normal\";\n")
            jf.write("Executable = \"/usr/bin/python\";\n")
            jf.write("Arguments = \"-u job.py job.list job.proxy %s %s %s %s %s\";\n"%(PROD_NAME,jobName,PROD_RECO_VERSION,PROD_STORAGE_DIR,PROD_SRM))
            jf.write("StdOutput = \"job.out\";\n")
            jf.write("StdError = \"job.err\";\n")
            jf.write("InputSandbox = {\"job.py\",\"job.list\",\"job.proxy\"};\n")
            jf.write("OutputSandbox = {\"job.out\", \"job.err\", \"job.sh\"};\n")
            jf.write("OutputSandboxBaseDestURI=\"gsiftp://localhost\";\n")
            jf.write("]\n")

        # Create job entry in DB and register job (jobCfg and jobSeeds are only used in MC jobs)
        jobCfg = ""
        with open(jobListFile,"r") as jlf: jobList = jlf.read()
        jobSeeds = ""
        DB.create_job(prodId,jobName,jobLocalDir,jobCfg,jobList,jobSeeds)

    # From now on we do not need the DB anymore: close connection
    DB.close_db()

    # Prepare daemon context

    # Assume that the current directory is the top level Production directory
    top_prod_dir = os.getcwd()
    print "Production top working dir: %s"%top_prod_dir

    # Lock file with daemon pid is located inside the production directory
    prod_lock = "%s/%s/%s.pid"%(top_prod_dir,PROD_DIR,PROD_NAME)
    print "Production lock file: %s"%prod_lock

    # Redirect stdout and stderr to log/err files inside the production directory
    prod_log_file = "%s/%s/%s.log"%(top_prod_dir,PROD_DIR,PROD_NAME)
    prod_err_file = "%s/%s/%s.err"%(top_prod_dir,PROD_DIR,PROD_NAME)
    print "Production log file: %s"%prod_log_file
    print "Production err file: %s"%prod_err_file

    # Start Padme Production Server as a daemon
    context = daemon.DaemonContext()
    context.working_directory = top_prod_dir
    context.umask = 0o002
    context.pidfile = daemon.pidfile.PIDLockFile(prod_lock)
    context.open()
    PadmeProdServer(PROD_NAME,PROD_DEBUG)
    context.close()

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
