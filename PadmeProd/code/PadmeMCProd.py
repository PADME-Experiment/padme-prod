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
import random

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

# List of available submission sites and corresponding default CE nodes
PADME_CE_NODE = {
    "LNF":   "atlasce1.lnf.infn.it:8443/cream-pbs-padme_c7",
    "CNAF":  "ce04-lcg.cr.cnaf.infn.it:8443/cream-lsf-padme",
    "SOFIA": "cream.grid.uni-sofia.bg:8443/cream-pbs-cms"
}

# Initialize global parameters and set some default values
PROD_NAME = ""
PROD_NJOBS = 0
PROD_NJOBS_MAX = 1000
PROD_MACRO_FILE = ""
PROD_STORAGE_DIR = ""
PROD_DIR = ""
PROD_SCRIPT = "%s/PadmeProd/script/padmemc_prod.py"%PADME_PROD
PROD_CE_NODE = ""
PROD_CE_PORT = "8443"
PROD_CE_QUEUE = ""
PROD_RUN_SITE = "LNF"
PROD_STORAGE_SITE = "CNAF"
PROD_MC_VERSION = ""
PROD_PROXY_FILE = ""
PROD_DEBUG = 0
PROD_DESCRIPTION = "TEST"
PROD_USER_REQ = "Unknown"
PROD_NEVENTS_REQ = 0
PROD_RANDOM_LIST = ""

def print_help():

    print "PadmeMCProd -n <prod_name> -j <number_of_jobs> -v <version> [-m <macro_file>] [-s <submission_site>] [-C <CE_node> [-P <CE_port>] -Q <CE_queue>] [-d <storage_site>] [-p <proxy>] [-D <description>] [-U <user>] [-N <events>] [-R <seed_list>] [-V] [-h]"
    print "  -n <prod_name>\tName for the production"
    print "  -j <number_of_jobs>\tNumber of production jobs to submit. Must be >0 and <=1000"
    print "  -v <version>\t\tVersion of PadmeMC to use for production. Must be installed on CVMFS."
    print "  -m <macro_file>\tMacro file with G4 cards to use. Default: macro/<prod_name>.mac"
    print "  -s <submission_site>\tSite to be used for job submission. Allowed: %s. Default: %s"%(",".join(PADME_CE_NODE.keys()),PROD_RUN_SITE)
    print "  -C <CE_node>\t\tCE node to be used for job submission. If defined, <submission_site> will not be used"
    print "  -P <CE_port>\t\tCE port. Default: %s"%PROD_CE_PORT
    print "  -Q <CE_queue>\t\tCE queue to use for submission. This parameter is mandatory if -C is specified"
    print "  -d <storage_site>\tSite where the jobs output will be stored. Allowed: %s. Default: %s"%(",".join(PADME_SRM_URI.keys()),PROD_STORAGE_SITE)
    print "  -p <proxy>\t\tLong lived proxy file to use for this production. If not defined it will be created."
    print "  -D <description>\tProduction description (to be stored in the DB). '%s' if not given."%PROD_DESCRIPTION
    print "  -U <user>\t\tName of user who requested the production (to be stored in the DB). '%s' if not given."%PROD_USER_REQ
    print "  -N <events>\t\tTotal number of events requested by user (to be stored in the DB). %d if not given."%PROD_NEVENTS_REQ
    print "  -R <seed_list>\tFile with list of random seed pairs to use for jobs. Default: generate automatically."
    print "  -V\t\t\tEnable debug mode. Can be repeated to increase verbosity"

def main(argv):

    # Declare that here we can possibly modify these global variables
    global PROD_NAME
    global PROD_NJOBS
    global PROD_MACRO_FILE
    global PROD_STORAGE_DIR
    global PROD_DIR
    global PROD_SCRIPT
    global PROD_CE_NODE
    global PROD_CE_PORT
    global PROD_CE_QUEUE
    global PROD_RUN_SITE
    global PROD_STORAGE_SITE
    global PROD_MC_VERSION
    global PROD_PROXY_FILE
    global PROD_DEBUG
    global PROD_DESCRIPTION
    global PROD_USER_REQ
    global PROD_NEVENTS_REQ
    global PROD_RANDOM_LIST

    try:
        opts,args = getopt.getopt(argv,"hVn:j:v:m:s:C:P:Q:d:p:D:U:N:R:",[])
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
        elif opt == '-n':
            PROD_NAME = arg
        elif opt == '-m':
            PROD_MACRO_FILE = arg
        elif opt == '-v':
            PROD_MC_VERSION = arg
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
        elif opt == '-U':
            PROD_USER_REQ = arg
        elif opt == '-R':
            PROD_RANDOM_LIST = arg
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
                PROD_NJOBS = int(arg)
            except ValueError:
                print "*** ERROR *** Invalid number of jobs: '%s'"%arg
                print_help()
                sys.exit(2)
        elif opt == '-N':
            try:
                PROD_NEVENTS_REQ = int(arg)
            except ValueError:
                print "*** ERROR *** Invalid total number of events requested: '%s'"%arg
                print_help()
                sys.exit(2)

    if not PROD_NAME:
        print "*** ERROR *** No production name specified."
        print_help()
        sys.exit(2)

    if not PROD_MC_VERSION:
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

    if PROD_NJOBS == 0:
        print "*** ERROR *** Number of jobs was not specified."
        print_help()
        sys.exit(2)

    if PROD_NJOBS < 0 or PROD_NJOBS > PROD_NJOBS_MAX:
        print "*** ERROR *** Invalid number of jobs requested: %d - Max allowed: %d."%(PROD_NJOBS,PROD_NJOBS_MAX)
        print_help()
        sys.exit(2)

    # If configuration file was not specified, use default
    if not PROD_MACRO_FILE: PROD_MACRO_FILE = "macro/%s.mac"%PROD_NAME

    # Check if configuration file exists
    if not os.path.isfile(PROD_MACRO_FILE):
        print "*** ERROR *** Macro file '%s' does not exist"%PROD_MACRO_FILE
        sys.exit(2)

    # If storage directory was not specified, use default
    if PROD_STORAGE_DIR == "":
        PROD_STORAGE_DIR = "/mc/%s/%s"%(PROD_MC_VERSION,PROD_NAME)

    # If production directory was not specified, use default
    if PROD_DIR == "":
        version_dir = "prod/%s"%PROD_MC_VERSION
        if not os.path.exists(version_dir):
            os.mkdir(version_dir)
        elif not os.path.isdir(version_dir):
            print "*** ERROR *** '%s' exists but is not a directory"%version_dir
            sys.exit(2)
        PROD_DIR = "%s/%s"%(version_dir,PROD_NAME)

    # Show info about required production
    print "- Starting production %s"%PROD_NAME
    print "- PadmeMC version %s"%PROD_MC_VERSION
    print "- Submitting %d jobs"%PROD_NJOBS
    print "- Submitting jobs to CE %s"%PROD_CE
    print "- Main production directory: %s"%PROD_DIR
    print "- Production script: %s"%PROD_SCRIPT
    print "- PadmeMC macro file: %s"%PROD_MACRO_FILE
    print "- Storage SRM: %s"%PROD_SRM
    print "- Storage directory: %s"%PROD_STORAGE_DIR
    if PROD_RANDOM_LIST:
        print "- Random seeds list: %s"%PROD_RANDOM_LIST
    else:
        print "- Random seeds automatically generated"
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

    # Create list of random seeds reading them from list, if available, or automatically
    random_seeds = []
    if PROD_RANDOM_LIST:
        if os.path.exists(PROD_RANDOM_LIST):
            with open(PROD_RANDOM_LIST,"r") as rl:
                for line in rl:
                    # Skip empty and comment lines
                    if re.match("^\s*$",run) or re.match("^\s*#.*$",line): continue
                    # Check if the format is <seed1>,<seed2>
                    if re.match("^\s*\d+,\d+\s*$",line):
                        random_seeds.append(line.strip())
                    else:
                        print "*** ERROR *** Ill formatted line found in random seeds list %s"%PROD_RANDOM_LIST
                        print line
                        sys.exit(2)
            # Verify we have enough random seeds
            if len(random_seeds) < PROD_NJOBS:
                print "*** ERROR *** Random seeds list %s contains %d seed pairs but % are required"%(PROD_RANDOM_LIST,len(random_seeds),PROD_NJOBS)
                sys.exit(2)

        else:
            print "*** ERROR *** Specified random seeds list %s was not found"%PROD_RANDOM_LIST
            sys.exit(2)
    else:
        # Generate random seed pairs for all jobs using the Python "random" package
        # As we do not initialize the seed in the "random" package,
        # "the default is to use the current system time in milliseconds from epoch (1970)"
        # Therefore each time we run the script we should get a different set of random seeds
        for j in range(0,PROD_NJOBS):
            random_seeds.append("%d,%d"%(random.randint(0,4294967295),random.randint(0,4294967295)))

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
    # This is needed to create the storage dir on the SRM server
    PH.renew_voms_proxy(JOB_PROXY_FILE)

    # Create new production in DB
    print "- Creating new production in DB"
    prodId = DB.create_mcprod(PROD_NAME,PROD_DESCRIPTION,PROD_USER_REQ,PROD_NEVENTS_REQ,PROD_CE,PROD_MC_VERSION,PROD_DIR,PROD_SRM,PROD_STORAGE_DIR,JOB_PROXY_FILE,PROD_NJOBS)

    # Create production directory in the storage SRM
    print "- Creating dir %s in %s"%(PROD_STORAGE_DIR,PROD_SRM)
    gfal_mkdir_cmd = "gfal-mkdir -p %s%s"%(PROD_SRM,PROD_STORAGE_DIR)
    if PROD_DEBUG: print ">",gfal_mkdir_cmd
    rc = subprocess.call(shlex.split(gfal_mkdir_cmd))

    # Create job structures
    print "- Creating directory structure for production jobs"
    for j in range(0,PROD_NJOBS):

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

        # Copy common configuration file to job dir
        jobCfgFile = "%s/job.mac"%jobDir
        try:
            shutil.copyfile(PROD_MACRO_FILE,jobCfgFile)
        except:
            print "*** ERROR *** Unable to copy job macro file %s to %s"%(PROD_MACRO_FILE,jobCfgFile)
            sys.exit(2)

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

        # Get random seed pair from list
        jobSeeds = random_seeds.pop(0)

        # Create JDL file in job dir
        jobJDL = "%s/job.jdl"%jobDir
        with open(jobJDL,"w") as jf:
            jf.write("[\n")
            jf.write("Type = \"Job\";\n")
            jf.write("JobType = \"Normal\";\n")
            jf.write("Executable = \"/usr/bin/python\";\n")
            jf.write("Arguments = \"-u job.py job.mac job.proxy %s %s %s %s %s %s\";\n"%(PROD_NAME,jobName,PROD_MC_VERSION,PROD_STORAGE_DIR,PROD_SRM,jobSeeds))
            jf.write("StdOutput = \"job.out\";\n")
            jf.write("StdError = \"job.err\";\n")
            jf.write("InputSandbox = {\"job.py\",\"job.mac\",\"job.proxy\"};\n")
            jf.write("OutputSandbox = {\"job.out\", \"job.err\", \"job.sh\"};\n")
            jf.write("OutputSandboxBaseDestURI=\"gsiftp://localhost\";\n")
            jf.write("]\n")

        # Create job entry in DB and register job (jobList is only used in Reco jobs)
        with open(jobCfgFile,"r") as jcf: jobCfg=jcf.read()
        jobList = ""
        DB.create_job(prodId,jobName,jobLocalDir,jobCfg,jobList,jobSeeds)

    # From now on we do not need the DB anymore: close connection
    DB.close_db()

    # Prepare daemon context

    # Assume that the current directory is the top level MC Production directory
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
