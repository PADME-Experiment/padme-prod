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
import pexpect
import getpass

from PadmeProdServer import PadmeProdServer
from PadmeMCDB import PadmeMCDB
from ProxyHandler import ProxyHandler

# Get location of padme-prod software from PADME_PROD env variable
# Default to ./padme-prod if not set
PADME_PROD = os.getenv('PADME_PROD',"./padme-prod")

# Create global handler to PadmeMCDB
DB = PadmeMCDB()

# ### Define PADME grid resources ###

# SRMs to access PADME area on the LNF and CNAF storage systems
PADME_SRM_URI = {
    "LNF": "root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org",
    "LNF2": "root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org_scratch",
    "CNAF": "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape",
    "CNAF2": "srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padme"
}

# List of available submission sites and corresponding default Condor CE nodes
PADME_CE_NODE = {
    "LNF":  ("atlasce3.lnf.infn.it:9619",),
    "CNAF":  ("ce01-htc.cr.cnaf.infn.it:9619","ce02-htc.cr.cnaf.infn.it:9619","ce03-htc.cr.cnaf.infn.it:9619","ce04-htc.cr.cnaf.infn.it:9619")
}

# Initialize global parameters and set some default values
PROD_NAME = ""
PROD_NJOBS = 0
PROD_NJOBS_MAX = 1000
PROD_MACRO_FILE = ""
PROD_STORAGE_DIR = ""
PROD_DIR = ""
PROD_SCRIPT = "%s/PadmeProdCondor/script/padmemc_prod.py"%PADME_PROD
PROD_CE_NODE = ""
PROD_CE_PORT = 9619
PROD_RUN_SITE = "CNAF"
PROD_STORAGE_SITE = "CNAF2"
PROD_MC_VERSION = ""
PROD_MYPROXY_SERVER = "myproxy.cnaf.infn.it"
PROD_MYPROXY_PORT = 7512
PROD_MYPROXY_LIFETIME = 720
PROD_MYPROXY_NAME = ""
PROD_MYPROXY_PASSWD = "myproxy"
PROD_PROXY_VOMS = "vo.padme.org"
PROD_PROXY_LIFETIME = 24
PROD_DEBUG = 0
PROD_DESCRIPTION_FILE = ""
PROD_USER_REQ = "Unknown"
PROD_NEVENTS_REQ = 0
PROD_RANDOM_LIST = ""

def print_help():

    print "PadmeMCProd -n <prod_name> -j <number_of_jobs> -v <version> [-m <macro_file>] [-s <submission_site>] [-C <CE_node> [-P <CE_port]] [-d <storage_site>] [-D <desc_file>] [-U <user>] [-N <events>] [-R <seed_list>] [-V] [-h]"
    print "  -n <prod_name>\tName for the production"
    print "  -j <number_of_jobs>\tNumber of production jobs to submit. Must be >0 and <=1000"
    print "  -v <version>\t\tVersion of PadmeMC to use for production. Must be installed on CVMFS."
    print "  -m <macro_file>\tMacro file with G4 cards to use. Default: macro/<prod_name>.mac"
    print "  -s <submission_site>\tSite to be used for job submission. Allowed: %s. Default: %s"%(",".join(PADME_CE_NODE.keys()),PROD_RUN_SITE)
    print "  -C <CE_node>\t\tCE node to be used for job submission. If defined, <submission_site> will not be used"
    print "  -P <CE_port>\t\tCE port. Default: %d"%PROD_CE_PORT
    print "  -d <storage_site>\tSite where the jobs output will be stored. Allowed: %s. Default: %s"%(",".join(PADME_SRM_URI.keys()),PROD_STORAGE_SITE)
    print "  -D <desc_file>\tFile containing a text describing the production (to be stored in the DB). Default: description/<prod_name>.txt"
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
    global PROD_RUN_SITE
    global PROD_STORAGE_SITE
    global PROD_MC_VERSION
    global PROD_MYPROXY_NAME
    global PROD_MYPROXY_PASSWD
    global PROD_DEBUG
    global PROD_DESCRIPTION_FILE
    global PROD_USER_REQ
    global PROD_NEVENTS_REQ
    global PROD_RANDOM_LIST

    try:
        opts,args = getopt.getopt(argv,"hVn:j:v:m:s:C:P:d:D:U:N:R:",[])
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
            try:
                PROD_CE_PORT = int(arg)
            except ValueError:
                print('*** ERROR *** CE Port (-P) must be an integer')
                sys.exit(2)
        elif opt == '-D':
            PROD_DESCRIPTION_FILE = arg
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

    # Choose submission CEs
    if PROD_CE_NODE:
        # If CE was explicitly defined, create a list with it
        PROD_CE = [ "%s:%d"%(PROD_CE_NODE,PROD_CE_PORT) ]
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
        PROD_STORAGE_DIR = "/mc/%s/%s/sim"%(PROD_MC_VERSION,PROD_NAME)

    # If production directory was not specified, use default
    if PROD_DIR == "":
        version_dir = "prod/%s"%PROD_MC_VERSION
        if not os.path.exists(version_dir):
            os.mkdir(version_dir)
        elif not os.path.isdir(version_dir):
            print "*** ERROR *** '%s' exists but is not a directory"%version_dir
            sys.exit(2)
        PROD_DIR = "%s/%s"%(version_dir,PROD_NAME)

    # If long-term myproxy name was not specified, use production name
    if PROD_MYPROXY_NAME == "":
        PROD_MYPROXY_NAME = PROD_NAME

    # If description file was not given, use default
    if not PROD_DESCRIPTION_FILE: PROD_DESCRIPTION_FILE = "description/%s.txt"%PROD_NAME

    # Read file with a description of the production
    if not os.path.isfile(PROD_DESCRIPTION_FILE):
        print "*** ERROR *** Description file '%s' does not exist"%PROD_DESCRIPTION_FILE
        sys.exit(2)
    with open(PROD_DESCRIPTION_FILE,"r") as df: PROD_DESCRIPTION = df.read()

    # Show info about required production
    print "- Starting production %s"%PROD_NAME
    print "- PadmeMC version %s"%PROD_MC_VERSION
    print "- Submitting %d jobs"%PROD_NJOBS
    print "- Submitting jobs to CE %s"%" ".join(PROD_CE)
    print "- Main production directory: %s"%PROD_DIR
    print "- Production script: %s"%PROD_SCRIPT
    print "- PadmeMC macro file: %s"%PROD_MACRO_FILE
    print "- Storage SRM: %s"%PROD_SRM
    print "- Storage directory: %s"%PROD_STORAGE_DIR
    print "- MyProxy name: %s"%PROD_MYPROXY_NAME
    if PROD_RANDOM_LIST:
        print "- Random seeds list: %s"%PROD_RANDOM_LIST
    else:
        print "- Random seeds automatically generated"
    if PROD_DEBUG:
        print "- Debug level: %d"%PROD_DEBUG

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
        # Therefore each time we run the script we should get a different set of random seeds.
        for j in range(0,PROD_NJOBS):
            random_seeds.append("%d,%d"%(random.randint(0,4294967295),random.randint(0,4294967295)))

    # Create long-lived proxy on MyProxy server (also create a local proxy to talk to storage SRM)
    grid_passwd = getpass.getpass(prompt="Enter GRID pass phrase for this identity:")
    proxy_cmd = "myproxy-init --proxy_lifetime %d --cred_lifetime %d --voms %s --pshost %s --dn_as_username --credname %s --local_proxy"%(PROD_PROXY_LIFETIME,PROD_MYPROXY_LIFETIME,PROD_PROXY_VOMS,PROD_MYPROXY_SERVER,PROD_MYPROXY_NAME)
    if PROD_DEBUG: print ">",proxy_cmd
    child = pexpect.spawn(proxy_cmd)
    try:
        child.expect("Enter GRID pass phrase for this identity:")
        if PROD_DEBUG: print child.before
        child.sendline(grid_passwd)
        child.expect("Enter MyProxy pass phrase:")
        if PROD_DEBUG: print child.before
        child.sendline(PROD_MYPROXY_PASSWD)
        child.expect("Verifying - Enter MyProxy pass phrase:")
        if PROD_DEBUG: print child.before
        child.sendline(PROD_MYPROXY_PASSWD)
    except:
        print "*** ERROR *** Unable to register long-lived proxy on %s"%PROD_MYPROXY_SERVER
        print str(child)
        sys.exit(2)

    # Get position of local proxy to be sent to Condor
    voms_proxy_local = ""
    voms_cmd = "voms-proxy-info"
    if PROD_DEBUG: print "> %s"%voms_cmd
    p = subprocess.Popen(shlex.split(voms_cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()
    if p.returncode == 0:
        for l in iter(out.splitlines()):
            if PROD_DEBUG > 1: print l
            r = re.match("^\s*path\s+:\s+(\S+)\s*$",l)
            if r:
                voms_proxy_local = r.group(1)
                break
    if voms_proxy_local == "":
        print "*** ERROR *** Unable get path to local VOMS proxy"
        sys.exit(2)
    print "- Local VOMS proxy at %s"%voms_proxy_local

    # Create production directory in the storage SRM (try 3 times before giving up)
    print "- Creating production dir %s on %s"%(PROD_STORAGE_DIR,PROD_SRM)
    gfal_mkdir_cmd = "gfal-mkdir -p %s%s"%(PROD_SRM,PROD_STORAGE_DIR)
    if PROD_DEBUG: print ">",gfal_mkdir_cmd
    n_try = 0
    while True:
        if subprocess.call(shlex.split(gfal_mkdir_cmd)) == 0: break
        n_try += 1
        if n_try >= 3:
            print "*** ERROR *** unable to create production dir %s on %s"%(PROD_STORAGE_DIR,PROD_SRM)
            sys.exit(2)
        print "WARNING gfal-mkdir failed. Retry in 5 seconds."
        time.sleep(5)

    # Create production directory to host support dirs for all jobs
    print "- Creating production dir %s"%PROD_DIR
    try:
        os.mkdir(PROD_DIR)
    except:
        print "*** ERROR *** unable to create local production dir %s"%PROD_DIR
        sys.exit(2)

    # Create new production in DB
    print "- Creating new production in DB"
    proxy_info = "%s:%d %s %s"%(PROD_MYPROXY_SERVER,PROD_MYPROXY_PORT,PROD_MYPROXY_NAME,PROD_MYPROXY_PASSWD)
    prodId = DB.create_mcprod(PROD_NAME,PROD_DESCRIPTION,PROD_USER_REQ,PROD_NEVENTS_REQ,PROD_CE,PROD_MC_VERSION,PROD_DIR,PROD_SRM,PROD_STORAGE_DIR,proxy_info,PROD_NJOBS)

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

        # Get random seed pair from list
        jobSeeds = random_seeds.pop(0)

        # Create SUB file in job dir
        jobSUB = "%s/job.sub"%jobDir
        with open(jobSUB,"w") as jf:
            jf.write("universe = vanilla\n")
            jf.write("+Owner = undefined\n")
            jf.write("executable = /usr/bin/python\n")
            jf.write("transfer_executable = False\n")
            jf.write("arguments = -u job.py job.mac %s %s %s %s %s %s\n"%(PROD_NAME,jobName,PROD_MC_VERSION,PROD_STORAGE_DIR,PROD_SRM,jobSeeds))
            jf.write("output = job.out\n")
            jf.write("error = job.err\n")
            jf.write("log = job.log\n")
            jf.write("should_transfer_files = yes\n")
            jf.write("transfer_input_files = job.py,job.mac,%s\n"%voms_proxy_local)
            jf.write("transfer_output_files = job.sh\n")
            jf.write("when_to_transfer_output = on_exit\n")
            jf.write("x509userproxy = %s\n"%voms_proxy_local)
            jf.write("MyProxyHost = %s:%d\n"%(PROD_MYPROXY_SERVER,PROD_MYPROXY_PORT))
            jf.write("MyProxyCredentialName = %s\n"%PROD_MYPROXY_NAME)
            jf.write("MyProxyPassword = %s\n"%PROD_MYPROXY_PASSWD)
            jf.write("MyProxyRefreshThreshold = 600\n")
            jf.write("MyProxyNewProxyLifetime = 1440\n")
            jf.write("queue\n")

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
