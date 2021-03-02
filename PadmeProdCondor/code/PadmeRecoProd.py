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

# Create proxy handler
PH = ProxyHandler()

# ### Define PADME grid resources ###

# SRMs to access PADME area on the LNF and CNAF storage systems
PADME_SRM_URI = {
    #"LNF":  "srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org",
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
PROD_RUN_NAME = ""
PROD_NAME = ""
PROD_FILES_PER_JOB = 10
PROD_FILES_PER_JOB_MAX = 1000
PROD_STORAGE_DIR = ""
PROD_DIR = ""
PROD_SCRIPT = "%s/PadmeProdCondor/script/padmereco_prod.py"%PADME_PROD
PROD_SOURCE_URI = PADME_SRM_URI["LNF"]
PROD_CE_NODE = ""
PROD_CE_PORT = 9619
PROD_RUN_SITE = "LNF"
PROD_STORAGE_SITE = "LNF"
PROD_RECO_VERSION = ""
PROD_YEAR = ""
PROD_MYPROXY_SERVER = "myproxy.cnaf.infn.it"
PROD_MYPROXY_PORT = 7512
PROD_MYPROXY_LIFETIME = 720
PROD_MYPROXY_NAME = ""
PROD_MYPROXY_PASSWD = "myproxy"
PROD_PROXY_VOMS = "vo.padme.org"
PROD_PROXY_LIFETIME = 24
PROD_DEBUG = 0
PROD_DESCRIPTION = "TEST"

def print_help():

    print "PadmeRecoProd -r <run_name> -v <version> [-y <year>] [-j <files_per_job>] [-n <prod_name>] [-s <submission_site>] [-S <source_uri>] [-C <CE_node> [-P <CE_port>]] [-d <storage_site>] [-D <description>] [-V] [-h]"
    print "  -r <run_name>\t\tname of the run to process"
    print "  -v <version>\t\tversion of PadmeReco to use for production. Must be installed on CVMFS."
    print "  -y <year>\t\tyear of run. N.B. used only if run name is not self-documenting"
    print "  -j <files_per_job>\tnumber of rawdata files to be reconstructed by each job. Default: %d"%PROD_FILES_PER_JOB
    print "  -n <prod_name>\tname for the production. Default: <run_name>_<version>"
    print "  -s <submission_site>\tsite to be used for job submission. Allowed: %s. Default: %s"%(",".join(PADME_CE_NODE.keys()),PROD_RUN_SITE)
    print "  -S <source_uri>\tURI where rawdata files are stored. Default: %s"%PROD_SOURCE_URI
    print "  -C <CE_node>\t\tCE node to be used for job submission. If defined, <submission_site> will not be used"
    print "  -P <CE_port>\t\tCE port. Default: %s"%PROD_CE_PORT
    print "  -d <storage_site>\tsite where the jobs output will be stored. Allowed: %s. Default: %s"%(",".join(PADME_SRM_URI.keys()),PROD_STORAGE_SITE)
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
    global PROD_RUN_SITE
    global PROD_STORAGE_SITE
    global PROD_RECO_VERSION
    global PROD_MYPROXY_NAME
    global PROD_MYPROXY_PASSWD
    global PROD_YEAR
    global PROD_DEBUG
    global PROD_DESCRIPTION

    try:
        opts,args = getopt.getopt(argv,"hVr:y:n:j:s:d:S:C:P:v:D:",[])
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
        # If CE was explicitly defined, create a list with it
        PROD_CE = [ "%s:%d"%(PROD_CE_NODE,PROD_CE_PORT) ]
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
    print "- Submitting jobs to CE %s"%" ".join(PROD_CE)
    print "- Main production directory: %s"%PROD_DIR
    print "- Production script: %s"%PROD_SCRIPT
    print "- Storage SRM: %s"%PROD_SRM
    print "- Storage directory: %s"%PROD_STORAGE_DIR
    print "- MyProxy name: %s"%PROD_MYPROXY_NAME
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

    # Get list of files for run to reconstruct
    # All files are assumed to be on the LNF SE and available via the ROOTX protocol
    file_list = get_run_file_list(PROD_RUN_NAME)
    job_file_lists = []
    job_file_list = []
    for f in file_list:
        if len(job_file_list) == PROD_FILES_PER_JOB:
            job_file_lists.append(job_file_list)
            job_file_list = []
        #file_url = "%s/daq/%s/rawdata/%s/%s"%(PADME_ROOT_URI["LNF"],PROD_YEAR,PROD_RUN_NAME,f)
        file_url = "%s/daq/%s/rawdata/%s/%s"%(PROD_SOURCE_URI,PROD_YEAR,PROD_RUN_NAME,f)
        job_file_list.append(file_url)
    if job_file_list: job_file_lists.append(job_file_list)

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
    prodId = DB.create_recoprod(PROD_NAME,PROD_RUN_NAME,PROD_DESCRIPTION,PROD_CE,PROD_RECO_VERSION,PROD_DIR,PROD_SRM,PROD_STORAGE_DIR,proxy_info,len(job_file_lists))

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

        # Create SUB file in job dir
        jobSUB = "%s/job.sub"%jobDir
        with open(jobSUB,"w") as jf:
            jf.write("universe = vanilla\n")
            jf.write("+Owner = undefined\n")
            jf.write("executable = /usr/bin/python\n")
            jf.write("transfer_executable = False\n")
            jf.write("arguments = -u job.py job.list %s %s %s %s %s %s\n"%(PROD_NAME,jobName,PROD_RECO_VERSION,PROD_STORAGE_DIR,PROD_SRM))
            jf.write("output = job.out\n")
            jf.write("error = job.err\n")
            jf.write("log = job.log\n")
            jf.write("should_transfer_files = yes\n")
            jf.write("transfer_input_files = job.py,job.list,%s\n"%voms_proxy_local)
            jf.write("transfer_output_files = job.sh\n")
            jf.write("when_to_transfer_output = on_exit\n")
            jf.write("x509userproxy = %s\n"%voms_proxy_local)
            jf.write("MyProxyHost = %s:%d\n"%(PROD_MYPROXY_SERVER,PROD_MYPROXY_PORT))
            jf.write("MyProxyCredentialName = %s\n"%PROD_MYPROXY_NAME)
            jf.write("MyProxyPassword = %s\n"%PROD_MYPROXY_PASSWD)
            jf.write("MyProxyRefreshThreshold = 600\n")
            jf.write("MyProxyNewProxyLifetime = 1440\n")
            jf.write("queue\n")

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
