#!/usr/bin/python

import os
import sys
import getopt
import re
import time
import shlex
import subprocess

from ProxyHandler import ProxyHandler

# Get some info about running script
thisscript = sys.argv[0]
SCRIPT_PATH,SCRIPT_NAME = os.path.split(thisscript)
# Solve all symbolic links to reach installation directory
while os.path.islink(thisscript): thisscript = os.readlink(thisscript)
SCRIPT_DIR,SCRIPT_FILE = os.path.split(os.path.abspath(thisscript))

# Look for PadmePrecoProd script
PADMERECOPROD = "%s/PadmeRecoProd.py"%SCRIPT_DIR
if not ( os.path.exists(PADMERECOPROD) and os.path.isfile(PADMERECOPROD) and os.access(PADMERECOPROD,os.X_OK) ):
    print "*** ERROR *** Script %s not found or not executable"%PADMERECOPROD
    sys.exit(2)

# Create proxy handler
PH = ProxyHandler()

# Define site descriptions

# List of available submission sites and corresponding CE nodes
PADME_CE_NODE_LIST = {
    "LNF":   [ "atlasce1.lnf.infn.it", "atlasce2.lnf.infn.it", "atlasce4.lnf.infn.it" ],
    "CNAF":  [ "ce04-lcg.cr.cnaf.infn.it" ],
    "SOFIA": [ "cream.grid.uni-sofia.bg" ]
}

# Name of default queue to use for each site. Can be changed using -Q argument
PADME_CE_QUEUE = {
    "LNF":   "cream-pbs-padme_c7",
    "CNAF":  "cream-lsf-padme",
    "SOFIA": "cream-pbs-cms"
}

PADME_STORAGE_SITES = [ "LNF","CNAF" ]

# Default CE port to use. Can be changed using the "-P <port>" argument.
# If the CE definition in PADME_CE_NODE_LIST includes ":<port>" then it will be used instead.
PROD_CE_PORT_DEFAULT = "8443"

# Define global defaults
PROD_DEBUG = 0
PROD_FILES_PER_JOB = 100
PROD_RECO_VERSION = ""
PROD_RUN_SITE = "LNF"
PROD_STORAGE_SITE = "LNF"
PROD_SUBMIT_DELAY = 60
PROD_PROXY_FILE = "prod/long_proxy"
PROD_SOURCE_URI = ""

# Initialize list of runs to process
PROD_RUN_LIST = []

def print_help():

    print "%s [-L <run_list_file>] [-r <run>] -v <version> [-j <files_per_job>] [-s <submission_site>] [-Q <CE_queue>] [-P <CE_port>] [-S <source_uri>] [-d <storage_site>] [-D <submit_delay>] [-V] [-h]"%SCRIPT_NAME
    print "  -L <run_list_file>\tfile with list of runs to process"
    print "  -r <run_name>\t\tname of run to process"
    print "  -v <version>\t\tversion of PadmeReco to use for production. Must be installed on CVMFS."
    print "  -j <files_per_job>\tnumber of rawdata files to be reconstructed by each job. Default: %d"%PROD_FILES_PER_JOB
    print "  -s <submission_site>\tsite to be used for job submission. Allowed: %s. Default: %s"%(",".join(PADME_CE_NODE_LIST.keys()),PROD_RUN_SITE)
    print "  -P <CE_port>\t\tCE port. Default: %s"%PROD_CE_PORT_DEFAULT
    print "  -Q <CE_queue>\t\tCE queue to use for submission. Default from submission site"
    print "  -S <source_uri>\tURI to use to get list of files for production run"
    print "  -d <storage_site>\tsite where the jobs output will be stored. Allowed: %s. Default: %s"%(",".join(PADME_STORAGE_SITES),PROD_STORAGE_SITE)
    print "  -D <submit_delay>\tDelay in sec between run submissions. Default: %d sec"%PROD_SUBMIT_DELAY
    print "  -V\t\t\tenable debug mode. Can be repeated to increase verbosity"
    print "  N.B. Multiple -L and -r options can be combined to create a single list of runs. Duplicated runs will be automatically removed."

def add_run(run):

    global PROD_RUN_LIST

    # Add run to list
    PROD_RUN_LIST.append(run)

def add_run_list(run_list):

    global PROD_RUN_LIST

    # Check if file with list of runs exists and is readable
    if not ( os.path.exists(run_list) and os.path.isfile(run_list) and os.access(run_list,os.R_OK) ):
        print "*** ERROR *** Run list file %s not found or not readable"%run_list
        sys.exit(2)

    # Read file and add runs to list
    with open(run_list,"r") as rl:
        for run in rl:
            # Skip empty and comment lines
            if re.match("^\s*$",run) or re.match("^\s*#.*$",run): continue
            PROD_RUN_LIST.append(run.strip())

def main(argv):

    # Declare that here we can possibly modify these global variables
    global PROD_CE_PORT_DEFAULT
    global PROD_DEBUG
    global PROD_FILES_PER_JOB
    global PROD_RECO_VERSION
    global PROD_RUN_SITE
    global PROD_STORAGE_SITE
    global PROD_SUBMIT_DELAY
    global PROD_SOURCE_URI

    global PROD_RUN_LIST

    PROD_CE_NODE = ""
    PROD_CE_PORT = ""
    PROD_CE_QUEUE = ""

    try:
        opts,args = getopt.getopt(argv,"hVL:r:j:v:s:P:Q:d:D:S:",[])
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
        elif opt == '-L':
            add_run_list(arg)
        elif opt == '-r':
            add_run(arg)
        elif opt == '-v':
            PROD_RECO_VERSION = arg
        elif opt == '-P':
            PROD_CE_PORT_DEFAULT = arg
        elif opt == '-Q':
            PROD_CE_QUEUE = arg
        elif opt == '-S':
            PROD_SOURCE_URI = arg
        elif opt == '-j':
            try:
                PROD_FILES_PER_JOB = int(arg)
            except ValueError:
                print "*** ERROR *** Invalid parameter in number of files per job: '%s'"%arg
                print_help()
                sys.exit(2)
            if PROD_FILES_PER_JOB < 1 or PROD_FILES_PER_JOB > 10000:
                print "*** ERROR *** Invalid number of files per job: %d"%PROD_FILES_PER_JOB
                print_help()
                sys.exit(2)
        elif opt == '-s':
            if arg in PADME_CE_NODE_LIST.keys():
                PROD_RUN_SITE = arg
            else:
                print "*** ERROR *** Invalid submission site %s. Valid: %s"%(arg,",".join(PADME_CE_NODE_LIST.keys()))
                print_help()
                sys.exit(2)
        elif opt == '-d':
            if arg in PADME_STORAGE_SITES:
                PROD_STORAGE_SITE = arg
            else:
                print "*** ERROR *** Invalid storage site %s. Valid: %s"%PADME_STORAGE_SITES
                print_help()
                sys.exit(2)
        elif opt == '-D':
            try:
                PROD_SUBMIT_DELAY = int(arg)
            except ValueError:
                print "*** ERROR *** Invalid parameter for submit delay: '%s'"%arg
                print_help()
                sys.exit(2)
            if PROD_SUBMIT_DELAY < 0 or PROD_SUBMIT_DELAY > 3600:
                print "*** ERROR *** Invalid delay between submissions: %d"%PROD_SUBMIT_DELAY
                print_help()
                sys.exit(2)

    # Remove duplicates from list of runs to be processed and sort it
    PROD_RUN_LIST = list(set(PROD_RUN_LIST))
    PROD_RUN_LIST.sort()
    n_runs = len(PROD_RUN_LIST)

    # Check if at least one run was specified
    if n_runs == 0:
        print "*** ERROR *** No runs specified."
        print_help()
        sys.exit(2)

    if not PROD_RECO_VERSION:
        print "*** ERROR *** No software version specified."
        print_help()
        sys.exit(2)

    # If queue was not defined, use default queue for submission site
    if not PROD_CE_QUEUE:
        PROD_CE_QUEUE = PADME_CE_QUEUE[PROD_RUN_SITE]

    # Create a long-lived proxy (30 days) to be used for all submissions
    print "- Creating long-lived proxy file %s"%PROD_PROXY_FILE
    proxy_cmd = "voms-proxy-init --valid 720:0 --out %s"%PROD_PROXY_FILE
    if PROD_DEBUG: print "> %s"%proxy_cmd
    if subprocess.call(shlex.split(proxy_cmd)):
        print "*** ERROR *** while generating long-lived proxy file %s"%PROD_PROXY_FILE
        sys.exit(2)
    # Create a new VOMS proxy using long-lived proxy
    #PH.create_voms_proxy(PROD_PROXY_FILE)

    # CEs at submission site will be used in round robin to avoid overload
    PROD_CE_INDEX = 0

    n_run = 0
    print "- Creating production for %d runs"%n_runs
    for run in PROD_RUN_LIST:

        # Wait before submitting next run
        if n_run > 0: time.sleep(PROD_SUBMIT_DELAY)
        n_run += 1

        print
        print "=== %4d/%-4d === Submitting run %s ==="%(n_run,n_runs,run)

        # Choose CE from site list and extract port number (if any)
        PROD_CE = PADME_CE_NODE_LIST[PROD_RUN_SITE][PROD_CE_INDEX]
        r = re.match("^(\S+)\:(\d+)$",PROD_CE)
        if r:
            PROD_CE_NODE = r.group(1)
            PROD_CE_PORT = r.group(2)
        else:
            PROD_CE_NODE = PROD_CE
            PROD_CE_PORT = PROD_CE_PORT_DEFAULT

        PROD_CMD = "%s -r %s -j %d -v %s -C %s -P %s -Q %s -p %s"%(PADMERECOPROD,run,PROD_FILES_PER_JOB,PROD_RECO_VERSION,PROD_CE_NODE,PROD_CE_PORT,PROD_CE_QUEUE,PROD_PROXY_FILE)

        # Add surce URI if specified
        if PROD_SOURCE_URI:
            PROD_CMD += " -S %s"%PROD_SOURCE_URI

        # Add debug option(s) if required
        if PROD_DEBUG:
            for i in range(0,PROD_DEBUG): PROD_CMD += " -V"

        # Call PadmeRecoProd for this run
        if PROD_DEBUG: print PROD_CMD
        if subprocess.call(shlex.split(PROD_CMD)):
            print "*** ERROR *** Production submission command returned an error"
            sys.exit(2)

        # Change CE for next run
        PROD_CE_INDEX += 1
        if PROD_CE_INDEX >= len(PADME_CE_NODE_LIST[PROD_RUN_SITE]): PROD_CE_INDEX = 0

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
