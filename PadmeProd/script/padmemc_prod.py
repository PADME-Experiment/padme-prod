#!/usr/bin/python

import os
import sys
import re
import getopt
import signal
import time
import subprocess
import shlex
import select
import errno

PROXY_FILE = ""
PROXY_RENEW_TIME = 6*3600

def now_str():

    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def get_adler32(outfile):

    adler_cmd = "adler32 %s"%outfile
    p = subprocess.Popen(shlex.split(adler_cmd),stdin=None,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    return p.communicate()[0].strip()

def renew_proxy_handler(signum,frame):

    global PROXY_FILE
    global PROXY_RENEW_TIME

    # Obtain new VOMS proxy from long-lived proxy
    proxy_cmd = "voms-proxy-init --noregen --cert %s --key %s --voms vo.padme.org --valid 24:00"%(PROXY_FILE,PROXY_FILE)
    print ">",proxy_cmd
    rc = subprocess.call(proxy_cmd.split())

    # Reset alarm
    signal.alarm(PROXY_RENEW_TIME)

def main(argv):

    global PROXY_FILE
    global PROXY_RENEW_TIME

    # Top CVMFS directory for PadmeMC
    padmemc_cvmfs_dir = "/cvmfs/padme.infn.it/PadmeMC"

    (macro_file,PROXY_FILE,prod_name,job_name,mc_version,storage_dir,srm_uri,rndm_seeds) = argv

    job_dir = os.getcwd()

    print "=== PadmeMC Production %s Job %s ==="%(prod_name,job_name)
    print "Job starting at %s (UTC)"%now_str()
    print "Job running on node %s as user %s in dir %s"%(os.getenv('HOSTNAME'),os.getenv('USER'),job_dir)

    print "PadmeMC version %s"%mc_version
    print "SRM server URI %s"%srm_uri
    print "Storage directory %s"%storage_dir
    print "MC macro file %s"%macro_file
    print "Proxy file %s"%PROXY_FILE
    print "Random seeds %s"%rndm_seeds

    # Change permission rights for long-lived proxy (must be 600)
    os.chmod(PROXY_FILE,0600)

    # Extract random seeds
    r = re.match("(\d+),(\d+)",rndm_seeds)
    if r:
        padme_seed1 = r.group(1)
        padme_seed2 = r.group(2)
    else:
        print "ERROR Random string has wrong format: %s"%rndm_seeds
        exit(2)

    # Check if software directory for this version is available on CVMFS (try a few times before giving up)
    padmemc_version_dir = "%s/%s"%(padmemc_cvmfs_dir,mc_version)
    n_try = 0
    while not os.path.isdir(padmemc_version_dir):
        n_try += 1
        if n_try >= 5:
            print "ERROR Directory %s not found"%padmemc_version_dir
            exit(2)
        print "WARNING Directory %s not found (%d) - Pause and try again"%(padmemc_version_dir,n_try)
        time.sleep(5)

    # Check if PadmeMC configuration file is available
    padmemc_init_file = "%s/config/padme-configure.sh"%padmemc_version_dir
    if not os.path.exists(padmemc_init_file):
        print "ERROR File %s not found"%padmemc_init_file
        exit(2)

    # Create local link to GDML files needed for geometry definition
    padmemc_gdml_dir = "%s/gdml"%padmemc_version_dir
    os.symlink(padmemc_gdml_dir,"gdml")

    # Enable timer to renew VOMS proxy every 6h
    signal.signal(signal.SIGALRM,renew_proxy_handler)
    signal.alarm(PROXY_RENEW_TIME)

    # Prepare shell script to run PadmeMC
    script = """#!/bin/bash
echo "--- Starting PADMEMC production ---"
date
. %s
if [ -z "$PADME" ]; then
    echo "Variable PADME is not set: aborting"
    exit 1
else 
    echo "PADME = $PADME"
fi
if [ -z "$PADMEMC_EXE" ]; then
    echo "Variable PADMEMC_EXE is not set: aborting"
    exit 1
else
    echo "PADMEMC_EXE = $PADMEMC_EXE"
fi
echo "LD_LIBRARY_PATH = $LD_LIBRARY_PATH"
export PADME_SEED1=%s
export PADME_SEED2=%s
echo "Random seeds: $PADME_SEED1 $PADME_SEED2"
$PADMEMC_EXE %s
pwd
ls -l
date
echo "--- Ending PADMEMC production ---"
exit 0
"""%(padmemc_init_file,padme_seed1,padme_seed2,macro_file)
    with open("job.sh","w") as sf: sf.write(script)

    # Run job script sending its output/error to stdout/stderr
    print "Program starting at %s (UTC)"%now_str()
    job_cmd = "/bin/bash job.sh"
    p = subprocess.Popen(shlex.split(job_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    run_problems = False
    while True:

        # Handle script output and error streams with select.
        # Trap "Interrupted system call" error (happens when proxy is renewed)
        reads = [p.stdout.fileno(),p.stderr.fileno()]
        try:
            ret = select.select(reads,[],[],1.)
        except select.error as ex:
            if ex[0] == errno.EINTR:
                continue
            else:
                raise

        # Here we can parse the full stdout and stderr streams of the job checking for problems
        for fd in ret[0]:
            if fd == p.stdout.fileno():
                read = p.stdout.readline()
                sys.stdout.write(read)
            elif fd == p.stderr.fileno():
                read = p.stderr.readline()
                sys.stderr.write(read)

        if p.poll() != None: break

    rc_mc = p.returncode

    print "PADMEMC program ended at %s (UTC) with return code %s"%(now_str(),rc_mc)

    if rc_mc != 0 or run_problems:
        if run_problems:
            print "WARNING Problems found while parsing program output. Please check log."
        if rc_mc != 0:
            print "WARNING Simulation ended with non-zero return code. Please check log."
        print "Output files will not be saved to tape storage."
        sys.exit(1)

    print "--- Saving output files ---"

    data_ok = True
    if os.path.exists("data.root"):

        data_src_file = "data.root"
        data_size = os.path.getsize(data_src_file)
        data_adler32 = get_adler32(data_src_file)
        data_src_url = "file://%s/%s"%(job_dir,data_src_file)

        data_dst_file = "%s_%s_data.root"%(prod_name,job_name)
        data_dst_url = "%s%s/%s"%(srm_uri,storage_dir,data_dst_file)

        print "Copying",data_src_url,"to",data_dst_url
        data_copy_cmd = "gfal-copy %s %s"%(data_src_url,data_dst_url)
        print ">",data_copy_cmd
        rc = subprocess.call(data_copy_cmd.split())
        if rc:
            print "WARNING - gfal-copy returned error status %d"%rc
            data_ok = False
        else:
            print "MCDATA file %s with size %s and adler32 %s copied"%(data_dst_file,data_size,data_adler32)

    else:

        print "WARNING File data.root does not exist in current directory"
        data_ok = False

    hsto_ok = True
    if os.path.exists("hsto.root"):

        hsto_src_file = "hsto.root"
        hsto_size = os.path.getsize(hsto_src_file)
        hsto_adler32 = get_adler32(hsto_src_file)
        hsto_src_url = "file://%s/%s"%(job_dir,hsto_src_file)
        
        hsto_dst_file = "%s_%s_hsto.root"%(prod_name,job_name)
        hsto_dst_url = "%s%s/%s"%(srm_uri,storage_dir,hsto_dst_file)

        print "Copying %s to %s"%(hsto_src_url,hsto_dst_url)
        hsto_copy_cmd = "gfal-copy %s %s"%(hsto_src_url,hsto_dst_url)
        print ">",hsto_copy_cmd
        rc = subprocess.call(hsto_copy_cmd.split())
        if rc:
            print "WARNING - gfal-copy returned error status %d"%rc
            hsto_ok = False
        else:
            print "MCHSTO file %s with size %s and adler32 %s copied"%(hsto_dst_file,hsto_size,hsto_adler32)

    else:

        print "WARNING File hsto.root does not exist in current directory"
        hsto_ok = False

    #if not (os.path.exists("data.root") and os.path.exists("hsto.root")): sys.exit(1)
    if not (data_ok and hsto_ok):
        sys.exit(1)

    print "Job ending at %s (UTC)"%now_str()

# Execution starts here
if __name__ == "__main__":

    main(sys.argv[1:])
