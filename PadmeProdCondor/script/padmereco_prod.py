#!/usr/bin/python -u

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
import getpass
import socket

def now_str():

    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def get_adler32(outfile):

    adler_cmd = "adler32 %s"%outfile
    p = subprocess.Popen(shlex.split(adler_cmd),stdin=None,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    return p.communicate()[0].strip()

def export_file(src_url,dst_url):

    print "Copying",src_url,"to",dst_url

    # Check if destination file already exists and rename it
    # This can happen if job log retrieval fails after the job has successfully completed
    stat_cmd = "gfal-stat %s"%dst_url
    print ">",stat_cmd
    rc = subprocess.call(shlex.split(stat_cmd))
    if rc == 0:
        print "WARNING - File %s exists. Attempting to rename it."%dst_url
        idx = 0
        while idx<100:
            new_url = "%s.%02d"%(dst_url,idx)
            rename_cmd = "gfal-rename %s %s"%(dst_url,new_url)
            print ">",rename_cmd
            rc = subprocess.call(shlex.split(rename_cmd))
            if rc == 0:
                # Rename succeeded: we can proceed with the copy
                print "WARNING - Existing file renamed to %s"%new_url
                break
            # Rename failed: file already exists. Try next index
            idx += 1
            if idx == 100:
                print "ERROR - File %s - Too many copies. Cannot rename existing file."%dst_url
                return 1

    # Now execute the copy command
    copy_cmd = "gfal-copy %s %s"%(src_url,dst_url)
    print ">",copy_cmd
    p = subprocess.Popen(shlex.split(copy_cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()
    print out
    if p.returncode != 0:
        sys.stderr.write(err)
    return p.returncode

def main(argv):

    # Immediately create an empty shell script to avoid Condor holding jobs when this file is not found
    open("job.sh","w").close()

    # Top CVMFS directory for PadmeReco
    padmereco_cvmfs_dir = "/cvmfs/padme.infn.it/PadmeReco"

    output_file = "data.root"

    (input_list,prod_name,job_name,reco_version,configuration,storage_dir,srm_uri) = argv

    job_dir = os.getcwd()
    try:
        host_name = socket.gethostname()
    except:
        host_name = "UNKNOWN"
    try:
        user_name = getpass.getuser()
    except:
        user_name = "UNKNOWN"

    # Get processor model (useful to troubleshoot variations in execution time)
    processor = "UNKNOWN"
    if os.path.exists("/proc/cpuinfo"):
        with open("/proc/cpuinfo","r") as cpuinfo:
            for l in cpuinfo:
                m = re.match("^\s*model name\s+:\s+(.*)$",l)
                if m:
                    processor = m.group(1)
                    break

    print "=== PadmeReco Production %s Job %s ==="%(prod_name,job_name)
    print "Job starting at %s (UTC)"%now_str()
    print "Job running on node %s as user %s in dir %s"%(host_name,user_name,job_dir)
    print "Processor %s"%processor
    print "PadmeReco version",reco_version
    print "SRM server URI",srm_uri
    print "Storage directory",storage_dir
    print "Input file list",input_list

    # Check if software directory for this version is available on CVMFS (try a few times before giving up)
    padmereco_version_dir = "%s/%s"%(padmereco_cvmfs_dir,reco_version)
    n_try = 0
    while not os.path.isdir(padmereco_version_dir):
        n_try += 1
        if n_try >= 5:
            print "ERROR Directory %s not found"%padmereco_version_dir
            exit(2)
        print "WARNING Directory %s not found (%d) - Pause and try again"%(padmereco_version_dir,n_try)
        time.sleep(5)

    # Define location of config directory
    config_dir = "%s/config"%padmereco_version_dir
    if not os.path.isdir(config_dir):
        print "ERROR Directory %s not found"%config_dir
        exit(2)

    # Check if Padme configuration file is available
    padmereco_init_file = "%s/padme-configure.sh"%config_dir
    if not os.path.exists(padmereco_init_file):
        print "ERROR File %s not found"%padmereco_init_file
        exit(2)

    # Create soft link to config dir and check if required PadmeReco configuration file is available
    os.symlink(config_dir,"config")
    padmereco_config_file = "config/%s"%configuration
    if not os.path.exists(padmereco_config_file):
        print "ERROR File %s not found"%padmereco_config_file
        exit(2)

    # Prepare shell script to run PadmeReco
    script = """#!/bin/bash
echo "--- Starting PADMERECO production ---"
date
. %s
if [ -z "$PADME" ]; then
    echo "Variable PADME is not set: aborting"
    exit 1
else 
    echo "PADME = $PADME"
fi
if [ -z "$PADMERECO_EXE" ]; then
    echo "Variable PADMERECO_EXE is not set: aborting"
    exit 1
else
    echo "PADMERECO_EXE = $PADMERECO_EXE"
fi
echo "LD_LIBRARY_PATH = $LD_LIBRARY_PATH"
cmd="$PADMERECO_EXE -l %s -o %s -n 0 -c %s"
echo $cmd
$cmd
rc=$?
if [ $rc -ne 0 ]; then
  echo "*** ERROR *** PadmeReco returned error code $rc"
fi
pwd
ls -l
date
echo "--- Ending PADMERECO production ---"
exit $rc
"""%(padmereco_init_file,input_list,output_file,padmereco_config_file)
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
                if re.match("^.*Error in <TNetXNGFile::Open>: \[ERROR\]",read): run_problems = True

        if p.poll() != None: break

    rc_reco = p.returncode

    print "Program ending at %s (UTC)"%now_str()
    print "Script exited with return code %s"%rc_reco

    if rc_reco != 0 or run_problems:
        if run_problems:
            print "WARNING Problems found while parsing program output. Please check log."
        if rc_reco != 0:
            print "WARNING Reconstruction ended with non-zero return code. Please check log."
        print "Output files will not be saved to tape storage."
        sys.exit(1)

    # Show info about available proxy
    print "--- VOMS proxy information ---"
    proxy_cmd = "voms-proxy-info --all"
    print ">",proxy_cmd
    rc = subprocess.call(shlex.split(proxy_cmd))

    print "--- Saving output files ---"

    data_ok = True
    if os.path.exists(output_file):

        data_src_file = output_file
        data_size = os.path.getsize(data_src_file)
        data_adler32 = get_adler32(data_src_file)
        data_src_url = "file://%s/%s"%(job_dir,data_src_file)

        data_dst_file = "%s_%s_reco.root"%(prod_name,job_name)
        data_dst_url = "%s%s/%s"%(srm_uri,storage_dir,data_dst_file)

        rc = export_file(data_src_url,data_dst_url)
        if rc:
            print "WARNING - gfal-copy returned error status %d"%rc
            data_ok = False
        else:
            print "RECODATA file %s with size %s and adler32 %s copied"%(data_dst_file,data_size,data_adler32)

    else:

        print "WARNING File %s does not exist in current directory"%output_file
        data_ok = False

    if not data_ok:
        sys.exit(1)

    print "Job ending at %s (UTC)"%now_str()

# Execution starts here
if __name__ == "__main__":

    main(sys.argv[1:])
