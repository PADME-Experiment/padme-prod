#!/usr/bin/python

import os
import sys
import getopt
import signal
import time
import subprocess
import shutil

PROXY_FILE = ""
PROXY_RENEW_TIME = 6*3600

def now_str():

    return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())

def get_adler32(outfile):

    adler_cmd = "adler32 %s"%outfile
    p = subprocess.Popen(adler_cmd.split(),stdin=None,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
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

    # Top CVMFS directory for PadmeReco
    padmereco_cvmfs_dir = "/cvmfs/padme.infn.it/PadmeReco"

    output_file = "data.root"

    (input_list,PROXY_FILE,prod_name,job_name,reco_version,storage_dir,srm_uri) = argv

    job_dir = os.getcwd()

    print "=== PadmeReco Production %s Job %s ==="%(prod_name,job_name)
    print "Job starting at %s (UTC)"%now_str()
    print "Job running on node %s as user %s in dir %s"%(os.getenv('HOSTNAME'),os.getenv('USER'),job_dir)

    print "PadmeReco version",reco_version
    print "SRM server URI",srm_uri
    print "Storage directory",storage_dir
    print "Input file list",input_list
    print "Proxy file",PROXY_FILE

    # Change permission rights for long-lived proxy (must be 600)
    os.chmod(PROXY_FILE,0600)

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

    # Create local soft link to the config directory
    config_dir = "%s/config"%padmereco_version_dir
    if not os.path.isdir(config_dir):
        print "ERROR Directory %s not found"%config_dir
        exit(2)
    os.symlink(config_dir,"config")

    # Check if PadmeReco configuration file is available
    padmereco_init_file = "%s/padme-configure.sh"%config_dir
    if not os.path.exists(padmereco_init_file):
        print "ERROR File %s not found"%padmereco_init_file
        exit(2)

    # Enable timer to renew VOMS proxy every 6h
    signal.signal(signal.SIGALRM,renew_proxy_handler)
    signal.alarm(PROXY_RENEW_TIME)

    # Prepare shell script to run PadmeReco
    script = """#!/bin/bash
echo "--- Starting PADMERECO production ---"
date
. %s
if [-z "$PADME"]; then
    echo "Variable PADME is not set: aborting"
    exit 1
else 
    echo "PADME = $PADME"
fi
if [-z "$PADMERECO_EXE"]; then
    echo "Variable PADMERECO_EXE is not set: aborting"
    exit 1
else
    echo "PADMERECO_EXE = $PADMERECO_EXE"
fi
echo "LD_LIBRARY_PATH = $LD_LIBRARY_PATH"
$PADMERECO_EXE -l %s -o %s -n 0
pwd
ls -l
date
echo "--- Ending PADMERECO production ---"
exit 0
"""%(padmereco_init_file,input_list,output_file)
    with open("job.sh","w") as sf: sf.write(script)
    #sf = open("job.sh","w")
    #sf.write("#!/bin/bash\n")
    #sf.write("echo \"--- Starting PADMERECO production ---\"\n")
    #sf.write(". %s\n"%padmereco_init_file)
    #sf.write("echo \"PADME = $PADME\"\n")
    #sf.write("echo \"PADMERECO_EXE = $PADMERECO_EXE\"\n")
    #sf.write("echo \"LD_LIBRARY_PATH = $LD_LIBRARY_PATH\"\n")
    #sf.write("$PADMERECO_EXE -l %s -o %s -n 0\n"%(input_list,output_file))
    #sf.write("pwd; ls -l\n")
    #sf.close()

    # Run job script sending its output/error to stdout/stderr
    print "Program starting at %s (UTC)"%now_str()
    job_cmd = "/bin/bash job.sh"
    rc_reco = subprocess.call(job_cmd.split())
    print "Program ending at %s (UTC)"%now_str()

    print "PADMERECO program ended with return code %s"%rc_reco

    if rc_reco == 0:

        print "--- Saving output files ---"

        if os.path.exists(output_file):

            data_src_file = output_file
            data_size = os.path.getsize(data_src_file)
            data_adler32 = get_adler32(data_src_file)
            data_src_url = "file://%s/%s"%(job_dir,data_src_file)

            data_dst_file = "%s_%s_reco.root"%(prod_name,job_name)
            data_dst_url = "%s%s/%s"%(srm_uri,storage_dir,data_dst_file)

            print "Copying",data_src_url,"to",data_dst_url
            data_copy_cmd = "gfal-copy %s %s"%(data_src_url,data_dst_url)
            print ">",data_copy_cmd
            rc = subprocess.call(data_copy_cmd.split())

            print "RECODATA file %s with size %s and adler32 %s copied"%(data_dst_file,data_size,data_adler32)

        else:

            print "WARNING File %s does not exist in current directory"%output_file

    else:

        print "ERROR Some errors occourred during reconstruction. Please check log."
        print "Output files will not be saved to tape storage."
        sys.exit(1)

    print "Job ending at %s (UTC)"%now_str()

# Execution starts here
if __name__ == "__main__":

    main(sys.argv[1:])
