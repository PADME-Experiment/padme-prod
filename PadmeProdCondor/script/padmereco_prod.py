#!/usr/bin/python3 -u

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
import zlib

def now_str():

    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def get_adler32(outfile):

    ## Old version of adler32 using external program not installed on AlmaLinux9
    #adler_cmd = f"adler32 {outfile}"
    #p = subprocess.Popen(
    #    shlex.split(adler_cmd),
    #    stdin=None,
    #    stdout=subprocess.PIPE,
    #    stderr=subprocess.PIPE
    #)
    #stdout, stderr = p.communicate()
    #return stdout.decode().strip()

    # New version using python3 zlib package
    asum = 1
    # Read in 1MB chunks to handle large files efficiently
    with open(outfile, 'rb') as f:
        while True:
            chunk = f.read(1048576)
            if not chunk:
                break
            asum = zlib.adler32(chunk, asum)
    return asum & 0xffffffff

def export_file(src_url, dst_url):

    print("Copying", src_url, "to", dst_url)

    # Check if destination file already exists and rename it
    # This can happen if job log retrieval fails after the job has successfully completed
    stat_cmd = f"/usr/bin/gfal-stat {dst_url}"
    print(">", stat_cmd)
    rc = subprocess.call(shlex.split(stat_cmd))
    if rc == 0:
        print(f"WARNING - File {dst_url} exists. Attempting to rename it.")
        idx = 0
        while idx < 100:
            new_url = f"{dst_url}.{idx:02d}"
            rename_cmd = f"/usr/bin/gfal-rename {dst_url} {new_url}"
            print(">", rename_cmd)
            rc = subprocess.call(shlex.split(rename_cmd))
            if rc == 0:
                # Rename succeeded: we can proceed with the copy
                print(f"WARNING - Existing file renamed to {new_url}")
                break
            # Rename failed: file already exists. Try next index
            idx += 1
            if idx == 100:
                print(f"ERROR - File {dst_url} - Too many copies. Cannot rename existing file.")
                return 1

    # Now execute the copy command
    copy_cmd = f"/usr/bin/gfal-copy {src_url} {dst_url}"
    print(">", copy_cmd)
    p = subprocess.Popen(
        shlex.split(copy_cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = p.communicate()

    # In Python 3 stdout/stderr sono bytes
    if out:
        print(out.decode(), end="")
    if p.returncode != 0 and err:
        sys.stderr.write(err.decode())

    return p.returncode

def main(argv):

    # Immediately create an empty shell script to avoid Condor holding jobs when this file is not found
    open("job.sh", "w").close()

    # Top CVMFS directory for PadmeReco
    padmereco_cvmfs_dir = "/cvmfs/padme.infn.it/PadmeReco"

    output_file = "data.root"

    (input_list, prod_name, job_name, reco_version, configuration, storage_dir, srm_uri) = argv

    job_dir = os.getcwd()
    try:
        host_name = socket.gethostname()
    except Exception:
        host_name = "UNKNOWN"
    try:
        user_name = getpass.getuser()
    except Exception:
        user_name = "UNKNOWN"

    # Get processor model and frequency (useful to troubleshoot variations in execution time)
    processor = "UNKNOWN"
    cpu_freq = "UNKNOWN"
    if os.path.exists("/proc/cpuinfo"):
        with open("/proc/cpuinfo", "r") as cpuinfo:
            for l in cpuinfo:
                m = re.match(r"^\s*model name\s+:\s+(.*)$", l)
                if m:
                    processor = m.group(1)
                m = re.match(r"^\s*cpu MHz\s+:\s+(.*)$", l)
                if m:
                    cpu_freq = m.group(1)

    print(f"=== PadmeReco Production {prod_name} Job {job_name} ===")
    print(f"Job starting at {now_str()} (UTC)")
    print(f"Job running on node {host_name} as user {user_name} in dir {job_dir}")
    print(f"Processor {processor} at {cpu_freq} MHz")
    print("PadmeReco version", reco_version)
    print("SRM server URI", srm_uri)
    print("Storage directory", storage_dir)
    print("Input file list", input_list)

    # Check if software directory for this version is available on CVMFS
    padmereco_version_dir = f"{padmereco_cvmfs_dir}/{reco_version}"
    n_try = 0
    while not os.path.isdir(padmereco_version_dir):
        n_try += 1
        if n_try >= 5:
            print(f"ERROR Directory {padmereco_version_dir} not found")
            sys.exit(2)
        print(f"WARNING Directory {padmereco_version_dir} not found ({n_try}) - Pause and try again")
        time.sleep(5)

    # Define location of config directory
    config_dir = f"{padmereco_version_dir}/config"
    if not os.path.isdir(config_dir):
        print(f"ERROR Directory {config_dir} not found")
        sys.exit(2)

    # Check if Padme configuration file is available
    padmereco_init_file = f"{config_dir}/padme-configure.sh"
    if not os.path.exists(padmereco_init_file):
        print(f"ERROR File {padmereco_init_file} not found")
        sys.exit(2)

    # Create soft link to config dir and check configuration file
    os.symlink(config_dir, "config")
    padmereco_config_file = f"config/{configuration}"
    if not os.path.exists(padmereco_config_file):
        print(f"ERROR File {padmereco_config_file} not found")
        sys.exit(2)

    # Define location of include directory (needed for ROOT cint) and create locaal soft link to it
    include_dir = f"{padmereco_version_dir}/include"
    if not os.path.isdir(include_dir):
        print(f"ERROR Directory {include_dir} not found")
        sys.exit(2)
    os.symlink(include_dir, "include")

    # Prepare shell script to run PadmeReco
    script = f"""#!/bin/bash
echo "--- Starting PADMERECO production script ---"
date
. {padmereco_init_file}
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
echo "--- Environment variables ---"
env
#echo "LD_LIBRARY_PATH = $LD_LIBRARY_PATH"
echo "--- Starting PADMERECO executable ---"
cmd="$PADMERECO_EXE -l {input_list} -o {output_file} -n 0 -c {padmereco_config_file}"
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
"""
    with open("job.sh", "w") as sf:
        sf.write(script)

    # Run job script
    print(f"Program starting at {now_str()} (UTC)")
    job_cmd = "/bin/bash job.sh"
    p = subprocess.Popen(
        shlex.split(job_cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    run_problems = False
    while True:

        # Handle script output and error streams with select.
        # Trap "Interrupted system call" error (happens when proxy is renewed)
        reads = [p.stdout.fileno(), p.stderr.fileno()]
        try:
            ret = select.select(reads, [], [], 1.0)
        except OSError as ex:
            if ex.errno == errno.EINTR:
                continue
            else:
                raise

        # Here we can parse the full stdout and stderr streams of the job checking for problems
        for fd in ret[0]:
            if fd == p.stdout.fileno():
                read = p.stdout.readline().decode()
                sys.stdout.write(read)
            elif fd == p.stderr.fileno():
                read = p.stderr.readline().decode()
                sys.stderr.write(read)
                if re.match(r"^.*Error in <TNetXNGFile::", read):
                    run_problems = True

        if p.poll() is not None:
            break

    rc_reco = p.returncode

    print(f"Program ending at {now_str()} (UTC)")
    print(f"Script exited with return code {rc_reco}")

    if rc_reco != 0 or run_problems:
        if run_problems:
            print("WARNING Problems found while parsing program output. Please check log.")
        if rc_reco != 0:
            print("WARNING Reconstruction ended with non-zero return code. Please check log.")
        print("Output files will not be saved to tape storage.")
        sys.exit(1)

    # Show info about available proxy
    print("--- VOMS proxy information ---")
    proxy_cmd = "voms-proxy-info --all"
    print(">", proxy_cmd)
    subprocess.call(shlex.split(proxy_cmd))

    print("--- Saving output files ---")

    data_ok = True
    if os.path.exists(output_file):

        data_src_file = output_file
        data_size = os.path.getsize(data_src_file)
        data_adler32 = get_adler32(data_src_file)
        data_src_url = f"file://{job_dir}/{data_src_file}"

        data_dst_file = f"{prod_name}_{job_name}_reco.root"
        data_dst_url = f"{srm_uri}{storage_dir}/{data_dst_file}"

        rc = export_file(data_src_url, data_dst_url)
        if rc:
            print(f"WARNING - gfal-copy returned error status {rc}")
            data_ok = False
        else:
            print(f"RECODATA file {data_dst_file} with size {data_size} and adler32 {data_adler32} copied")

    else:

        print(f"WARNING File {output_file} does not exist in current directory")
        data_ok = False

    if not data_ok:
        sys.exit(1)

    print(f"Job ending at {now_str()} (UTC)")

# Execution starts here
if __name__ == "__main__":

    main(sys.argv[1:])
