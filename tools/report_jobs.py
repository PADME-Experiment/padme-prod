#!/usr/bin/python

import sys
import subprocess
import re
import shlex

def print_job(jobid,status,exitcode,description,workernode,localuser):
    user = "%s@%s"%(localuser,workernode)
    print "%-50s %-34s %-15s"%(jobid,user,status),
    if exitcode:    print "ExitCode %s"%exitcode,
    if description: print "'%s'"%description,
    print

endpoints = [
    "atlasce1.lnf.infn.it:8443",
    "atlasce2.lnf.infn.it:8443",
    "atlasce4.lnf.infn.it:8443"
]

for endpoint in endpoints:

    cmd = "glite-ce-job-status --level 2 --all --endpoint %s"%endpoint
    #print "> %s"%cmd
    try:
        out = subprocess.check_output(shlex.split(cmd))
    except subprocess.CalledProcessError as e:
        print "ERROR Command returned RC %d"%e.returncode
        sys.exit(1)

    jobid = ""
    status = ""
    exitcode = ""
    description = ""
    workernode = ""
    localuser = ""
    for l in iter(out.splitlines()):

        r = re.match("^\*+\s*JobID\s*=\s*\[(\S*)\].*",l)
        if r:
            if jobid: print_job(jobid,status,exitcode,description,workernode,localuser)
            jobid = r.group(1)
            status = ""
            exitcode = ""
            description = ""
            workernode = ""
            localuser = ""

        r = re.match("^\s*Current Status\s*=\s*\[(\S*)\].*",l)
        if r: status = r.group(1)

        r = re.match("^\s*ExitCode\s*=\s*\[(\S*)\].*",l)
        if r: exitcode = r.group(1)

        r = re.match("^\s*Description\s*=\s*\[(.*)\].*",l)
        if r: description = r.group(1)

        r = re.match("^\s*Worker Node\s*=\s*\[(.*)\].*",l)
        if r: workernode = r.group(1)

        r = re.match("^\s*Local User\s*=\s*\[(.*)\].*",l)
        if r: localuser = r.group(1)

    # Make sure we print the last job found
    if jobid: print_job(jobid,status,exitcode,description,workernode,localuser)
