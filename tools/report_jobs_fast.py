#!/usr/bin/python

import sys
import os
import subprocess
import getopt
import re
import shlex

# List of endpoints to check
ENDPOINTS = [
    "atlasce3.lnf.infn.it:9619",
#    "ce01-htc.cr.cnaf.infn.it:9619",
    "ce02-htc.cr.cnaf.infn.it:9619",
    "ce03-htc.cr.cnaf.infn.it:9619",
    "ce04-htc.cr.cnaf.infn.it:9619",
    "ce05-htc.cr.cnaf.infn.it:9619",
    "ce06-htc.cr.cnaf.infn.it:9619",
    "ce07-htc.cr.cnaf.infn.it:9619"
]

# Name of job owner to check
OWNERS = [
    "padme003",
#    "padme008",
    "padme008",
    "padme008",
    "padme008",
    "padme008",
    "padme008",
    "padme008"
]

def print_help():
    print "report_jobs_fast [-O owner] [-h]"
    print "-O <owner>\tName of the job owner, ALL for all users. Default depends on endpoint."
    print "-h\t\tShow this help message and exit"

### Main program starts here ###

def main(argv):

    try:
        opts,args = getopt.getopt(argv,"O:h",[])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    OWNER = ""
    for opt,arg in opts:
        if opt == '-O':
            OWNER = arg
        elif opt == '-h':
            print_help()
            sys.exit(0)

    #for endpoint in ENDPOINTS:
    for ep in range(len(ENDPOINTS)):

        endpoint = ENDPOINTS[ep]
        if OWNER == "":
            owner = OWNERS[ep]
        else:
            owner = OWNER

        (ep_host,ep_port) = endpoint.split(":")
        cmd = "condor_q -nobatch -pool %s -name %s"%(endpoint,ep_host)
        p = subprocess.Popen(shlex.split(cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (out,err) = p.communicate()
        if p.returncode:
            print "** ERROR *** Command returned RC %d"%p.returncode
            print "> %s"%cmd
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err
            sys.exit(1)

        # Get all jobs in this endpoint belonging to our user
        job_total     = 0
        job_completed = 0
        job_running   = 0
        job_idle      = 0
        job_held      = 0
        job_removed   = 0
        job_suspended = 0
        job_transin   = 0
        job_transout  = 0
        for l in iter(out.splitlines()):
            if owner == "ALL":
                r = re.match("^\s*([\d\.]+)\s+\S+\s+\d\d/\d\d\s+\d\d:\d\d\s+(\S+)\s+(\S+)\s+.*$",l)
            else:
                r = re.match("^\s*([\d\.]+)\s+%s\s+\d\d/\d\d\s+\d\d:\d\d\s+(\S+)\s+(\S+)\s+.*$"%owner,l)
            if r:
                job_id = r.group(1)
                job_time = r.group(2)
                job_status = r.group(3)
                job_total += 1
                if job_status == "C":
                    job_completed += 1
                elif job_status == "R":
                    job_running += 1
                elif job_status == "I":
                    job_idle += 1
                elif job_status == "H":
                    job_held += 1
                elif job_status == "X":
                    job_removed += 1
                elif job_status == "S":
                    job_suspended += 1
                elif job_status == "<":
                    job_transin += 1
                elif job_status == ">":
                    job_transout += 1
                else:
                    print "ERROR - Unknown job status %s"%job_status
                    print r.group(0)

        if job_total > 0:
            print "Endpoint %s (%s)"%(endpoint,owner)
            print "Completed    %6d"%job_completed
            print "Running      %6d"%job_running
            print "Idle         %6d"%job_idle
            print "Held         %6d"%job_held
            print "Removed      %6d"%job_removed
            print "Suspended    %6d"%job_suspended
            print "Transfer in  %6d"%job_transin
            print "Transfer out %6d"%job_transout
            print "Total jobs   %6d"%job_total

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
