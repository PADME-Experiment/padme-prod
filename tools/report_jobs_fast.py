#!/usr/bin/python

import sys
import os
import subprocess
import getopt
import re
import shlex
import MySQLdb

# List of endpoints to check
ENDPOINTS = [
    "atlasce3.lnf.infn.it:9619",
##    "ce01-htc.cr.cnaf.infn.it:9619",
#    "ce02-htc.cr.cnaf.infn.it:9619",
#    "ce03-htc.cr.cnaf.infn.it:9619",
#    "ce04-htc.cr.cnaf.infn.it:9619",
#    "ce05-htc.cr.cnaf.infn.it:9619",
#    "ce06-htc.cr.cnaf.infn.it:9619",
#    "ce07-htc.cr.cnaf.infn.it:9619"
]

# Name of job owner to check
#OWNER = "padme008"
OWNERS = [
    "padme003",
    "padme008",
    "padme008",
    "padme008",
    "padme008",
    "padme008",
    "padme008"
]

# "ALL": Show all jobs belonging to owner
# "PROD": Show only jobs associted to a production
SHOW_JOB = "ALL"

# Define Condor job status map
CONDOR_JOB_STATUS = {
    "1": "IDLE",
    "2": "RUNNING",
    "3": "REMOVING",
    "4": "COMPLETED",
    "5": "HELD",
    "6": "TRANSFERRING OUTPUT",
    "7": "SUSPENDED"
}

# Get DB connection parameters from environment variables
DB_HOST   = os.getenv('PADME_MCDB_HOST'  ,'percona.lnf.infn.it')
DB_PORT   = int(os.getenv('PADME_MCDB_PORT'  ,'3306'))
DB_USER   = os.getenv('PADME_MCDB_USER'  ,'padmeMCDB')
DB_PASSWD = os.getenv('PADME_MCDB_PASSWD','unknown')
DB_NAME   = os.getenv('PADME_MCDB_NAME'  ,'PadmeMCDB')

# Connect to database
try:
    CONN = MySQLdb.connect(host   = DB_HOST,
                           port   = DB_PORT,
                           user   = DB_USER,
                           passwd = DB_PASSWD,
                           db     = DB_NAME)
except:
    print "*** ERROR *** Unable to connect to DB. Exception: %s"%sys.exc_info()[0]
    sys.exit(2)

def print_help():
    print "report_jobs_condor [-O owner] [-A|-P] [-h]"
    print "-O <owner>\tName of the job owner. Default depends on endpoint"
    print "-A|-P\t\tShow all jobs (-A) or only jobs associated to a production (-P). Default: %s"%SHOW_JOB
    print "-h\t\tShow this help message and exit"

def job_production(jobid):
    sql_code = """
SELECT p.name
FROM job_submit s
  INNER JOIN job j ON s.job_id=j.id
  INNER JOIN production p ON j.production_id=p.id
WHERE s.ce_job_id='%s'
"""%jobid
    c = CONN.cursor()
    c.execute(sql_code)
    res = c.fetchone()
    CONN.commit()
    if res == None: return ""
    return res[0]

def print_job(jobid,status,exitcode,user,prod):
    print "%-60s %-40s %-20s %-15s"%(prod,jobid,user,status),
    if exitcode: print "ExitCode %s"%exitcode,
    print

### Main program starts here ###

def main(argv):

    global OWNER
    global SHOW_JOB

    try:
        opts,args = getopt.getopt(argv,"O:PAh",[])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    for opt,arg in opts:
        if opt == '-O':
            OWNER = arg
        elif opt == '-P':
            SHOW_JOB = "PROD"
        elif opt == '-A':
            SHOW_JOB = "ALL"
        elif opt == '-h':
            print_help()
            sys.exit(0)

    #for endpoint in ENDPOINTS:
    for ep in range(len(ENDPOINTS)):

        endpoint = ENDPOINTS[ep]
        owner = OWNERS[ep]

        (ep_host,ep_port) = endpoint.split(":")
        cmd = "condor_q -pool %s -name %s"%(endpoint,ep_host)
        p = subprocess.Popen(shlex.split(cmd),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (out,err) = p.communicate()
        if p.returncode:
            print "** ERROR *** Command returned RC %d"%p.returncode
            print "> %s"%cmd
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err
            sys.exit(1)

        # Get all jobs in this endpoint belonging to our user
        job_total = 0
        job_done = 0
        job_run = 0
        job_idle = 0
        job_held = 0
        job_list = []
        for l in iter(out.splitlines()):
            #r = re.match("^\s*%s\s+ID:\s+(\d+)\s+.*$"%OWNER,l)
            r = re.match("^\s*%s\s+ID:\s+(\d+)\s+\d\d/\d\d\s+\d\d:\d\d\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+.*$"%owner,l)
            if r:
                job_list.append(r.group(1))
                if r.group(2) != "_":
                    try:
                        job_done += int(r.group(2))
                    except:
                        pass
                if r.group(3) != "_":
                    try:
                        job_run += int(r.group(3))
                    except:
                        pass
                if r.group(4) != "_":
                    try:
                        job_idle += int(r.group(4))
                    except:
                        pass
                if r.group(5) != "_":
                    try:
                        job_held += int(r.group(5))
                    except:
                        pass
                if r.group(6) != "_":
                    try:
                        job_total += int(r.group(6))
                    except:
                        pass

        print "Endpoint %s (%s)"%(endpoint,owner)
        print "Jobs done    %d"%job_done
        print "Jobs running %d"%job_run
        print "Jobs idle    %d"%job_idle
        print "Jobs held    %d"%job_held
        print "Jobs total   %d"%job_total

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
