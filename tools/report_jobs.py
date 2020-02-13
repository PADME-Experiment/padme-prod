#!/usr/bin/python

import sys
import os
import subprocess
import re
import shlex
import MySQLdb

# List of endpoints to check
endpoints = [
    "atlasce1.lnf.infn.it:8443",
    "atlasce2.lnf.infn.it:8443",
    "atlasce4.lnf.infn.it:8443",
    "ce04-lcg.cr.cnaf.infn.it:8443"
]

# Get DB connection parameters from environment variables
DB_HOST   = os.getenv('PADME_MCDB_HOST'  ,'percona.lnf.infn.it')
DB_PORT   = int(os.getenv('PADME_MCDB_PORT'  ,'3306'))
DB_USER   = os.getenv('PADME_MCDB_USER'  ,'padmeMCDB')
DB_PASSWD = os.getenv('PADME_MCDB_PASSWD','unknown')
DB_NAME   = os.getenv('PADME_MCDB_NAME'  ,'PadmeMCDB')

# Connect to database
try:
    conn = MySQLdb.connect(host   = DB_HOST,
                           port   = DB_PORT,
                           user   = DB_USER,
                           passwd = DB_PASSWD,
                           db     = DB_NAME)
except:
    print "*** ERROR *** Unable to connect to DB. Exception: %s"%sys.exc_info()[0]
    sys.exit(2)
c = conn.cursor()

def job_production(jobid):
    sql_code = """
SELECT p.name
FROM job_submit s
  INNER JOIN job j ON s.job_id=j.id
  INNER JOIN production p ON j.production_id=p.id
WHERE s.ce_job_id='%s'
"""%jobid
    c.execute(sql_code)
    res = c.fetchone()
    conn.commit()
    if res == None: return ""
    return res[0]

def print_job(jobid,status,exitcode,description,workernode,localuser):
    prod = job_production(jobid)
    if prod != "":
        user = "%s@%s"%(localuser,workernode)
        print "%-30s %-55s %-50s %-15s"%(prod,jobid,user,status),
        if exitcode:    print "ExitCode %s"%exitcode,
        if description: print "'%s'"%description,
        print

### Main program starts here ###

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
