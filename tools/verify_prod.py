#!/usr/bin/python

import MySQLdb
import os
import sys
import time
import getopt
import re
import shlex
import subprocess

# Get DB connection parameters from environment variables
DB_HOST   = os.getenv('PADME_MCDB_HOST'  ,'percona.lnf.infn.it')
DB_PORT   = int(os.getenv('PADME_MCDB_PORT'  ,'3306'))
DB_USER   = os.getenv('PADME_MCDB_USER'  ,'padmeMCDB')
DB_PASSWD = os.getenv('PADME_MCDB_PASSWD','unknown')
DB_NAME   = os.getenv('PADME_MCDB_NAME'  ,'PadmeMCDB')

def execute_command(command):
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()
    return (p.returncode,out,err)

def get_job_ce_status(ce_job_id):
    
    status      = "UNDEF"
    exit_code   = ""
    worker_node = "UNKNOWN"
    local_user  = "UNKNOWN"
    delegation  = ""

    # Retrieve status of job
    job_status_cmd = "glite-ce-job-status --level 2 %s"%ce_job_id

    # Handle job status info collection. Trap errors and allow for multiple retries
    retries = 0
    while True:

        (rc,out,err) = execute_command(job_status_cmd)
        if rc == 0:
            for l in iter(out.splitlines()):
                r = re.match("^\s*Current Status\s+=\s+\[(.+)\].*$",l)
                if r: status = r.group(1)
                r = re.match("^\s*ExitCode\s+=\s+\[(.+)\].*$",l)
                if r: exit_code = r.group(1)
                r = re.match("^\s*Worker Node\s+=\s+\[(.+)\].*$",l)
                if r: worker_node = r.group(1)
                r = re.match("^\s*Local User\s+=\s+\[(.+)\].*$",l)
                if r: local_user = r.group(1)
                r = re.match("^\s*Deleg Proxy ID\s+=\s+\[(.+)\].*$",l)
                if r: delegation = r.group(1)
            break

        print "  WARNING glite-ce-job-status returned error code %d"%rc
        print "- STDOUT -\n%s"%out
        print "- STDERR -\n%s"%err

        # Abort if too many attempts failed
        retries += 1
        if retries >= 3:
            print "  WARNING unable to retrieve job status info. Retried %d times"%retries
            break

        # Wait a bit before retrying
        time.sleep(5)

    return (status,exit_code,worker_node,local_user,delegation)

def main(argv):

    try:
        opts,args = getopt.getopt(argv,"hp:",[])
    except getopt.GetoptError:
        #print_help()
        sys.exit(2)

    prod_name = ""
    for opt,arg in opts:
        if opt == '-p':
            prod_name = arg

    prod_select = ""
    if prod_name: prod_select = "WHERE p.name='%s'"%prod_name
 
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

    sql_code = """
SELECT
  p.name,
  j.name,
  j.status,
  s.submit_index,
  s.status,
  s.ce_job_id
FROM job_submit s
  INNER JOIN job j ON s.job_id=j.id
  INNER JOIN production p ON j.production_id=p.id
%s
ORDER BY p.name,j.name,s.submit_index
"""%prod_select
    c.execute(sql_code)

    res = c.fetchall()
    conn.commit()
    if res == None:
        print "No jobs associated to production %s"%prod_name
        sys.exit(0)

    prod_name_old = ""
    for js in res:

        (prod_name,job_name,job_status,sub_index,sub_status,ce_job_id) = js
        if prod_name != prod_name_old:
            print "\t\t=== Production %s ==="%prod_name
            prod_name_old = prod_name

        if job_status == 2:
            print "%-8s %d %d %d %s %s"%(job_name,job_status,sub_index,sub_status,ce_job_id,"SUCCESS")
            continue

        if ce_job_id:
            (status,exit_code,worker_node,local_user,delegation) = get_job_ce_status(ce_job_id)
            print "%-8s %d %d %d %s %s %s %s@%s"%(job_name,job_status,sub_index,sub_status,ce_job_id,status,exit_code,local_user,worker_node)
            continue

        print "%-8s %d %d %d %s %s"%(job_name,job_status,sub_index,sub_status,ce_job_id,"UNSUBMITTED")

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
