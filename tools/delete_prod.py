#!/usr/bin/python

import MySQLdb
import os
import sys
import time
import getopt
import re
import shlex
import subprocess
import getpass

# Get DB connection parameters from environment variables
DB_HOST   = os.getenv('PADME_MCDB_HOST'  ,'percona.lnf.infn.it')
DB_PORT   = int(os.getenv('PADME_MCDB_PORT'  ,'3306'))
DB_USER   = os.getenv('PADME_MCDB_USER'  ,'padmeMCDB')
DB_PASSWD = os.getenv('PADME_MCDB_PASSWD','unknown')
DB_NAME   = os.getenv('PADME_MCDB_NAME'  ,'PadmeMCDB')

# Define (empty) list of productions to delete
PROD_LIST = []

def execute_command(command):
    p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate()
    rc = p.returncode
    return (rc,out,err)

def execute_command_with_stdin(command,input_string):
    p = subprocess.Popen(shlex.split(command),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (out,err) = p.communicate(input=input_string)
    rc = p.returncode
    return (rc,out,err)

def print_help():
    print "delete_prod [-p prod_name] [-L prod_list] [-f]"
    print "-p <prod_name>\tName of the production to \"delete\""
    print "-L <prod_list>\tName of file containing list of productions to \"delete\""
    print "-f\t\tEnable FAKE mode: show commands only"
    print "Note: -p and -L parameters can be mixed and repeated multiple times to create a"
    print "      list of productions to delete. Duplicates will be automatically removed."

def renew_proxy():

    cmd = "voms-proxy-info"
    print "> %s"%cmd
    (rc,out,err) = execute_command(cmd)
    if rc == 0:
        for line in out.splitlines():
            r = re.match("^timeleft  \: (\d+)\:.*$",line)
            if r and int(r.group(1))>=2: return
    else:
        print "WARNING proxy info command returned error %d"%rc
        #print "- STDOUT -\n%s"%out
        #print "- STDERR -\n%s"%err

    pwd = getpass.getpass("Enter GRID pass phrase for this identity:")
    cmd = "voms-proxy-init --voms vo.padme.org --valid 24:00 --pwstdin"
    print "> %s"%cmd
    (rc,out,err) = execute_command_with_stdin(cmd,pwd)
    if rc != 0:
        print "WARNING proxy renewal command returned error %d"%rc
        print "- STDOUT -\n%s"%out
        print "- STDERR -\n%s"%err
        sys.exit(1)

def add_prod(prod):

    global PROD_LIST

    # Add prod to list
    PROD_LIST.append(prod)

def add_prod_list(prod_list):

    global PROD_LIST

    # Check if file with list of prods exists and is readable
    if not ( os.path.exists(prod_list) and os.path.isfile(prod_list) and os.access(prod_list,os.R_OK) ):
        print "*** ERROR *** Prod list file %s not found or not readable"%prod_list
        sys.exit(2)

    # Read file and add prods to list
    with open(prod_list,"r") as pl:
        for prod in pl: PROD_LIST.append(prod.strip())

def main(argv):

    global PROD_LIST

    try:
        opts,args = getopt.getopt(argv,"fhp:L:",[])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    prod_name = ""
    fake_mode = False
    for opt,arg in opts:
        if opt == '-p':
            add_prod(arg)
        if opt == '-L':
            add_prod_list(arg)
        elif opt == '-f':
            print "FAKE mode enabled"
            fake_mode = True
        elif opt == '-h':
            print_help()
            sys.exit(0)

    # Remove duplicates from list of prods to be processed and sort it
    PROD_LIST = list(set(PROD_LIST))
    PROD_LIST.sort()
    n_prods = len(PROD_LIST)

    # Check if at least one prod was specified
    if n_prods == 0:
        print "*** ERROR *** No prods specified"
        print_help()
        sys.exit(2)

    # Verify that we are in the main production directory
    if not os.path.exists("prod") or not os.path.isdir("prod"):
        print "ERROR Directory 'prod' not found. This script must be run from the main production directory"
        sys.exit(1)

    # Connect to the database
    try:
        conn = MySQLdb.connect(host   = DB_HOST,
                               port   = DB_PORT,
                               user   = DB_USER,
                               passwd = DB_PASSWD,
                               db     = DB_NAME)
    except:
        print "ERROR Unable to connect to DB. Exception: %s"%sys.exc_info()[0]
        sys.exit(1)

    # Loop over all productions in list
    n_prod = 0
    for prod_name in PROD_LIST:

        n_prod += 1
        print
        print "=== %4d/%-4d === Deleting prod %s ==="%(n_prod,n_prods,prod_name)

        c = conn.cursor()

        # Make sure proxy is valid
        renew_proxy()

        # Get production id from DB
        c.execute("""SELECT id,prod_dir,storage_uri,storage_dir,n_jobs FROM production WHERE name=%s""",(prod_name,))
        if c.rowcount == 0:
            print "ERROR Production '%s' does not exist in the DB"%prod_name
            sys.exit(1)
        (prod_id,prod_dir,storage_uri,storage_dir,prod_n_jobs) = c.fetchone()
        print "- Production %s has id %d"%(prod_name,prod_id)

        # Choose new name for production adding "_deleted_NN" extension
        index = 0
        while True:
            prod_name_new = "%s_deleted_%02d"%(prod_name,index)
            prod_dir_new = "%s_deleted_%02d"%(prod_dir,index)
            storage_dir_new = "%s_deleted_%02d"%(storage_dir,index)
            c.execute("""SELECT id FROM production WHERE name=%s""",(prod_name_new,))
            if c.rowcount == 0: break
            index += 1
        print "Renaming production %s to %s"%(prod_name,prod_name_new)
        print "Moving log files from %s to %s"%(prod_dir,prod_dir_new)
        print "Moving output files on %s from %s to %s"%(storage_uri,storage_dir,storage_dir_new)

        # Rename production dir
        production_dir_renamed = False
        if os.path.exists(prod_dir) and os.path.isdir(prod_dir):
            if fake_mode:
                print "os.rename(\"%s\",\"%s\")"%(prod_dir,prod_dir_new)
                production_dir_renamed = True
            else:
                try:
                    os.rename(prod_dir,prod_dir_new)
                except:
                    print "WARNING Unable to rename directory %s to %s. Exception: %s"%(prod_dir,prod_dir_new,sys.exc_info()[0])
                else:
                    production_dir_renamed = True
        else:
            print "WARNING Cannot find directory %s"%prod_dir

            # Change production dir in DB
            if production_dir_renamed:
                if fake_mode:
                    print "UPDATE production SET prod_dir = %s WHERE id = %d"%(prod_dir_new,prod_id)
                else:
                    c.execute("""UPDATE production SET prod_dir = %s WHERE id = %s""",(prod_dir_new,prod_id))

        # Get list of jobs
        job_list = []
        c.execute("""SELECT id FROM job WHERE production_id=%s""",(prod_id,))
        if c.rowcount == 0:
            print "WARNING No jobs are associated to production '%s'"%prod_name
        else:
            res = c.fetchall()
            for job in res: job_list.append(job[0])

        # Rename files associated to the jobs
        for job_id in job_list:
            c.execute("""SELECT id,name FROM file WHERE job_id=%s""",(job_id,))
            if c.rowcount != 0:
                res = c.fetchall()
                for (file_id,file_name) in res:

                    file_name_new = file_name.replace(prod_name,prod_name_new)

                    # Rename file on storage
                    cmd = "gfal-rename %s%s/%s %s%s/%s"%(storage_uri,storage_dir,file_name,storage_uri,storage_dir,file_name_new)
                    print "> %s"%cmd
                    file_renamed = False
                    if fake_mode:
                        file_renamed = True
                    else:
                        attempts = 0
                        while True:
                            (rc,out,err) = execute_command(cmd)
                            if rc == 0:
                                file_renamed = True
                                break
                            print "WARNING command returned error %d"%rc
                            print "- STDOUT -\n%s"%out
                            print "- STDERR -\n%s"%err
                            attempts += 1
                            if attempts >= 3:
                                print "WARNING %d renaming attempts failed: giving up"%attempts
                                break

                    # Rename file in DB
                    if file_renamed:
                        if fake_mode:
                            print "UPDATE file SET name = %s WHERE id = %d"%(file_name_new,file_id)
                        else:
                            c.execute("""UPDATE file SET name = %s WHERE id = %s""",(file_name_new,file_id))

        # Rename storage dir
        cmd = "gfal-rename %s%s %s%s"%(storage_uri,storage_dir,storage_uri,storage_dir_new)
        print "> %s"%cmd
        storage_dir_renamed = False
        if fake_mode:
            storage_dir_renamed = True
        else:
            attempts = 0
            while True:
                (rc,out,err) = execute_command(cmd)
                if rc == 0:
                    storage_dir_renamed = True
                    break
                print "WARNING command returned error %d"%rc
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err
                attempts += 1
                if attempts >= 3:
                    print "WARNING %d renaming attempts failed: giving up"%attempts
                    break

        # Change storage dir in DB
        if storage_dir_renamed:
            if fake_mode:
                print "UPDATE production SET storage_dir = %s WHERE id = %d"%(storage_dir_new,prod_id)
            else:
                c.execute("""UPDATE production SET storage_dir = %s WHERE id = %s""",(storage_dir_new,prod_id))

        # Finally change production name
        if fake_mode:
            print "UPDATE production SET name = %s WHERE id = %d"%(prod_name_new,prod_id)
        else:
            c.execute("""UPDATE production SET name = %s WHERE id = %s""",(prod_name_new,prod_id))

        print "Production %s was \"deleted\""%prod_name

        conn.commit()

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
