#!/usr/bin/python

import os
import sys
import time
import subprocess
import re
import shlex
import random

from PadmeMCDB import PadmeMCDB
from Logger import Logger
from ProxyHandler import ProxyHandler
from ProdJob import ProdJob

class PadmeProdServer:

    def __init__(self,prod_name,debug):

        self.db = PadmeMCDB()

        # Create ProxyHandler and set its debug level. Later the voms_proxy file will be added.
        self.ph = ProxyHandler()
        self.ph.debug = debug

        self.debug = debug

        self.prod_name = prod_name

        self.prod_id = None

        self.job_list = []

        # Delay between two checks. Interval is flat between 3m and 5m
        self.prod_check_delay = 180
        self.prod_check_delay_spread = 120

        # Define environment variables for Condor authentication
        os.environ['_condor_SEC_CLIENT_AUTHENTICATION_METHODS'] = 'GSI'

        self.start_production()

    def start_production(self):

        # This is the daemonized code to start and handle the full production
        # It is structured to only depend on DB information

        # Uncomment to enable terminal output when not running in daemon mode
        #sys.stdout.interactive = True
        #sys.stderr.interactive = True

        # Verify that production exists in DB and retrieve production id
        if not self.db.is_prod_in_db(self.prod_name):
            print "*** ERROR *** Production '%s' not found in DB"%self.prod_name
            sys.exit(1)
        self.prod_id = self.db.get_prod_id(self.prod_name)
    
        # Get some info about this prod
        (dummy,prod_ce,prod_dir,proxy_info,prod_njobs) = self.db.get_prod_info(self.prod_id)

        # Check if production dir exists
        if not os.path.isdir(prod_dir):
            print "*** ERROR *** Production directory '%s' not found"%prod_dir
            sys.exit(1)

        # Redirect stdout and stderr to files with automatic time logging
        log_file_name = "%s/%s.log"%(prod_dir,self.prod_name)
        sys.stdout = Logger(log_file_name)
        err_file_name = "%s/%s.err"%(prod_dir,self.prod_name)
        sys.stderr = Logger(err_file_name)

        if self.debug:
            print "Production %s"%self.prod_name
            print "CE list: %s"%prod_ce
            print "Production directory: %s"%prod_dir
            print "Proxy configuration: %s"%proxy_info
            print "Number of jobs: %d"%prod_njobs

        # Define name of control file: if found, this production will cleanly quit
        quit_file = "%s/quit"%prod_dir

        # Get list of job ids for this production
        job_id_list = self.db.get_job_list(self.prod_id)
        if len(job_id_list) != prod_njobs:
            print "*** ERROR *** Number of jobs in DB and in production are different: %s != %s"%(len(job_id_list),prod_njobs)
            sys.exit(1)

        # Get list of available CEs
        ce_list = list(prod_ce.split(" "))

        # Configure proxy renewal service
        r = re.match("^\s*(\S+):(\d+)\s+(\S+)\s+(\S+)\s*$",proxy_info)
        if r:
            (self.ph.myproxy_server,self.ph.myproxy_port,self.ph.myproxy_name,self.ph.myproxy_passwd) = r.groups()
        else:
            print "*** ERROR *** Unable to decode MyProxy info: \"%s\""%proxy_info
            sys.exit(1)

        # All checks are good: ready to start real production activities
        print "=== Starting Production %s ==="%self.prod_name

        # Create and configure job handlers. Assign each job to a different CE (round robin)
        ce_idx = random.randint(0,len(ce_list)-1)
        for job_id in job_id_list:
            self.job_list.append(ProdJob(job_id,ce_list[ce_idx],self.db,self.debug))
            ce_idx += 1
            if ce_idx >= len(ce_list): ce_idx = 0
    
        # Define absolute path of VOMS proxy file which will be used for this production and pass it to the proxy handler
        voms_proxy = "%s/%s/%s.voms"%(os.getcwd(),prod_dir,self.prod_name)
        if self.debug: print "VOMS proxy for this production: %s"%voms_proxy
        self.ph.voms_proxy = voms_proxy

        # Create voms proxy to be used for this production
        self.ph.create_voms_proxy()

        # Main production loop
        undef_counter = 0
        jobs_success_old = 0
        jobs_fail_old = 0
        while True:
    
            # Renew proxy if needed
            self.ph.renew_voms_proxy()
    
            # Check quit control file and send quit command to all jobs if found.
            if os.path.exists(quit_file):
                print "*** Quit file %s found: quitting production ***"%quit_file
                self.quit_production()

            # Call method to check jobs status and handle each job accordingly
            (jobs_created,jobs_active,jobs_success,jobs_fail,jobs_undef) = self.handle_jobs()

            # Update database if any new job reached final state
            if ( (jobs_success != jobs_success_old) or (jobs_fail != jobs_fail_old) ):
                self.db.set_prod_job_numbers(self.prod_id,jobs_success,jobs_fail)
                self.db.set_prod_n_events(self.prod_id,self.db.get_prod_total_events(self.prod_id))
                jobs_success_old = jobs_success
                jobs_fail_old = jobs_fail

            # Show current production state
            print "Jobs: unsubmitted %d active %d success %d fail %d undef %d"%(jobs_created,jobs_active,jobs_success,jobs_fail,jobs_undef)

            # If all jobs are in a final state (either success or fail), production is over
            if jobs_created+jobs_active+jobs_undef == 0:
                print "--- No unfinished jobs left: production is done ---"
                break

            # Handle UNDEF condition in a relaxed way as it might be a temporary glitch of the CE
            if jobs_undef == 0:
                undef_counter = 0
            else:
                undef_counter += 1
                if undef_counter < 10:
                    print "  WARNING: %d jobs in UNDEF state for %d iteration(s)"%(jobs_undef,undef_counter)
                else:
                    print "*** More than 10 consecutive iterations with jobs in UNDEF state: quitting production ***"
                    self.quit_production()

            # Release DB connection while idle
            self.db.close_db()
    
            # Sleep for a while (use random to avoid coherent checks when concurrent productions are active)
            time.sleep(self.prod_check_delay+random.randint(0,self.prod_check_delay_spread+1))
    
        # Production is over: get total events, tag production as done and say bye bye
        n_events = self.db.get_prod_total_events(self.prod_id)
        print "- Jobs submitted: %d - Jobs successful: %d - Jobs failed: %d - Total events: %d"%(prod_njobs,jobs_success,jobs_fail,n_events)
        self.db.close_prod(self.prod_id,jobs_success,jobs_fail,n_events)
    
        # Release DB connection before exiting
        self.db.close_db()

        print "=== Ending Production %s ==="%self.prod_name
        sys.exit(0)
    
    def handle_jobs(self):
    
        jobs_created = 0
        jobs_active = 0
        jobs_success = 0
        jobs_fail = 0
        jobs_undef = 0

        print "--- Checking status of production jobs ---"

        for job in self.job_list:

            status = job.update()
            if   status == "CREATED":    jobs_created += 1
            elif status == "ACTIVE":     jobs_active  += 1
            elif status == "SUCCESSFUL": jobs_success += 1
            elif status == "FAILED":     jobs_fail    += 1
            elif status == "UNDEF":      jobs_undef   += 1
            else:
                print "  WARNING ProdJob returned unknown status '%s'"%status
                jobs_undef += 1

        return (jobs_created,jobs_active,jobs_success,jobs_fail,jobs_undef)

    def quit_production(self):

        # Tell all jobs to quit as fast as possible
        for job in self.job_list: job.job_quit = True

        # When in quit mode, speed up final checks
        self.prod_check_delay = 60
        self.prod_check_delay_spread = 0
