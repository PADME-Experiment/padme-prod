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

class PadmeProdServer:

    def __init__(self,prod_name,debug):

        self.db = PadmeMCDB()

        self.ph = ProxyHandler()
        self.ph.debug = debug

        self.debug = debug

        self.prod_name = prod_name

        # Delay between two checks. Interval is flat between 3m and 5m
        self.prod_check_delay = 180
        self.prod_check_delay_spread = 120

        self.job_submissions_max = 3

        self.start_production()

    def run_command(self,command):
        if self.debug: print "> %s"%command
        p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        return iter(p.stdout.readline,b'')

    def execute_command(self,command):
        if self.debug: print "> %s"%command
        p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (out,err) = p.communicate()
        return (p.returncode,out,err)

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
        prod_id = self.db.get_prod_id(self.prod_name)
    
        # Get some info about this prod
        (dummy,prod_ce,prod_dir,proxy_file,prod_njobs) = self.db.get_prod_info(prod_id)

        # Check if production dir exists
        if not os.path.isdir(prod_dir):
            print "*** ERROR *** Production directory '%s' not found"%prod_dir
            sys.exit(1)
    
        # Check if proxy file exists
        if not os.path.isfile(proxy_file):
            print "*** ERROR *** Long-lived proxy file '%s' not found"%proxy_file
            sys.exit(1)

        # Redirect stdout and stderr to files with automatic time logging
        log_file_name = "%s/%s.log"%(prod_dir,self.prod_name)
        sys.stdout = Logger(log_file_name)
        err_file_name = "%s/%s.err"%(prod_dir,self.prod_name)
        sys.stderr = Logger(err_file_name)
            
        # Get list of job ids for this production
        job_id_list = self.db.get_job_list(prod_id)
        if len(job_id_list) != prod_njobs:
            print "*** ERROR *** Number of jobs in DB and in production are different: %s != %s"%(len(job_id_list),prod_njobs)
            sys.exit(1)

        # Extract CE endpoint and register it for proxy renewal
        r = re.match("^(.*)/.*$",prod_ce)
        if r:
            self.ph.cream_ce_endpoint = r.group(1)
        else:
            print "WARNING Unable to extract CE endpoint from production CE %s"%prod_ce
    
        print "=== Starting Production %s ==="%self.prod_name
    
        # Main production loop
        undef_counter = 0
        while True:
    
            # Renew proxy if needed
            self.ph.renew_voms_proxy(proxy_file)
    
            # Call method to check jobs status and handle each job accordingly
            (jobs_submit,jobs_idle,jobs_active,jobs_success,jobs_fail,jobs_cancel,jobs_undef) = self.handle_jobs(prod_ce,job_id_list)
    
            # Show current production state
            print "Jobs: submitted %d idle %d running %d success %d fail %d cancel %d undef %d"%(jobs_submit,jobs_idle,jobs_active,jobs_success,jobs_fail,jobs_cancel,jobs_undef)

            # If all jobs are in a final state, production is over
            if jobs_submit+jobs_idle+jobs_active+jobs_undef == 0:
                print "--- No unfinished jobs left: exiting ---"
                break

            # Handle UNDEF condition in a relaxed way as it might be a temporary glitch of the CE
            if jobs_undef == 0:
                undef_counter = 0
            else:
                undef_counter += 1
                if undef_counter < 10:
                    print "WARNING: %d jobs in UNDEF state for %d iteration(s)"%(jobs_undef,undef_counter)
                else:
                    print "*** More than 10 consecutive iterations with jobs in UNDEF state: exiting ***"
                    break

            # Release DB connection while idle
            self.db.close_db()
    
            # Sleep for a while (use random to avoid coherent checks when multiple runs are active)
            time.sleep(self.prod_check_delay+random.randint(0,self.prod_check_delay_spread+1))
    
        # Production is over: get total events, tag production as done and say bye bye
        n_events = self.db.get_prod_total_events(prod_id)
        print "- Jobs submitted: %d - Jobs successful: %d - Total events: %d"%(prod_njobs,jobs_success,n_events)
        self.db.close_prod(prod_id,jobs_success,n_events)
    
        # Release DB connection before exiting
        self.db.close_db()

        print "=== Ending Production %s ==="%self.prod_name
        sys.exit(0)
    
    def handle_jobs(self,prod_ce,job_id_list):
    
        jobs_submit = 0
        jobs_idle = 0
        jobs_active = 0
        jobs_success = 0
        jobs_fail = 0
        jobs_cancel = 0
        jobs_undef = 0

        # Reset proxy delegations array
        self.ph.delegations = []
    
        print "--- Checking status of production jobs ---"
    
        # Job status in DB:
        # 0: Unsubmitted
        # 1: Submitted (PENDING, REGISTERED, IDLE)
        # 2: Active (RUNNING, REALLY-RUNNING)
        # 3: Successful (DONE-OK and finalized)
        # 4: Unsuccessful (DONE-OK and NOT finalized)
        # 5: Failed (DONE-FAILED and finalized)
        # 6: Failed (DONE-FAILED and NOT finalized)
        # 7: Cancelled (CANCELLED)
        # 8: Undefined

        for job_id in job_id_list:
    
            (job_name,job_dir,job_status) = self.db.get_job_info(job_id)
    
            # If status is 0, job was not submitted yet: do it now
            if job_status == 0:
                (job_sub_id,ce_job_id) = self.submit_job(job_id,job_dir,prod_ce)
                print "- %s %s SUBMITTED"%(job_name,ce_job_id)
                self.db.set_job_status(job_id,1)
                jobs_submit += 1
                continue

            # Get info about associated job submission
            job_sub_id = self.db.get_job_submit_id(job_id)
            (job_sub_index,job_sub_status,ce_job_id,worker_node,wn_user) = self.db.get_job_submit_info(job_sub_id)

            # Status 2: Job was successful
            if job_status == 2:
                print "- %s %s %s"%(job_name,ce_job_id,"DONE_OK - Output OK")
                jobs_success += 1
                continue

            # Status 3: Job failed. Show how it failed
            if job_status == 3:
                if job_sub_status == 4:
                    print "- %s %s %s"%(job_name,ce_job_id,"DONE_OK - Output Fail")
                    jobs_fail += 1
                if job_sub_status == 5:
                    print "- %s %s %s"%(job_name,ce_job_id,"DONE_FAILED - Output OK")
                    jobs_fail += 1
                if job_sub_status == 6:
                    print "- %s %s %s"%(job_name,ce_job_id,"DONE_FAILED - Output Fail")
                    jobs_fail += 1
                if job_sub_status == 7:
                    print "- %s %s %s"%(job_name,ce_job_id,"CANCELLED")
                    jobs_cancel += 1
                continue

            # Status is 1: Job is being processed
            if job_status == 1:

                # Get info about running job from CE
                (job_ce_status,job_worker_node,job_local_user,job_delegation) = self.get_job_ce_status(ce_job_id)
                print "- %s %s %s %s@%s"%(job_name,ce_job_id,job_ce_status,job_local_user,job_worker_node)

                # Register job delegation for proxy renewals
                self.ph.delegations.append(job_delegation)

                # Check current job status and update DB if it changed
                job_resubmit = False
                if job_ce_status == "PENDING" or job_ce_status == "REGISTERED" or job_ce_status == "IDLE":
                    jobs_idle += 1
                elif job_ce_status == "RUNNING" or job_ce_status == "REALLY-RUNNING":
                    jobs_active += 1
                    if job_sub_status == 1:
                        self.db.set_job_submit_status(job_sub_id,2)
                        self.db.set_job_worker_node(job_sub_id,job_worker_node)
                        self.db.set_job_wn_user(job_sub_id,job_local_user)
                elif job_ce_status == "DONE-OK":
                    if self.finalize_job(job_id,job_sub_id,ce_job_id):
                        self.db.close_job_submit(job_sub_id,3)
                        self.db.close_job(job_id,2)
                        jobs_success += 1
                    else:
                        self.db.close_job_submit(job_sub_id,5)
                        job_resubmit = True
                elif job_ce_status == "DONE-FAILED":
                    if self.finalize_job(job_id,job_sub_id,ce_job_id):
                        self.db.close_job_submit(job_sub_id,4)
                    else:
                        self.db.close_job_submit(job_sub_id,6)
                    job_resubmit = True
                elif job_ce_status == "CANCELLED":
                    self.db.close_job_submit(job_sub_id,7)
                    # Change to make CANCEL resubmittable for debugging purposes
                    self.db.close_job(job_id,3)
                    jobs_cancel += 1
                    #job_resubmit = True
                elif job_ce_status == "UNDEF":
                    self.db.set_job_submit_status(job_sub_id,8)
                    jobs_undef += 1
                else:
                    print "  WARNING - unrecognized job status %s returned by glite-ce-job-status"%job_ce_status

                if job_resubmit:

                    # If we get here, the job was either DONE-FAILED or DONE-OK but with problems in the
                    # output files: see if we can resubmit it

                    if self.db.get_job_submissions(job_id) >= self.job_submissions_max:
                        # Job was resubmitted too many times, tag it as failed
                        print "  WARNING - job %s failed %d times and will not be resubmitted"%(job_name,self.job_submissions_max)
                        self.db.close_job(job_id,3)
                        jobs_fail += 1
                    else:
                        # Resubmit the job
                        (job_sub_id,ce_job_id) = self.submit_job(job_id,job_dir,prod_ce)
                        print "- %s %s RESUBMITTED"%(job_name,ce_job_id)
                        jobs_submit += 1

        return (jobs_submit,jobs_idle,jobs_active,jobs_success,jobs_fail,jobs_cancel,jobs_undef)
    
    def submit_job(self,job_id,job_dir,prod_ce):
    
        # Save main directory, i.e. top production manager directory
        main_dir = os.getcwd()
    
        # Go to job working directory
        os.chdir(job_dir)
    
        # Create new job submission
        job_sub_id = self.db.create_job_submit(job_id)

        # Submit job and log event to DB
        submit_cmd = "glite-ce-job-submit --autm-delegation --resource %s job.jdl"%prod_ce

        #p = subprocess.Popen(submit_cmd.split(),stdin=None,stdout=subprocess.PIPE)
        #ce_job_id = p.communicate()[0].rstrip()

        #for l in self.run_command(submit_cmd):
        #    if self.debug: print l.rstrip()
        #    if l: ce_job_id = l.rstrip()

        # Handle job submission trapping errors and allowing for multiple retries
        submits = 0
        while True:
            (rc,out,err) = self.execute_command(submit_cmd)
            #if self.debug: print "Submission returned %d"%rc
            if rc == 0:
                ce_job_id = ""
                for l in iter(out.splitlines()):
                    if self.debug > 1: print l
                    if re.match("^https://\S+:\d+/CREAM\S+$",l):
                        ce_job_id = l
                        break
                if ce_job_id:
                    if self.debug: print "CE job id is %s"%ce_job_id
                    break
                else:
                    print "  WARNING Submit successful but no CE job id returned."
            else:
                print "  WARNING Submit returned error code %d"%rc

            # Submission failed: show debug output
            if self.debug:
                print "- STDOUT -"
                print out
                print "- STDERR -"
                print err

            # Abort if too many attemps failed
            submits += 1
            if submits >= self.job_submissions_max:
                print "*** ERROR *** Job submission failed %d times. Aborting"%self.job_submissions_max
                sys.exit(1)

            # Wait a bit before retrying
            time.sleep(10)

        self.db.set_job_submitted(job_sub_id,ce_job_id)
    
        # Go back to main directory before returning
        os.chdir(main_dir)
    
        # Return submitted job identifier
        return (job_sub_id,ce_job_id)
  
    def get_job_ce_status(self,ce_job_id):
    
        # Retrieve status of job
        job_status_cmd = "glite-ce-job-status --level 2 %s"%ce_job_id
        if self.debug: print "> %s"%job_status_cmd
        p = subprocess.Popen(job_status_cmd.split(),stdin=None,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        status_info = p.communicate()[0].split("\n")
        status = "UNDEF"
        worker_node = "UNKNOWN"
        local_user = "UNKNOWN"
        delegation = ""
        #for l in self.run_command(job_status_cmd):
        for l in status_info:
            if self.debug >= 2: print l
            r = re.match("^\s*Current Status\s+=\s+\[(.+)\].*$",l)
            if r: status = r.group(1)
            r = re.match("^\s*Worker Node\s+=\s+\[(.+)\].*$",l)
            if r: worker_node = r.group(1)
            r = re.match("^\s*Local User\s+=\s+\[(.+)\].*$",l)
            if r: local_user = r.group(1)
            r = re.match("^\s*Deleg Proxy ID\s+=\s+\[(.+)\].*$",l)
            if r: delegation = r.group(1)
        return (status,worker_node,local_user,delegation)
  
    def finalize_job(self,job_id,job_sub_id,ce_job_id):
    
        # Save main directory, i.e. top production manager directory
        main_dir = os.getcwd()
    
        # Go to job working directory
        job_dir = self.db.get_job_dir(job_id)
        os.chdir(job_dir)
    
        # Recover output files from job
        print "\tRecovering job output from CE"
        getout_cmd = "glite-ce-job-output -N %s"%ce_job_id
        print "\t> %s"%getout_cmd
        rc = subprocess.call(getout_cmd.split())
        #for l in self.run_command(getout_cmd):
        #    if self.debug: print l

        # Get name of dir where output files are stored from the ce_job_id
        out_dir = ce_job_id[8:].replace(":","_").replace("/","_")

        # Check if job output dir exists
        if not os.path.isdir(out_dir):
            print "WARNING Job output dir %s not found"%out_dir
            return False

        # Rename output dir with submission name
        sub_dir = "submit_%s"%self.db.get_job_submit_index(job_sub_id)
        os.rename(out_dir,sub_dir)

        # Check if all expected output files are there

        output_ok = True

        out_file = "%s/job.out"%sub_dir
        if not os.path.exists(out_file):
            output_ok = False
            print "WARNING File %s not found"%out_file
    
        err_file = "%s/job.err"%sub_dir
        if not os.path.exists(err_file):
            output_ok = False
            print "WARNING File %s not found"%err_file
    
        sh_file  = "%s/job.sh"%sub_dir
        if not os.path.exists(sh_file):
            output_ok = False
            print "WARNING File %s not found"%sh_file

        # There was a problem retrieving output files: return an error
        if not output_ok:
            print "WARNING Problems while retrieving job output files: job will not be purged from CE"
            return False

        # Output was correctly retrieved: job can be purged
        print "\tPurging job from CE"
        purge_job_cmd = "glite-ce-job-purge -N %s"%ce_job_id
        print "\t> %s"%purge_job_cmd
        rc = subprocess.call(purge_job_cmd.split())
        #for l in self.run_command(purge_job_cmd):
        #    if self.debug: print l

        # Scan log file looking for some information

        worker_node = ""
        wn_user = ""
        wn_dir = ""

        time_start = ""
        time_end = ""
        prog_start = ""
        prog_end = ""

        file_list = []

        reco_processed_events = "0"
        reco_tot_cpu_time = ""
        reco_tot_run_time = ""
        reco_tot_evtproc_cpu_time = ""
        reco_tot_evtproc_run_time = ""
        reco_avg_evtproc_cpu_time = ""
        reco_avg_evtproc_run_time = ""

        jof = open(out_file,"r")
        for line in jof:

            r = re.match("^Job running on node (\S*) as user (\S*) in dir (\S*)\s*$",line)
            if r:
                worker_node = r.group(1)
                wn_user = r.group(2)
                wn_dir = r.group(3)

            r = re.match("^Job starting at (.*) \(UTC\)$",line)
            if r: time_start = r.group(1)

            r = re.match("^Job ending at (.*) \(UTC\)$",line)
            if r: time_end = r.group(1)

            r = re.match("^Program starting at (.*) \(UTC\)$",line)
            if r: prog_start = r.group(1)

            r = re.match("^Program ending at (.*) \(UTC\)$",line)
            if r: prog_end = r.group(1)

            # Extract PadmeReco final summary information
            if re.match("^RecoInfo - .*$",line):

                r = re.match("^.*Processed Events\s+(\d+)\s*$",line)
                if r: reco_processed_events = r.group(1)

                r = re.match("^.*Total CPU time\s+(\S+)\s+s*$",line)
                if r: reco_tot_cpu_time = r.group(1)

                r = re.match("^.*Total Run time\s+(\S+)\s+s*$",line)
                if r: reco_tot_run_time = r.group(1)

                r = re.match("^.*Total Event Processing CPU time\s+(\S+)\s+s*$",line)
                if r: reco_tot_evtproc_cpu_time = r.group(1)

                r = re.match("^.*Total Event Processing Run time\s+(\S+)\s+s*$",line)
                if r: reco_tot_evtproc_run_time = r.group(1)

                r = re.match("^.*Average Event Processing CPU time\s+(\S+)\s+s*$",line)
                if r: reco_avg_evtproc_cpu_time = r.group(1)

                r = re.match("^.*Average Event Processing Run time\s+(\S+)\s+s*$",line)
                if r: reco_avg_evtproc_run_time = r.group(1)

            # Extract info about produced output file(s)
            r = re.match("^(.*) file (.*) with size (.*) and adler32 (.*) copied.*$",line)
            if r:
                file_type = r.group(1)
                file_name = r.group(2)
                file_size = int(r.group(3))
                file_adler32 = r.group(4)
                file_list.append((file_type,file_name,file_size,file_adler32))

        jof.close()

        if worker_node:
            print "\tJob run on worker node %s"%worker_node
            self.db.set_job_worker_node(job_id,worker_node)

        if wn_user:
            print "\tJob run as user %s"%wn_user
            self.db.set_job_wn_user(job_id,wn_user)

        if wn_dir:
            print "\tJob run in directory %s"%wn_dir
            self.db.set_job_wn_dir(job_id,wn_dir)

        if time_start:
            print "\tJob started at %s (UTC)"%time_start
            self.db.set_job_time_start(job_id,time_start)

        if time_end:
            print "\tJob ended at %s (UTC)"%time_end
            self.db.set_job_time_end(job_id,time_end)

        if prog_start:
            print "\tProgram started at %s (UTC)"%prog_start
            self.db.set_run_time_start(job_id,prog_start)

        if prog_end:
            print "\tProgram ended at %s (UTC)"%prog_end
            self.db.set_run_time_end(job_id,prog_end)

        if reco_processed_events:
            print "\tJob processed %s events"%reco_processed_events
            self.db.set_job_n_events(job_id,reco_processed_events)

        if file_list:
            self.db.set_job_n_files(job_id,str(len(file_list)))
            for (file_type,file_name,file_size,file_adler32) in file_list:
                print "\t%s file %s with size %s adler32 %s"%(file_type,file_name,file_size,file_adler32)
                self.db.create_job_file(job_id,file_name,file_type,0,0,file_size,file_adler32)

        # Go back to top directory
        os.chdir(main_dir)
    
        # Need to define some error handling procedure
        return True
