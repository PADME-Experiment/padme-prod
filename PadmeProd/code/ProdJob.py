#!/usr/bin/python

import os
import sys
import time
import subprocess
import re
import shlex

#from PadmeMCDB import PadmeMCDB
#from Logger import Logger
#from ProxyHandler import ProxyHandler

class ProdJob:

    def __init__(self,job_id,ce,db,ph,debug):

        # Job identifier within the PadmeMCDB database
        self.job_id = job_id

        # CE to use for this job
        self.ce = ce

        # When caller changes this variable to True, job will be cancelled
        self.prod_quit = False

        # Connection to PadmeMCDB database
        self.db = db

        # Get some job info from DB
        self.job_name = self.db.get_job_name(self.job_id)
        self.job_dir = self.db.get_job_dir(self.job_id)

        # ProxyHandler is needed to store the job delegations
        self.ph = ph

        # Debug level
        self.debug = debug

        # Keep track of how many times this job was resubmitted
        self.resubmissions = 0

        # Number of times a job can be resubmitted before giving up
        self.resubmit_max = 3

        # Number of times job submission must retry before giving up and delay between attempts
        self.job_submission_max = 5
        self.job_submission_delay = 30

        # Number of times glite commands must retry before giving up and delay between attempts
        self.retries_max = 3
        self.retries_delay = 10

        # Initial job status is 0 (Created)
        self.job_status = 0

        # No job submission currently associated with this job
        self.job_sub_id = None
        self.ce_job_id = None

    def execute_command(self,command):

        if self.debug: print "> %s"%command
        p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        (out,err) = p.communicate()

        return (p.returncode,out,err)

    def update(self):
    
        # Job Status
        # 0: Created
        # 1: Active
        # 2: Successful
        # 3: Failed

        # Submitted Job Status
        #   0: UNSUBMITTED
        #   1: REGISTERED
        #   2: PENDING
        #   3: IDLE
        #   4: RUNNING
        #   5: REALLY-RUNNING
        #   6: HELD
        #   7: DONE-OK
        #   8: DONE-FAILED
        #   9: CANCELLED
        #  10: ABORTED
        #  11: UNKNOWN
        #  12: UNDEF
        # 107: DONE-OK, output problem
        # 108: DONE-FAILED, output problem
        # 109: CANCELLED, output problem
        # 207: DONE_OK, rc!=0

        # If status is 0, job was not submitted yet: do it now
        if self.job_status == 0:
            if self.prod_quit:
                print "- %-8s %-60s %s"%(self.job_name,"UNDEF","SUBMIT_CANCELLED")
                self.job_status = 3
                self.db.close_job(self.job_id,self.job_status)
                return "FAILED"
            if self.submit_job():
                print "- %-8s %-60s %s"%(self.job_name,self.ce_job_id,"SUBMITTED")
                self.job_status = 1
                self.db.set_job_status(self.job_id,self.job_status)
                return "ACTIVE"
            else:
                print "- %-8s %-60s %s"%(self.job_name,"UNDEF","SUBMIT_FAILED")
                self.job_status = 3
                self.db.close_job(self.job_id,self.job_status)
                return "FAILED"

        # Get current status of job submission
        (job_sub_status,worker_node,wn_user,description) = self.db.get_job_submit_info(self.job_sub_id)

        # Status 2: Job was successful
        if self.job_status == 2:
            print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"DONE_OK",description)
            return "SUCCESSFUL"

        # Status 3: Job failed. Show how it failed
        if self.job_status == 3:
            if not self.job_sub_id:
                print "- %-8s %-60s %s"%(self.job_name,"UNDEF","SUBMIT_FAILED")
            elif job_sub_status == 8:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"DONE_FAILED",description)
            elif job_sub_status == 9:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"CANCELLED",description)
            elif job_sub_status == 10:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"ABORTED",description)
            elif job_sub_status == 107:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"DONE_OK - Output Problem",description)
            elif job_sub_status == 108:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"DONE-FAILED - Output Problem",description)
            elif job_sub_status == 109:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"CANCELLED - Output Problem",description)
            elif job_sub_status == 207:
                print "- %-8s %-60s %s %s"%(self.job_name,self.ce_job_id,"DONE_OK - RC != 0",description)
            else:
                print "- %-8s %-60s FAILED with status %d (?) %s"%(self.job_name,self.ce_job_id,job_sub_status,description)
            return "FAILED"

        # Status is 1: Job is being processed
        if self.job_status == 1:

            # Get info about running job from CE
            (job_ce_status,job_exit_code,job_worker_node,job_local_user,job_delegation,job_description) = self.get_job_ce_status()
            print "- %-8s %-60s %s %s@%s %s"%(self.job_name,self.ce_job_id,job_ce_status,job_local_user,job_worker_node,job_description)

            # Register job delegation for proxy renewals
            self.ph.delegations.append(job_delegation)

            # Check current job status and update DB if it changed
            job_resubmit = False
            if job_ce_status == "REGISTERED" or job_ce_status == "PENDING" or job_ce_status == "IDLE" or job_ce_status == "RUNNING" or job_ce_status == "REALLY-RUNNING" or job_ce_status == "UNKNOWN":
                if job_ce_status == "REGISTERED" and job_sub_status != 1: self.db.set_job_submit_status(self.job_sub_id,1)
                if job_ce_status == "PENDING" and job_sub_status != 2: self.db.set_job_submit_status(self.job_sub_id,2)
                if job_ce_status == "IDLE" and job_sub_status != 3: self.db.set_job_submit_status(self.job_sub_id,3)
                if job_ce_status == "RUNNING" and job_sub_status != 4:
                    self.db.set_job_submit_status(self.job_sub_id,4)
                    self.db.set_job_worker_node(self.job_sub_id,job_worker_node)
                    self.db.set_job_wn_user(self.job_sub_id,job_local_user)
                if job_ce_status == "REALLY-RUNNING" and job_sub_status != 5:
                    self.db.set_job_submit_status(self.job_sub_id,5)
                    self.db.set_job_worker_node(self.job_sub_id,job_worker_node)
                    self.db.set_job_wn_user(self.job_sub_id,job_local_user)
                if job_ce_status == "HELD" and job_sub_status != 6: self.db.set_job_submit_status(self.job_sub_id,6)
                if job_ce_status == "UNKNOWN" and job_sub_status != 11: self.db.set_job_submit_status(self.job_sub_id,11)
                if self.prod_quit: self.cancel_job()
                return "ACTIVE"
            elif job_ce_status == "DONE-OK":
                if self.finalize_job():
                    if job_exit_code == "0":
                        self.db.close_job_submit(self.job_sub_id,7,job_description)
                        self.job_status = 2
                        self.db.close_job(self.job_id,self.job_status)
                        return "SUCCESSFUL"
                    else:
                        print "  WARNING job id DONE_OK but RC is %s"%job_exit_code
                        self.db.close_job_submit(self.job_sub_id,207,job_description)
                else:
                    self.db.close_job_submit(self.job_sub_id,107,job_description)
                job_resubmit = True
            elif job_ce_status == "DONE-FAILED":
                if self.finalize_job():
                    self.db.close_job_submit(self.job_sub_id,8,job_description)
                else:
                    self.db.close_job_submit(self.job_sub_id,108,job_description)
                job_resubmit = True
            elif job_ce_status == "CANCELLED":
                if self.finalize_job():
                    self.db.close_job_submit(self.job_sub_id,9,job_description)
                else:
                    self.db.close_job_submit(self.job_sub_id,109,job_description)
                job_resubmit = True
            elif job_ce_status == "ABORTED":
                self.db.close_job_submit(self.job_sub_id,10,job_description)
                job_resubmit = True
            elif job_ce_status == "UNKNOWN":
                if job_sub_status != 11: self.db.set_job_submit_status(self.job_sub_id,11)
                if self.prod_quit: self.cancel_job()
                return "UNDEF"
            else:
                if job_sub_status != 12:
                    print "  WARNING unrecognized job status %s returned by glite-ce-job-status"%job_ce_status
                    self.db.set_job_submit_status(self.job_sub_id,12)
                if self.prod_quit: self.cancel_job()
                return "UNDEF"

            if job_resubmit:

                # If we get here, the job finished with some problem: see if we can resubmit it

                self.resubmissions += 1
                if self.prod_quit or (self.resubmissions >= self.resubmit_max):
                    # Production was cancelled or job was resubmitted too many times: tag job as failed
                    if self.prod_quit:
                        print "  WARNING - production in quit mode: job %s will not be resubmitted"%self.job_name
                    if self.resubmissions >= self.resubmit_max:
                        print "  WARNING - job %s failed %d times and will not be resubmitted"%(self.job_name,self.resubmissions)
                    self.job_status = 3
                    self.db.close_job(self.job_id,self.job_status)
                    return "FAILED"
                elif self.submit_job():
                    print "- %s %s RESUBMITTED"%(self.job_name,self.ce_job_id)
                    return "ACTIVE"
                else:
                    print " WARNING - unable to resubmit job %s: tagging it as failed"%self.job_name
                    self.job_status = 3
                    self.db.close_job(self.job_id,self.job_status)
                    return "FAILED"
    
    def submit_job(self):
    
        # Save main directory, i.e. top production manager directory
        main_dir = os.getcwd()
    
        # Go to job working directory
        os.chdir(self.job_dir)
    
        # Create new job submission in DB
        self.job_sub_id = self.db.create_job_submit(self.job_id,self.resubmissions)

        # Submit job and log event to DB
        submit_cmd = "glite-ce-job-submit --autm-delegation --resource %s job.jdl"%self.ce

        # Handle job submission trapping errors and allowing for multiple retries
        submits = 0
        while True:
            (rc,out,err) = self.execute_command(submit_cmd)
            if rc == 0:
                self.ce_job_id = ""
                for l in iter(out.splitlines()):
                    if self.debug > 1: print l
                    if re.match("^https://\S+:\d+/CREAM\S+$",l):
                        self.ce_job_id = l
                        break
                if self.ce_job_id:
                    if self.debug: print "CE job id is %s"%self.ce_job_id
                    break
                print "  WARNING Submit successful but no CE job id returned."
            else:
                print "  WARNING Submit returned error code %d"%rc

            # Submission failed: show debug output
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err

            # Abort job if too many attemps failed
            submits += 1
            if submits >= self.job_submission_max:
                print "*** ERROR *** Job submission failed %d times."%submits
                os.chdir(main_dir)
                return False

            # Wait a bit before retrying
            time.sleep(self.job_submission_delay)

        self.db.set_job_submitted(self.job_sub_id,self.ce_job_id)
    
        # Go back to main directory before returning
        os.chdir(main_dir)
    
        # Return submitted job identifier
        return True
  
    def get_job_ce_status(self):
    
        status      = "UNDEF"
        exit_code   = ""
        worker_node = "UNKNOWN"
        local_user  = "UNKNOWN"
        delegation  = ""
        description = ""

        # Retrieve status of job
        job_status_cmd = "glite-ce-job-status --level 2 %s"%self.ce_job_id

        # Handle job status info collection. Trap errors and allow for multiple retries
        retries = 0
        while True:

            (rc,out,err) = self.execute_command(job_status_cmd)
            if rc == 0:
                for l in iter(out.splitlines()):
                    if self.debug >= 2: print l
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
                    r = re.match("^\s*Description\s*=\s*\[(.*)\].*",l)
                    if r: description = r.group(1)
                break

            print "  WARNING glite-ce-job-status returned error code %d"%rc
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err

            # Abort if too many attempts failed
            retries += 1
            if retries >= self.retries_max:
                print "  WARNING unable to retrieve job status info. Retried %d times"%retries
                break

            # Wait a bit before retrying
            time.sleep(self.retries_delay)

        return (status,exit_code,worker_node,local_user,delegation,description)
  
    def finalize_job(self):
    
        # Save main directory, i.e. top production manager directory
        main_dir = os.getcwd()
    
        # Go to job working directory (do not forget to go back to main_dir before returning!)
        os.chdir(self.job_dir)
    
        # Recover output files from job
        if self.debug: print "  Recovering job output from CE"
        getout_cmd = "glite-ce-job-output --noint %s"%self.ce_job_id
   
        # Handle output files retrieval. Trap errors and allow for multiple retries
        retries = 0
        while True:

            (rc,out,err) = self.execute_command(getout_cmd)
            if rc == 0: break

            print "  WARNING glite-ce-job-output returned error code %d"%rc
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err

            # Abort if too many attempts failed
            retries += 1
            if retries >= self.retries_max:
                print "  WARNING unable to retrieve output files. Retried %d times"%retries
                os.chdir(main_dir)
                return False

            # Wait a bit before retrying
            time.sleep(self.retries_delay)

        # Get name of dir where output files are stored from the ce_job_id
        out_dir = self.ce_job_id[8:].replace(":","_").replace("/","_")

        # Check if job output dir exists
        if not os.path.isdir(out_dir):
            print "  WARNING Job output dir %s not found"%out_dir
            os.chdir(main_dir)
            return False

        # Rename output dir with submission name
        sub_dir = "submit_%s"%self.db.get_job_submit_index(self.job_sub_id)
        os.rename(out_dir,sub_dir)

        # Check if all expected output files are there

        output_ok = True

        out_file = "%s/job.out"%sub_dir
        if not os.path.exists(out_file):
            output_ok = False
            print "  WARNING File %s not found"%out_file
    
        err_file = "%s/job.err"%sub_dir
        if not os.path.exists(err_file):
            output_ok = False
            print "  WARNING File %s not found"%err_file
    
        sh_file  = "%s/job.sh"%sub_dir
        if not os.path.exists(sh_file):
            output_ok = False
            print "  WARNING File %s not found"%sh_file

        # There was a problem retrieving output files: return an error
        if not output_ok:
            print "  WARNING Problems while retrieving job output files: job will not be purged from CE"
            os.chdir(main_dir)
            return False

        # Output was correctly retrieved: job can be purged
        if self.debug: print "  Purging job from CE"
        purge_job_cmd = "glite-ce-job-purge -N %s"%self.ce_job_id
        (rc,out,err) = self.execute_command(purge_job_cmd)
        if rc:
            print "  WARNING glite-ce-job-purge returned error code %d"%rc
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err

        # Scan log file looking for some information

        worker_node = ""
        wn_user = ""
        wn_dir = ""

        time_start = ""
        time_end = ""
        prog_start = ""
        prog_end = ""

        file_list = []

        reco_processed_events = ""
        reco_tot_cpu_time = ""
        reco_tot_run_time = ""
        reco_tot_evtproc_cpu_time = ""
        reco_tot_evtproc_run_time = ""
        reco_avg_evtproc_cpu_time = ""
        reco_avg_evtproc_run_time = ""

        mc_processed_events = ""

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

            # Extract PadmeMC final summary information
            if re.match("^PadmeMCInfo - .*$",line):

                r = re.match("^.*Total Events\s+(\d+)\s*$",line)
                if r: mc_processed_events = r.group(1)

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
            print "  Job run on worker node %s"%worker_node
            self.db.set_job_worker_node(self.job_sub_id,worker_node)

        if wn_user:
            print "  Job run as user %s"%wn_user
            self.db.set_job_wn_user(self.job_sub_id,wn_user)

        if wn_dir:
            print "  Job run in directory %s"%wn_dir
            self.db.set_job_wn_dir(self.job_sub_id,wn_dir)

        if time_start:
            print "  Job started at %s (UTC)"%time_start
            self.db.set_job_time_start(self.job_sub_id,time_start)

        if time_end:
            print "  Job ended at %s (UTC)"%time_end
            self.db.set_job_time_end(self.job_sub_id,time_end)

        if prog_start:
            print "  Program started at %s (UTC)"%prog_start
            self.db.set_run_time_start(self.job_sub_id,prog_start)

        if prog_end:
            print "  Program ended at %s (UTC)"%prog_end
            self.db.set_run_time_end(self.job_sub_id,prog_end)

        if reco_processed_events:
            print "  Job processed %s events"%reco_processed_events
            self.db.set_job_n_events(self.job_id,reco_processed_events)

        if mc_processed_events:
            print "  Job produced %s events"%mc_processed_events
            self.db.set_job_n_events(self.job_id,mc_processed_events)

        if file_list:
            self.db.set_job_n_files(self.job_id,str(len(file_list)))
            for (file_type,file_name,file_size,file_adler32) in file_list:
                print "\t%s file %s with size %s adler32 %s"%(file_type,file_name,file_size,file_adler32)
                self.db.create_job_file(self.job_id,file_name,file_type,0,0,file_size,file_adler32)

        # Go back to top directory
        os.chdir(main_dir)
    
        # Need to define some error handling procedure
        return True

    def cancel_job(self):

        cmd = "glite-ce-job-cancel --noint %s"%self.ce_job_id
        (rc,out,err) = self.execute_command(cmd)
        if rc != 0:
            print "  WARNING Job %s cancel command returned error code %d"%(self.ce_job_id,rc)
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err
