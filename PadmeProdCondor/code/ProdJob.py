#!/usr/bin/python

import os
import sys
import time
import subprocess
import re
import shlex

class ProdJob:

    def __init__(self,job_id,ce,db,debug):

        # Job identifier within the PadmeMCDB database
        self.job_id = job_id

        # CE to use for this job
        self.ce = ce
        (self.ce_host,self.ce_port) = ce.split(":")

        # When caller changes this variable to True, job will be cancelled
        self.job_quit = False

        # Connection to PadmeMCDB database
        self.db = db

        # Get some job info from DB
        self.job_name = self.db.get_job_name(self.job_id)
        self.job_dir = self.db.get_job_dir(self.job_id)

        # Debug level
        self.debug = debug

        # Keep track of how many times this job was resubmitted
        self.resubmissions = 0

        # Number of times a job can be resubmitted before giving up
        # This number is big as temporary instabilities on the CE can ABORT most jobs for
        # long periods of time (few hours)
        self.resubmit_max = 1000

        # Number of times job submission must retry before giving up and delay between attempts
        self.job_submission_max = 5
        self.job_submission_delay = 30

        # Number of times commands must retry before giving up and delay between attempts
        self.retries_max = 3
        self.retries_delay = 10

        # Initial job status is 0 (Created)
        self.job_status = 0

        # No job submission currently associated with this job
        self.job_sub_id = None
        self.ce_job_id = None

        # Define all known statuses for a submitted job
        self.job_sub_status_code = {
              0: "UNSUBMITTED",
              1: "REGISTERED",
              2: "PENDING",
              3: "IDLE",
              4: "RUNNING",
              5: "REALLY-RUNNING",
              6: "HELD",
              7: "DONE-OK",
              8: "DONE-FAILED",
              9: "CANCELLED",
             10: "ABORTED",
             11: "UNKNOWN",
             12: "UNDEF",
             13: "REMOVING",
             14: "TRANSFERRING",
             15: "SUSPENDED",
            100: "SUBMIT-FAILED",
            107: "DONE-OK, output problem",
            108: "DONE-FAILED, output problem",
            109: "CANCELLED, output problem",
            207: "DONE_OK, RC!=0"
        }

        # Define Condor job status map
        self.job_condor_status_code = {
            "1": "IDLE",
            "2": "RUNNING",
            "3": "REMOVING",
            "4": "COMPLETED",
            "5": "HELD",
            "6": "TRANSFERRING OUTPUT",
            "7": "SUSPENDED"
        }

        # Define quit file: if found, job will cleanly quit
        self.quit_file = "%s/quit"%self.job_dir

        if self.debug:
            print "--- Job %s initialized ---"%self.job_name

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
    
        # Check quit control file and quit job if found.
        if os.path.exists(self.quit_file):
            print "*** Quit file %s found: quitting job ***"%self.quit_file
            self.job_quit = True

        # If status is 0, job must be submitted
        if self.job_status == 0:

            if self.job_quit or (self.resubmissions >= self.resubmit_max):
                if self.resubmissions >= self.resubmit_max:
                    print "*** Resubmission %d exceeds max allowed %d: job tagged as FAILED ***"%(self.resubmissions,self.resubmit_max)
                print "- %-8s %-60s %s"%(self.job_name,"UNDEF","SUBMIT_CANCELLED")
                self.job_status = 3
                self.db.close_job(self.job_id,self.job_status)
                return "FAILED"
            elif self.submit_job():
                print "- %-8s %-60s %s"%(self.job_name,self.full_ce_job_id,"SUBMITTED")
                self.job_status = 1
                self.db.set_job_status(self.job_id,self.job_status)
                return "ACTIVE"
            else:
                # If submission failed, leave job in CREATED mode and try again next time
                self.db.set_job_submit_status(self.job_sub_id,100)
                print "- %-8s %-60s %s"%(self.job_name,"UNDEF","SUBMIT_FAILED")
                return "CREATED"

        # Get previous status of job submission from DB
        (job_sub_status,worker_node,wn_user,description) = self.db.get_job_submit_info(self.job_sub_id)
        if description == None: description = ""
        location = "%s@%s"%(wn_user,worker_node)

        # Status 2: Job was successful
        if self.job_status == 2:
            print "- %-8s %-60s %s %s %s"%(self.job_name,self.full_ce_job_id,"DONE_OK",location,description)
            return "SUCCESSFUL"

        # Status 3: Job failed. Show how it failed
        if self.job_status == 3:
            if not self.job_sub_id:
                print "- %-8s %-60s %s"%(self.job_name,"UNDEFINED","SUBMIT_FAILED")
            elif job_sub_status in self.job_sub_status_code.keys():
                print "- %-8s %-60s %s %s %s"%(self.job_name,self.full_ce_job_id,self.job_sub_status_code[job_sub_status],location,description)
            else:
                print "- %-8s %-60s %s %s %s"%(self.job_name,self.full_ce_job_id,"FAILED with status %d (?)"%job_sub_status,location,description)
            return "FAILED"

        # Status is 1: Job is being processed
        if self.job_status == 1:

            # Get current status of job submission from CE
            #(job_ce_status,job_exit_code,job_worker_node,job_local_user,job_delegation,job_description) = self.get_job_ce_status()
            job_ce_status   = "UNDEF"
            job_exit_code   = ""
            job_worker_node = "UNKNOWN"
            job_local_user  = "UNKNOWN"
            job_description = ""
            job_info = self.get_job_ce_status()
            if "status"      in job_info: job_ce_status   = job_info["status"]
            if "exit_code"   in job_info: job_exit_code   = job_info["exit_code"]
            if "worker_node" in job_info: job_worker_node = job_info["worker_node"]
            if "local_user"  in job_info: job_local_user  = job_info["local_user"]
            if "description" in job_info: job_description = job_info["description"]
            job_location = "%s@%s"%(job_local_user,job_worker_node)
            print "- %-8s %-60s %s %s %s"%(self.job_name,self.full_ce_job_id,job_ce_status,job_location,job_description)

            # Check current job status and update DB if it changed
            if job_ce_status == "UNDEF":

                if job_sub_status != 12:
                    self.db.set_job_submit_status(self.job_sub_id,12)
                if self.job_quit:
                    # Retrieve output but do not parse it
                    self.finalize_job()
                    self.cancel_job()
                return "UNDEF"

            elif job_ce_status == "CANCELLED":

                self.db.close_job_submit(self.job_sub_id,9,job_description,job_exit_code)

            elif job_ce_status == "COMPLETED":

                # Retrieve output files
                (finalize_ok,file_list) = self.finalize_job()

                if job_exit_code != "0":

                    print "  WARNING job is Completed but with RC %s"%job_exit_code
                    self.db.close_job_submit(self.job_sub_id,207,job_description,job_exit_code)

                elif not finalize_ok:

                    print "  WARNING job is Completed and RC is 0 but output retrieval failed"
                    self.db.close_job_submit(self.job_sub_id,107,job_description,job_exit_code)

                else:

                    parse_ok = True

                    if not ( ("out" in file_list) and self.parse_out_file(file_list["out"]) ):
                        print "  WARNING problems while parsing output file %s"%file_list["out"]
                        parse_ok = False

                    if not ( ("err" in file_list) and self.parse_err_file(file_list["err"]) ):
                        print "  WARNING problems while parsing error file %s"%file_list["err"]
                        parse_ok = False

                    if parse_ok:
                        self.db.close_job_submit(self.job_sub_id,7,job_description,job_exit_code)
                        self.job_status = 2
                        self.db.close_job(self.job_id,self.job_status)
                        return "SUCCESSFUL"
                    else:
                        print "  WARNING job is Completed, RC is 0, output retrieval succeeded but parsing failed"
                        self.db.close_job_submit(self.job_sub_id,107,job_description,job_exit_code)

            else:

                if   job_ce_status == "IDLE" and job_sub_status != 3:
                    self.db.set_job_submit_status(self.job_sub_id,3)
                elif job_ce_status == "RUNNING" and job_sub_status != 4:
                    self.db.set_job_submit_status(self.job_sub_id,4)
                    self.db.set_job_worker_node(self.job_sub_id,job_worker_node)
                    self.db.set_job_wn_user(self.job_sub_id,job_local_user)
                elif job_ce_status == "HELD" and job_sub_status != 6:
                    self.db.set_job_submit_status(self.job_sub_id,6)
                elif job_ce_status == "REMOVING" and job_sub_status != 13:
                    self.db.set_job_submit_status(self.job_sub_id,13)
                elif job_ce_status == "TRANSFERRING OUTPUT" and job_sub_status != 14:
                    self.db.set_job_submit_status(self.job_sub_id,14)
                elif job_ce_status == "SUSPENDED" and job_sub_status != 15:
                    self.db.set_job_submit_status(self.job_sub_id,15)

                if self.job_quit:
                    # Retrieve output but do not parse it
                    self.finalize_job()
                    self.cancel_job()

                return "ACTIVE"

            # If we are quitting, tag job as FAILED
            if self.job_quit:
                self.job_status = 3
                self.db.set_job_status(self.job_id,self.job_status)
                return "FAILED"
        
            # Otherwise tag job as CREATED (i.e. resubmittable)
            self.job_status = 0
            self.db.set_job_status(self.job_id,self.job_status)
            return "CREATED"
    
    def submit_job(self):
    
        # Save main directory, i.e. top production manager directory
        main_dir = os.getcwd()
    
        # Go to job working directory
        os.chdir(self.job_dir)
    
        # Create new job submission in DB and count it
        self.job_sub_id = self.db.create_job_submit(self.job_id,self.resubmissions)
        self.resubmissions += 1

        # Command to submit job
        submit_cmd = "condor_submit -pool %s -remote %s -spool job.sub"%(self.ce,self.ce_host)

        # Handle job submission trapping errors and allowing for multiple retries
        submits = 0
        while True:
            (rc,out,err) = self.execute_command(submit_cmd)
            if rc == 0:
                self.ce_job_id = ""
                for l in iter(out.splitlines()):
                    if self.debug > 1: print l
                    r = re.match("^.* submitted to cluster (\d+)\.\s*$",l)
                    if r:
                        self.ce_job_id = r.group(1)
                        break
                if self.ce_job_id:
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

        # Save submission info to DB
        self.full_ce_job_id = "%s/%s"%(self.ce,self.ce_job_id)
        if self.debug: print "CE job id is %s"%self.full_ce_job_id
        self.db.set_job_submitted(self.job_sub_id,self.full_ce_job_id)
    
        # Go back to main directory before returning
        os.chdir(main_dir)
    
        return True
  
    def get_job_ce_status(self):
    
        job_info = {}

        # Retrieve status of job
        job_status_cmd = "condor_q -long -pool %s -name %s %s"%(self.ce,self.ce_host,self.ce_job_id)

        # Handle job status info collection. Trap errors and allow for multiple retries
        retries = 0
        while True:

            (rc,out,err) = self.execute_command(job_status_cmd)
            if rc == 0:
                # If condor_q succeeds but output is empty, the job was cancelled with condor_rm
                if out == "":
                    job_info["status"] = "CANCELLED"
                else:
                    for l in iter(out.splitlines()):
                        if self.debug >1: print l
                        r = re.match("^\s*JobStatus\s+=\s+(\d+)\s*$",l)
                        if r:
                            if r.group(1) in self.job_condor_status_code:
                                job_info["status"] = self.job_condor_status_code[r.group(1)]
                            else:
                                print "  WARNING condor_q returned unknown job status '%s'"%r.group(1)
                                job_info["status"] = "UNDEF"
                        r = re.match("^\s*ExitCode\s+=\s+(\d+)\s*$",l)
                        if r: job_info["exit_code"] = r.group(1)
                        r = re.match("^\s*Owner\s+=\s+\"(\S+)\"\s*$",l)
                        if r: job_info["local_user"] = r.group(1)
                break

            print "  WARNING condor_q command returned error code %d"%rc
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

        return job_info
  
    def finalize_job(self):
    
        # Save main directory, i.e. top production manager directory
        main_dir = os.getcwd()
    
        # Go to job working directory (do not forget to go back to main_dir before returning!)
        os.chdir(self.job_dir)

        # Save final job status
        job_status_cmd = "condor_q -long -pool %s -name %s %s"%(self.ce,self.ce_host,self.ce_job_id)
        (rc,out,err) = self.execute_command(job_status_cmd)
        if rc == 0:
            with open("job.status","w") as jf: jf.write(out)
        else:
            print "  WARNING final condor_q command returned error code %d"%rc
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err

        # Handle output files retrieval. Trap errors and allow for multiple retries
        retries = 0
        while not self.retrieve_job_output():
            # Abort if too many attempts failed
            retries += 1
            if retries >= self.retries_max:
                print "  WARNING unable to retrieve output files. Retried %d times"%retries
                break
            # Wait a bit before retrying
            time.sleep(self.retries_delay)

        # Create directory to hold submission results
        sub_dir = "submit_%03d"%self.db.get_job_submit_index(self.job_sub_id)
        try:
            os.mkdir(sub_dir)
        except:
            print "  WARNING Unable to create directory %s"%sub_dir
            os.chdir(main_dir)
            return (False,{})

        output_ok = True

        # Move all final files to submission directory

        file_list = {
            "out"    : "job.out",
            "err"    : "job.err",
            "log"    : "job.log",
            "sh"     : "job.sh",
            "status" : "job.status",
        }
        for k in file_list:
            f = file_list[k]
            if os.path.exists(f):
                os.rename(f,"%s/%s"%(sub_dir,f))
                file_list[k] = "%s/%s/%s"%(self.job_dir,sub_dir,f)
            else:
                output_ok = False
                print "  WARNING File %s not found"%f
                file_list[k] = ""

        # Go back to top directory
        os.chdir(main_dir)

        return (output_ok,file_list)

    def retrieve_job_output(self):

        if self.debug: print "  Retrieveing output for job %s from CE %s"%(self.ce_job_id,self.ce)
        output_job_cmd = "condor_transfer_data -pool %s -name %s %s"%(self.ce,self.ce_host,self.ce_job_id)
        (rc,out,err) = self.execute_command(output_job_cmd)
        if rc:
            print "  WARNING Retrieve output command for job %s returned error code %d"%(self.ce_job_id,rc)
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err
            return False
        return True

    def cancel_job(self):

        if self.debug: print "  Cancelling job %s from CE %s"%(self.ce_job_id,self.ce)
        cancel_job_cmd = "condor_rm -pool %s -name %s %s"%(self.ce,self.ce_host,self.ce_job_id)
        (rc,out,err) = self.execute_command(cancel_job_cmd)
        if rc != 0:
            print "  WARNING Job %s cancel command returned error code %d"%(self.ce_job_id,rc)
            if self.debug:
                print "- STDOUT -\n%s"%out
                print "- STDERR -\n%s"%err
            return False
        return True

    def parse_out_file(self,out_file):

        # Parse out file and write information to DB

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

        return True

    def parse_err_file(self,err_file):

        n_errors = 0

        jef = open(err_file,"r")
        for line in jef:

            # Trap errors of final gfal-copy
            r = re.match("^gfal-copy error:\s+(\d+)\s+\(.*\)\s+-\s+(.*)$",line)
            if r:
                (err_nr,err_type,err_msg) = r.group()
                print "  ERROR from gfal-copy command"
                print "\tError number : %s"%err_nr
                print "\tError type   : %s"%err_type
                print "\tError message: %s"%err_msg
                n_errors += 1

        jef.close()

        if n_errors: return False

        return True
