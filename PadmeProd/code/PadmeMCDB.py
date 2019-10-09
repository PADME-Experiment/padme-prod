#!/usr/bin/python

import MySQLdb
import os
import sys
import time

class PadmeMCDB:

    def __init__(self):

        # Get DB connection parameters from environment variables
        self.DB_HOST   = os.getenv('PADME_MCDB_HOST'  ,'percona.lnf.infn.it')
        self.DB_PORT   = int(os.getenv('PADME_MCDB_PORT'  ,'3306'))
        self.DB_USER   = os.getenv('PADME_MCDB_USER'  ,'padmeMCDB')
        self.DB_PASSWD = os.getenv('PADME_MCDB_PASSWD','unknown')
        self.DB_NAME   = os.getenv('PADME_MCDB_NAME'  ,'PadmeMCDB')

        self.conn = None

    def __del__(self):

        self.close_db()

    def connect_db(self):

        self.close_db()

        try:
            self.conn = MySQLdb.connect(host   = self.DB_HOST,
                                        port   = self.DB_PORT,
                                        user   = self.DB_USER,
                                        passwd = self.DB_PASSWD,
                                        db     = self.DB_NAME)
        except:
            print "*** PadmeMCDB ERROR *** Unable to connect to DB. Exception: %s"%sys.exc_info()[0]
            sys.exit(2)

    def close_db(self):

        if (self.conn):
            self.conn.close()
            self.conn = None

    def check_db(self):

        if self.conn:
            try:
                self.conn.ping()
            except:
                self.connect_db()
        else:
            self.connect_db()

    def is_prod_in_db(self,prod_name):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT COUNT(id) FROM production WHERE name=%s""",(prod_name,))
        (n,) = c.fetchone()
        self.conn.commit()
        if n: return 1
        return 0

    def create_recoprod(self,name,run,description,prod_ce,reco_version,prod_dir,storage_uri,storage_dir,proxy_file,n_jobs):

        prod_id = self.create_prod(name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,n_jobs)

        self.check_db()
        c = self.conn.cursor()
        c.execute("""INSERT INTO reco_prod (production_id,description,run,reco_version) VALUES (%s,%s,%s,%s)""",(prod_id,description,run,reco_version))
        self.conn.commit()

        return prod_id

    def create_mcprod(self,name,description,user_req,n_events_req,prod_ce,mc_version,prod_dir,storage_uri,storage_dir,proxy_file,time_create,n_jobs):

        prod_id = self.create_prod(name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,n_jobs)

        self.check_db()
        c = self.conn.cursor()
        c.execute("""INSERT INTO mc_prod (production_id,description,user_req,n_events_req,mc_version) VALUES (%s,%s,%s,%s,%s)""",(prod_id,description,user_req,n_events_req,mc_version))
        self.conn.commit()

        return prod_id

    def create_prod(self,name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,n_jobs):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""INSERT INTO production (name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,time_create,n_jobs) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,self.__now__(),n_jobs))
        prod_id = c.lastrowid
        self.conn.commit()
        return prod_id

    def close_prod(self,prod_id,n_jobs_ok,n_events):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE production SET time_complete = %s, n_jobs_ok = %s, n_events = %s WHERE id = %s""",(self.__now__(),n_jobs_ok,n_events,prod_id))
        self.conn.commit()

    def get_prod_total_events(self,prod_id):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT n_events FROM job WHERE production_id = %s""",(prod_id,))
        res = c.fetchall()
        self.conn.commit()

        total_events = 0
        for r in res:
            try:
                n_events = int(r[0])
            except:
                n_events = 0
            total_events += n_events

        return total_events

    def get_prod_id(self,name):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT id FROM production WHERE name=%s""",(name,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return -1
        (id,) = res
        return id

    def get_prod_info(self,pid):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT name,prod_ce,prod_dir,proxy_file,n_jobs FROM production WHERE id=%s""",(pid,))
        res = c.fetchone()
        self.conn.commit()
        return res

    def get_job_list(self,prod_id):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT id FROM job WHERE production_id=%s""",(prod_id,))
        res = c.fetchall()
        self.conn.commit()

        job_list = []
        for j in res: job_list.append(j[0])
        return job_list    

    def create_job(self,prod_id,name,job_dir,configuration,input_list):

        # Random job configuration is not handled yet
        random = ""

        # Jobs are created in idle status
        status = 0

        self.check_db()
        c = self.conn.cursor()
        c.execute("""INSERT INTO job (production_id,name,job_dir,configuration,input_list,random,status,time_create) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(prod_id,name,job_dir,configuration,input_list,random,status,self.__now__()))
        self.conn.commit()

    def close_job(self,job_id,status):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job SET status = %s, time_complete = %s WHERE id = %s""",(status,self.__now__(),job_id))
        self.conn.commit()

    def get_job_id(self,prod_id,name):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT id FROM job WHERE production_id=%s AND name=%s""",(prod_id,name,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return -1
        (id,) = res
        return id

    def get_job_dir(self,job_id):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT job_dir FROM job WHERE id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return ""
        (job_dir,) = res
        return job_dir

    def get_job_info(self,job_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT name,job_dir,status FROM job WHERE id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return (None,None,None)
        return res

    def get_job_submissions(self,job_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT COUNT(*) FROM job_submit WHERE job_id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return 0
        (n_subs,) = res
        return n_subs

    def create_job_submit(self,job_id):

        # Job submissions are created in idle status
        status = 0

        # Assume that this is he first submission
        index = 0

        # Check if this is a resubmission
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT MAX(submit_index) FROM job_submit WHERE job_id = %s""",(job_id,))
        res = c.fetchone()
        if res != None and res[0] != None:
            (idx_max,) = res
            index = idx_max+1

        # Create new job submission
        c.execute("""INSERT INTO job_submit (job_id,submit_index,status,time_submit) VALUES (%s,%s,%s,%s)""",(job_id,index,status,self.__now__()))
        job_sub_id = c.lastrowid
        self.conn.commit()

        # Return job submission id
        return job_sub_id

    def close_job_submit(self,job_sub_id,status,description=''):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET status = %s, time_complete = %s WHERE id = %s""",(status,self.__now__(),job_sub_id))
        if description:
            c.execute("""UPDATE job_submit SET description = %s WHERE id = %s""",(description,job_sub_id))
        self.conn.commit()

    def get_job_submit_id(self,job_id):
    
        self.check_db()
        c = self.conn.cursor()

        # Find job submission with highest index

        max_index = 0
        c.execute("""SELECT MAX(submit_index) FROM job_submit WHERE job_id = %s""",(job_id,))
        res = c.fetchone()
        if res == None:
            self.conn.commit()
            return None
        (max_index,) = res

        c.execute("""SELECT id FROM job_submit WHERE job_id = %s AND submit_index = %s""",(job_id,max_index))
        res = c.fetchone()
        self.conn.commit()
        (job_sub_id,) = res

        return job_sub_id

    def get_job_submit_info(self,job_sub_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT submit_index,status,ce_job_id,worker_node,wn_user FROM job_submit WHERE id=%s""",(job_sub_id,))
        res = c.fetchone()
        self.conn.commit()
        return res

    def get_job_submit_index(self,job_sub_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT submit_index FROM job_submit WHERE id=%s""",(job_sub_id,))
        res = c.fetchone()
        self.conn.commit()
        (index,) = res
        return index

    def create_job_file(self,job_id,file_name,file_type,seq_n,n_events,size,adler32):

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""INSERT INTO file (job_id,name,type,seq_index,n_events,size,adler32) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(job_id,file_name,file_type,seq_n,n_events,size,adler32))
        except:
            print "MySQL command",c._last_executed
            sys.exit(2)
        self.conn.commit()

    def set_job_submitted(self,job_sub_id,ce_job_id):
  
        # Job status changes to 1 after submission
        status = 1

        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET status = %s, ce_job_id = %s, time_submit = %s WHERE id = %s""",(status,ce_job_id,self.__now__(),job_sub_id))
        self.conn.commit()

    def set_job_status(self,job_id,status):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job SET status = %s WHERE id = %s""",(status,job_id))
        self.conn.commit()

    def set_job_submit_status(self,job_sub_id,status):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET status = %s WHERE id = %s""",(status,job_sub_id))
        self.conn.commit()

    def set_job_time_complete(self,job_id,time_complete):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job SET time_complete = %s WHERE id = %s""",(time_complete,job_id))
        self.conn.commit()

    def set_job_time_start(self,job_sub_id,time_start):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET time_job_start = %s WHERE id = %s""",(time_start,job_sub_id))
        self.conn.commit()

    def set_job_time_end(self,job_sub_id,time_end):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET time_job_end = %s WHERE id = %s""",(time_end,job_sub_id))
        self.conn.commit()

    def set_run_time_start(self,job_sub_id,time_start):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET time_run_start = %s WHERE id = %s""",(time_start,job_sub_id))
        self.conn.commit()

    def set_run_time_end(self,job_sub_id,time_end):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET time_run_end = %s WHERE id = %s""",(time_end,job_sub_id))
        self.conn.commit()

    def set_job_worker_node(self,job_sub_id,worker_node):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET worker_node = %s WHERE id = %s""",(worker_node,job_sub_id))
        self.conn.commit()

    def set_job_wn_user(self,job_sub_id,wn_user):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET wn_user = %s WHERE id = %s""",(wn_user,job_sub_id))
        self.conn.commit()

    def set_job_wn_dir(self,job_sub_id,wn_dir):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job_submit SET wn_dir = %s WHERE id = %s""",(wn_dir,job_sub_id))
        self.conn.commit()

    def set_job_n_files(self,job_id,n_files):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job SET n_files = %s WHERE id = %s""",(n_files,job_id))
        self.conn.commit()

    def set_job_n_events(self,job_id,n_events):
        self.check_db()
        c = self.conn.cursor()
        c.execute("""UPDATE job SET n_events = %s WHERE id = %s""",(n_events,job_id))
        self.conn.commit()

    def __now__(self):
        return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())
