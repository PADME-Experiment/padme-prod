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

        self.ATTEMPTS_MAX = 100
        self.ATTEMPTS_DELAY = 10

        self.conn = None

    def __del__(self):

        self.close_db()

    def connect_db(self):

        self.close_db()

        attempts = 0
        while True:
            try:
                self.conn = MySQLdb.connect(host   = self.DB_HOST,
                                            port   = self.DB_PORT,
                                            user   = self.DB_USER,
                                            passwd = self.DB_PASSWD,
                                            db     = self.DB_NAME)
            except MySQLdb.Error as e:
                print "*** MySQLdb ERROR while connecting to DB (%3d/%3d). Exception: %d:%s"%(attempts,self.ATTEMPTS_MAX,e.args[0],e.args[1])
                attempts += 1
                if attempts >= self.ATTEMPTS_MAX:
                    print "*** PadmeMCDB ERROR *** Unable to connect to DB for %d times: aborting production."%attempts
                    sys.exit(2)
                time.sleep(self.ATTEMPTS_DELAY)
                continue
            break

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
        if n: return True
        return False

    def create_recoprod(self,name,run,description,prod_ce,reco_version,prod_dir,storage_uri,storage_dir,proxy_file,n_jobs):

        prod_id = self.create_prod(name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,n_jobs)

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""INSERT INTO reco_prod (production_id,description,run,reco_version) VALUES (%s,%s,%s,%s)""",(prod_id,description,run,reco_version))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

        return prod_id

    def create_mcprod(self,name,description,user_req,n_events_req,prod_ce,mc_version,prod_dir,storage_uri,storage_dir,proxy_info,n_jobs):

        prod_id = self.create_prod(name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_info,n_jobs)

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""INSERT INTO mc_prod (production_id,description,user_req,n_events_req,mc_version) VALUES (%s,%s,%s,%s,%s)""",(prod_id,description,user_req,n_events_req,mc_version))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

        return prod_id

    def create_prod(self,name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_info,n_jobs):

        prod_id = 0
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""INSERT INTO production (name,prod_ce,prod_dir,storage_uri,storage_dir,proxy_file,time_create,n_jobs,n_jobs_ok,n_jobs_fail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,0)""",(name,' '.join(prod_ce),prod_dir,storage_uri,storage_dir,proxy_info,self.__now__(),n_jobs))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            prod_id = c.lastrowid
        self.conn.commit()
        return prod_id

    def close_prod(self,prod_id,n_jobs_ok,n_jobs_fail,n_events):

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE production SET time_complete = %s, n_jobs_ok = %s, n_jobs_fail = %s, n_events = %s WHERE id = %s""",(self.__now__(),n_jobs_ok,n_jobs_fail,n_events,prod_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def get_prod_type(self,prod_id):

        self.check_db()
        c = self.conn.cursor()

        c.execute("""SELECT id FROM reco_prod WHERE production_id = %s""",(prod_id,))
        if c.rowcount != 0:
            self.conn.commit()
            return "RECO"

        c.execute("""SELECT id FROM mc_prod WHERE production_id = %s""",(prod_id,))
        if c.rowcount != 0:
            self.conn.commit()
            return "MC"

        self.conn.commit()
        return "UNKNOWN"

    def set_prod_job_numbers(self,prod_id,jobs_ok,jobs_fail):

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE production SET n_jobs_ok = %s, n_jobs_fail = %s WHERE id = %s""",(jobs_ok,jobs_fail,prod_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_prod_n_events(self,prod_id,n_events):

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE production SET n_events = %s WHERE id = %s""",(n_events,prod_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
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

    def create_job(self,prod_id,name,job_dir,configuration,input_list,random):

        # Jobs are created in idle status
        status = 0

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""INSERT INTO job (production_id,name,job_dir,configuration,input_list,random,status,time_create) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(prod_id,name,job_dir,configuration,input_list,random,status,self.__now__()))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def close_job(self,job_id,status):

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job SET status = %s, time_complete = %s WHERE id = %s""",(status,self.__now__(),job_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
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
        c.execute("""SELECT p.prod_dir,j.job_dir FROM production p INNER JOIN job j ON j.production_id=p.id WHERE j.id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return ""
        return "%s/%s"%res

    def get_job_local_dir(self,job_id):

        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT job_dir FROM job WHERE id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return ""
        return res[0]

    def get_job_name(self,job_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT name FROM job WHERE id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return ""
        return res[0]

    def get_job_status(self,job_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT status FROM job WHERE id=%s""",(job_id,))
        res = c.fetchone()
        self.conn.commit()
        if (res == None): return -1
        return res[0]

    def create_job_submit(self,job_id,job_sub_index):

        # Job submissions are created in idle status
        status = 0

        # Create new job submission
        job_sub_id = 0
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""INSERT INTO job_submit (job_id,submit_index,status,time_submit) VALUES (%s,%s,%s,%s)""",(job_id,job_sub_index,status,self.__now__()))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            job_sub_id = c.lastrowid
        self.conn.commit()

        # Return job submission id
        return job_sub_id

    def close_job_submit(self,job_sub_id,status,description='',exit_code=''):

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET status = %s, time_complete = %s WHERE id = %s""",(status,self.__now__(),job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        if description:
            try:
                c.execute("""UPDATE job_submit SET description = %s WHERE id = %s""",(description,job_sub_id))
            except MySQLdb.Error as e:
                print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        if exit_code:
            try:
                c.execute("""UPDATE job_submit SET exit_code = %s WHERE id = %s""",(exit_code,job_sub_id))
            except MySQLdb.Error as e:
                print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def get_job_submit_info(self,job_sub_id):
    
        self.check_db()
        c = self.conn.cursor()
        c.execute("""SELECT status,worker_node,wn_user,description FROM job_submit WHERE id=%s""",(job_sub_id,))
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
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_submitted(self,job_sub_id,ce_job_id):
  
        # Job status changes to 1 (REGISTERED) after submission
        status = 1

        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET status = %s, ce_job_id = %s, time_submit = %s WHERE id = %s""",(status,ce_job_id,self.__now__(),job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_status(self,job_id,status):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job SET status = %s WHERE id = %s""",(status,job_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_submit_status(self,job_sub_id,status):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET status = %s WHERE id = %s""",(status,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_time_complete(self,job_id,time_complete):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job SET time_complete = %s WHERE id = %s""",(time_complete,job_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_time_start(self,job_sub_id,time_start):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET time_job_start = %s WHERE id = %s""",(time_start,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_time_end(self,job_sub_id,time_end):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET time_job_end = %s WHERE id = %s""",(time_end,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_run_time_start(self,job_sub_id,time_start):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET time_run_start = %s WHERE id = %s""",(time_start,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_run_time_end(self,job_sub_id,time_end):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET time_run_end = %s WHERE id = %s""",(time_end,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_worker_node(self,job_sub_id,worker_node):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET worker_node = %s WHERE id = %s""",(worker_node,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_wn_user(self,job_sub_id,wn_user):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET wn_user = %s WHERE id = %s""",(wn_user,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_wn_dir(self,job_sub_id,wn_dir):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job_submit SET wn_dir = %s WHERE id = %s""",(wn_dir,job_sub_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_n_files(self,job_id,n_files):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job SET n_files = %s WHERE id = %s""",(n_files,job_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def set_job_n_events(self,job_id,n_events):
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""UPDATE job SET n_events = %s WHERE id = %s""",(n_events,job_id))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        self.conn.commit()

    def get_prod_dir(self,prod_name):

        prod_dir = ""
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""SELECT storage_dir FROM production WHERE name=%s""",(prod_name,))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            (prod_dir,) = c.fetchone()
        self.conn.commit()
        return prod_dir

    def get_prod_file_list(self,prod_name):

        file_list = []
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""
SELECT f.name 
FROM file f
    INNER JOIN job j ON j.id = f.job_id
    INNER JOIN production p ON p.id = j.production_id
WHERE p.name=%s
            """,(prod_name,))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            res = c.fetchall()
            for (prod_file,) in res:
                file_list.append("%s"%prod_file)
            file_list.sort()
        self.conn.commit()
        return file_list

    def get_prod_files_attr(self,prod_name):

        # Return file attributes (size and adler32 checksum) of all files in a production as dictionaries
        size = {}
        checksum = {}
        self.check_db()
        c = self.conn.cursor()
        try:
            c.execute("""
SELECT f.name,f.size,f.adler32 
FROM file f
    INNER JOIN job j ON j.id = f.job_id
    INNER JOIN production p ON p.id = j.production_id
WHERE p.name=%s
            """,(prod_name,))
        except MySQLdb.Error as e:
            print "MySQL Error:%d:%s"%(e.args[0],e.args[1])
        else:
            res = c.fetchall()
            for (file_name,file_size,file_checksum) in res:
                size[file_name] = int(file_size)
                checksum[file_name] = file_checksum
        self.conn.commit()
        return (size,checksum)

    def __now__(self):
        return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())
