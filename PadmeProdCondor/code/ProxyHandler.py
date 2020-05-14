#!/usr/bin/python

import re
import subprocess
import shlex
import os

class ProxyHandler:

    def __init__(self):

        # Set to 1 or more to enable printout of executed commands
        self.debug = 0

        # Define VOMS proxy validity in hours
        self.proxy_validity = 24

        # If proxy validity is less than this time (seconds), renew it
        self.proxy_renew_threshold = 3600

        # VO to use
        self.proxy_vo = "vo.padme.org"

        # Check if the VOMS proxy is stored in a non-standard position
        self.voms_proxy = os.environ.get('X509_USER_PROXY','')

        # Initialize MyProxy information (no default)
        self.myproxy_server = ""
        self.myproxy_port = ""
        self.myproxy_name = ""
        self.myproxy_passwd = ""

    def renew_voms_proxy(self):

        # Check if current proxy is still valid and renew it if less than 2 hours before it expires
        info_cmd = "voms-proxy-info --actimeleft"
        if self.voms_proxy: info_cmd += " --file %s"%self.voms_proxy
        if self.debug: print "> %s"%info_cmd
        renew = True
        p = subprocess.Popen(shlex.split(info_cmd),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        (out,err) = p.communicate()
        if p.returncode == 0:
            for l in iter(out.splitlines()):
                if self.debug >= 2: print l.rstrip()
                r = re.match("^\s*(\d+)\s*$",l)
                if r and int(r.group(1))>=self.proxy_renew_threshold:
                    renew = False
        else:
            print "  WARNING voms-proxy-info returned error code %d"%p.returncode
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err

        if renew:
            if self.debug:
                if self.voms_proxy:
                    print "- VOMS proxy %s is missing or will expire in less than 2 hours."%self.voms_proxy
                else:
                    print "- Standard VOMS proxy is missing or will expire in less than 2 hours."
            self.create_voms_proxy()

    def create_voms_proxy(self):

        # Create a VOMS proxy from MyProxy server
        if self.debug:
            if self.voms_proxy:
                print "- Creating new %s VOMS proxy from MyProxy server %s"%(self.voms_proxy,self.myproxy_server)
            else:
                print "- Creating new standard VOMS proxy from MyProxy server %s"%self.myproxy_server
        logon_cmd = "myproxy-logon --voms %s --pshost %s:%s --dn_as_username --credname %s --stdin_pass"%(self.proxy_vo,self.myproxy_server,self.myproxy_port,self.myproxy_name)
        if self.voms_proxy: logon_cmd += " --out %s"%self.voms_proxy
        if self.debug: print "> %s"%logon_cmd
        p = subprocess.Popen(shlex.split(logon_cmd),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        (out,err) = p.communicate(input=self.myproxy_passwd)
        if p.returncode == 0:
            if self.debug: print out
        else:
            print "  WARNING myproxy-logon returned error code %d"%p.returncode
            print "- STDOUT -\n%s"%out
            print "- STDERR -\n%s"%err
