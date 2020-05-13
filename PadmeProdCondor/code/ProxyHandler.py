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
        self.myproxy_port = 0
        self.myproxy_name = ""
        self.myproxy_passwd = ""

    def run_command(self,command):

        if self.debug: print "> %s"%command
        p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        return iter(p.stdout.readline,b'')

    def renew_voms_proxy(self):

        # Check if current proxy is still valid and renew it if less than 2 hours before it expires
        info_cmd = "voms-proxy-info --actimeleft"
        if self.voms_proxy: info_cmd += " --file %s"%self.voms_proxy
        renew = True
        for line in self.run_command(info_cmd):
            if self.debug >= 2: print line.rstrip()
            r = re.match("^\s*(\d+)\s*$",line)
            if r and int(r.group(1))>=self.proxy_renew_threshold:
                renew = False

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
        logon_cmd = "myproxy-logon --voms %s --pshost %s:%d --dn_as_username --credname %s --stdin_pass"%(self.proxy_vo,self.myproxy_server,self.myproxy_port,self.myproxy_name)
        if self.voms_proxy: logon_cmd += " --out %s"%self.voms_proxy
        for line in self.run_command(logon_cmd):
            if self.debug: print line.rstrip()
