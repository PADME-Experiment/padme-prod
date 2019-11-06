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

        # These need to be set from calling program to enable delegation renewal
        self.cream_ce_endpoint = ""
        self.delegations = []

        # Check if the VOMS proxy is stored in a non-standard position
        self.voms_proxy = os.environ.get('X509_USER_PROXY','')

    def run_command(self,command):

        if self.debug: print "> %s"%command
        p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        return iter(p.stdout.readline,b'')

    def renew_voms_proxy(self,long_proxy_file):

        # Check if current proxy is still valid and renew it if less than 2 hours before it expires
        info_cmd = "voms-proxy-info"
        if self.voms_proxy: info_cmd += " --file %s"%self.voms_proxy
        renew = True
        for line in self.run_command(info_cmd):
            if self.debug >= 2: print line.rstrip()
            r = re.match("^timeleft  \: (\d+)\:.*$",line)
            if r and int(r.group(1))>=2: renew = False

        if renew:
            if self.debug:
                if self.voms_proxy:
                    print "- VOMS proxy %s is missing or will expire in less than 2 hours."%self.voms_proxy
                else:
                    print "- Standard VOMS proxy is missing or will expire in less than 2 hours."
            self.create_voms_proxy(long_proxy_file)
            self.renew_delegations()

    def create_voms_proxy(self,long_proxy_file):

        # Create a VOMS proxy from long lived proxy
        if self.debug:
            if self.voms_proxy:
                print "- Creating new %s VOMS proxy using %s"%(self.voms_proxy,long_proxy_file)
            else:
                print "- Creating new standard VOMS proxy using %s"%long_proxy_file
        init_cmd = "voms-proxy-init --noregen --cert %s --key %s --voms vo.padme.org --valid %d:00"%(long_proxy_file,long_proxy_file,self.proxy_validity)
        if self.voms_proxy: init_cmd += " --out %s"%self.voms_proxy
        for line in self.run_command(init_cmd):
            if self.debug: print line.rstrip()

    def register_delegations(self):

        # Register delegations using current VOMS proxy (glite commands automatically use X509_USER_PROXY)
        if self.cream_ce_endpoint and self.delegations:
            if self.debug: print "- Registering proxy delegations using current VOMS proxy"
            for delegation in self.delegations:
                cmd = "glite-ce-delegate-proxy --endpoint %s %s"%(self.cream_ce_endpoint,delegation))
                for line in self.run_command(cmd):
                    if self.debug: print line.rstrip()

    def renew_delegations(self):

        # Renew delegations using current VOMS proxy (glite commands automatically use X509_USER_PROXY)
        if self.cream_ce_endpoint and self.delegations:
            if self.debug: print "- Renewing proxy delegations using current VOMS proxy"
            cmd = "glite-ce-proxy-renew --endpoint %s %s"%(self.cream_ce_endpoint,' '.join(self.delegations))
            for line in self.run_command(cmd):
                if self.debug: print line.rstrip()
