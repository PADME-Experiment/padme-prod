#!/usr/bin/python

import re
import subprocess
import shlex

class ProxyHandler:

    def __init__(self):

        # Set to 1 or more to enable printout of executed commands
        self.debug = 0

        # These need to be set from calling program to enable delegation renewal
        self.cream_ce_endpoint = ""
        self.delegations = []

    def run_command(self,command):

        if self.debug: print "> %s"%command
        p = subprocess.Popen(shlex.split(command),stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        return iter(p.stdout.readline,b'')

    def renew_voms_proxy(self,long_proxy_file):

        # Check if current proxy is still valid and renew it if less than 2 hours before it expires
        renew = True
        for line in self.run_command("voms-proxy-info"):
            if self.debug >= 2: print line.rstrip()
            r = re.match("^timeleft  \: (\d+)\:.*$",line)
            if r and int(r.group(1))>=2: renew = False

        if renew:
            print "- VOMS proxy is missing or will expire in less than 2 hours."
            self.create_voms_proxy(long_proxy_file)
            self.renew_delegations()

    def create_voms_proxy(self,long_proxy_file):

        # Create a 24h VOMS proxy from long lived proxy
        print "- Creating VOMS proxy using %s"%long_proxy_file
        cmd = "voms-proxy-init --noregen --cert %s --key %s --voms vo.padme.org --valid 24:00"%(long_proxy_file,long_proxy_file)
        for line in self.run_command(cmd):
            if self.debug: print line.rstrip()

    def renew_delegations(self):

        # Renew delegations to all jobs using new VOMS proxy
        if self.cream_ce_endpoint and self.delegations:
            print "- Renewing proxy delegations using new VOMS proxy"
            cmd = "glite-ce-proxy-renew --endpoint %s %s"%(self.cream_ce_endpoint,' '.join(self.delegations))
            for line in self.run_command(cmd):
                if self.debug: print line.rstrip()
