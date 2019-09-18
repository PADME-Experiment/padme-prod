#!/usr/bin/python

import re
import subprocess

class ProxyHandler:

    def __init__(self):
        # These need to be set from calling program to enable delegation renewal
        self.cream_ce_endpoint = ""
        self.delegations = []

    def run_command(self,command):
        print "> %s"%command
        p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
        return iter(p.stdout.readline,b'')

    def renew_voms_proxy(self,long_proxy_file):

        # Check if current proxy is still valid and renew it if less than 2 hours before it expires
        renew = True
        for line in self.run_command("voms-proxy-info"):
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
        print "> %s"%cmd
        for line in self.run_command(cmd): print(line.rstrip())

    def renew_delegations(self):

        # Renew delegations to all jobs using new VOMS proxy
        if self.cream_ce_endpoint and self.delegations:
            print "- Renewing proxy delegations using new VOMS proxy"
            cmd = "glite-ce-proxy-renew --endpoint %s %s"%(self.cream_ce_endpoint,' '.join(self.delegations))
            print "> %s"%cmd
            for line in self.run_command(cmd): print(line.rstrip())
