#!/usr/bin/python

import os
import sys
import getopt
import pexpect
import getpass

MYPROXY_SERVER = "myproxy.cnaf.infn.it"
MYPROXY_PORT = 7512
MYPROXY_LIFETIME = 720
MYPROXY_NAME = ""
MYPROXY_PASSWD = "myproxy"

PROXY_VOMS = "vo.padme.org"
PROXY_LIFETIME = 24

DEBUG = 0

def print_help():

    print "create_myproxy -N <name> [-P passwd] [-s <server>] [-p <port>] [-V] [-h]"
    print "  -N <name>\t\tname of this MyProxy instance."
    print "  -P <passwd>\t\tpassword for this MyProxy instance. Default: %s"%MYPROXY_PASSWD
    print "  -s <server>\t\tMyProxy server to use. Default: %s"%MYPROXY_SERVER
    print "  -p <port>\t\tMyProxy server port to use. Default: %s"%MYPROXY_PORT
    print "  -V\t\t\tenable debug mode. Can be repeated to increase verbosity"
    print "  -h\t\t\tshow this help message and exit."
    print "MyProxy info output format: server:port:name:password"

def main(argv):

    global MYPROXY_SERVER
    global MYPROXY_PORT
    global MYPROXY_NAME
    global MYPROXY_PASSWD
    global DEBUG

    try:
        opts,args = getopt.getopt(argv,"hVN:P:s:p:",[])
    except getopt.GetoptError as e:
        print "Option error: %s"%str(e)
        print_help()
        sys.exit(2)

    for opt,arg in opts:
        if opt == '-h':
            print_help()
            sys.exit(0)
        elif opt == '-V':
            DEBUG += 1
        elif opt == '-N':
            MYPROXY_NAME = arg
        elif opt == '-P':
            MYPROXY_PASSWD = arg
        elif opt == '-s':
            MYPROXY_SERVER = arg
        elif opt == '-p':
            MYPROXY_PORT = arg

    if not MYPROXY_NAME:
        print "*** ERROR *** No name for the MyProxy instance specified."
        print_help()
        sys.exit(2)

    if DEBUG:
        print "- MyProxy Name\t\t%s"%MYPROXY_NAME
        print "- MyProxy Password\t%s"%MYPROXY_PASSWD
        print "- MyProxy Lifetime\t%s hours"%MYPROXY_LIFETIME
        print "- MyProxy Server\t%s:%s"%(MYPROXY_SERVER,MYPROXY_PORT)
        print "- Proxy VOMS\t\t%s"%PROXY_VOMS
        print "- Proxy Lifetime\t%s hours"%PROXY_LIFETIME
        print "- Debug Level\t\t%d"%DEBUG

    # Create long-lived proxy on MyProxy server (also create a local proxy to talk to storage SRM)
    #grid_passwd = getpass.getpass(prompt="Enter GRID pass phrase for this identity:")
    grid_passwd = os.getenv("GLOBUS_PASSWORD",default="")
    if grid_passwd == "":
        grid_passwd = getpass.getpass(prompt="Enter GRID pass phrase for this identity:")
    proxy_cmd = "myproxy-init --proxy_lifetime %d --cred_lifetime %d --voms %s --pshost %s --psport %s --dn_as_username --credname %s --local_proxy"%(PROXY_LIFETIME,MYPROXY_LIFETIME,PROXY_VOMS,MYPROXY_SERVER,MYPROXY_PORT,MYPROXY_NAME)
    if DEBUG: print ">",proxy_cmd
    child = pexpect.spawn(proxy_cmd)
    try:
        child.expect("Enter GRID pass phrase for this identity:")
        if DEBUG: print child.before
        child.sendline(grid_passwd)
        child.expect("Enter MyProxy pass phrase:")
        if DEBUG: print child.before
        child.sendline(MYPROXY_PASSWD)
        child.expect("Verifying - Enter MyProxy pass phrase:")
        if DEBUG: print child.before
        child.sendline(MYPROXY_PASSWD)
    except:
        print "*** ERROR *** Unable to register long-lived proxy on %s"%MYPROXY_SERVER
        print str(child)
        sys.exit(2)

    print "%s:%s:%s:%s"%(MYPROXY_SERVER,MYPROXY_PORT,MYPROXY_NAME,MYPROXY_PASSWD)

    sys.exit(0)

# Execution starts here
if __name__ == "__main__": main(sys.argv[1:])
