#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2020  Yogesh Rajashekharaiah
# All Rights Reserved

import os
import sys
import subprocess
import socket
from datetime import datetime, date
from StringIO import StringIO
import argparse
import json
import csv
import pycurl

_B_URL = 'http://localhost:8088'

def _get_info(args, url):
    buffer = StringIO()
    header = ['Accept: application/json',]
    pyc = pycurl.Curl()
    pyc.setopt(pyc.URL, str(url))
    pyc.setopt(pyc.WRITEFUNCTION, buffer.write)
    pyc.setopt(pyc.FOLLOWLOCATION, True)
    pyc.setopt(pyc.HTTPHEADER, header) 
    if args.cacert:
        pyc.setopt(pyc.CAINFO, args.cacert)
    if args.verbose == 'y':
        pyc.setopt(pyc.VERBOSE, True)
    if args.kerberos == 'y':
        pyc.setopt(pyc.HTTPAUTH, pyc.HTTPAUTH_GSSNEGOTIATE)
        pyc.setopt(pyc.USERPWD, ':')
    try:
        pyc.perform()
        if pyc.getinfo(pyc.RESPONSE_CODE) == 200:
            return buffer.getvalue()
        else:
            print("Failed to fetch results from API:%s , Respose_code:%s", url, pyc.getinfo(pyc.RESPONSE_CODE))
            raise Exception('PYCURL-01')
    except Exception as err:
        print("PyCurl Error:%s" %err)
        raise Exception(err)
    finally:
        pyc.close()

def run_shell(cmd):
    scmd = str(cmd)
    subprocess.check_call(scmd, shell=True)
    
def create_lst(qmsg, qlst, currTime):
    for each in qmsg["queues"]["queue"]:
        qdet = {"absoluteMaxCapacity":each["absoluteMaxCapacity"],
            "absoluteUsedCapacity":each["absoluteUsedCapacity"],
            "allocatedContainers":each["allocatedContainers"],
            "pendingContainers":each["pendingContainers"],
            "queueName":each["queueName"],
            "capacity":each["capacity"],
            "parentQueue": qmsg["queueName"],
            "state":each["state"],
            "numApplications":each["numApplications"],
            "memoryUsed":each["resourcesUsed"]["memory"],
            "vCoresUsed":each["resourcesUsed"]["vCores"],
            "dateTime":"%s" %currTime,
            "capacity":each["capacity"],
            "maxCapacity":each["maxCapacity"]
            }
        if each.get("type", ""):
            qdet["type"] = "leaf"
            qdet["numActiveApplications"] = each["numActiveApplications"]
            qdet["numPendingApplications"] = each["numPendingApplications"]
        else:
            create_lst(each, qlst, currTime)
        qlst.append(qdet)

def get_queues(args, fname):
    if args.TGT == 'y':
        cmd = "kinit -kt /etc/security/keytabs/yarn.service.keytab yarn/%s" %socket.gethostname()
        run_shell(cmd)
    url = args.base_url + '/ws/v1/cluster/scheduler'
    qs = _get_info(args, url)
    qmsg = json.loads(qs)
    qmsg = qmsg["scheduler"]["schedulerInfo"]
    qlst = []
    create_lst(qmsg, qlst, datetime.now())
    cols = sorted(set([x for row in qlst for x in row.keys()]))
    fmode = 'w'
    hdr = True
    if os.path.isfile(fname):
        fmode = 'a'
        hdr = False
    with open(fname, fmode) as fl:
        csvwrtr = csv.writer(fl)
        if hdr:
            csvwrtr.writerow(cols)
        for ln in qlst:
            csvwrtr.writerow([ln.get(i, "") for i in cols])
       
def runmain():
    epi = "Make sure the TGT is in the cache, if --TGT is set to n "  
    fldir = os.environ['HOME'] or '/tmp'
    parser = argparse.ArgumentParser(description="Save Yarn queue metrics to a csv file",
                                     epilog=epi)
    parser.add_argument('--base_url', default=_B_URL,
                        help='URL for Yarn Resource Manager, Default: %s' %_B_URL)
    parser.add_argument('--kerberos', default='y',
                        help='Kerberos Authentication enabled(y)/disabled(n), Default: Enabled(y)')
    parser.add_argument('--TGT', default='n',
                        help='Create TGT(kinit) enabled(y)/disabled(n), Default: Disabled(n)')
    parser.add_argument('--cacert', default=None,
                        help='Location of CACERT, e.g /opt/anaconda3/lib/python2.7/site-packages/certifi/cacert.pem, Default: None')
    parser.add_argument('--dir', default=fldir,
                        help="Folder to save the csv files, Default: %s" %fldir)
    parser.add_argument('--verbose',
                        default='n',
                        help='Debug Curl request, Default: n')
    args = parser.parse_args()
    print("Starting queue metrics collection: %s" %datetime.now())
    fname = "%s/queues_%s.csv" %(args.dir, date.today())   
    try:
        get_queues(args, fname)
    except Exception as err:
        print(err)
        print("Unable to capture queue metrics to csv file")
        sys.exit(1)       
    print("Collected queue metrics to csv files: %s" %datetime.now())
    sys.exit(0)


if __name__ == '__main__':
    runmain()
