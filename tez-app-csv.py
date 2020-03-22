#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2020  Yogesh Rajashekharaiah
# All Rights Reserved

import os
import sys
import subprocess
import socket
from datetime import datetime, timedelta
from StringIO import StringIO
import argparse
import json
import csv
import pycurl

_B_URL = 'http://localhost:8188'

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

def get_time(stime, ftime):
    ttime = ""
    btime = timedelta(milliseconds=int(stime))
    etime = timedelta(milliseconds=int(ftime))
    ttl = etime - btime
    min, sec = divmod(ttl.seconds, 60)
    hrs, min = divmod(min, 60)
    sfmt = datetime.utcfromtimestamp(int(stime)/1000).isoformat(' ')
    efmt = datetime.utcfromtimestamp(int(ftime)/1000).isoformat(' ')
    if hrs:
        plural = 's' if hrs > 1 else ''
        ttime = "%s hr%s " %(hrs, plural)
    if min:
        plural = 's' if min > 1 else ''
        ttime += "%s min%s " %(min, plural)
    plural = 's' if sec > 1 else ''
    ttime += "%s sec%s" %(sec, plural)
    return sfmt, efmt, ttime

def run_shell(cmd):
    scmd = str(cmd)
    subprocess.check_call(scmd, shell=True)

def get_tasks(args, fname):
    url = args.base_url + '/ws/v1/applicationhistory/apps/' + args.appid + '/appattempts'
    atts = _get_info(args, url)
    jmsg = json.loads(atts)
    jmsg = jmsg["appAttempt"]
    jdet = []
    for each in jmsg:
        url += '/' + each["appAttemptId"]
        att = _get_info(args, url)
        tmsg = json.loads(att)
        cid = tmsg["amContainerId"]
        tmpfl = "/tmp/fltr_%s" %cid
        cmd = 'yarn logs --containerId %s |grep ".*HISTORY.*DAG.*TASK_FINISHED.*"|cut -d: -f7,8 |cut -f3- -d, >%s' %(cid, tmpfl)
        run_shell(cmd)
        with open(tmpfl) as csvfl:
            for ln in csvfl.readlines():
                lst = [item.split('=') for item in ln.strip().split(', ')]
                dct = {item[0]:item[1] for item in lst if len(item) > 1}
                dct["startTime"], dct["finishTime"], dct["totalTime"] = get_time(dct["startTime"],
                                                                                 dct["finishTime"])
                jdet.append(dct)
        os.remove(tmpfl)
    cols = list(set([x for row in jdet for x in row.keys()]))
    with open(fname, 'w') as fl:
        csvwrtr = csv.writer(fl)
        csvwrtr.writerow(cols)
        for cntr, ln in enumerate(jdet):
            csvwrtr.writerow([ln.get(i, "") for i in cols])
       
def runmain():
    epi = "Make sure the TGT is in the cache e.g. sudo kinit -kt /etc/security/keytabs/yarn.service.keytab yarn/%s and change the ownership of the cache file on /tmp" %socket.gethostname()  
    parser = argparse.ArgumentParser(description="Save Tez job counters information as a csv file",
                                     epilog=epi)
    parser.add_argument('appid', help='Yarn Application Id')
    parser.add_argument('--base_url',
                        default=_B_URL,
                        help='URL for Yarn Timeline server, Default: %s' %_B_URL)
    parser.add_argument('--kerberos',
                        default='y',
                        help='Kerberos Authentication enabled(y)/disabled(n), Default: Enabled(y)')
    parser.add_argument('--cacert',
                        default=None,
                        help='Location of CACERT, e.g /opt/anaconda3/lib/python2.7/site-packages/certifi/cacert.pem, Default: None')
    fldir = os.environ['HOME'] or '/tmp'
    parser.add_argument('--dir',
                        default=fldir,
                        help="Folder to save the csv files, Default: %s" %fldir)
    parser.add_argument('--verbose',
                        default='n',
                        help='Debug Curl request, Default: n')
    args = parser.parse_args()
    print("Starting metrics collection: %s" %datetime.now())
    try:
        fname = args.dir + '/' + args.appid + '_tasks.csv'
        get_tasks(args, fname)
    except Exception as err:
        print(err)
        print("Unable to capture Tez details and create csv file")
        sys.exit(1)       
    print("Created Tez task details csv files: %s" %datetime.now())
    sys.exit(0)


if __name__ == '__main__':
    runmain()
