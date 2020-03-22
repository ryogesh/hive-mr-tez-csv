#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2020  Yogesh Rajashekharaiah
# All Rights Reserved

import os
import sys
from datetime import datetime, timedelta
from StringIO import StringIO
import argparse
import json
import csv
import pycurl

_B_URL = 'http://localhost:19888'

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

def get_time(stime, ftime, sbtime=''):
    ttime = ""
    btime = timedelta(milliseconds=stime)
    etime = timedelta(milliseconds=ftime)
    ttl = etime - btime
    min, sec = divmod(ttl.seconds, 60)
    hrs, min = divmod(min, 60)
    sfmt = datetime.utcfromtimestamp(stime/1000).isoformat(' ')
    efmt = datetime.utcfromtimestamp(ftime/1000).isoformat(' ')
    if hrs:
        plural = 's' if hrs > 1 else ''
        ttime = "%s hr%s " %(hrs, plural)
    if min:
        plural = 's' if min > 1 else ''
        ttime += "%s min%s " %(min, plural)
    plural = 's' if sec > 1 else ''
    ttime += "%s sec%s" %(sec, plural)
        
    if sbtime:
        submittime = datetime.utcfromtimestamp(sbtime/1000).isoformat(' ')
        return sfmt, efmt, ttime, submittime
    else:
        return sfmt, efmt, ttime

def get_job(args, fname):
    url = args.base_url + '/ws/v1/history/mapreduce/jobs/' + args.jobid
    msg = _get_info(args, url)
    jmsg = json.loads(msg)
    jmsg = jmsg["job"]
    jsum = {"state":jmsg["state"], "mapsTotal":jmsg["mapsTotal"], "queue":jmsg["queue"],
            "user":jmsg["user"], "reducesTotal":jmsg["reducesTotal"], "avgShuffleTime":jmsg["avgShuffleTime"]}
    jsum["startTime"], jsum["finishTime"], jsum["totalTime"], jsum["submitTime"] = get_time(jmsg["startTime"], 
                                                                                            jmsg["finishTime"], 
                                                                                            jmsg["submitTime"])
    jbat = _get_info(args, url + '/jobattempts')    
    jmsg = json.loads(jbat)
    jmsg = jmsg["jobAttempts"]["jobAttempt"][-1]
    jsum["nodeId"] = jmsg["nodeId"]
    jsum["containerId"] = jmsg["containerId"]

    ctrs = _get_info(args, url + '/counters')
    jmsg = json.loads(ctrs)
    jsmg = jmsg["jobCounters"]["counterGroup"]
    for each in jsmg:
        for item in each["counter"]:
            jsum[item["name"]] = item["totalCounterValue"]
    cols = jsum.keys()
    with open(fname, 'w') as fl:
        csvwrtr = csv.writer(fl)
        csvwrtr.writerow(cols)
        csvwrtr.writerow([jsum.get(i, "") for i in cols])

def get_tasks(args, fname):
    url = args.base_url + '/ws/v1/history/mapreduce/jobs/' + args.jobid + '/tasks'
    tasks = _get_info(args, url)
    jmsg = json.loads(tasks)
    jmsg = jmsg["tasks"]["task"]
    jdet = []
    for each in jmsg:
        row = {"id": each["id"], "type":each["type"]}
        tatt = _get_info(args, url + '/' + each["id"] + '/attempts')
        amsg = json.loads(tatt)
        amsg = amsg["taskAttempts"]["taskAttempt"]
        for att in amsg:
            row["nodeHttpAddress"] = att["nodeHttpAddress"]
            row["assignedContainerId"] = att["assignedContainerId"]
            row["state"] = att["state"]
            row["attemptId"] = att["id"]
            row["startTime"], row["finishTime"], row["totalTime"] = get_time(att["startTime"],
                                                                             att["finishTime"])
            tcntr = _get_info(args, url + '/' + each["id"] + '/attempts' + '/' + row["attemptId"] + '/counters')
            tmsg = json.loads(tcntr)
            tsmg = tmsg["jobTaskAttemptCounters"]["taskAttemptCounterGroup"]
            for msg in tsmg:
                for item in msg["counter"]:
                    row[item["name"]] = item["value"]
            jdet.append(row)

    cols = list(set([x for row in jdet for x in row.keys()]))    
    with open(fname, 'w') as fl:
        csvwrtr = csv.writer(fl)
        csvwrtr.writerow(cols)
        for ln in jdet:
            csvwrtr.writerow([ln.get(i, "") for i in cols])
        
def runmain():
    epi = "Make sure the TGT is in the cache e.g. kinit <userid>"
    parser = argparse.ArgumentParser(description="Save MapReduce job and counters information as csv files.Creates job summary and task details files",
                                     epilog=epi)
    parser.add_argument('jobid', help='Mapreduce Job Id')
    parser.add_argument('--base_url',
                        default=_B_URL,
                        help='URL for Mapreduce job history server, Default: %s' %_B_URL)
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
        fname = args.dir + '/' + args.jobid + '_summary.csv'
        get_job(args, fname)
        fname = args.dir + '/' + args.jobid + '_tasks.csv'
        get_tasks(args, fname)
    except Exception as err:
        print(err)
        print("Unable to capture job details and create csv files")
        sys.exit(1)       
    print("Created job summary and task details csv files: %s" %datetime.now())
    sys.exit(0)

if __name__ == '__main__':
    runmain()
