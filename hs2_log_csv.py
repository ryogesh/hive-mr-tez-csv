#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2020  Yogesh Rajashekharaiah
# All Rights Reserved

import os
import socket
from datetime import datetime, date
import argparse
import csv
try:
    import cPickle as pk
except ImportError:
    import pickle as pk
try:
    import ujson as json
except ImportError:
    import json

_FNAME = '/var/log/hive/hiveserver2.log'


def create_csv(csvfl, fmode, hdr, qdct):
    cols = ['queryId', 'Count', 'Query', 'user', 'queueName',
            'applicationId', 'CompileStartTime',
            'CompileEndTime', 'CompileTime',
            'ExecuteStartTime', 'ExecuteEndTime',
            'ExecuteTime', 'sessionName', 'Thread']
    with open(csvfl, fmode) as wfl:
        csvwrtr = csv.writer(wfl)
        if hdr:
            csvwrtr.writerow(cols)
        for key in qdct:
            csvwrtr.writerow([key, qdct[key]['Count'],
                              qdct[key]['Query'], qdct[key]['user'], qdct[key]['queueName'],
                              qdct[key]['applicationId'], qdct[key]['Compile']['Start'],
                              qdct[key]['Compile']['End'], qdct[key]['Compile']['TimeTaken'],
                              qdct[key]['Execute']['Start'], qdct[key]['Execute']['End'],
                              qdct[key]['Execute']['TimeTaken'], qdct[key]['sessionName'], qdct[key]['Thread']])

def create_json(jfl, fmode, qdct):
    with open(jfl, fmode) as wfl:
        json.dump(qdct, wfl, indent=2)

def get_queries(args):
    qdct = {}
    tdct = {}
    query_id = ''
    prevln = ''
    plines = 0
    tlines = 0
    last_ts = "2000-01-01T00:00:01,1"
    dts = "2000-01-01T00:00:01,2"
    runfl = "%s/.queries.dat" %args.dir
    # Read the contents of last_run, use the timestamp to read the log file since last run
    # Previous incomplete queries dct will be updated and then appended to the csv file
    if args.periodic == 'y' and os.path.isfile(runfl):
        with open(runfl) as rfl:
            try:
                run_data = pk.load(rfl)
            except EOFError:
                pass
            else:
                last_ts = run_data['ts']
                qdct = run_data['queries']
                print("Log file last timestamp:%s" %last_ts)
    # Parse the log file, check for queryId|sessionId|sessionName
    # Build the dictionary with queryId as key and query details
    # sessionName and sessionId is used to identify the appId, user, queue
    with open(_FNAME) as rfl:
        mqry_lns = False
        mlns_dag = False
        for ln in rfl:
            tlines += 1
            dts = ln.split()
            if len(dts) > 0:
                dts = dts[0]
                if dts.startswith("20") and dts.count('-')==2 and dts.count(':')==2 and dts.count('T')==1:
                    if dts >= last_ts:
                        if mqry_lns:
                            mqry_lns = False
                            qdct[query_id]['Query'] = query
                    else:
                        continue
                else:
                    # Check for the timestamp at the beginning of the line
                    # Multi-line queries does not have the timestamp
                    if mqry_lns:
                        plines += 1
                        query += ln.replace('\n', ' ')
                    elif mlns_dag:
                        # Multi-line dagName
                        plines += 1
                        try:
                            query_id = ln.split('callerId=')[1].rstrip(' }\n')
                        except IndexError:
                            # Till we get the line with callerId, continue
                            continue
                        mlns_dag = False
                        try:
                            qdct[query_id]['sessionName'] = session_name
                            qdct[query_id]['applicationId'] = app_id
                        except KeyError:
                            pass
                    continue
            else:
                continue
            plines += 1
            if 'Compiling command' in ln:
                vals = ln.split('queryId=')
                dtm = vals[0].split()[0].replace('T', ' ')
                qdet = vals[1].split('):')
                query_id, query = qdet[0], ' '.join(qdet[1:]).replace('\n', ' ')
                qdct[query_id] = {'Query':'',
                                  'Compile': {'Start':dtm,
                                              'End':'',
                                              'TimeTaken':''},
                                  'Execute': {'Start':'',
                                              'End':'',
                                              'TimeTaken':''},
                                  'sessionName':'',
                                  'sessionId': '',
                                  'applicationId':'',
                                  'user':'',
                                  'queueName':'',
                                  'Count':'',
                                  'Thread':''
                                 }
                mqry_lns = True
            elif 'Completed compiling command' in ln:
                vals = ln.split('queryId=')
                dtm = vals[0].split()[0].replace('T', ' ')
                qdet = vals[1].split(');')
                query_id, ttaken = qdet[0], qdet[1].split(': ')[1]
                try:
                    qdct[query_id]['Compile']['TimeTaken'] = ttaken.rstrip()
                    qdct[query_id]['Compile']['End'] = dtm
                except KeyError:
                    # This should not happen normally
                    # Perhaps when starting first time, or when a log rotation has happened
                    pass
            elif 'Executing command' in ln:
                vals = ln.split('queryId=')
                query_id = vals[1].split('):')[0]
                vals = vals[0].split()
                dtm = vals[0].replace('T', ' ')
                thrd = vals[3][:-2]
                tdct[thrd] = query_id
                try:
                    qdct[query_id]['Thread'] = thrd
                    qdct[query_id]['Execute']['Start'] = dtm
                except KeyError:
                    pass
            elif 'Completed executing command' in ln:
                vals = ln.split('queryId=')
                dtm = vals[0].split()[0].replace('T', ' ')
                qdet = vals[1].split(');')
                query_id, ttaken = qdet[0], qdet[1].split(': ')[1]
                try:
                    qdct[query_id]['Execute']['TimeTaken'] = ttaken.rstrip()
                    qdct[query_id]['Execute']['End'] = dtm
                except KeyError:
                    pass
            elif 'Submitting dag to TezSession, sessionName' in ln:
                #callerId is the same as queryId
                vals = ln.split('callerId=')
                app = vals[0].split('applicationId=')
                app_id = app[1].split(',')[0]
                session_name = app[0].split('sessionName=')[1].split(',')[0]
                try:
                    query_id = vals[1].rstrip(' }\n')
                except IndexError:
                    #Sometimes dagName is multi-line
                    mlns_dag = True
                    continue
                try:
                    qdct[query_id]['sessionName'] = session_name
                    qdct[query_id]['applicationId'] = app_id
                except KeyError:
                    pass
            elif 'Closing tez session if not default: sessionId=' in ln:
                vals = ln.split('sessionId=')
                tpls = vals[1].split(', ')
                session_id = tpls[0]
                queue_name = tpls[1].split('=')[1]
                quser = tpls[2].split('=')[1]
                #Get the queryId for the sessionId, using sessionName
                for key in qdct.keys():
                    if qdct[key]['sessionName'].endswith(session_id):
                        query_id = key
                        break
                if query_id:
                    try:
                        qdct[query_id]['user'] = quser
                        qdct[query_id]['queueName'] = queue_name
                    except KeyError:
                        pass
            elif 'RECORDS_OUT_INTERMEDIATE_Map_1:' in ln and 'RECORDS_OUT_' in prevln:
                vals = prevln.split("]:")
                thrd = vals[0].split()[-1]
                rcnt = vals[1].split()[-1]
                try:
                    query_id = tdct[thrd]
                    qdct[query_id]['Count'] = rcnt
                except KeyError:
                    pass
            prevln = ln # store the previous ln, for query row count, see above 
    sdct = {}
    if qdct:
        # Query partial details in log file: save and publish on next run
        for key in qdct:
            if qdct[key]['Compile']['Start'] and not qdct[key]['Compile']['End'] or \
               qdct[key]['Execute']['Start'] and not qdct[key]['Execute']['End']:
                sdct[key] = qdct[key]
        for key in sdct:
            del qdct[key]
    if args.format in ('c', 'b'):
        if qdct:
            fmode = 'w'
            hdr = True
            csvfl = "%s/%s_queries_%s.csv" %(args.dir, socket.gethostname(), date.today())
            if os.path.isfile(csvfl):
                fmode = 'a'
                hdr = False
            create_csv(csvfl, fmode, hdr, qdct)
        if sdct:
            csvfl = "%s/%s_incomplete_queries_%s.csv" %(args.dir, socket.gethostname(), date.today())
            create_csv(csvfl, 'w', True, sdct)
    if args.format in ('j', 'b'):
        if qdct:
            jfl = "%s/%s_queries_%s.json" %(args.dir, socket.gethostname(), date.today())
            fmode = 'w'
            if os.path.isfile(jfl):
                fmode = 'a'
            create_json(jfl, fmode, qdct)
        if sdct:
            jfl = "%s/%s_incomplete_queries_%s.json" %(args.dir, socket.gethostname(), date.today())
            create_json(jfl, 'w', sdct)

    print("Log file, Total lines: %s, Processed:%s, Total queries:%s" %(tlines+1, plines, len(qdct)))
    if args.periodic == 'y':
        #Store last run details in .dat file
        datdct = {'ts':dts, 'queries':sdct}
        with open(runfl, 'w') as wfl:
            pk.dump(datdct, wfl)

def runmain():
    epi = "Make sure the running program has read access to the log file"
    fldir = os.environ['HOME'] or '/dtmp'
    parser = argparse.ArgumentParser(description="Save user queries execution metrics to a csv file",
                                     epilog=epi)
    parser.add_argument('--logfile', default=_FNAME,
                        help='HiveServer2 Log file, Default: %s' %_FNAME)
    parser.add_argument('--dir', default=fldir,
                        help="Folder to save the csv files, Default: %s" %fldir)
    parser.add_argument('--periodic', default='n', choices={'y', 'n'},
                        help="Capture queries periodically, use with scheduler, Default:n")
    parser.add_argument('--format', default='c', choices={'c', 'j', 'b'},
                        help="File format(csv:c, json:j, both:b, Default:csv(c)")
    args = parser.parse_args()
    print("Starting queue metrics collection: %s" %datetime.now())
    get_queries(args)
    print("Collected query metrics to csv files: %s" %datetime.now())

if __name__ == '__main__':
    runmain()
