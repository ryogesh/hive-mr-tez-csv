
# Capture Map Reduce and Tez Job Counters
## Overview

This repository contains scripts to capture Hive Queries, Yarn Queue usage, Map Reduce(MR) and Tez Job Counters as csv files. Comes handy when analyzing activities on the Hadoop cluster.

## Script Features
- mr-jobs-csv.py: Capture MR job counters and additional information using the MR REST API. Creates a MR job summary and tasks details csv files.
- tez-app-csv.py: Capture Tez job counters and additional information using the Yarn Timeline Server API and Yarn logs command. Creates a Tez job counters csv file.
- yarn-queues-csv.py: Capture Yarn queue usage. Can be scheduled to gather metrics periodically.
- hs2_log_csv.py: Extracts Hive queries and it's metrics from HiveServer2 log file. Can be scheduled to gather metrics periodically.

## Installation
- A Linux/Unix based system
- [Python](https://www.python.org/downloads/) 2.7

### Additional python modules used
pycurl

### Running the scripts
- Capturing Hive queries. Creates a csv(default), json or both file with compile, execution times, queryuser, queuename, full query and yarn application id.

   ```
   [user@localhost ~]$ python hs2_log_csv.py
   ```    
   
    Additional information on the script
    ```
    [user@localhost ~]$ python hs2_log_csv.py -h
    usage: hs2_log_csv.py [-h] [--logfile LOGFILE] [--dir DIR] [--periodic {y,n}]
                          [--format {c,b,j}]

    Save user queries execution metrics to a csv file

    optional arguments:
      -h, --help         show this help message and exit
      --logfile LOGFILE  HiveServer2 Log file, Default:
                         /var/log/hive/hiveserver2.log
      --dir DIR          Folder to save the csv files, Default: /home/user
      --periodic {y,n}   Capture queries periodically, use with scheduler,
                         Default:n
      --format {c,b,j}   File format(csv:c, json:j, both:b, Default:csv(c)

    Make sure the program has access to the log file
    ```
        
  Scheduling using cron to run every 15 minutes
  ```
  */15 * * * * python hs2_log_csv.py --format b --periodic y >> ~/queues.log 2>&1
  ```

- Capturing MR job information, requires MR jobId

    ```
    [user@localhost ~]$ python mr-jobs-csv.py <jobId>
    ```
    
    Additional information on the script
    ```
    [user@localhost ~]$ python mr-jobs-csv.py -h
    usage: mr-jobs-csv.py [-h] [--base_url BASE_URL] [--kerberos KERBEROS]
                          [--cacert CACERT] [--dir DIR] [--verbose VERBOSE]
                          jobid

    Save MapReduce job and counters information as csv files.Creates job summary
    and task details files

    positional arguments:
      jobid                Mapreduce Job Id

    optional arguments:
      -h, --help           show this help message and exit
      --base_url BASE_URL  URL for Mapreduce job history server, Default:
                           http://localhost:19888
      --kerberos KERBEROS  Kerberos Authentication enabled(y)/disabled(n),
                           Default: Enabled(y)
      --cacert CACERT      Location of CACERT, e.g /opt/anaconda3/lib/python2.7
                           /site-packages/certifi/cacert.pem, Default: None
      --dir DIR            Folder to save the csv files, Default: /home/user
      --verbose VERBOSE    Debug Curl request, Default: n

    Make sure the TGT is in the cache e.g. kinit <userid>
    ```
    
- Capturing Tez job information, requires Yarn applicationId

    ```
    [user@localhost ~]$ python mr-jobs-csv.py <applicationId>
    ```
    
    Additional information on the script
    ```
    [user@localhost ~]$ python tez-app-csv.py -h
    usage: tez-app-csv.py [-h] [--base_url BASE_URL] [--kerberos KERBEROS]
                          [--cacert CACERT] [--dir DIR] [--verbose VERBOSE]
                          appid

    Save Tez job counters information as a csv file

    positional arguments:
      appid                Yarn Application Id

    optional arguments:
      -h, --help           show this help message and exit
      --base_url BASE_URL  URL for Yarn Timeline server, Default:
                           http://localhost:8188
      --kerberos KERBEROS  Kerberos Authentication enabled(y)/disabled(n),
                           Default: Enabled(y)
      --cacert CACERT      Location of CACERT, e.g /opt/anaconda3/lib/python2.7
                           /site-packages/certifi/cacert.pem, Default: None
      --dir DIR            Folder to save the csv files, Default: /home/user
      --verbose VERBOSE    Debug Curl request, Default: n

    Make sure the TGT is in the cache e.g. sudo kinit -kt
    /etc/security/keytab/yarn.service.keytab yarn/localhost.localdomain
    ```
- Capturing Yarn Queue Usage
    
    ```
    [user@localhost ~]$ python  yarn-queues-csv.py
    ```
 
    Additional information on the script
    ```
    [user@localhost ~]$ python  yarn-queues-csv.py -h
    usage: yarn-queues-csv.py [-h] [--base_url BASE_URL] [--kerberos KERBEROS]
                          [--TGT TGT] [--cacert CACERT] [--dir DIR]
                          [--verbose VERBOSE]

    Save Yarn queue metrics to a csv file

    optional arguments:
      -h, --help           show this help message and exit
      --base_url BASE_URL  URL for Yarn Resource Manager, Default:
                           http://localhost:8088
      --kerberos KERBEROS  Kerberos Authentication enabled(y)/disabled(n),
                           Default: Enabled(y)
      --TGT TGT            Create TGT(kinit) enabled(y)/disabled(n), Default:
                           Disabled(n)
      --cacert CACERT      Location of CACERT, e.g /opt/anaconda3/lib/python2.7
                           /site-packages/certifi/cacert.pem, Default: None
      --dir DIR            Folder to save the csv files, Default: /home/user
      --verbose VERBOSE    Debug Curl request, Default: n

    Make sure the TGT is in the cache, if --TGT is set to n
    ```
    
  Scheduling using cron to run every minute
  ```
  */1 * * * * python yarn-queues-csv.py >> ~/queues.log 2>&1
  ```
