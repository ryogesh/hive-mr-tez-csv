
# Capture Map Reduce and Tez Job Counters
## Overview

This repository contains the scripts to capture Map Reduce(MR) and Tez Job Counters as csv files. Comes handy when analyzing job details.

## Script Features
- Capture MR job counters and additional information using the MR REST API
- For MR jobs, creates a job summary and tasks details csv files
- Capture Tez job counters and additional information using the Yarn Timeline Server API and Yarn logs command
- For Tez jobs, creates a job counters csv file

## Installation
- A Linux/Unix based system
- [Python](https://www.python.org/downloads/) 2.7

### Additional python modules used
pycurl

### Running the scripts
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
