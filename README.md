qumulo-analytics-elasticsearch

This script will collect analytics data from qumulo storage clusters, transform and format data, and submit the results to an elasticsearch cluster of your choice

It includes the following:

- metrics.py: A script that queries qumulo storage clusters and submits analytics data to elasticsearch
- credentials.json: Simple json for hostname and credential combinations for the qumulo rest client to query
- requirements.txt: A list of python packages required for the script to run
- qMetricsExamples.json: A quick-start template for visualizing script output in Kibana

For quick testing/development setup:
1. Download the qumulo command line tools
   * Visit the storage cluster web interface
   * Under 'APIs & Tools' -> click 'Download command-line tools'
2. Create Qumulo API user
   * Visit the web interfaces of the storage clusters to query, login as admin
   * Under 'Sharing' -> 'Users & Groups' create a qumulo-metrics user with API read privileges
3. Prepare the python requirements
   * Clone this repository
   * Install requirements via 'pip install -r requirements.txt from inside the folder
4. Prepare required files
   * Copy 'metrics.py' and 'credentials.json' to the unzipped qumulo comand line tools folder
   * Open credentials.json and fill out credentials from step 2 for each cluster
   * Add/remove json entries as needed
5. Customize elasticsearch output endpoint(s)
   * Inside metrics.py enter the hostname of your elasticsearch endpoint(s)
   * If you use deviantony/docker-elk customize the 'docker-compose.yml' for additional JVM heap size with "ES_JAVA_OPTS: "-Xmx4g -Xms4g" and increase your docker engine to a minimum of 8GB memory and 4 cores
   * This script will default to the localhost elasticsearch cluster
6. Start the script
   * In the converged folder execute 'python metrics.py' and monitor the output of the script to ensure the shipper is working.
7. Visualize Outputs:
   * Visit Kibana at: http://127.0.0.1:5601
   * create default index pattern 'qperf-*' and 'create'
   * Under 'Management' -> 'Index Patterns' -> 'Create Index Pattern' add 'qcapacity-*' and 'qfiles-*'
   * Under 'Management' -> 'Saved Objects' -> 'Import' upload the qMetricsExamples.json file
   * Under 'Dashboard' -> 'Open' select 'MasterDashQ' and edit the time-frame as desired.

Author: Felix Russell
(c) University of Washington Institute for Health Metrics and Evaluation
