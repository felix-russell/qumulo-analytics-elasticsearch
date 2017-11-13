import json
import socket
import collections
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from qumulo.rest_client import RestClient

### Dict of clusters and credentials ###
with open('credentials.json') as credentialData:
    loadedCreds = json.load(credentialData)
    qClusters = loadedCreds['qClusters']
    credentialData.close()

### Declare domain-specific-language query body for searching for max document ID in index ###
esMaxIdSearchBody = {
  "sort": {
    "_uid": {
      "order": "desc"
    }
  },
  "size": 1
}

### Provide dict of ES index names to object names ###
esIndexes = {'qperf':'client', 'qfiles':'fileId', 'qcapacity':'path'}

### Initialize Elasticsearch Client Object, set Epoch Time ###
esClient = Elasticsearch("localhost", maxsize=1000)
epoch_time = int(time.time())

while True:
    ## Create Index for the day if it doesn't exist ###
    timeNow = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
    for index in esIndexes:
        iName = (index + '-' + str(datetime.now().date()))
        print('searching for index: ' + str(iName))
        if not esClient.indices.exists(index=iName):
            esClient.indices.create(index=iName)
            print('created index: ' + str(iName))
    
    ### Create Dict of Per-cluster Client Objects ###
    clusObjDct = collections.OrderedDict()
    for cluster in qClusters.keys():
        clusObjDct[cluster] = {}
        clusObjDct[cluster]['qClient'] = RestClient(cluster, 8000)
        clusObjDct[cluster]['qClient'].login(qClusters[cluster]['username'],qClusters[cluster]['password'])

    ### Retrieve Performance and Capacity Metrics from the Objects ###
    for cluster in clusObjDct.keys():
        print('fetching values for cluster: ' + cluster)
        clusObjDct[cluster]['entriesProcessed'] = 0
        clusObjDct[cluster]['qcapacity'] = clusObjDct[cluster]['qClient'].analytics.capacity_history_files_get(epoch_time)
        clusObjDct[cluster]['qperf'] = clusObjDct[cluster]['qClient'].analytics.current_activity_get()
        ### Dump JSON object with standard formatter, Load as Python-addressable Dict ###
        clusObjDct[cluster]['qcapacityJ'] = json.dumps(clusObjDct[cluster]['qcapacity'])
        clusObjDct[cluster]['qperfJ'] = json.dumps(clusObjDct[cluster]['qperf'])
        clusObjDct[cluster]['qcapacityJL'] = json.loads(clusObjDct[cluster]['qcapacityJ'])
        clusObjDct[cluster]['qperfJL'] = json.loads(clusObjDct[cluster]['qperfJ'])

    ### Initialize Dict for Combined/Transformed Performance Metrics ###
    aggregatorDict = collections.OrderedDict()
    for index in esIndexes.keys():
        aggregatorDict[index] = collections.OrderedDict()

    ### Init fun debug variables ###
    hostEntriesCount = fileEntriesCount = processedEntriesCount = perfDocsSubmitted = 0
    
    ## Init vars for fetching file paths ###
    batch_size = 100000
    fileIDpathDict = {}
    fileIDs = []

    ### Method for data transformation/combination of combined Cluster Performance Metrics ###
    def construct_client_data():
        if rateType in aggregatorDict['qperf'][client]['nodeTotals']:
            aggregatorDict['qperf'][client]['nodeTotals'][rateType] += rate
        else:
            aggregatorDict['qperf'][client]['nodeTotals'][rateType] = rate

    def construct_files_data():
        if rateType in aggregatorDict['qfiles'][fileId]['fileTotals']:
            aggregatorDict['qfiles'][fileId]['fileTotals'][rateType] += rate
        else: 
            aggregatorDict['qfiles'][fileId]['fileTotals'][rateType] = rate

    ### Create Dictionary of File IDs to paths ###
    for cluster in clusObjDct.keys():
        for entry in clusObjDct[cluster]['qperfJL']['entries']:
            fileIDs.append(entry['id'])
        for offset in range(0, len(fileIDs), batch_size):
            resolve_data = clusObjDct[cluster]['qClient'].fs.resolve_paths(fileIDs[offset:offset+batch_size])
            for id_path in resolve_data:
                fileIDpathDict[id_path['id']] = id_path['path']

    ### Iterate over clusters ###
    for cluster in clusObjDct.keys():
        ### Add Capacity Data for Clusters ###
        for path in clusObjDct[cluster]['qcapacityJL']['largest_paths']:
            pathName = path['path']
            aggregatorDict['qcapacity'][pathName] = {}
            aggregatorDict['qcapacity'][pathName]['path'] = path['path']
            aggregatorDict['qcapacity'][pathName]['capacityUsed'] = int(path['capacity_used'])
            aggregatorDict['qcapacity'][pathName]['@timestamp'] = timeNow
            aggregatorDict['qcapacity'][pathName]['cluster'] = cluster
        ### Iterate over entries in cluster's metrics entries ###
        for entry in clusObjDct[cluster]['qperfJL']['entries']:
            processedEntriesCount += 1
            clusObjDct[cluster]['entriesProcessed'] += 1
            ### Extract data from loaded dict ###
            hostIp = entry['ip']
            rateType = entry['type']
            rate = entry['rate']
            fileId = entry['id']
            ### Attempt to find hostname of the client ###
            try: 
                client = socket.gethostbyaddr(hostIp)[0]
            except:
                client = hostIp
            ### Add per-client performance metrics to the combined dict ###
            if client not in aggregatorDict['qperf']:
                hostEntriesCount += 1
                aggregatorDict['qperf'][client] = {}
                aggregatorDict['qperf'][client]['@timestamp'] = timeNow
                aggregatorDict['qperf'][client]['host'] = client
                aggregatorDict['qperf'][client]['nodeTotals'] = {}
                construct_client_data()
            else:
                construct_client_data()
            ### Add per-file performance metrics to the combined dict ###
            if fileId not in aggregatorDict['qfiles']:
                fileEntriesCount += 1
                aggregatorDict['qfiles'][fileId] = {}
                aggregatorDict['qfiles'][fileId]['@timestamp'] = timeNow
                aggregatorDict['qfiles'][fileId]['cluster'] = cluster
                aggregatorDict['qfiles'][fileId]['file'] = fileIDpathDict[fileId]
                aggregatorDict['qfiles'][fileId]['fileTotals'] = {}
                construct_files_data()
            else:
                construct_files_data()

    ### Print total entries Processed ###
    print('Total entries-collected:' + str(processedEntriesCount))
    for cluster in clusObjDct.keys():
        print(str(clusObjDct[cluster]['entriesProcessed']) + ' Entries: ' + cluster)

    ### Initialize variables for fileIDgenerator ###
    for index, subType in esIndexes.items():
        iName = (index + '-' + str(datetime.now().date()))
        print('Inserting Documents at : ' + str(iName))
        try:
            returnBody = esClient.search(index=iName, body=esMaxIdSearchBody)
            currentDocID = returnBody['hits']['total']
            print('Highest Doc ID for ' + iName + ' is ' + str(currentDocID))
        except:
            currentDocID = 0
        for entry in aggregatorDict[index]:
            currentDocID += 1
            aggDictJson = json.dumps(aggregatorDict[index][entry],indent=4,sort_keys=True)
            esClient.create(index=iName, doc_type=subType, body=aggregatorDict[index][entry], id=currentDocID)