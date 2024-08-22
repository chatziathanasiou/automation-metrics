''' This script replicates any new records from the cloud database to the local one on VM02 '''
''' Records that get updates after being replicated will not be replicated again so we need to be careful with the timing of this script '''

import pymongo
from pymongo import WriteConcern

# Local Database Connection Info
local_db_conn_string = 'mongodb://10.194.33.118:27017/qadb'
local_db_client = pymongo.MongoClient(local_db_conn_string)
local_db_name = local_db_client.automation_metrics_db

# Cloud Database Connection Info
cloud_db_connection_string = 'mongodb+srv://georgiadis088:asdASD123@automationmetrics.jexoq.mongodb.net/automationMetrics?retryWrites=true&w=majority'
cloud_db_client = pymongo.MongoClient(cloud_db_connection_string)
cloud_db_name = cloud_db_client.automation_metrics_db

def replicate_collection(name):
    ''' Takes the collection name as an argument and
    replicates new items in that collection from the
    cloud DB to the local one '''

    print(f'Replicting for the {name} collection')
    local_db_scenario_collection = local_db_name[name]
    cloud_db_scenario_collection = cloud_db_name[name]

    # Queries for the last available date in the local database so that it replicates anything after that
    projection = {'date':1}
    newest_coll_item_date = local_db_scenario_collection.find({}, projection).sort('date',pymongo.DESCENDING).limit(1)
    
    # This check is in case the local DB is empty. We could possibly delete this as it was only needed once.
    try:
        newest_coll_item_date = newest_coll_item_date[0]['date']
        print(f'The date of the last item that was found is {newest_coll_item_date}')
        query = {'date': {'$gt': newest_coll_item_date}}
    except KeyError:
        print('There were no items found for this collection')
        query = ''

    # Using the data above, we query the cloud DB for any newer items
    item_list = list(cloud_db_scenario_collection.find(query))
    
    # If any items are found, we insert them into the local DB
    if len(item_list) > 0:
        print(f'Number of items to replicate: {len(item_list)}')
        replication_result = local_db_scenario_collection.insert_many(item_list)
        print(f'Number of items replicated: {len(replication_result.inserted_ids)}')
    else:
        print('There were no items to be replicated')


# Main
replicate_collection('scenarios')
replicate_collection('test_runs')
replicate_collection('merge_commits')
