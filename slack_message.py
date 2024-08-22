''' Script to send slack notifications '''
import json
import sys
from datetime import datetime, timedelta, time
import pymongo
import requests


# Database Configurations
CONNECTION_STRING = 'mongodb+srv://georgiadis088:asdASD123@automationmetrics.jexoq.mongodb.net/automationMetrics?retryWrites=true&w=majority'
CLIENT = pymongo.MongoClient(CONNECTION_STRING)
DATABASE = CLIENT.automation_metrics_db
SCENARIO_COLLECTION = DATABASE.scenarios
TEST_RUNS_COLLECTION = DATABASE.test_runs


def get_total_scenarios(brand):
    ''' Gets the total scenarios for that speficic installation for yesterday'''
    query = {
        'date': {
            '$gt': datetime.combine((datetime.today() - timedelta(days=1)), time()),
            '$lt': datetime.combine((datetime.today() - timedelta(days=1)), time(hour=23,minute=59))
        },
        'brand': brand
    }
    total_scen = len(list(SCENARIO_COLLECTION.find(query)))
    return total_scen


def get_failed_scenarios(brand):
    ''' Gets the failing scenarios for that speficic installation for yesterday'''
    query = {
        'date': {
            '$gt': datetime.combine((datetime.today() - timedelta(days=1)), time()),
            '$lt': datetime.combine((datetime.today() - timedelta(days=1)), time(hour=23,minute=59))
        },
        'brand': brand,
        'result': 'failed'
    }

    failed_scen = len(list(SCENARIO_COLLECTION.find(query)))
    return failed_scen


def get_run_duration(brand):
    ''' Gets the failing scenarios for that speficic installation for yesterday'''
    query = {
        'date': {
            '$gt': datetime.combine((datetime.today() - timedelta(days=1)), time()),
            '$lt': datetime.combine((datetime.today() - timedelta(days=1)), time(hour=23,minute=59))
        },
        'brand': brand
    }

    try:
        total_duration = list(TEST_RUNS_COLLECTION.find(query))[0]['parallel_duration_in_s']
        return int(total_duration)
    except KeyError:
        total_duration = None
        print(f'No test run information could be retrieved for {brand}')
        return False


def prepare_message(brand, total_tests, failed_tests, pass_rate, duration):
    ''' Prepares the slack message to be sent '''
    if brand == 'OB1':
        title = ':Coral: OB1 Regression Results'
        color = '2272EC'
    elif brand == 'OB2':
        title = ':Coral: OB2 Regression Results'
        color = '38ACDF'
    elif brand == 'OB3':
        title = ':Ladbrokes: OB3 Regression Results'
        color = 'E01C08'
    elif brand == 'OB2_Poolbets':
        title = ':Coral: OB2 Poolbets Regression Results'
        color = '2C0501'

    message = f"Total Tests: `{total_tests}`  "
    message += f"Failed Tests: `{failed_tests}`"
    message += f"\nPass Rate: `{pass_rate}%`"
    message += f"\nRun Duration: `{timedelta(seconds=duration)}`"

    slack_data = {
        "username": "Nightly Regression",
        "attachments": [
            {
                "color": color,
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}

    return slack_data, headers


def send_message(url, brand, total_tests, failed_tests, pass_rate, duration):
    ''' Sends the slack message '''
    slack_data,headers = prepare_message(brand, total_tests, failed_tests, pass_rate, duration)
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)


# main
url= 

# Grabbing in the script arguments
report_brand = sys.argv[1]

# Getting the required data
total_scenarios = get_total_scenarios(report_brand)
if total_scenarios > 0:
    failed_scenarios = get_failed_scenarios(report_brand)
    pass_rate = round(100 - (failed_scenarios/total_scenarios) * 100, 2)
else:
    print(f'No scenarios were found yesterday for {report_brand}')
    sys.exit()

duration = get_run_duration(report_brand)
if duration is False:
    print(f'No test run data were found yesterday for {report_brand}')
    sys.exit()

# Sending the message
send_message(url, report_brand, total_scenarios, failed_scenarios, pass_rate, duration)
