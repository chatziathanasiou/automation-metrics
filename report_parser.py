'''Parses the automation json files, processes them and inserts selective data in the db'''
from datetime import datetime, timedelta, time
import sys
import json
import pymongo
from pymongo import WriteConcern

# Database Configurations
CONNECTION_STRING = 'mongodb+srv://georgiadis088:asdASD123@automationmetrics.jexoq.mongodb.net/automationMetrics?retryWrites=true&w=majority'
CLIENT = pymongo.MongoClient(CONNECTION_STRING)
DATABASE = CLIENT.automation_metrics_db
SCENARIO_COLLECTION = DATABASE.scenarios
TEST_RUNS_COLLECTION = DATABASE.test_runs


def parse_json(filename):
    ''' Opens the filename and parses json into a file'''
    file = open(filename, encoding="utf-8")
    report = json.loads(file.read())
    # Closing file
    file.close()
    return report


def retrieve_scenarios(report, brand, scope, date):
    ''' Processes the report and returns an array of all the relevant scenarios '''
    # Please don't try reading this
    total_duration = 0
    scenario_array = []
    failed_scenario_count = 0

    for og_feature in report:
        for og_scenario in og_feature['elements']:
            scenario = {}
            scenario['brand'] = brand
            scenario['scope'] = scope
            scenario['date'] = date
            scenario['feature'] = og_feature['name']
            scenario['name'] = og_scenario['name']
            scenario['tags'] = []
            if 'tags' in og_scenario: # We are appending all feature and scenario tags into scenario
                for tag in og_scenario['tags']:
                    scenario['tags'].append(tag['name'])
            if 'tags' in og_feature:
                for tag in og_feature['tags']:
                    scenario['tags'].append(tag['name'])
            scenario['result'] = 'passed'
            scenario['duration_in_ns'] = 0

            # Checking to see if the background step/scenario failed
            # to get the error to use for the actual scenario
            if 'type' in og_scenario:
                if og_scenario['type'] == 'background':
                    for step in og_scenario['steps']:
                        if 'result' in step:
                            if step['result']['status'] == 'failed':
                                error = step['result']['error_message']

            # For each step in each scenario
            if 'before' in og_scenario:
                for step in og_scenario['before']:
                    if 'result' in step:
                        if 'duration' in step['result']:
                            scenario['duration_in_ns'] += step['result']['duration']
                            total_duration += step['result']['duration']
                    if step['result']['status'] == 'failed' or step['result']['status'] == 'skipped':
                        scenario['result'] = 'failed'
                        if 'error_message' in step['result']:
                            error = step['result']['error_message']

            if 'steps' in og_scenario:
                for step in og_scenario['steps']:
                    if 'result' in step:
                        if 'duration' in step['result']:
                            scenario['duration_in_ns'] += step['result']['duration']
                            total_duration += step['result']['duration']
                    if step['result']['status'] == 'failed' or step['result']['status'] == 'skipped':
                        scenario['result'] = 'failed'
                        if 'error_message' in step['result']:
                            error = step['result']['error_message']

            if 'after' in og_scenario:
                for step in og_scenario['after']:
                    if 'result' in step:
                        if 'duration' in step['result']:
                            scenario['duration_in_ns'] += step['result']['duration']
                            total_duration += step['result']['duration']
                    if step['result']['status'] == 'failed' or step['result']['status'] == 'skipped':
                        scenario['result'] = 'failed'
                        if 'error_message' in step['result']:
                            error = step['result']['error_message']

            # Only appending the ones that are actual scenarios
            if 'type' in og_scenario:
                if og_scenario['type'] == 'scenario':
                    scenario['duration_in_ms'] = round(scenario['duration_in_ns'] / 1000000.00, 2)
                    scenario['duration_in_s'] = round(scenario['duration_in_ms'] / 1000.00, 2)

                    # Appending all the extra fields if the scenario failed
                    if scenario['result'] == 'failed':
                        scenario['error'] = error
                        scenario['root_cause'] = ''
                        scenario['related_jira'] = ''
                        failed_scenario_count += 1

                    scenario_array.append(scenario)

    return scenario_array, total_duration


def db_insert_scenarios(scenario_array):
    ''' Inserts the scenarios into the database '''
    SCENARIO_COLLECTION.with_options(write_concern=WriteConcern(w=0)).insert_many(scenario_array)


def db_insert_test_run(brand, date, duration):
    ''' Inserts the test run into the database or updates existing one '''

    query = {
        'date': {
            '$gt': datetime.combine(date, time()),
            '$lt': datetime.combine(date, time(hour=23, minute=59))
        },
        'brand': brand
    }

    test_run = list(TEST_RUNS_COLLECTION.find(query))

    # If no existing test run is found for the specific day, insert it
    if len(test_run) == 0:
        print('No test runs found for the specific day')

        test_run = {
            'date': date,
            'brand': brand,
            'parallel_duration_in_s': round(duration / 1000000000, 2), # Converting to seconds
            'full_duration_in_s': round(duration / 1000000000, 2)
        }
        TEST_RUNS_COLLECTION.with_options(write_concern=WriteConcern(w=0)).insert_one(test_run)

    # Else get the existing run so that we can decide how to calculate the parallel
    # And sequential durations and edit the existing record
    else:
        existing_run_max_parallel_duration = test_run[0]['parallel_duration_in_s']
        existing_run_max_full_duration = test_run[0]['full_duration_in_s']

        # Checking the previous noted duration against 
        # the one of this run to determine which is slower
        if existing_run_max_parallel_duration > round(duration / 1000000000, 2):
            parallel_duration_in_s = existing_run_max_parallel_duration
        else:
            parallel_duration_in_s = round(duration / 1000000000, 2)

        new_values = {
            '$set': {
                'full_duration_in_s': existing_run_max_full_duration + round(duration / 1000000000, 2),
                'parallel_duration_in_s': parallel_duration_in_s
            }
        }
        TEST_RUNS_COLLECTION.update_one(query, new_values)


def archive_data():
    ''' Archives old data from the database '''

    archive_limit = 45
    archive_period = datetime.today() - timedelta(days=archive_limit)
    query = {
        'date':{'$lt': archive_period}
        }

    print(f'Archiving records from the scenario collection, older than {archive_limit} days old')
    scenario_cursor = SCENARIO_COLLECTION.delete_many(query)
    print(scenario_cursor.deleted_count, ' documents deleted from the scenarios collection')

    print(f'Archiving records from the test runs collection, older than {archive_limit} days old')
    test_run_cursor = TEST_RUNS_COLLECTION.delete_many(query)
    print(test_run_cursor.deleted_count, ' documents deleted from the test runs collection')


def main():
    ''' main '''

    # Archive data older than 75 days
    archive_data()

    # Grabbing in the script arguments
    try:
        json_filename = sys.argv[1]
        report_brand = sys.argv[2]
        report_scope = sys.argv[3]
    except KeyError:
        print('Please provide a sufficient number of arguments')
        exit()

    # Setting date as 16 hours before the script runs to ensure it falls within the previous day
    date_of_run = datetime.today() - timedelta(hours=16)

    # processing the json report
    test_run_report = parse_json(json_filename)
    scenario_array, report_total_duration = retrieve_scenarios(test_run_report, report_brand, report_scope, date_of_run)

    # Inserting the scenarios into the scenarios collection
    db_insert_scenarios(scenario_array)

    # Inserting or editing an exisisting test run record
    db_insert_test_run(report_brand, date_of_run, report_total_duration)


main()
