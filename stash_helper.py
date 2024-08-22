''' Module to help us interact with the stash aPI and get daily merged code '''

import datetime as dt
import time
import requests
import pymongo
from pymongo import WriteConcern

class StashHelper():
    ''' Class to help us interact with the stash API '''
    def __init__(self) -> None:
        self.base_url = 'https://bitbucket.openbet.com/rest/api/1.0/projects/MARS'
        self.auth = ('builder','to1Sahcu')


    def get_repositories(self):
        ''' Gets all the repositories in the MARS project '''
        url = f'{self.base_url}/repos'
        parameters = {'limit': 250}

        response = requests.get(url, auth=self.auth, params=parameters)
        repositories = response.json()

        repository_slug_list = []
        for repository in repositories['values']:
            if 'deprecated' not in repository['slug'].lower() and 'abandoned' not in repository['slug'].lower():
                repository_slug_list.append(repository['slug'])

        return repository_slug_list


    def get_merge_commits_for_today(self, repository_list):
        ''' Gets the merge commits for a specific date '''

        # Generating start and end of date datetime objects to compare commits
        date = dt.datetime.combine(dt.datetime.today() - dt.timedelta(days=1), dt.time())

        parameters = {
            'limit': 5,
            'merges': 'only'
        }

        todays_commit_list = []
        for repository in repository_list:
            time.sleep(1)
            url = f'{self.base_url}/repos/{repository}/commits'
            response = requests.get(url, auth=self.auth, params=parameters)
            commits = response.json()

            # If it was merged today, append it into the array, otherwise go to the next repository
            for commit in commits['values']:
                if dt.datetime.fromtimestamp(commit['authorTimestamp']/1000) > date:
                    commit['repository'] = repository
                    try:
                        commit['related_jira'] = commit['properties']['jira-key'][0]
                    except KeyError:
                        pass
                    commit['date'] = dt.datetime.fromtimestamp(commit['authorTimestamp']/1000)
                    todays_commit_list.append(commit)
                else:
                    break

        return todays_commit_list


    def add_merge_commits(self, commit_list):
        ''' Adds the commits for the day into the database '''
        connection_string = 'mongodb+srv://georgiadis088:asdASD123@automationmetrics.jexoq.mongodb.net/automationMetrics?retryWrites=true&w=majority'
        client = pymongo.MongoClient(connection_string)
        database = client.automation_metrics_db
        merge_commit_collection = database.merge_commits

        if len(commit_list) > 0:
            merge_commit_collection.with_options(write_concern=WriteConcern(w=0)).insert_many(commit_list)
        else:
            print('No results were returned for yesterday')


# Main
stash_helper = StashHelper()
repository_list = stash_helper.get_repositories()
commit_list = stash_helper.get_merge_commits_for_today(repository_list)
stash_helper.add_merge_commits(commit_list)