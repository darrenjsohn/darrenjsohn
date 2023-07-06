import pandas as pd
import tweepy
import datetime as DT
import re
import requests
import json
from collections import namedtuple
from contextlib import closing
import sqlite3
from prefect import task, Flow
import pyodbc
import pandas as pd
import os
from prefect.schedules import IntervalSchedule
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, WEEKLY
import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import RRuleClock
from datetime import date, timedelta
import datetime as dt
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc 

bearer_token = #
consumer_key = #
consumer_secret = #
access_token = #
access_token_secret = #
##Extract1
@task 

def initialization(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret):
# Initializing the client
    client = tweepy.Client(
        bearer_token = bearer_token,
        consumer_key = consumer_key,
        consumer_secret = consumer_secret,
        access_token = access_token,
        access_token_secret = access_token_secret,
        wait_on_rate_limit = True
    )
    return client

#Extract2
@task
def call_twitter_api(client):
    query = '(\"boot barn\" OR \"bootbarn\") -is:retweet'

    #Search the past seven days for tweets that include the words boot barn or bootbarn
    tweet_paginator = tweepy.Paginator(
        client.search_recent_tweets,
        query = query, 
        expansions = 'author_id',
        max_results = 100,
        tweet_fields = ["created_at", "text", "public_metrics"],
        user_fields = ["name", "username", "location", "verified"]
    )
    return tweet_paginator
    

##Transform 
@task  

def transform_data(client, tweet_paginator):
    tweets = []
    users = []

    for tweet_page in tweet_paginator:
        tweets += tweet_page.data
        users += tweet_page.includes["users"]

    # Data frame for tweet metrics
    text_data = pd.DataFrame.from_dict(tweets)
    # text_data.head(10)

    text_data[['retweet_count', 'reply_count', 'like_count', 'quote_count']] = pd.DataFrame(text_data.public_metrics.tolist(), 
                                                                                          index = text_data.index)
    text_data = text_data.drop(['public_metrics'], axis = 1)

    for i in range(len(text_data['text'])):
        text_data.iloc[i, 3] = re.sub(r'https\S+', '', text_data['text'][i])

    # Data frame for user metrics
    user_data = pd.DataFrame.from_dict(users)
    user_data = user_data.rename(columns = {'id': 'author_id'})
    # user_data.head()

    # Number of unique users this week
    user_count = len(user_data['author_id'])

    # Creating empty lists for follower and following count to add to result data frame
    follower_count = [0] * user_count
    following_count = [0] * user_count    

    # Looping through all users and running get_users endpoint to get the number of followers and following
    for i in range(user_count):
        user_metrics = client.get_users(ids = user_data['author_id'][i], user_fields = ['public_metrics'])
        follower_count[i] = user_metrics.data[0].public_metrics['followers_count']
        following_count[i] = user_metrics.data[0].public_metrics['following_count']

    # Adding new metrics to the user data frame
    user_data['follower_count'] = follower_count
    user_data['following_count'] = following_count

    # Combining tweet and user metrics into single data frame
    result = pd.merge(text_data, user_data)
    # result

    # Cleaning the data frame to be exported (remove duplicates, reindexing, reordering columns)
    result = result.drop_duplicates()
    result = result.reset_index()
    result = result.drop(columns = 'index')
    result = result[['id', 'text', 'created_at', 
                     'retweet_count', 'reply_count', 'like_count', 'quote_count', 
                     'author_id', 'username', 'name', 'follower_count', 'following_count', 
                     'location', 'verified']]
    result=result.reset_index(drop=True)
    return result

##load
### DF TO SQL 

##servername: bbsqldev01
##database: sql database name - crm, dw2
##data: dataframe in python 
##method: ‘fail’, ‘replace’, ‘append’
##myTable: name of table in SQL database

class data_to_sql():
    def __init__(self,servername, database, data, method, myTable):
        self.servername=servername
        self.database=database
        self.data=data
        self.myTable=myTable
        self.method=method
    def run(self): 
            ##create connection
        connection_string = "Driver={SQL Server Native Client 11.0};"+"Server={0};Database={1};Trusted_connection=yes;".format(self.servername,self.database)
        quoted = urllib.parse.quote_plus(connection_string)
            ##connecting to the server
        engine=create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
            #taking the column names of table on sql
        query= "SELECT TOP 1 * FROM {0}".format(self.myTable)
        sql_table = pd.read_sql_query(query, engine)
            
            #convert column name to list and make them all lowercase
        a=(map(lambda x: x.lower(), sql_table.columns))
        b=(map(lambda x: x.lower(), self.data.columns))
            
            #if column name in table on sql is the same as data column name, then export data. Else, error message
        if list(a)==list(b):
            try:
                with engine.connect() as cnn: 
                    for i in range(len(self.data)):
                        try:
                            self.data.iloc[i:i+1].to_sql(self.myTable,con=cnn, if_exists=self.method, index=False)
                        except IntegrityError:
                            pass
                    print(self.myTable+' loaded successfully')      
            except Exception as e :
                e=str(e).replace(".","")
                print(f"{e} in Database." )
        else:
            print("header name doesn't match")

@task
def loader(result):
    data_to_sql(servername='BBSQLDEV01',database='crm', data=result, method='append', myTable='twitter_feed').run()
    
    
start_date = datetime(2022,7,18,1,0,0)
r_rule = rrule(WEEKLY, byweekday=0, byhour=8)

schedule = Schedule(clocks=[RRuleClock(r_rule, start_date=start_date)])
##schedule = IntervalSchedule(start_date=datetime(2022,7,18,13,57,0),interval=timedelta(minutes=1))
with Flow("Twitter Feed", schedule=schedule) as f:
    client=initialization(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret)
    tweet_paginator= call_twitter_api(client)
    result=transform_data(client, tweet_paginator)
    loader(result)
    
f.run()
