# -*- coding: utf-8 -*-
'''
    File name: twitter_functions.py
    Author: Henry Letton
    Date created: 2020-08-17
    Python Version: 3.7.3
    Desciption: Functions to extract data, output datasets, and write data to 
                database.
'''

#%% Required modules
import requests
import pandas as pd
import base64
import numpy as np
import mysql.connector

#%% Get search data
def store_search_data(search):
    
    tweets_list = search_tweets(search = search)
    
    tweets_f, users_f = list_to_data(tweets_list)

    data_to_db(tweets = tweets_f, users = users_f, search = search)


#%% Generate access token - https://benalexkeen.com/interacting-with-the-twitter-api-using-python/
def generate_access_token(client_key = '',
                          client_secret = '',
                          bearer_token = ''):
    
    # Code taken from link above
    key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')
    b64_encoded_key = base64.b64encode(key_secret)
    b64_encoded_key = b64_encoded_key.decode('ascii')
    
    base_url = 'https://api.twitter.com/'
    auth_url = '{}oauth2/token'.format(base_url)
    
    auth_headers = {
        'Authorization': 'Basic {}'.format(b64_encoded_key),
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
    }
    
    auth_data = {
        'grant_type': 'client_credentials'
    }
    
    auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)
    
    auth_resp.status_code
    auth_resp.json().keys()
    return auth_resp.json()['access_token']


#%% Get twitter list based on search
def search_tweets(search = '',
                  results = 10,
                  api_requests_limit = 3,
                  results_type = 'recent'):

    # Inital variable to start while loop
    num_results = 999
    api_requests = 0
    tweets = []
    max_id = 9999999999999999999
    
    # Recall api until no more responses, or api_requests_limit hit
    while num_results > 0 and api_requests < api_requests_limit:
    
        # API call
        base_url = 'https://api.twitter.com/'
        access_token = generate_access_token()
        search_headers = { 'Authorization': 'Bearer {}'.format(access_token) }
        search_params = {"q": search,
                         "result_type": results_type,
                         #"until" : '2020-07-29',
                         #"locations" : '-0.03,51.23,0.01,51.27',
                         #"since_id" : '',
                         "max_id" : max_id,
                         "geocode": '51.23,-0.42,30km',
                         "lang" : 'en',
                         "tweet_mode" : 'extended',
                         "count" : str(results)}
        search_url = '{}1.1/search/tweets.json'.format(base_url)
        search_resp = requests.get(search_url, headers=search_headers, params=search_params)
        
        # 200 means successful query
        print(search_resp.status_code)
        
        # Append current api call to all previous
        tweet_data = search_resp.json()
        tweets = tweets + tweet_data['statuses']
        
        # Update loop variables
        max_id = pd.DataFrame(tweets)['id'].min() - 1
        num_results = len(tweet_data['statuses'])
        api_requests += 1
        
    # Extract any extra tweet data from the quoted or retweeted options
    for tweet in tweets.copy():
        # If they quoted a tweet, get that info
        try:
            tweets.append(tweet['quoted_status'])
        except:
            pass
        
        # If they retweeted, get original and delete retweet
        try:
            test = tweet['retweeted_status']
            tweets.append(tweet['retweeted_status'])
            tweets.remove(tweet)
        except:
            pass
        
    return tweets


#%% Uses twitter search(es) to get data sets
def list_to_data(tweets_list):

    # To data frame
    tweets = pd.DataFrame(tweets_list)
    users = pd.DataFrame(list(tweets['user']))
    
    # Dedupe
    tweets_d = tweets.drop_duplicates('id')
    users_d = users.drop_duplicates('id')
    
    # Tweet data - extract, rename, select
    tweets_d['longitude'] = [d.get('coordinates')[0] if d is not None else np.NaN for d in tweets_d['coordinates']]
    tweets_d['latitude'] = [d.get('coordinates')[1] if d is not None else np.NaN for d in tweets_d['coordinates']]
    tweets_d['place'] = [d.get('full_name') if d is not None else None for d in tweets_d['place']]
    tweets_d['user_id'] = [d.get('id') if d is not None else None for d in tweets_d['user']]
    try:
        tweets_d['quoted_status_id'] = int(tweets_d['quoted_status_id_str'])
    except:
        tweets_d['quoted_status_id'] = np.NaN
    tweets_d['created_at'] = pd.to_datetime(tweets_d['created_at'], format='%a %b %d %H:%M:%S +0000 %Y')
    tweets_d.rename(columns={'id':'tweet_id', 'favorite_count':'favourite_count'}, inplace=True)
    tweets_f = tweets_d[['tweet_id', 'full_text', 'favourite_count', 'retweet_count',\
                     'longitude', 'latitude', 'place',\
                     'user_id', 'quoted_status_id', 'created_at']]
    
    # User data - extract, rename, select
    users_d['created_at'] = pd.to_datetime(users_d['created_at'], format='%a %b %d %H:%M:%S +0000 %Y')
    users_d.rename(columns={'id':'user_id', 'created_at':'user_created_at'}, inplace=True)
    users_f = users_d[['user_id', 'name', 'screen_name', 'description',\
                     'favourites_count', 'followers_count', 'friends_count',\
                     'statuses_count', 'location', 'user_created_at']]
    
    return tweets_f, users_f


#%% Data into database
def data_to_db(tweets = [],
               users = [],
               search = ''):

    # Connect to database
    conn = mysql.connector.connect(user='u235764393_HL', 
                                  password='',
                                  host='sql134.main-hosting.eu',
                                  database='u235764393_HLDB')
    cursor = conn.cursor()
    
    # Only load in tweets if given
    if len(tweets) > 0:
    
        # Cleaning
        tweets_clean = tweets.astype(str)
        tweets_clean = tweets_clean.replace(r"^nan$", r"NULL", regex = True)
        tweets_clean = tweets_clean.replace(r"^None$", r"", regex = True)
        tweets_clean = tweets_clean.replace(r"\\", r"\\\\", regex = True)
        tweets_clean = tweets_clean.replace(r"'", r"\'", regex = True)
        
        # Insert list
        insert_text = "REPLACE INTO Twitter_Tweets "\
                       "(tweet_id, full_text, favourite_count, retweet_count, "\
                       "longitude, latitude, place, user_id, quoted_status_id, "\
                       "created_at) "\
                       "VALUES (" + tweets_clean['tweet_id'] + ", '" + tweets_clean['full_text'] + "', " +\
                       tweets_clean['favourite_count'] + ", " + tweets_clean['retweet_count'] + ", " +\
                       tweets_clean['longitude'] + ", " + tweets_clean['latitude'] + ", '" +\
                       tweets_clean['place'] + "', " + tweets_clean['user_id'] + ", " +\
                       tweets_clean['quoted_status_id'] + ", '" + tweets_clean['created_at'] + "')"
        
        for insert in insert_text:
            cursor.execute(insert)
            print('Row inserted')
    
    # Only load in users if given
    if len(users) > 0:
    
        # Cleaning
        users_clean = users.astype(str)
        users_clean = users_clean.replace(r"^nan$", r"NULL", regex = True)
        users_clean = users_clean.replace(r"^None$", r"", regex = True)
        users_clean = users_clean.replace(r"\\", r"\\\\", regex = True)
        users_clean = users_clean.replace(r"'", r"\'", regex = True)
        
        # Insert list
        insert_text = "REPLACE INTO Twitter_Users "\
                       "(user_id, name, screen_name, description, "\
                       "favourites_count, followers_count, friends_count, statuses_count, location, "\
                       "user_created_at) "\
                       "VALUES (" + users_clean['user_id'] + ", '" + users_clean['name'] + "', '" +\
                       users_clean['screen_name'] + "', '" + users_clean['description'] + "', " +\
                       users_clean['favourites_count'] + ", " + users_clean['followers_count'] + ", " +\
                       users_clean['friends_count'] + ", " + users_clean['statuses_count'] + ", '" +\
                       users_clean['location'] + "', '" + users_clean['user_created_at'] + "')"
        
        for insert in insert_text:
            cursor.execute(insert)
            print('Row inserted')
        
    # Only include search if actually searched
    if search != '':
        
        # Insert list
        insert_text = "REPLACE INTO Twitter_Searches "\
                       "(tweet_id, search) "\
                       "VALUES (" + tweets_clean['tweet_id'] + ", '" + search + "')"
        
        for insert in insert_text:
            cursor.execute(insert)
            print('Row inserted')
            
    # Close connection to database, commit changes first
    conn.commit()
    conn.close()


#%% Test run

#tweets_list = search_tweets(search = 'surrey park')

#tweets_f, users_f = list_to_data(tweets_list)

#data_to_db(tweets = tweets_f, users = users_f, search = 'surrey park')

#store_search_data('surrey park')


#%% Future ideas: refresh users, refresh popular tweets, get all user tweets


# Fix date
#from datetime import datetime
#from time import strftime
#date_time_obj = datetime.strptime(users_d['created_at'][0], '%a %b %d %H:%M:%S +0000 %Y')
#date_time_obj.strftime('%Y-%m-%d %H:%M:%S')



