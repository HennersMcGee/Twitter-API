# -*- coding: utf-8 -*-
"""
Created on Fri Jul 31 13:28:57 2020

@author: Henry
"""

#%% Set up

client_key = ''
client_secret = ''
bearer_token = ''

import requests
import pandas as pd
import base64

key_secret = '{}:{}'.format(client_key, client_secret).encode('ascii')
b64_encoded_key = base64.b64encode(key_secret)
b64_encoded_key = b64_encoded_key.decode('ascii')

#%%

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

#%%

auth_resp.status_code
auth_resp.json().keys()
access_token = auth_resp.json()['access_token']

#%%

search_headers = {
    'Authorization': 'Bearer {}'.format(access_token)    
}

search_params = {"q": 'surrey',
                 "result_type": 'recent',
                 #"until" : '2020-07-29',
                 #"locations" : '-0.03,51.23,0.01,51.27',
                 #"since_id" : '',
                 #"max_id" : '',
                 "geocode": '51.25,-0.01,30km',
                 "lang" : 'en',
                 "tweet_mode" : 'extended',
                 "count" : '10'}

search_url = '{}1.1/search/tweets.json'.format(base_url)

search_resp = requests.get(search_url, headers=search_headers, params=search_params)

search_resp.status_code

#%%

tweet_data = search_resp.json()

tweets = tweet_data['statuses']

tweets_df = pd.DataFrame(tweets)

for x in tweet_data['statuses']:
    try:
        print(x['created_at'] + ' - ' + x['retweeted_status']['full_text'] + '\n')
    except:
        print(x['created_at'] + ' - ' + x['full_text'] + '\n')


