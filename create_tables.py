# -*- coding: utf-8 -*-
'''
    File name: create_tables.py
    Author: Henry Letton
    Date created: 2020-08-17
    Python Version: 3.7.3
    Desciption: Create empty tables to store Twitter data
'''

#%% Required modules

import mysql.connector

#%% Connect to database
conn = mysql.connector.connect(user='u235764393_HL', 
                              password='',
                              host='sql134.main-hosting.eu',
                              database='u235764393_HLDB')

cursor = conn.cursor()

#%% Create table to store tweets
try:
    cursor.execute("CREATE TABLE Twitter_Tweets ("
            "tweet_id BIGINT,"
            "full_text TEXT,"
            "favourite_count INT,"
            "retweet_count INT,"
            "longitude FLOAT,"
            "latitude FLOAT,"
            "place TEXT,"
            "user_id BIGINT,"
            "quoted_status_id BIGINT,"
            "created_at TIMESTAMP,"
            "timestamp_t TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( tweet_id ) )")
except:
    print("Tweet table already exists")

#%% Create table to store users
try:
    cursor.execute("CREATE TABLE Twitter_Users ("
            "user_id BIGINT,"
            "name TEXT,"
            "screen_name TEXT,"
            "description TEXT,"
            "favourites_count INT,"
            "followers_count INT,"
            "friends_count INT,"
            "statuses_count INT,"
            #"geo_enabled TINYINT,"
            "location TEXT,"
            "user_created_at TIMESTAMP,"
            "timestamp_u TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( user_id ) )")
except:
    print("User table already exists")

#%% Create table to store searches
try:
    cursor.execute("CREATE TABLE Twitter_Searches ("
            "tweet_id BIGINT,"
            "search TEXT,"
            "timestamp_s TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( tweet_id,search(50) ) )")
except:
    print("Search table already exists")

#%% Close connection
conn.close()
