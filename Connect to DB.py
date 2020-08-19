# -*- coding: utf-8 -*-
'''
    File name: Connect to DB.py
    Author: Henry Letton
    Date created: 2020-08-11
    Python Version: 3.7
    Desciption: COnnect to DB testing
'''


#%% Connect to database

import mysql.connector
import pandas as pd

conn = mysql.connector.connect(user='u235764393_HL', 
                              password='',
                              host='sql134.main-hosting.eu',
                              database='u235764393_HLDB')

cursor = conn.cursor()

#%% Get data out

#query = ("SELECT * FROM Music_Track_Listens")

#sql_query = pd.read_sql_query(query,conn)

#%% create table

try:
    cursor.execute("CREATE TABLE Test_Python ("
            "Test_Char TEXT,"
            "Test_Int INT,"
            "Test_Float FLOAT,"
            "Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY ( Test_Int ) )")
except:
    print("Table already exists")

#cursor.execute("INSERT INTO Twitter_Tweets (tweet_id, full_text, favourite_count, retweet_count, longitude, latitude, place, user_id, quoted_status_id, created_at) VALUES (1295772967566225408, '@Bletchman @cbradbee @surreymirror @PrivateEyeNews Cllr Bramhall stated the Longmead demo/ development and the Colebrook development are linked but 1. £3M Homes England grant is for Colebrook 2. Surrey Choice could be relocated on the car park of Longmead. How is the future Colebrook community hub funded? By erasing Longmead AEC?', 0, 0, NULL, NULL, '', 4908916414, NULL, '2020-08-18 17:22:04')")

#for insert in insert_text:
#    cursor.execute(insert)
#    print('Row inserted')

query2 = ("SELECT * FROM Twitter_Tweets")

sql_query2 = pd.read_sql_query(query2,conn)


#%% close database connection

conn.commit()

conn.close()



