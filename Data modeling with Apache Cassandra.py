import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


print(os.getcwd())

filepath = os.getcwd() + '/event_data'

or root, dirs, files in os.walk(filepath):
    
    file_path_list = glob.glob(os.path.join(root,'*'))
    

ull_data_rows_list = [] 
    
for f in file_path_list:
 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile:  
        csvreader = csv.reader(csvfile) 
        next(csvreader)
         
        for line in csvreader:
            full_data_rows_list.append(line) 
            
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

from cassandra.cluster import Cluster
cluster = Cluster()

session = cluster.connect()
session.execute(""" CREATE KEYSPACE IF NOT EXISTS sparkify
                    WITH REPLICATION = 
                    {'class':'SimpleStrategy','replication_factor':1}""")


session.set_keyspace('sparkify')
query = """
CREATE TABLE IF NOT EXISTS song_play_detail (
    sessionId int,
    itemInSession int,
    artist text,
    song text,
    length float,
    PRIMARY KEY (sessionId, itemInSession)
)
"""
session.execute(query)

query1 = """
CREATE TABLE IF NOT EXISTS user_song_his(
   userId int,
   sessionId int,
   itemInsession int,
   song text,
   artist text,
   firstName text,
   lastName text,
   PRIMARY KEY ((userId,sessionId),itemInSession))"""
session.execute(query1)

query2 = """
CREATE TABLE IF NOT EXISTS song_listen(
    song text,
    userId int,
    firstName text,
    lastName text,
    PRIMARY KEY(song,userId))"""
session.execute(query2)
                    
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) 
    for line in csvreader:
        query = "INSERT INTO song_play_detail (sessionId, itemInSession,artist,song,length)"
        query = query + "VALUES (%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[8]), int(line[3]),line[0],line[9], float(line[5])) )
        
        query1 = "INSERT INTO user_song_his (userId,sessionId,itemInsession,song,artist,firstName,lastName)"
        query1 = query1 + "VALUES(%s,%s,%s,%s,%s,%s,%s)"
        session.execute(query1, (int(line[10]),int(line[8]),int(line[3]),line[9],line[0],line[1],line[4]))
        
        query2 = "INSERT INTO song_listen(song,userId,firstName,lastName)"
        query2 = query2 + "VALUES(%s,%s,%s,%s)"
        session.execute(query2, (line[9], int(line[10]), line[1], line[4]))

rows = session.execute("SELECT artist,song,length FROM song_play_detail WHERE sessionId = 338 AND itemInSession = 4")
for row in rows:
    print(row)

rows = session.execute("SELECT * FROM user_song_his")
for row in rows:
    print(row)

rows = session.execute("SELECT artist, song,firstName, lastName FROM user_song_his WHERE userId = 10 AND sessionId = 182")
for row in rows:
    print(row)

rows = session.execute("SELECT firstName, lastName FROM song_listen WHERE song = 'All Hands Against His Own'")
for row in rows:
    print(row)
                    
session.execute("DROP TABLE song_play_detail")
session.execute("DROP TABLE ")

session.shutdown()
cluster.shutdown()


