#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import redis
import time



pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list


# try local to instantiate a client instance
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )

### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)



def insert_mongo_db(_conn,_db,_collection,_values):
    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database

    collection = db[_collection] ## collection 
    collection.insert(_values)


def read_mongo_db(_conn,_db,_collection,dicct,_columns):

    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database
 
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)

def drop_mongo_db(_conn,_db,_collection):

    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database

    collection = db[_collection] ## collection 
    collection.drop()


def sync_mongo_data():

    _collection_list= ['web_url','ETF_code','exclude_tag','extra_tag']

    dicct = {}
    _columns ={'_id':0}

    for idx in _collection_list : 


        
        altas_mydoc = read_mongo_db('conn','stock',idx,dicct,_columns)
       
        drop_mongo_db('c','stock',idx)

        time.sleep(1) 

        insert_mongo_db('c','stock',idx,altas_mydoc)

sync_mongo_data()
