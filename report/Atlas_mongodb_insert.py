#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
#from datetime import date,datetime
import redis
import time
#import line_notify
import requests
from pymongo import MongoClient
import json


### connection redis 

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list




### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )


def read_mongo_db(_db,_collection,_dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(_dicct,_columns)


### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')



mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)

def atlas_insert_mongo_db(_db,_collection,_values):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert(_values)


def atlas_insert_many_mongo_db(_db,_collection,_values):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)


def atlas_read_mongo_db(_db,_collection,_dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(_dicct,_columns)

def atlas_drop_mongo_db(_db,_collection):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.drop()

def atlas_delete_many_mongo_db(_db,_collection,_dicct):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.delete_many(_dicct)


            

### execute python with sys 
def Check_sys_conn() :
    import sys
    try :
          _db = sys.argv[1]
          _collection = sys.argv[2]
          _dicct= json.loads(sys.argv[3])
         

    except :
           print('please execute python with sys_argv :　_db,_collection,_dicct')
           sys.exit(1)

    return _db,_collection,_dicct



def data_insert_mongo(_db, _collection, _dicct):

   ## python3.6 Atlas_mongodb_insert.py 'stock' 'Rep_Stock_Exchange' '{"last_modify":{"$gte": "20230101" , "$lt" : "20230330"}}' 
   ## python3.6 Atlas_mongodb_insert.py 'stock' 'Rep_Stock_Holder' '{"date":{"$gte": "2023401"}}'
   ## python3.6 Atlas_mongodb_insert.py '591' 'sale_house'  '{"last_modify" : {"$gte": "2023-10-05T00:00:00"}}'
   _columns = {"_id":0}
  
   ### get local mongo  data
   local_data = read_mongo_db(_db,_collection,_dicct,_columns)

   if local_data.count() > 0 : 

     d_dicct = {}

     ## delete old atlas mongo data  
     atlas_delete_many_mongo_db(_db,_collection,d_dicct)     

     atlas_insert_many_mongo_db(_db,_collection,local_data)
      


if __name__ == '__main__':


     try :

         _db, _collection, _dicct = Check_sys_conn()
         data_insert_mongo(_db, _collection, _dicct)

         print(_db , "insert " + _collection + " data done")

     except : 

         print(_db , "insert " + _collection + " data failed")
