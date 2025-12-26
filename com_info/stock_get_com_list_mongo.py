#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from datetime import datetime
import redis
from pymongo import MongoClient ,IndexModel
import random
from fake_useragent import UserAgent
from io import StringIO
import urllib3
import re
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)



# try local to instantiate a client instance
c = MongoClient(
        host = 'localhost', 
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list


### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)



def insert_many_mongo_db(_conn,_db,_collection,_values):
    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database

    collection = db[_collection] ## collection 
    collection.insert_many(_values)




def drop_collection(_conn,_db,_collection):
    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database

    collection = db[_collection] ## collection 
    collection.drop()


def createIndex_mongo_db(_conn,_db,_collection,_values):
    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database

    collection = db[_collection] ## collection 
    collection.create_indexes([_values])






def read_mongo_db(_conn,_db,_collection,dicct,_columns):
    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database  

    collection = db[_collection] ## collection 
    #return collection.find({_code:_qq},{"code":1,"name":0,"_id":0,"last_modify":0})
    #return collection.find(dicct,{"code": 1,"name": 1,"_id": 0})
    return collection.find(dicct,_columns)



def get_colleciton_name(_conn,_db) :
    if _conn == 'c' :
      db = c[_db] ## database
    elif _conn == 'conn':
      db = conn[_db] ## database  

    collection_names = db.list_collection_names()
    return collection_names




def insert_redis_data(_key,_values):

        r.hmset(_key,_values)



def delete_redis_data(key):

    if r.exists(key) : ### del yesterday key
       r.delete(key)



def llist(df_len):

    llist =[]
    for i in range(df_len) :
      llist.append(i)
    return llist






#####  get com list for monthly refresh 
def Get_Com_List_v1(rows) :

  #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
  ###https://data.gov.tw/dataset/18419 上市
  ###https://data.gov.tw/dataset/25036 上櫃

  sii = 'https://quality.data.gov.tw/dq_download_json.php?nid=18419&md5_url=9791ec942cbcb925635aa5612ae95588'  ## 下載上市清單  
  otc = 'https://quality.data.gov.tw/dq_download_json.php?nid=25036&md5_url=1aae8254db1d14b0d113dd93f2265d06'  ## 下載上櫃清單

  dfs = pd.DataFrame()
  url_list = [sii,otc]
  types =  ['sii','otc']
  count = 0 
  for url in url_list :
   #j_coms = pd.read_json('https://quality.data.gov.tw/dq_download_json.php?nid=18419&md5_url=9791ec942cbcb925635aa5612ae95588').head() 
   
   df = pd.read_json(url).head(rows)
   #df['公司代號'] = df.values.tolist() ## rows to  a rows
   #print(df.info)
   df = df.iloc[:,[1,3]]
   df['type']= types[count]
   dfs = pd.concat([dfs,df] ,ignore_index = True)
   count +=1
   time.sleep(1)
  return dfs





def get_com_auth_stock() : 

  #url = 'https://mops.twse.com.tw/mops/web/ajax_t51sb01'
  url = 'https://mopsov.twse.com.tw/mops/web/ajax_t51sb01'
  url_ETF ='https://mopsov.twse.com.tw/mops/web/t51sb11_q2'
  dfs = pd.DataFrame()
  types =  ['sii','otc']
  for type in types :
   payload = { 'encodeURIComponent':1 
         ,'step':1
         ,'firstin':1
         ,'TYPEK': type
         ,'code': '' }  ## default is all
   
   user_agent = UserAgent()

   r = requests.post(url, headers={ 'user-agent': user_agent.random },data=payload,verify=False)
   r.encoding = 'utf8'
   df = pd.read_html(StringIO(r.text),thousands=',')[0]
   df = df.iloc[:,[0,2,17]]
   df['type'] = type
   df.columns= llist(len(df.columns))
   dfs = pd.concat([dfs,df] ,ignore_index = True)
  

  dfs.columns =['id','name','auth_stock','type']
  dfs.drop(dfs.loc[dfs['id']=='公司代號'].index, inplace=True)
  ### 過濾auth_stock中文bug
  dfs['auth_stock'] = pd.to_numeric(dfs['auth_stock'], errors='coerce')
  dfs = dfs.dropna(subset=['auth_stock'])
  dfs = dfs.astype({'auth_stock':'int64'})
  dfs['auth_stock'] = round((dfs['auth_stock']/1000),0).astype(int) ## 

  return dfs




def get_ETF_com_auth_stock() :

  url = 'https://mopsov.twse.com.tw/mops/web/ajax_t51sb11'
  dfs = pd.DataFrame()
  types =  ['sii']
  for type in types :
   payload = { 'encodeURIComponent':1
         ,'step':0
         ,'firstin':1
         ,'run': '' }  ## default is all


   user_agent = UserAgent()

   #eaders = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

   r = requests.post(url, headers={ 'user-agent': user_agent.random },data=payload,verify=False)
   #r = requests.post(url, headers=headers,data=payload)
   r.encoding = 'utf8'
   df = pd.read_html(StringIO(r.text),thousands=',')[0]

   
   df = df.iloc[:,[0,1,23]]
   df['type'] = type
   df.columns= llist(len(df.columns))
   dfs = pd.concat([dfs,df] ,ignore_index = True)


  dfs.columns =['id','name','auth_stock','type']
  dfs.drop(dfs.loc[dfs['id']=='公司代號'].index, inplace=True)
  ### 過濾auth_stock中文bug
  dfs['auth_stock'] = pd.to_numeric(dfs['auth_stock'], errors='coerce')
  dfs = dfs.dropna(subset=['auth_stock'])
  dfs = dfs.astype({'auth_stock':'int64'})
  dfs['auth_stock'] = round((dfs['auth_stock']/1000),0).astype(int) ## 
  
  return dfs







#com_lists= Get_Com_List_v1(2000)
#com_lists.columns = ['id','name','type']
#print(com_lists.info())

"""
delete_redis_data('com_lists')

for index in range(len(com_lists)) : 
   # print(str(df.iloc[index,0])+' "' +str(df.iloc[index,1])+'" ',str(df.iloc[index:0])+'_p "'+str(df.iloc[index:2])+'"')
   ### for dic {1234 : "aaa" , 1234_p : "otc"}
   _values = { str(com_lists.iloc[index,0]) :  str(com_lists.iloc[index,1]) , str(com_lists.iloc[index,0]) + '_p' : str(com_lists.iloc[index,2]) }
    
   insert_redis_data("com_lists",_values )
"""
#print(check_mongo())

com_lists = get_com_auth_stock()
ETF_com_lists = get_ETF_com_auth_stock()


com_lists = pd.concat([com_lists,ETF_com_lists])

### insert to redis 

delete_redis_data('com_lists')

for index in range(len(com_lists)) :
   ### for dic {1234 : "stock_name" , 1234_p : "otc" , 1234_au : "auth_stock" }

   #_values = { str(com_lists.iloc[index,0]) :  str(com_lists.iloc[index,1]) , str(com_lists.iloc[index,0]) + '_au' : str(com_lists.iloc[index,2] ) ,  str(com_lists.iloc[index,0]) + '_p' :  str(com_lists.iloc[index,3])  }
   _values = {  str(com_lists.iloc[index,0])+':code' :  str(com_lists.iloc[index,1]) , str(com_lists.iloc[index,0]) + ':auth_stock' : str(com_lists.iloc[index,2] ) , str(com_lists.iloc[index,0]) +':type' :  str(com_lists.iloc[index,3])  }

   insert_redis_data("com_lists",_values )

time.sleep(random.randrange(1, 3, 1))


### insert to local mongo & altas mongo 


drop_collection('c','stock','com_lists')
#drop_collection('c','stock','com_list') ### atlas sync com_list to local
drop_collection('conn','stock','com_lists')
time.sleep(random.randrange(1, 3, 1))

#auth_stock_count = 0 
_values_lists=[]

for index in range(len(com_lists)) : 
   _values = { 'code' : str(com_lists.iloc[index,0]) ,'name': str(com_lists.iloc[index,1]) , 'auth_stock' : str(com_lists.iloc[index,2]),'type' : str(com_lists.iloc[index,3]),'last_modify':datetime.now() }
   _values_lists.append(_values)



### defind index 
com_list_index= IndexModel([('code',1)],name='code_1' , unique=True ,background=True )
com_lists_index= IndexModel([('code',1),('type',1)],name='code_type' , unique=True ,background=True )


### get atlas com_list data 

collection_list = get_colleciton_name('conn','stock')

name_list = []
dicct ={}
_columns = {"_id" : 0}

for c_name in  collection_list :

  if re.search(r'com_list$',c_name) :
     name_list.append(c_name)

for  idx_name in name_list :

    ### atlas sync com_list to local

     _values = read_mongo_db('conn','stock',idx_name,dicct,_columns)

     mydoc = pd.DataFrame(list(_values))

     if  not mydoc.empty : 

        #drop_collection('c','stock',idx_name) 
        #insert_many_mongo_db('c','stock',idx_name,_values)
        ### add index for com_list
        createIndex_mongo_db('c','stock',idx_name,com_list_index)
        createIndex_mongo_db('conn','stock',idx_name,com_list_index)




   
### insert data to local mongodb & altas mogodb    
insert_many_mongo_db('c','stock','com_lists',_values_lists)
insert_many_mongo_db('conn','stock','com_lists',_values_lists)


### add index for com_list
createIndex_mongo_db('c','stock','com_list',com_list_index)
createIndex_mongo_db('conn','stock','com_list',com_list_index)

### add index for com_lists
createIndex_mongo_db('c','stock','com_lists',com_lists_index)
createIndex_mongo_db('conn','stock','com_lists',com_lists_index)


dictt = {}
_columns= {"code": 1,"name": 1,"auth_stock":1,"type":1,"_id": 0}

mydoc = read_mongo_db('c','stock','com_lists',dictt,_columns)

for idx in mydoc :
  #print(idx.get('code'),idx.get('name'))
  print(idx.get('code'),idx.get('name'),idx.get('auth_stock'),idx.get('type'))
