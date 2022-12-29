#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
#from datetime import datetime
import datetime
from pymongo import MongoClient
import redis
import send_mail

import json
#from json import load
from io import StringIO
import numpy as np

mail_time = '12:00:00'

### redis connection & data 

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



def read_mongo_db(_db,_collection,dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)


def read_aggregate_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)



### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)



def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



def get_Stock_Holder_Set(cal_date):
    my_doc = read_mongo_db('stock','Rep_Stock_Exchange',{'last_modify': cal_date},{"code":1,"price":1,"last_modify":1,"_id":0})
    df =pd.DataFrame(list(my_doc))
    
    return df.reset_index(drop=True)


def get_mongo_cal_date(cal_day):
 ### mongo query for last ? days
 dictt_set = [ {"$group": { "_id" : { "$toInt" : "$date" } }},{"$sort" : {"_id" :-1}} , { "$limit" : cal_day}]

 ### mongo dict data

 set_doc =  read_aggregate_mongo_db('stock','Rep_Stock_Holder',dictt_set)

 idx_date =[]
 ### for lists  get cal date 
 for idx in set_doc:

  idx_date.append(str(idx.get("_id")))

 #set_date = str(idx_date)
 return idx_date



def get_Rep_Stock_Holder(com_num):
    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
    0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'} 
    url ='https://opendata.tdcc.com.tw/getOD.ashx?id=1-5'
    res = requests.get(url, headers=headers)
    df = pd.read_csv(StringIO(res.text))
    df = df.astype(str)
    
    if com_num == 'nan' :
      com_list = read_mongo_db('stock','com_list',{},{'code':1,'_id':0})
      my_doc_list = pd.DataFrame(list(com_list)) ##cours to dric
          
      df = df[df['證券代號'].isin(list(my_doc_list['code']))]
    else : 
   
      df = df[df['證券代號'].isin([com_num])] 

    df = df.rename(columns={
        '證券代號': 'stock_id',
        '股數': '持有股數', '占集保庫存數比例%': '占集保庫存數比例'
    })
   
    
    # 官方有時會有不同格式誤傳，做例外處理
    if '占集保庫存數比例' not in df.columns:
        df = df.rename(columns={'佔集保庫存數比例%': '占集保庫存數比例'})
        
    # 持股分級=16時，資料都為0，要拿掉
    df = df[df['持股分級'] != '16']
    df['持股分級'] = df['持股分級'].replace(['17'],'16')
    
    # 資料轉數字
    float_cols = ['人數', '持有股數', '占集保庫存數比例']
    df[float_cols] = df[float_cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))
    # 抓表格上的時間資料做處理
     
    #df['date'] = datetime.datetime.strptime(df.iloc[0,0],"%Y%m%d")
    
    #只要第二層欄位名稱
    #df = df.drop(columns=df.columns[0])
    
    # 索引設置 unique index
    #df = df.set_index(['stock_id', 'date', '持股分級'])
    df.columns = ['date','code','level','owner','stock','percentage']
    return df.reset_index(drop=True)



###"""

### get url  & insert into mongo 
match_row = get_Rep_Stock_Holder('nan')

match_row['last_modify']=datetime.datetime.now()


records =match_row.to_dict(orient='records')
insert_many_mongo_db('stock','Rep_Stock_Holder' , records )


"""

####### make  holder report with level 

#mydoc = read_mongo_db('stock','Rep_Stock_Holder',{},{'_id':0})

#df = pd.DataFrame(list(mydoc))

###get cal date( units 3 week)

cal_day = 3

cal_date = get_mongo_cal_date(cal_day)


my_doc_set = pd.DataFrame()

### get idx_date price to set
for idx_date in cal_date : 

 idx_date_doc = get_Stock_Holder_Set(idx_date)

 my_doc_set = pd.concat([my_doc_set,idx_date_doc],axis=0)


my_doc_set['price'] = my_doc_set['price'].astype(float)
my_doc_set.columns=['code','price','date']


## get holder data
mydoc = read_mongo_db('stock','Rep_Stock_Holder',{'date':{ '$gte' : cal_date[cal_day-1]}},{'_id':0,'last_modify':0})

## set level
#stock = ['1-999','1,000-5,000','5,001-10,000','10,001-15,000','15,001-20,000','20,001-30,000',
#    '30,001-40,000','40,001-50,000','50,001-100,000','100,001-200,000','200,001-400,000',
#    '400,001-600,000','600,001-800,000','800,001-1,000,000','1,000,001以上','合　計']

stock = ['0-0.9','1-5','5.1-10','10.1-15','15.1-20','20.1-30',
    '30.1-40','40.1-50','50.1-100','100.1-200','200.1-400',
    '400.1-600','600.1-800','800.1-1,000','1,000.1張以上','合　計']
map_data = {"level": range(1,17,1) ,  "stock_num" : stock}


## mapping & merge data 

level_map = pd.DataFrame(data= map_data,columns=['level','stock_num'])
level_map['level'] =level_map['level'].astype(str)




df = pd.DataFrame(list(mydoc))
#合併等級對應股數
df = df.merge(level_map, how='left', on='level' )
#合併code股價
df = df.merge(my_doc_set, how='left', on=['code','date'] )


# price >$50 ,stock(400) ,
# price <$50 ,stock(1000)

#for set in range(len(my_doc_set)) :
#  if df['code'] == index(set).get :



#df.drop(df.index[0:11], axis=0, inplace=True)

#df.info()

df['price'] =df['price'].astype(float)

df['level'] =df['level'].astype(int)

#df = df[df['level'] >= 12]

hold_df = pd.DataFrame()

for idx in range(len(my_doc_set)):
  
  chek_df = pd.DataFrame() 

  # 找出 code對資料 
  chek_df = df[df['code'] == my_doc_set.iloc[idx,0]]

  # 找出price <= 50 & 樣比1000張 level 15
  if my_doc_set.iloc[idx,1] <= 50 : 
    chek_df = chek_df[chek_df['level'] >= 14]
  
  elif my_doc_set.iloc[idx,1] <= 100 :     
    chek_df = chek_df[chek_df['level'] >=13]

  elif my_doc_set.iloc[idx,1] <= 200 :     
    chek_df = chek_df[chek_df['level'] >= 12]  

  elif my_doc_set.iloc[idx,1] > 200 :     
    chek_df = chek_df[chek_df['level'] >= 11]         



  hold_df = pd.concat([hold_df,chek_df],axis=0) 
   
df = hold_df.copy()
#df['stock']=round(df[df['stock']]/1000,0)
###股數轉張數
df['stock']=round((df['stock']/1000),0)

df.where(df["code"] == "3706" , inplace = True)

table = pd.pivot_table(df,index=['code', 'date','price', 'level','stock_num']).sort_values(by=['date'],ascending = False)
#table = pd.pivot_table(df,index=['code', 'date','price'],columns=['level','stock_num'])


print(table)

"""
