#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import datetime
import redis
from fake_useragent import UserAgent

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)



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

  url = 'https://mops.twse.com.tw/mops/web/ajax_t51sb01'
  dfs = pd.DataFrame()
  types =  ['sii','otc']
  for type in types :
   payload = { 'encodeURIComponent':1
         ,'step':1
         ,'firstin':1
         ,'TYPEK': type
         ,'code': '' }  ## default is all

   user_agent = UserAgent()

   r = requests.post(url, headers={ 'user-agent': user_agent.random },data=payload)
   r.encoding = 'utf8'
   df = pd.read_html(r.text,thousands=',')[0]
   df = df.iloc[:,[0,2,17]]
   df['type'] = type
   df.columns= llist(len(df.columns))
   dfs = pd.concat([dfs,df] ,ignore_index = True)

  dfs.columns =['id','name','auth_stock','type']
  dfs.drop(dfs.loc[dfs['id']=='公司代號'].index, inplace=True)
  dfs = dfs.astype({'auth_stock':'int64'})
  dfs['auth_stock'] = round((dfs['auth_stock']/1000),0).astype(int) ## 

  return dfs



#com_lists= Get_Com_List_v1(3000) 
com_lists = get_com_auth_stock()
#com_lists.columns = ['id','name','type']
#print(com_lists)


delete_redis_data('com_lists')

for index in range(len(com_lists)) : 
   # print(str(df.iloc[index,0])+' "' +str(df.iloc[index,1])+'" ',str(df.iloc[index:0])+'_p "'+str(df.iloc[index:2])+'"')
   ### for dic {1234 : "aaa" , 1234_p : "otc"}
   #_values = { str(com_lists.iloc[index,0]) :  str(com_lists.iloc[index,1]) , str(com_lists.iloc[index,0]) + '_p' : str(com_lists.iloc[index,2] ) }
   _values = { str(com_lists.iloc[index,0]) :  str(com_lists.iloc[index,1]) , str(com_lists.iloc[index,0]) + '_au' : str(com_lists.iloc[index,2] ) ,  str(com_lists.iloc[index,0]) + '_p' :  str(com_lists.iloc[index,3])  }

   #print(_values)    
   insert_redis_data("com_lists",_values )

