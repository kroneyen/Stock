#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-


import requests
import pandas as pd
from datetime import datetime
import time
import redis
import random
from pymongo import MongoClient
from fake_useragent import UserAgent


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


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


def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)

def send_line_notify(token,msg):

    requests.post(
    url='https://notify-api.line.me/api/notify',
    headers={"Authorization": "Bearer " + token},
    data={'message': msg}
    )


def send_tg_bot_msg(token,chat_id,msg):

  #url = f"https://api.telegram.org/bot{'+token+'}/sendMessage?chat_id={'+chat_id}&text={msg}&parse_mode=HTML"
  url = "https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={msg}&parse_mode=HTML".format(token = token ,chat_id=chat_id,msg=msg)
  
  try :
       requests.get(url)

  except :
       time.sleep(random.random()) ### 0~1 num   
       requests.get(url) 
  



def match_row_5 ( get_updatetime ,match_row,extend) :

          line_key_list =[]
          tg_key_list=[]
          tg_chat_id=[]

          line_key_list.append( get_redis_data('line_key_hset','hget','Stock_ETF','NULL')) ## for exchange_rate (Stock_YoY/Stock/rss_google/Exchange_Rate)
          tg_key_list.append(get_redis_data('tg_bot_hset','hget','@stock_broadcast_2024bot','NULL')) ## for rss_google of signle
          tg_chat_id.append(get_redis_data('tg_chat_id','hget','stock_broadcast','NULL')) ## for rss_google of signle


          for match_row_index in range(0,len(match_row),5) :
              #msg = get_updatetime + "  " ## for line br
              #msg = get_updatetime  +"\n " + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
              msg = get_updatetime + extend  + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
              tg_msg ="【Stock_ETF】 "+ "\n" + msg

              ### for multiple line group
              deadline_check = datetime.today().strftime("%Y-%m-%d")
              if  deadline_check <= '2025-03-31' :

                  for line_key in  line_key_list : ##
                      send_line_notify(line_key, msg)
                      time.sleep(random.randrange(1, 3, 1))


              for tg_key in  tg_key_list : ## 
                  send_tg_bot_msg(tg_key,tg_chat_id[0],tg_msg)               
                  time.sleep(random.random())


def ETF_Over_Price(today):

  code_list =[]
  url = 'https://mis.twse.com.tw/stock/data/all_etf.txt?'
  mydoc = atlas_read_mongo_db('stock','ETF_code',{},{'_id':0,'code':1})
  mydoc_ETF =  pd.DataFrame(mydoc)
  code_list = list(mydoc_ETF['code'])
  user_agent = UserAgent()

  dfs =pd.DataFrame()
  r = requests.get(url ,  headers={ 'user-agent': user_agent.random })
  r.encoding = 'utf8'
 
  doc = pd.read_json(url)['a1']                                                                       
  for idx in range(0,len(doc)-1,1) :                                                                  
                                                                                                      
    df = pd.DataFrame(data=doc[idx]['msgArray'],columns=['a','b','c','d','e','f','g','h','i','j','k'])
    df = df[df['a'].isin(code_list)]                                                                  
                                                                                                      
    if not df.empty :
      df['d'] = df['d'].astype(str).str.replace(',','').astype(float) ##change type
      df['d']=round((df['d']/1000),0).astype(int)
      dfs = pd.concat([dfs,df],ignore_index=True)                                                     
                                                                                                      
  dfs = dfs.loc[:,['a','d','e','f','g']]                                                          
  
  dfs.columns = ['code','前日買賣','成交價','預估淨值','折溢價%']

      
  return dfs


today = datetime.now()

match_row=ETF_Over_Price(today)


if today.hour < 18  :

        match_row = match_row[['code','成交價','預估淨值','折溢價%']]

        ## 只顯示折價 
        #match_row['折溢價%'] = match_row['折溢價%'].apply(lambda x: float(0) if x == '-'  else float(x)  )
        try :
             match_row['折溢價%'] = match_row['折溢價%'].apply(lambda x: 0 if (x == '-'  or x == '--' or x == '') else float(x)  )
             #match_row['折溢價%'] = match_row['折溢價%'].apply(lambda x: float(x)  if  float(x)  else  0 )

        except :        
               print('debug:', match_row)

        match_row = match_row[match_row['折溢價%'] < 0 ]
        
      
if not match_row.empty  :

    get_updatetime = datetime.now().strftime('%Y%m%d %H:%M:%S')
    
    match_row = match_row.sort_values(by=['折溢價%'])

    match_row_5(get_updatetime ,match_row,"\n ")

    """
    for idx in match_row['折溢價%'] :
              if idx == '-' :
                 idx = 0 
              else :    
                 idx = float(idx) 
              
              ## 折價 才顯示  
              if idx < thread :
     
                 get_updatetime = datetime.now().strftime('%Y%m%d %H:%M:%S')

                 match_row_5(get_updatetime ,match_row,"\n ")
                  
                 break
    """ 

          
