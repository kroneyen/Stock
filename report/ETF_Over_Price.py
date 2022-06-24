
#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-


import requests
import pandas as pd
from datetime import datetime
import time
import redis
import random



pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list



def send_line_notify(token,msg):

    requests.post(
    url='https://notify-api.line.me/api/notify',
    headers={"Authorization": "Bearer " + token},
    data={'message': msg}
    )



def match_row_5 ( get_updatetime ,match_row,extend) :

          line_key_list =[]
          line_key_list.append( get_redis_data('line_key_hset','hget','Stock_ETF','NULL')) ## for exchange_rate (Stock_YoY/Stock/rss_google/Exchange_Rate)
          for match_row_index in range(0,len(match_row),5) :
              #msg = get_updatetime + "  " ## for line br
              #msg = get_updatetime  +"\n " + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
              msg = get_updatetime + extend  + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 

              ### for multiple line group
              for line_key in  line_key_list : ## 
                  send_line_notify(line_key, msg)
                  time.sleep(random.randrange(1, 3, 1))



def ETF_Over_Price():

  dfs = pd.DataFrame()
  url = 'https://mis.twse.com.tw/stock/data/all_etf.txt?'
  code_list =['00891','00892']
  dfs =pd.DataFrame()
  r = requests.get(url)
  r.encoding = 'utf8'
  for idx in [4,8] : ## 中信　／　富邦
    df = pd.read_json(url)['a1'][idx]['msgArray']
    df = pd.DataFrame(data=df,columns=['a','b','c','d','e','f','g','h','i','j','k']) 
    #df['last_modify'] = df['i'] +' '+ df['j']
    #df =df.loc[:,['a','b','d','e','f','g','last_modify']]
    df =df.loc[:,['a','b','d','e','f','g']]
    df = df[df['a'].isin(code_list)]
    ## stock to /1000 
    df['d'] = df['d'].astype(str).str.replace(',','').astype(float) ##change type
    df['d']=round((df['d']/1000),0).astype(int)
    dfs = pd.concat([dfs,df],ignore_index=True)
    #dfs.columns = ['code','name','前日買賣','成交價','預估淨值','折溢價%','last_modify']
  dfs.columns = ['code','name','前日買賣','成交價','預估淨值','折溢價%']
  return dfs



match_row=ETF_Over_Price()

thread= 0

if not match_row.empty  :

          match_row = match_row.astype({'折溢價%':'float'})
         
          skip =0  
          for idx in match_row['折溢價%'] :
              if idx < thread and skip ==0 :
     
                 get_updatetime = datetime.now().strftime('%Y%m%d %H:%M:%S')

                 match_row_5(get_updatetime ,match_row,"\n ")
                  
                 skip=1

