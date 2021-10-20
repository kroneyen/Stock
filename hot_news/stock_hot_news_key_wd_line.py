#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import pandas as pd
import datetime
#import redis
import time
#import line_notify
import requests
from bs4 import BeautifulSoup
#import json
import re
import redis
import line_notify

url ='https://mops.twse.com.tw/mops/web/t05sr01_1'
##new_windwos =https://mops.twse.com.tw/mops/web/ajax_t05sr01_1?TYPEK=all&step=1
#&SEQ_NO=1&SPOKE_TIME=145909&SPOKE_DATE=20210331&COMPANY_ID=6173&skey=6173202103311&firstin=true
today = datetime.date.today()
mail_time = "21:00:00"



pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)

#key_wd_lists = ['失火','火災','工安意外','自結財務','澄清媒體報導'] 

def get_redis_data(_key):
    _list = r.lrange(_key,'0','-1')
    return _list


def delete_redis_data(today):
    yy = today - datetime.timedelta(days=2)
    yy_key = yy.strftime("%Y%m%d")+'skey_lists'

    if r.exists(yy_key) : ### del yesterday key
       r.delete(yy_key)





def insert_redis_data(_key,_values,today):
    line_display = []
    #yy = today - datetime.timedelta(days=2)   
    #yy_key = yy.strftime("%Y%m%d")+'skey_lists'
    #if r.exists(yy_key) : ### del yesterday key
    #   r.delete(yy_key)
     
    if r.exists(_key) :
           _list = r.lrange(_key,'0','-1')
           for i_value in _values : 
               if i_value.replace("'","") not in _list :
                  line_display.append(i_value.replace("'","")) 
                  r.rpush(_key,i_value.replace("'",""))

    else : 
       for i_value in _values : 
           r.rpush(_key,i_value.replace("'",""))
           line_display.append(i_value.replace("'",""))
    
    return line_display
    
    
def func(x,key_wd_lists) :

        for y in key_wd_lists :
          codi = r'.*%s.*'%y
          if re.search(codi,x):
            return 1
            break



def  hot_new_key_wd(url,today):  
     #today = datetime.date.today()
     pd.options.display.max_rows = 200
     pd.options.display.max_columns = 200
     pd.options.display.max_colwidth = 200 
    
     r = requests.post(url)
     r.encoding = 'utf8'    
     #df = pd.read_html(url)[7]
     
     soup = BeautifulSoup(r.text, 'html.parser')
     link = soup.find_all('input',attrs={"value": "詳細資料"})
     df = pd.read_html(r.text)[7]
     df_8 = pd.read_html(r.text)[8]
     if not df_8.empty :
       df_8.columns=['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
       df = pd.concat([df,df_8])
     
     """
     id = soup.find(id="t59sb01_form") ## 不同板塊
     
     if len(id) >0  :
       df = pd.read_html(r.text)[7]
       df_8 = pd.read_html(r.text)[8] ## 新增板塊
       if not df_8.empty :
         df_8.columns=['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
       df = pd.concat([df,df_8])
     
     else :
       df = pd.read_html(r.text)[7]  
     """
     #print(df.info())

     link_list = []
     skey_list = []   
     for onclick_value in link : 

       sttr = onclick_value.get('onclick').split(';',6)
       
       if sttr[0].split('=')[0] == 'document.fm_t05sr01_1.SEQ_NO.value' :
        
        SEQ_NO = sttr[0].split('=')[1]
        SPOKE_TIME = sttr[1].split('=')[1]
        SPOKE_DATE = sttr[2].split('=')[1]
        COMPANY_ID = sttr[3].split('=')[1]
        SKEY = sttr[4].split('=')[1]
        llink = '<a href="'+ ('https://mops.twse.com.tw/mops/web/ajax_t05sr01_1?TYPEK=all&step=1&SEQ_NO='+SEQ_NO\
        +'&SPOKE_TIME='+SPOKE_TIME+'&SPOKE_DATE='+SPOKE_DATE+'&COMPANY_ID='+COMPANY_ID+'&skey='+SKEY+'&firstin=true')\
        .replace("'","")+ '">' + '詳細資料</a>'
        
       else  :

           COMPANY_ID = sttr[1].split('=')[1]
           SKEY = sttr[2].split('=')[1]
           SPOKE_DATE = sttr[3].split('=')[1]
           llink = '<a href="'+ ('https://mops.twse.com.tw/mops/web/ajax_t59sb01?firstin=true&co_id='+ COMPANY_ID\
           + '&TYPEK=all&YEAR=' + str(today.year - 1911) + '&MONTH='+ str(today.month) + '&SDAY='+SPOKE_DATE+'&EDAY='+SPOKE_DATE\
           +'&DATE1='+ SPOKE_DATE + '&SKEY='+ SKEY +'&step=2b').replace("'","")+ '">' + '詳細資料</a>'
        
       #skey_list.append(SKEY)
       skey_list.append(SKEY.replace("'","")) 
       link_list.append(llink) 


     df_hot_new =  pd.DataFrame()
     ###['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
     ###[發言日期      發言時間  公司代號   公司名稱                                                 主旨         NaN]
     df = df.iloc[:,[0,1,2,3,4]]  ## fileter columns data
     #print('df:',df.info(),'link_list:',len(link_list))
    
   
     ##### tunning
     df['詳細資料'] = link_list
     

     key_wd_lists = get_redis_data("key_wd_lists")

     df['key_wd'] = df['主旨'].apply(lambda x: 1 if func(x,key_wd_lists) else 0)
     
     df['skey_lists'] = skey_list

     match_row = df[df['key_wd'] == 1]
     
     #insert_redis_data('skey_lists',match_row['skey_lists'].values)     
     #### for line display 
     #rediskeys = today.strftime("%Y%m%d")+'skey_lists'  
     #print(match_row.info())
     ###today.year - 1911 = 110 , today.year 2021
     rediskeys = match_row.iloc[0,2].replace("/","").replace(str(today.year - 1911),str(today.year)) + 'skey_lists'
     ##1100420skey_lists
     #print('rediskeys:',rediskeys)
     
     ### delete acrchive data 
     delete_redis_data(today) 

     line_display_list = insert_redis_data(rediskeys,match_row['skey_lists'].values,today)     

     line_key = get_redis_data("line_key") 

     #match_row_line = match_row[match_row['skey_lists'].isin(line_display_list)] ### new arrary
     match_row_line = match_row[match_row['skey_lists'].isin(line_display_list)] ### new arrary
     

     #### line not support html
     #match_row_line = match_row_line.iloc[:,0:6].to_html(escape=False)
     match_row_line_notify = match_row_line.iloc[:,[0,1,3,4]]  ## for line notify
     match_row_line_notify = match_row_line_notify.astype({'公司代號':int})  ##fix dtype folat to int64
     #print(match_row_line_notify.info())

   
     if not match_row_line.empty:
        line_notify.post_line_notify(line_key,match_row_line_notify.to_string(index = False))


     return match_row_line_notify
     #return match_row.info()
     

print(hot_new_key_wd(url,today))

"""
#### send to mail with link
if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
    #match_row.to_html('hot_news_link.log')
    if len(match_row) > 0 :
       body = match_row.to_html(escape=False)
       #log_date = datetime.date.today().strftime("%Y-%m-%d")
       send_mail.send_email('stock_hot_news_key_wd_link',body)
else :
    print(match_row.to_string(index=False))
"""
