#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import pandas as pd
import datetime
import redis
import time
#import line_notify
from bs4 import BeautifulSoup
import logging
import send_mail
import requests
from pymongo import MongoClient
from fake_useragent import UserAgent
import del_png
from io import StringIO
import urllib3
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

### del images/*.png
del_png.del_images()



##hot news 
url='https://mopsov.twse.com.tw/mops/web/t05sr01_1' ##改版 舊版
#url='https://mopsov.twse.com.tw/mops/web/t05sr01_2' ##改版 舊版
##new_windwos =https://mops.twse.com.tw/mops/web/ajax_t05sr01_1?TYPEK=all&step=1
#&SEQ_NO=1&SPOKE_TIME=145909&SPOKE_DATE=20210331&COMPANY_ID=6173&skey=6173202103311&firstin=true
today_week = datetime.date.today().strftime("%w")
mail_time = "21:00:00"
#mail_time = "10:00:00"
today = datetime.date.today()


def get_redis_data(_key,_type,_field_1,_field_2):
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    r = redis.StrictRedis(connection_pool=pool)

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)


    elif _type == "hkeys" :
       _list = r.hkeys(_key)


    return _list






### mongodb atlas connection
user = get_redis_data('mongodb_user',"hget","user",'NULL')
pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

conn = MongoClient('mongodb+srv://'+user+':'+pwd+'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



def  hot_new_detail(url,today,com_list):  

     user_agent = UserAgent()
    
     r = requests.post(url,  headers={ 'user-agent': user_agent.random },verify=False)
     r.encoding = 'utf8'    

     soup = BeautifulSoup(r.text, 'html.parser')
     link = soup.find_all('input',attrs={"value": "詳細資料"})

     df = pd.read_html(StringIO(r.text))[7]
     df_8 = pd.read_html(StringIO(r.text))[8]
     if not df_8.empty :
       df_8.columns=['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
       df = pd.concat([df,df_8])

     
     link_list = []

     for onclick_value in link : 

       sttr = onclick_value.get('onclick').split(';',6)

       if sttr[0].split('=')[0] == 'document.fm_t05sr01_1.SEQ_NO.value' :
        
        SEQ_NO = sttr[0].split('=')[1]
        SPOKE_TIME = sttr[1].split('=')[1]
        SPOKE_DATE = sttr[2].split('=')[1]
        COMPANY_ID = sttr[3].split('=')[1]
        skey = sttr[4].split('=')[1]
        #llink = '<a href="'+ ('https://mops.twse.com.tw/mops/web/ajax_t05sr01_1?TYPEK=all&step=1&SEQ_NO='+SEQ_NO\
        llink = '<a href="'+ ('https://mopsov.twse.com.tw/mops/web/ajax_t05sr01_1?TYPEK=all&step=1&SEQ_NO='+SEQ_NO\
        +'&SPOKE_TIME='+SPOKE_TIME+'&SPOKE_DATE='+SPOKE_DATE+'&COMPANY_ID='+COMPANY_ID+'&skey='+skey+'&firstin=true')\
        .replace("'","")+ '">' + '詳細資料</a>'
      
       else  :

           COMPANY_ID = sttr[1].split('=')[1]
           SKEY = sttr[2].split('=')[1]
           SPOKE_DATE = sttr[3].split('=')[1]
           llink = '<a href="'+ ('https://mops.twse.com.tw/mops/web/ajax_t59sb01?firstin=true&co_id='+ COMPANY_ID\
           + '&TYPEK=all&YEAR=' + str(today.year - 1911) + '&MONTH='+ str(today.month) + '&SDAY='+SPOKE_DATE+'&EDAY='+SPOKE_DATE\
           +'&DATE1='+ SPOKE_DATE + '&SKEY='+ SKEY +'&step=2b').replace("'","")+ '">' + '詳細資料</a>'
        
    
       link_list.append(llink) 
       #print(SEQ_NO,SPOKE_TIME,SPOKE_DATE,COMPANY_ID,skey)
       

     #df_hot_new =  pd.DataFrame()
     ###['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
     ###[發言日期      發言時間  公司代號   公司名稱                                                 主旨         NaN]
     df = df.iloc[:,[0,1,2,3,4]]  ## fileter columns data
    
     
     ##### tunning
     df['URL'] = link_list
     #com_lists = get_redis_data("com_list")  
     #df['公司代號'] = df['公司代號'].astype(str).str.replace(' ','')
     df['公司代號'] = df['公司代號'].astype(str)
     
     match_row=df[df['公司代號'].isin(com_list)].copy()
    
     return match_row.sort_values(by=['發言時間'],ascending = False ,ignore_index = True) , df




### main code 

dictt = {}
_columns= {"code":1,"_id":0}
com_list=[]


mydoc = atlas_read_mongo_db('stock','com_list',dictt,_columns)

for idx in mydoc :
    com_list.append(idx.get('code'))




match_row ,ori_df = hot_new_detail(url,today,com_list)


if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
    #match_row.to_html('hot_news_link.log')
    if not match_row.empty :
        
       body = match_row.to_html(escape=False)
       #log_date = datetime.date.today().strftime("%Y-%m-%d")
       send_mail.send_email('Stock_Hot_News_Link_%s' % today ,body)

    else :
      print('com_list not match')
      print(ori_df.head(100).to_string(index=False))

else :
    print(' time not match ')
    print(ori_df.head(100).to_string(index=False))


