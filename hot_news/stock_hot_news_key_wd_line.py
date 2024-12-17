#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import pandas as pd
import datetime
import redis
import time
import requests
from bs4 import BeautifulSoup
import json
import re
#import line_notify
import random
from fake_useragent import UserAgent



url ='https://mops.twse.com.tw/mops/web/t05sr01_1'
##new_windwos =https://mops.twse.com.tw/mops/web/ajax_t05sr01_1?TYPEK=all&step=1
#&SEQ_NO=1&SPOKE_TIME=145909&SPOKE_DATE=20210331&COMPANY_ID=6173&skey=6173202103311&firstin=true
today = datetime.date.today()



pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)



def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "lrange_head" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list



def delete_redis_data(today):
    
     dd = 10 ##days :10
     while  len(r.keys('*_skey_lists')) >2 and dd > 1 :
 
       yy = today - datetime.timedelta(days=dd)
       yy_key = yy.strftime("%Y%m%d")+'_skey_lists'

       if r.exists(yy_key) : ### del archive  key
          r.delete(yy_key)
          
       dd -=1  
     
"""
def reurl_API(link_list):
     
     reurl_link_list = []

     reurl_api_key = "".join(get_redis_data("reurl_key",'lrange',0,-1))  ## list_to_str

     s_reurl = 'https://api.reurl.cc/shorten'
     s_header = {"Content-Type": "application/json" , "reurl-api-key": reurl_api_key}
     
     for link in link_list : 
       uurl = link.split('href="')[1].split('">')[0] 
       s_data = {"url": uurl}

       r = requests.post(s_reurl, json= s_data , headers=s_header ).json()

       try :
           rerul_link = r["short_url"]
       except :
           rerul_link = link

       reurl_link_list.append(rerul_link)

#       r = requests.post(s_reurl, json= s_data , headers=s_header ).json()
#       reurl_link_list.append(r["short_url"])

       time.sleep(round(random.uniform(0.5, 1.0), 10))

     return reurl_link_list

"""



def reurl_API(link_list,_key):

     reurl_link_list = []
     reurl_api_key = "".join(get_redis_data('short_url_key','hget',_key,'NULL'))  ## list_to_str


     if _key  ==  "reurl_key" :


        s_reurl = 'https://api.reurl.cc/shorten'
        s_header = {"Content-Type": "application/json" , "reurl-api-key": reurl_api_key}

        for link in link_list :

            uurl = link.split('href="')[1].split('">')[0]
            #print('uurl:',uurl)
            s_data = {"url": uurl}
            r = requests.post(s_reurl, json= s_data , headers=s_header ).json()

            try :
                rerul_link = r["short_url"]
            except :
                rerul_link = link

            reurl_link_list.append(rerul_link)


            time.sleep(round(random.uniform(0.5, 1.0), 20))


     elif   _key  ==  "ssur_key" :


        for link in link_list :

            uurl = link.split('href="')[1].split('">')[0]
            #print('uurl:',uurl)
            s_reurl = 'https://ssur.cc/api.php?'
            s_data = {"format": "json" , "appkey": reurl_api_key ,"longurl" : uurl}
      
            r = requests.post(s_reurl, data= s_data  ).json()
            try :
                  rerul_link = r["ae_url"]
            except :
                  rerul_link = link

            reurl_link_list.append(rerul_link)


            time.sleep(round(random.uniform(0.5, 1.0), 20))


     return reurl_link_list





def insert_redis_data(_key,_values):
    line_display = []
     
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


def send_line_notify(token,msg):

    requests.post(
    url='https://notify-api.line.me/api/notify',
    headers={"Authorization": "Bearer " + token},
    data={'message': msg} 
    )


def send_tg_bot_msg(token,chat_id,msg):

  url = "https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={msg}&parse_mode=HTML".format(token = token ,chat_id=chat_id,msg=msg)
  requests.get(url)




def  hot_new_key_wd(url,today):  
     #pd.options.display.max_rows = 200
     #pd.options.display.max_columns = 200
     #pd.options.display.max_colwidth = 200 

     user_agent = UserAgent()   
     #r = requests.post(url)
     r =  requests.post(url ,  headers={ 'user-agent': user_agent.random })
     r.encoding = 'utf8'    
     
     soup = BeautifulSoup(r.text, 'html.parser')
     try : 
         link = soup.find_all('input',attrs={"value": "詳細資料"})
         df = pd.read_html(r.text)[7]
         df_8 = pd.read_html(r.text)[8]

         if not df_8.empty :
            df_8.columns=['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
            df = pd.concat([df,df_8])
     
     except : 
       return {}           

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
        
       skey_list.append(SKEY.replace("'","")) 
       link_list.append(llink) 
       #link_list.append(llink.split('href="')[1].split('">')[0]) 


     ###['公司代號', '公司簡稱', '發言日期', '發言時間', '主旨', 'Unnamed: 5']
     ###[發言日期      發言時間  公司代號   公司名稱                                                 主旨         NaN]
     df = df.iloc[:,[0,1,2,3,4]]  ## fileter columns data
    
   
     ###  merge link
     df['URL_All'] = link_list
      
     key_wd_lists = get_redis_data("key_wd_lists","lrange",0,-1)
     
     ### mapping key_wd
     df['key_wd'] = df['主旨'].apply(lambda x: 1 if func(x,key_wd_lists) else 0)
     
     df['skey_lists'] = skey_list

     match_row = df[df['key_wd'] == 1]
     #print(match_row.info()) 
     #### for line display 
     if not match_row.empty :
       ###today.year - 1911 = 110 , today.year 2021
       rediskeys = match_row.iloc[0,2].replace(str(today.year - 1911),str(today.year)).replace("/","") + '_skey_lists'
       ##1100420skey_lists
       ### delete acrchive data 
       delete_redis_data(today)
 
       #insert_redis_data('skey_lists',match_row['skey_lists'].values) 
       line_display_list = insert_redis_data(rediskeys,match_row['skey_lists'].values)     
       line_key_list=[]
       tg_key_list=[]
       tg_chat_id=[]
       line_key_list.append( get_redis_data('line_key_hset','hget','Stock_YoY','NULL'))  ## use signle line key
       tg_key_list.append(get_redis_data('tg_bot_hset','hget','@stock_broadcast_2024bot','NULL')) ## for tg_bot of signle
       tg_chat_id.append(get_redis_data('tg_chat_id','hget','stock_broadcast','NULL')) ## for tg_bot of signle

       ## check new array 

       match_row_line = match_row[match_row['skey_lists'].isin(line_display_list)].copy() ### new arrary
       ##short url 
       match_row_line['URL'] = reurl_API(match_row_line['URL_All'].values , 'reurl_key')
       #match_row_line['URL'] = reurl_API(match_row_line['URL_All'].values , 'ssur_key') -- bug
       
       #match_row_line['URL'] = reurl_API(match_row_line['URL_All'].values )

       match_row_line_notify = match_row_line.iloc[:,[0,1,3,4,8]]  ## for line notify
       match_row_line_notify = match_row_line_notify.astype({'公司代號':int})  ##fix dtype folat to int64
       match_row_line_notify.columns = ['代號','簡稱','發言時間','主旨','URL']
  
       ## check new array
       if not match_row_line.empty:
          
          for match_row_index in range(0,len(match_row_line),5) :
          
              ### for line notify msg 1000  character limit 
              msg = "\n " + match_row_line_notify.iloc[match_row_index:match_row_index+5,:].to_string(index = False)   
              tg_msg ="【Stock_NEWS】 "+ "\n" + msg
 
              ### for multiple line group
              #for line_key in  range(len(line_key_list)-3) : ## Stock_YoY[0]/Stock[1]/rss_google[2]
              #for line_key in  range(len(line_key_list)) : 
                  #send_line_notify(line_key_list[line_key],msg) 
              for line_key in  line_key_list : ##
                  send_line_notify(line_key, msg)   
                  time.sleep(random.randrange(1, 3, 1))

              for tg_key in  tg_key_list : ## 
                  send_tg_bot_msg(tg_key,tg_chat_id[0],tg_msg)
                  time.sleep(random.randrange(1, 3, 1))

       
     else : 
           match_row_line_notify = match_row.info()
           print(match_row_line_notify)


     #return match_row_line_notify
     

hot_new_key_wd(url,today)
