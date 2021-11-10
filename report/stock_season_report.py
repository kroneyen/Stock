#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
import requests
import line_notify
from bs4 import BeautifulSoup
import send_mail

mail_time = "18:00:00"



def get_redis_data(_key):
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    r = redis.StrictRedis(connection_pool=pool)
    _list = r.lrange(_key,'0','-1')
    return _list


def llist(df_len):       
    llist =[] 
    for i in range(df_len) : 
      llist.append(i)
    return llist

def stock_season_report(year, season, yoy_up,yoy_low):
    pd.options.display.max_rows = 3000
    pd.options.display.max_columns 
    pd.options.display.max_colwidth = 200 

    url_sii = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year='+ str(year-1911)  +'&season='+ str(season)
    url_otc = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=otc&year='+ str(year-1911)  +'&season='+ str(season)    
    url_list =[url_sii,url_otc]
    #type_list=['sii','otc']    
     
    df_f = pd.DataFrame()
    u_index = 0
    for url in url_list : 
       dfs=pd.DataFrame()
       r =  requests.post(url)
       r.encoding = 'utf8'
       soup = BeautifulSoup(r.text, 'html.parser')
       tables = soup.find_all('table',attrs={"class": "hasBorder"})
       for i in range(1,len(tables)+1) : 
        df = pd.read_html(r.text,thousands=",")[i] ## 多表格取的[i]為dataframe
        dfs = pd.concat([dfs,df],ignore_index=True) ##合併
       dfs.columns= llist(len(dfs.columns)) ##重新編列columns
       
       if u_index == 0 : ##取的欄位
        dfs = dfs.iloc[:,[0,1,21]]
        dfs.columns= llist(len(dfs.columns)) ##重新編列columns
       else : ##取的欄位
        dfs = dfs.iloc[:,[0,1,21]] 
        dfs.columns= llist(len(dfs.columns)) ##重新編列columns
       
       df_f = pd.concat([df_f,dfs],ignore_index=True) ###合併
       
       u_index +=1 

       time.sleep(1)


    df_f = df_f.sort_values(by=[2],ascending = False,ignore_index = True) ## 排序
    sst = "Q%s累計盈餘" %(season)
    #df_f.columns = ['公司代號', '公司簡稱','基本每股盈餘（元）' ]  ##定義欄位
    df_f.columns = ['公司代號', '公司簡稱',sst ]  ##定義欄位
    com_lists = get_redis_data("com_list")

    match_row_f =df_f[df_f['公司代號'].isin(com_lists)]

    return match_row_f


###  5/15,8/14,11/14,3/31

today = datetime.date.today()

if today.month > 4 and today.month< 6 :
  season = 1
elif  today.month > 7 and today.month< 9 :
  season = 2 

elif  today.month > 10 and today.month< 12 :
  season = 3
else :
  season = 4 

#match_row = stock_season_report(today.year,season,'undefined','undefined')
         
#### 累計EPS年增率

s_df = pd.DataFrame()

last_year = stock_season_report(today.year -1 ,season ,'undefined','undefined')  ## last year season

time.sleep(1)

the_year = stock_season_report(today.year,season,'undefined','undefined')  ## the year season

s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司簡稱'],suffixes=('_%s' % str(today.year) , '_%s' % str(today.year -1))).copy() ## merge 2021_Q3 & 2020_Q3 

#### (累計EPS / 去年累計EPS - 1) * 100% 
s_df['EPS成長%'] = (( (s_df.iloc[:,2] - (s_df.iloc[:,3]).abs() )/ (s_df.iloc[:,3]).abs() ) * 100 ).round(2) ## 計算成長% (2021- 2020) /2020 * 100%

#s_df['EPS成長%'] = (( (s_df['Q3累積盈餘_2021'] - (s_df['Q3累積盈餘_2020']).abs() )/ (s_df['Q3累積盈餘_2020']).abs() ) * 100 ).round(2) 


match_row = s_df.sort_values(by=['EPS成長%'],ascending = False).copy()

         



if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
    #match_row.to_html('hot_news_link.log')
    if not match_row.empty :
       body = match_row.to_html(escape=False)
       #log_date = datetime.date.today().strftime("%Y-%m-%d")
       send_mail.send_email('{year} stock_Q{season}_report' .format(year = today.year ,season=season) ,body)
else :
    print(match_row.to_string(index=False))
