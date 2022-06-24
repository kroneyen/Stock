#! /usr/bin/env python3.7
# -*- coding: utf-8 -*-
import requests
import pandas as pd
import numpy as np
import time
from io import StringIO
import json
import datetime
import random
import os
#from openpyxl.workbook import Workbook
#import openpyxl

def get_redis_data(_key):
    import redis
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    r = redis.StrictRedis(connection_pool=pool)
    _list = r.lrange(_key,'0','-1')
    return _list


def llist(df_len):
    llist =[]
    for i in range(df_len) :
      llist.append(i)
    return llist



def Dividend(years,date_s,date_d) :
   

   if years >= 1000:
     
     years -= 1911
     url ='https://stock.wespai.com/rate'+ str(years)
    
   else:
        print('type does not match')
         
   df_all = pd.DataFrame()
   pd.set_option('display.max_rows', None)
   pd.set_option('display.max_columns', None)
   pd.set_option("precision", 2)

   headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}
   
   #print(url)

   com_lists = get_redis_data("com_list")

   r = requests.post(url,headers=headers)
   r.encoding = 'utf8'
   time.sleep(random.randrange(1, 3, 1))
   
   df = pd.read_html(r.text,thousands=",")[0]
   df_len = len(df.columns)
   df.columns= llist(len(df.columns)) ##編列columns

   df = df.iloc[:,[0,1,2,3,4,7,8,10,11,16,17,18,19,20]] ## 取的欄位
   #df = df.iloc[:,[0,1,2,3,4,7,8,10,11,16,17,18,19,20,21]] ## 取的欄位
   df.columns= llist(len(df.columns))
   df = df[df[0].isin(com_lists)] ##比對 
   #time.sleep(1)
   df.columns = ['代號', '公司', '配息', '除息日', '配股', '現金殖利率%', '殖利率%','配息率%','董監持股%','1QEPS','2QEPS', '3QEPS', '今年累積EPS', '去年EPS']
   #df.columns = ['代號', '公司', '配息', '除息日', '配股', '現金殖利率', '殖利率','配息率','董監持股','1QEPS',
   #    '2QEPS', '3QEPS', '今年累積EPS', '去年EPS', '本益比'] 

  
   #df = pd.concat([df for df in dfs if df.shape[1] <=50 and df.shape[1] > 5] ,sort = False, ignore_index = True) 
   #df = df [['代號','公司','配息',]]
   #match_row=df[df['代號'].isin(com_lists)]

   #if dfs != [] :
   
   #df = df.fillna('12/31')
   
  
   """
   mask_1 = df['除息日'] > date_s
   mask_2 = df['除息日'] < date_d
   match_row = df[(mask_1 & mask_2)].sort_values(by=['配息'],ascending = False).sort_values(by=['除息日']).reset_index(drop=True)
   return match_row.iloc[:,[0,1,2,3,4,6,7]]
   """
   """
        #match_row = df[df['累計YoY(%)'] > yoy_up] < yoy_do].sort_values(by=['累計YoY(%)'],ascending = False)
   '代號', '公司', '配息', '除息日', '配股', '除權日', '股價', '現金殖利率', '殖利率', '還原殖利率',
       '發息日', '配息率', '董監持股', '3年平均股利', '6年平均股利', '10年平均股利', '10年股利次數', '1QEPS',
       '2QEPS', '3QEPS', '今年累積EPS', '去年EPS', '本益比', '股價淨值比', '多少張以上要繳健保費',
       '一張繳健保費'
   df_1 = df[df['除息日'] >today].sort_values(by=['除息日','配息'],ascending = [True,False]).reset_index(drop=True).iloc[:,[0,1,2,3,4,6,7]]
   df_2 = df[df['除息日'].isna()].sort_values(by=['配息'],ascending = [False]).reset_index(drop=True).iloc[:,[0,1,2,3,4,6,7]]

   df = pd.concat([df_1,df_2] ,sort = False,ignore_index = True)
   df['公司'] = df['代號'].map(str) + ' ' + df['公司']
   df = df.iloc[:,1:7]   
   """
   return df.sort_values(by=['今年累積EPS'],ascending = False ,ignore_index = True)
  


today = datetime.date.today().strftime("%m/%d")
today_csv = datetime.date.today().strftime("%Y%m%d")
year= datetime.date.today().strftime("%Y")
#today_csv = '20211014'
#print(today)

#print(Dividend(2021,today,'08/31'))
os.getcwd() ## get path
rr = Dividend(int(year),today,'08/31')
try :
     with pd.ExcelWriter('%s_Yield_Report_v1.xlsx' % year, mode='a') as writer: 
        rr.to_excel(writer ,sheet_name=today_csv,index=0, encoding='utf-8')
except : 
        rr.to_excel('%s_Yield_Report_v1.xlsx' % year,sheet_name=today_csv,index=0, encoding='utf-8')

print(rr)

