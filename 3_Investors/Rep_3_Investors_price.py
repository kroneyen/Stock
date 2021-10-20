#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
#import line_notify
import requests
import send_mail
from IPython.display import display, clear_output
from io import StringIO


mail_time = "16:00:00"
#mail_time = "09:00:00"

date_sii = datetime.date.today().strftime('%Y%m%d')
date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')

#date_sii='20210923'
#date_otc='110/09/23'


def tableColor(val):
    if val > 0:
        color = 'red'
    elif val < 0:
        color = 'green'
    else:
        color = 'block'
    return 'color: %s' % color


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




def Rep_3_Investors(date_sii,date_otc,com_lists) : 


  url_sii = 'https://www.twse.com.tw/fund/T86?response=html&date='+ date_sii +'&selectType=ALL'
  url_otc ='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=htm&se=EW&t=D&d=' + date_otc + '&s=0,asc'
  url_list = [url_sii,url_otc]
  dfs = pd.DataFrame()
  u_index = 0
  
  #com_lists = get_redis_data("com_list") ## get  redis data
  
  for url in url_list :
     
     r = requests.get(url)
     r.encoding = 'utf8'
     df = pd.read_html(r.text,thousands=",")[0]
     df_len = len(df.columns)
     df.columns= llist(len(df.columns)) ##編列columns

     
     ### 0,1,4,13,22 /*sii*/
     if u_index == 0 :

       df = df.iloc[:,[0,1,4,10,11]] ## 取的欄位
       df.columns= llist(len(df.columns))
       df = df[df[0].isin(com_lists)] ##比對 
       
     
     #### 0,1,4,13,22/*otc*/
     else :
      
       df = df.iloc[:,[0,1,4,13,22]]
       df.columns= llist(len(df.columns)) ## define new column array
       df = df[df[0].isin(com_lists)] ##比對

     ##df = df[df[0].isin(com_lists)] ##比對
     
     dfs = pd.concat([dfs,df],ignore_index=True) ##合併
          
     time.sleep(1)
     u_index += 1

  

  dfs = dfs.astype({2:'int64',3:'int64',4:'int64'}) ##change type

  dfs[2] =round((dfs[2]/1000),0).astype(int)
  dfs[3] =round((dfs[3]/1000),0).astype(int)
  dfs[4] =round((dfs[4]/1000),0).astype(int)
  #dfs = dfs.style.apply(color_negative_red)
  dfs.columns = ['公司代號', '公司簡稱', '法人', '投信', '自營商'] 
  #dfs = dfs.sort_values(by=['公司代號'])    
  #dfs = dfs.style.applymap(tableColor, subset=['法人', '投信', '自營商'])


  return dfs.sort_values(by=['公司代號'])



#S_Price_v1('2021/08/26',4919,'sii')
#Rep_3_Investors(url_list)

def Rep_price(date_sii,date_otc,com_lists):  
  
       url_sii = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=' + date_sii + '&type=ALL'
       #https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=20210910&type=ALL
       
       url_otc = 'https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=' + date_otc +'&o=htm&s=0,asc,0'
       #https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=110/09/10&o=htm&s=0,asc,0
       
       url_list = [url_sii,url_otc]
       
       u_index = 0
       dfs = pd.DataFrame()
       for url in url_list :
              r = requests.get(url)
              r.encoding = 'utf8'
              if u_index == 0 :
                df = pd.read_html(r.text,thousands=",")[8] ## []list to pandas
              else : 
                df = pd.read_html(r.text,thousands=",")[0] ## []list to pandas
              
              df_len = len(df.columns)
              df.columns= llist(len(df.columns)) ##編列columns
              df = df[df[0].isin(com_lists)] ##比對
              
              #### 0,1,2,3/*sii(證券代號	證券名稱 收盤價	漲跌(+/-)	漲跌價差)*/ 
              if u_index == 0 :
                df[16] = df[9] + df[10].astype(str) ##合併 (漲跌(+/-)	漲跌價差)
                df = df.iloc[:,[0,1,8,16]] ## 取的欄位 
                df.columns= llist(len(df.columns))
                       
              
              #### 0,1,2,3/*otc代號	名稱	收盤	漲跌*/
              else :
               
                df = df.iloc[:,[0,1,2,3]] ##代號	名稱	收盤	漲跌
                df.columns= llist(len(df.columns)) ## define new column array
                
              ##df = df[df[0].isin(com_lists)] ##比對
              
              dfs = pd.concat([dfs,df],ignore_index=True) ##合併
              #dfs[3] = df[3].fillna(0)
                   
              time.sleep(1)
              u_index += 1
              #print(df.info())
       dfs = dfs.fillna(0)
       dfs.columns = ['公司代號', '證券名稱', '收盤價', '漲跌(+/-)'] 
       
       return dfs




df_s = pd.DataFrame()

com_lists = get_redis_data("com_list") ## get  redis data

df_Rep_3_Investors = Rep_3_Investors(date_sii,date_otc,com_lists)
df_Rep_price = Rep_price(date_sii,date_otc,com_lists)

df_s = pd.merge(df_Rep_3_Investors,df_Rep_price, on =['公司代號']) ##dataframe join by columns
df_s['漲跌(+/-)'] = df_s['漲跌(+/-)'].str.replace('X','').str.replace('除息','0').str.replace(' ---','0').str.replace('--- ','0').astype({'漲跌(+/-)':'float'}).fillna(0).round(2)
df_s = df_s.iloc[:,[0,1,2,3,4,6,7]]
#df_s = df_s.fillna(0)
#match_row = df_s.style.applymap(tableColor, subset=['法人', '投信', '自營商','漲跌(+/-)']).set_precision(2)
match_row = df_s



#match_row=Rep_3_Investors(url_list)

if time.strftime("%H:%M:%S", time.localtime()) > mail_time :

    if len(match_row) > 0 :
       body = match_row.to_html(escape=False)

       send_mail.send_email('Rep_3_Investors_%s' % date_sii ,body)

else :
    print('Rep_3_Investors_%s' % date_sii )
    print(match_row)
    #display(match_row)

