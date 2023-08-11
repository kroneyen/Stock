#! /usr/bin/env python3.7
# -*- coding: utf-8 -*-
import pandas as pd
import logging
import send_mail
import datetime
import redis
import time
from dateutil.relativedelta import relativedelta
from tabulate import tabulate
import requests
from pymongo import MongoClient
import del_png
from fake_useragent import UserAgent



### del images/*.png
del_png.del_images()


today_week = datetime.date.today().strftime("%w")
mail_time = "18:00:00"
#ail_time = "09:00:00"

"""
def get_redis_data():
    import redis
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    r = redis.StrictRedis(connection_pool=pool)
    com_list = r.lrange('com_list','0','-1')
    return com_list 
"""

def get_redis_data(_key,_type,_field_1,_field_2):
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    r = redis.StrictRedis(connection_pool=pool)

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list


### mongodb atlas connection
user = get_redis_data('mongodb_user',"hget","user",'NULL')
pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

conn = MongoClient('mongodb+srv://'+user+':'+pwd+'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



def tableize(df):
    if not isinstance(df, pd.DataFrame):
        return
    df_columns = df.columns.tolist() 
    max_len_in_lst = lambda lst: len(sorted(lst, reverse=True, key=len)[0])
    align_center = lambda st, sz: "{0}{1}{0}".format(" "*(1+(sz-len(st))//2), st)[:sz] if len(st) < sz else st
    align_right = lambda st, sz: "{0}{1} ".format(" "*(sz-len(st)-1), st) if len(st) < sz else st
    max_col_len = max_len_in_lst(df_columns)
    max_val_len_for_col = dict([(col, max_len_in_lst(df.iloc[:,idx].astype('str'))) for idx, col in enumerate(df_columns)])
    col_sizes = dict([(col, 2 + max(max_val_len_for_col.get(col, 0), max_col_len)) for col in df_columns])
    build_hline = lambda row: '+'.join(['-' * col_sizes[col] for col in row]).join(['+', '+'])
    build_data = lambda row, align: "|".join([align(str(val), col_sizes[df_columns[idx]]) for idx, val in enumerate(row)]).join(['|', '|'])
    hline = build_hline(df_columns)
    out = [hline, build_data(df_columns, align_center), hline]
    for _, row in df.iterrows():
        out.append(build_data(row.tolist(), align_right))
    out.append(hline)
    return "\n".join(out)


def monthly_report():

     #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'}
     report_day = datetime.date.today() + relativedelta(months=-1)
     url   ='https://mops.twse.com.tw/nas/t21/sii/t21sc03_' + str(report_day.year - 1911) + '_' + str(report_day.month) +'_0.html'
     ###上櫃
     url_1 ='https://mops.twse.com.tw/nas/t21/otc/t21sc03_' + str(report_day.year - 1911) + '_' + str(report_day.month) +'_0.html'
     today_week = datetime.date.today().strftime("%w")
     #mail_time = "21:00:00"
    
     user_agent = UserAgent() 
     url_list = [url,url_1]
     df_report = pd.DataFrame()
     for url_index in url_list :
         r = requests.get(url_index,  headers={ 'user-agent': user_agent.random } )
         r.encoding = 'big5'
         report_table = pd.read_html(r.text)
          
         for index in range(2,len(report_table),2):
           df_table = report_table[index]
           df_table = pd.DataFrame(df_table) ##redefine DataFrame
           df_table = df_table.iloc[:-1,[0,1,2,4,6,7,8,9]]   ###row -1 去除合計 ; columns : 公司代號 公司名稱 當月營收 去年當月營收 去年同月增減(%)  當月累計營收 去年累計營收 前期比較增減(%)
           df_report = pd.concat([df_report,df_table],ignore_index = True)
     #print(df_report.columns)    
     df_report.columns=["公司代號","公司名稱","當月營收","去年當月營收", "YoY(%)" , "當月累計營收","去年累計營收", "累計YoY(%)"]
     #com_lists = get_redis_data()
     
     ### get atlas mongodb data
     mydoc = atlas_read_mongo_db("stock","com_list",{},{"_id":0,"code":1})
     com_lists = []
     for idx in mydoc :
        com_lists.append(idx.get('code'))

     df_report['公司代號'] = df_report['公司代號'].astype('str')

     match_row = df_report[df_report['公司代號'].isin(com_lists)]
     #match_row = match_row.sort_values(by=['累計YoY(%)'],ascending = False,ignore_index = True) 

     df_report = pd.DataFrame()
     df_report = match_row.iloc[:,[0,1,4,7]].copy()       
     df_report['當月營收(百萬)']= round(match_row['當月營收']/1000,2)
     df_report['去年當月(百萬)']= round(match_row['去年當月營收']/ 1000,2)
     df_report['當月累計(千萬)'] = round(match_row['當月累計營收']/ 1000000,2)
     df_report['去年累計(千萬)'] = round(match_row['去年累計營收']/ 1000000,2)    
     df_report = df_report.iloc[:,[0,1,4,5,2,6,7,3]]
     df_report = df_report.sort_values(by=['累計YoY(%)'],ascending = False,ignore_index = True)
     #return match_row.iloc[:,[0,1,8,9,4,10,11,7]]
     #return match_row.iloc[:,[0,1,8,9,4,10,11,7]]
     return df_report,report_day

report ,report_day = monthly_report()
#report = report.to_markdown(index= 0)
#report = report.to_html()
#report = tableize(report)
#print(report_day)
for idx in list(range(2, 8)) :
    if idx == 4 or  idx == 7 :
       #report.iloc[:,idx] = report.iloc[:,idx].apply(lambda  x: f'<font color="red">%s</font>' % str(x) if x < 0 else str(x))
       report.iloc[:,idx] = report.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s</font>' % x if x > 0 else  f'<font color="green">%s</font>' % x)


#print(report.style.render())
mail_month =str(report_day.month)


if not report.empty :

    if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
       body = report.to_html(escape=False)
       #print('body:',body)
       #send_mail.send_email('stock_month_report',body)
       send_mail.send_email('stock_month_%s_report' % mail_month ,body)

    else :
      report = report.to_markdown(index= 0)
      #print(report.to_string(index=False))
      print('stock_month_%s_report' % mail_month)
      print(report)
 
