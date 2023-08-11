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
from pymongo import MongoClient
from fake_useragent import UserAgent
import del_png
import re

### del images/*.png
del_png.del_images()




mail_time = "18:00:00"
#mail_time = "09:00:00"



def llist(df_len):       
    llist =[] 
    for i in range(df_len) : 
      llist.append(i)
    return llist



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



### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )


def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)

def delete_many_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.delete_many(dicct)

def read_mongo_db(_db,_collection,dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)





def stock_season_report(year, season, yoy_up,yoy_low,com_lists):
    #pd.options.display.max_rows = 3000
    #pd.options.display.max_columns 
    #pd.options.display.max_colwidth = 200 

    user_agent = UserAgent()

    url_sii = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year='+ str(year-1911)  +'&season='+ str(season)
    url_otc = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=otc&year='+ str(year-1911)  +'&season='+ str(season)    
    url_list =[url_sii,url_otc]
    
    df_f = pd.DataFrame() ## sii & otc

    for url in url_list : 
       dfs=pd.DataFrame()
       r =  requests.post(url ,  headers={ 'user-agent': user_agent.random })
       r.encoding = 'utf8'
       soup = BeautifulSoup(r.text, 'html.parser')
       tables = soup.find_all('table',attrs={"class": "hasBorder"})
       

       for i in range(1,len(tables)+1) : 

         df = pd.read_html(r.text,thousands=",")[i] ## 多表格取的[i]為dataframe

         df.columns= llist(len(df.columns)) ##重新編列columns
         last_col_num = len(df.columns)-1  ## get last columns num (基本每股盈餘（元）) 17 or 21 or 29

         df = df.iloc[:,[0,1,last_col_num]] ## 每年欄位不同 
         df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
         dfs = pd.concat([dfs,df],ignore_index=True) ##合併

    
       df_f = pd.concat([df_f,dfs],ignore_index=True) ###合併

       time.sleep(1)
    """
    ### get atlas mongodb data 
    mydoc = atlas_read_mongo_db("stock","com_list",{},{"_id":0,"code":1})
    com_lists = []
    for idx in mydoc :
     com_lists.append(idx.get('code'))
    """

    df_f[0]= df_f[0].astype('str')
    ### filter com_lists data
    df_f =df_f[df_f[0].isin(com_lists)]
    df_f = df_f.sort_values(by=[2],ascending = False,ignore_index = True) ## 排序
    sst = "Q%s累計盈餘" %(season)
    df_f.columns = ['公司代號', '公司名稱',sst ]  ##定義欄位
    return df_f


###  5/15,8/14,11/14,3/31

today = datetime.date.today()
#today = datetime.datetime.strptime('20221101', '%Y%m%d')

if today.month > 4 and today.month<= 6 :
  season = 1
elif  today.month > 7 and today.month<= 9 :
  season = 2 

elif  today.month > 10 and today.month<= 12 :
  season = 3
else :
  season = 4 

         
#### 累計EPS年增率

s_df = pd.DataFrame()


###  season cal 
if season == 4  :
     yy = today.year -1
     last_yy = today.year -2
else :
### last season replort
      yy = today.year
      last_yy = today.year -1


#### get com_lists

com_lists = []

try :

  ### got mongo data from atlas
  dictt = {}
  _columns= {"code":1,"_id":0}

  #mydoc = atlas_read_mongo_db('stock','com_list',dictt,_columns)
  mydoc = atlas_read_mongo_db('stock','com_lists',dictt,_columns)

  for idx in mydoc :
    com_lists.append(idx.get('code'))

except :
   ### got redis data from local
   redis_lists = get_redis_data("com_lists","hkeys",'NULL','NULL') ## get  redis data
   for  idx in redis_lists : 
        if not re.match("(\w+_p$)", idx) : 
           com_lists.append(idx) 



try :

        last_year = stock_season_report( last_yy ,season ,'undefined','undefined',com_lists)  ## last year season

        time.sleep(1)

        the_year = stock_season_report(yy ,season,'undefined','undefined',com_lists)  ## the year season

except :
        
         
        ### if no data  get last season
        last_year = stock_season_report( last_yy ,season -1 ,'undefined','undefined',com_lists)  ## last year season

        time.sleep(1)

        the_year = stock_season_report( yy ,season -1 ,'undefined','undefined',com_lists)  ## the year season
 


s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司名稱'],suffixes=('_%s' % str(yy) , '_%s' % str(last_yy))).copy() ## merge 2021_Q3 & 2020_Q3 

#### (累計EPS / 去年累計EPS - 1) * 100% 
s_df['EPS成長%'] = (( (s_df.iloc[:,2] - (s_df.iloc[:,3]).abs() )/ (s_df.iloc[:,3]).abs() ) * 100 ).round(2) ## 計算成長% (2021- 2020) /2020 * 100%

match_row = s_df.sort_values(by=['EPS成長%'],ascending = False,ignore_index = True).copy()



### insert to mongo 
records = match_row.copy()

records['season']= season
records['years']= yy

records.columns =['code','code_name','the_year','last_year','EPS_g%','season','years']
records = records.astype({'season':'string','years':'string'})

#records = records.to_dict(orient='records')
dicct={"season" : str(season), "years" : str(yy)}

### check data is exist on  mongo 
db_records = read_mongo_db('stock','Rep_Stock_Season_Com',{"season" : str(season),"years" : str(yy)},{"_id":0})
db_records_df =  pd.DataFrame(list(db_records))

chk = records.equals(db_records_df)
if chk == False :

   dicct = {"season" : str(season),"years" : str(yy)}
   delete_many_mongo_db('stock','Rep_Stock_Season_Com',dicct)

   records =records.to_dict(orient='records')
   insert_many_mongo_db('stock','Rep_Stock_Season_Com',records)
time.sleep(1)


#### insert data only
"""
for idx in list(range(2,5)) :
   
    if idx == 4 :  

       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s</font>' % x if x > 0 else  f'<font color="green">%s</font>' % x)

    else :

       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % str(x) if x < 0 else str(x))


if time.strftime("%H:%M:%S", time.localtime()) > mail_time :

    if not match_row.empty :
       body = match_row.to_html(classes='table table-striped',escape=False)
       send_mail.send_email('{year}_stock_Q{season}_report_com' .format(year = yy ,season=season) ,body)
else :
    print(match_row.to_html(escape=False))
"""

