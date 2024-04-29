#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
import requests
import line_notify
import send_mail
from pymongo import MongoClient
from fake_useragent import UserAgent
import del_png
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager


# 改style要在改font之前
plt.style.use('seaborn')
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')



### del images/*.png
del_png.del_images()

user_agent = UserAgent()

mail_time = "09:00:00"
#mail_time = "18:00:00"



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

    return _list





### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )



def read_mongo_db(_db,_collection,dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def read_aggregate_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)


def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)


def delete_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    d=collection.delete_many(_values)
    #print(d.deleted_count ," documents deleted !!")


### mongodb atlas connection
user = get_redis_data('mongodb_user',"hget","user",'NULL')
pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

conn = MongoClient('mongodb+srv://'+user+':'+pwd+'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def get_mongo_last_date(cal_day):
 ### mongo query for last ? days
 dictt_set = [ {"$group": { "_id" : { "$toInt" : "$last_modify" } }} , {"$sort" : {"_id" :-1}} , { "$limit" : cal_day},{"$sort" : {"_id" :1}} , { "$limit" :1}]

 ### mongo dict data

 set_doc =  read_aggregate_mongo_db('stock','Rep_3_Investors',dictt_set)

 ### for lists  get cal date 
 for idx in set_doc:

  idx_date = idx.get("_id")

 #set_date = str(idx_date)
 return str(idx_date)




"""
def Dividend_report():
    
    ###公開資訊站API
    url = 'https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL'
      
   
    df_all = pd.DataFrame()
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}


    dfs = pd.read_json(url)
    ### Date	Code	Name	Exdividend	StockDividendRatio	SubscriptionRatio	SubscriptionPricePerShare	CashDividend	SharesOffered	SharesEmpOwner	SharesholderOwner	StockHoldingRatio
  
    #match_row = dfs[dfs['Code'].isin(com_lists)]

    #match_row = dfs.iloc[:,[1,3,7,0]]
    match_row = dfs.copy()


    return match_row
"""

def Dividend(year,com_lists) :


    url ='https://stock.wespai.com/rate'+str(year-1911)
    ### goodinfo
    ## sii / otc   
    ## https://goodinfo.tw/tw/StockDividendPolicyList.asp?MARKET_CAT=%E5%85%A8%E9%83%A8&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024 
    ## https://goodinfo.tw/tw/StockDividendPolicyList.asp?MARKET_CAT=%E4%B8%8A%E6%AB%83&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024

    ##    url_1 = 'https://mops.twse.com.tw/mops/web/ajax_t05st09_2?encodeURIComponent=1&step=1&firstin=1&off=1&isnew=0&co_id='
    ##    url_2 = '&date1=109&date2=109&qryType=2'
 

    #print('url:',url)

    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}

    r = requests.get(url, headers= headers)

    dfs = pd.read_html(r.text)[0]



    return dfs






###5/15,8/14,11/14,3/31

today = datetime.date.today()

if today.month >= 4 and today.month<= 6 :
  season = 1
elif  today.month >= 7 and today.month<= 9 :
  season = 2 

elif  today.month >= 10 and today.month<= 12 :
  season = 3
else :
  season = 4 


s_df = pd.DataFrame()


###  season cal 
if season == 4  :
     yy = today.year -1
     last_yy = today.year -2
else :
### last season replort
      yy = today.year
      last_yy = today.year -1




### get atlas mongodb data 

try :

  ### got mongo data from atlas
  dictt = {}
  _columns= {"code":1,"_id":0}
  com_lists = []

  mydoc = atlas_read_mongo_db('stock','com_list',dictt,_columns)

  for idx in mydoc :
    com_lists.append(idx.get('code'))

except :
   ### got redis data from local
   com_lists = get_redis_data("com_list","lrange",0,-1) ## get  redis data



### get season data from mongo

#print('yy,season:', yy,season)



###main code

#year=2024
#yy = year

div_data = Dividend(yy,com_lists)
#print(div_data.info())

div_data.columns= llist(len(div_data.columns))
div_data = div_data.iloc[:,[0,1,2,3,4,5,]]
div_data.rename(columns={0: 'code',1 :'code_name', 2:'cash_dividend',3: 'dividend_date',4: 'stock_dividend',5:'dividend_stock_date'}, inplace=True)


## get com_list


com_mydoc = read_mongo_db('stock','com_lists',{},{"_id":0 , "code" : 1,"name" : 1})

com_df = pd.DataFrame(list(com_mydoc))

com_df.rename(columns={'name' : 'code_name'}, inplace=True)


div_doc = com_df.merge(div_data,how='left' ,on=['code','code_name'])

#match_row = s_df.merge(match_row_roe,how='left' ,on=['code','code_name'])

### cash_dividend NaN is 0 for history data
if yy < today.year :

   div_doc['cash_dividend']=div_doc['cash_dividend'].fillna(0)


match_row = div_doc.copy()

if not match_row.empty  :

      records = match_row.copy()
      #records.columns =["code","code_name","the_year", "last_year" , "EPS_g%"]
      #records.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA']

      #records["season"] = str(season)
      records["years"]  = str(yy)

      #print('records:',records.info())
      ### rename for mongodb
      #records.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA','season','years']
      records =records.to_dict(orient='records')
      ## coding  bec not include ROE ROA
      #del_filter = {"years":str(yy),"season":str(season)}
      del_filter = {"years":str(yy)}
      #print("del_filter:",del_filter)
      delete_many_mongo_db('stock','Rep_Stock_dividind_Com',del_filter)
      insert_many_mongo_db('stock','Rep_Stock_dividind_Com',records)

      time.sleep(1)
