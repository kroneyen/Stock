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
import re
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import random
import del_png
import numpy as np


# 改style要在改font之前
plt.style.use('seaborn')
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')


options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--no-sandbox")
prefs = {"download.default_directory":"./Dividend_file"}
options.add_experimental_option("prefs",prefs)
web = webdriver.Chrome(options=options)

del_png.del_files()



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


def update_many_db(_db,_collection,dicct,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection
    return collection.update_many(dicct,{'$set' :_values })



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
    ## https://goodinfo.tw/tw/StockDividendPolicyList.asp?MARKET_CAT=%E4%B8%8A%E5%B8%82&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024 
    ## https://goodinfo.tw/tw/StockDividendPolicyList.asp?MARKET_CAT=%E4%B8%8A%E6%AB%83&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024
    ## https://goodinfo.tw/tw/StockDividendScheduleList.asp?MARKET_CAT=%E5%85%A8%E9%83%A8&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024
    ##    url_1 = 'https://mops.twse.com.tw/mops/web/ajax_t05st09_2?encodeURIComponent=1&step=1&firstin=1&off=1&isnew=0&co_id='
    ##    url_2 = '&date1=109&date2=109&qryType=2'
 

    #print('url:',url)

    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}

    r = requests.get(url, headers= headers)

    dfs = pd.read_html(r.text)[0]



    return dfs


def Dividend_goodinfo() :


    url = 'https://goodinfo.tw/tw/StockDividendScheduleList.asp?MARKET_CAT=%E5%85%A8%E9%83%A8&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024'

    #url = 'https://histock.tw/stock/dividend.aspx'
    url_html = './Dividend_file/SaleMonDetail.html'


    #prefs = {"download.default_directory":"/content"};

    # 偽瀏覽器
    #headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}

    #r = requests.get(url, headers= headers)
    #r.encoding = 'utf-8'
   
    #time.sleep(random.randrange(1, 2, 1))
    
    web.get(url)
    time.sleep(random.randrange(3, 5, 1))
    
    html_d ='/html/body/table[2]/tbody/tr[2]/td[3]/table/tbody/tr/td/div/form/nobr[4]/input[2]'
    ### download file
    file_download = WebDriverWait(web, 5).until(EC.element_to_be_clickable((By.XPATH,html_d)))
    file_download.click()
    time.sleep(random.randrange(10, 15, 1))
   
    
    #df = pd.read_html('SaleMonDetail.html') ## []list to pandas
    df = pd.read_html(url_html) ## []list to pandas

    dfs = df[0].iloc[:,[1,2,15,4,18,9]]
    dfs.columns = ['code','code_name','cash_dividend','dividend_date','stock_dividend','dividend_stock_date']
    dfs['dividend_date'] = dfs['dividend_date'].apply(lambda x: x.replace(' 即將除息','').replace("'24/","")  if pd.notnull(x) and re.match("^'24/", x)  else np.NAN if re.match("^'23/", x) else x )
    dfs.dropna(subset=['dividend_date'],inplace = True)

    return dfs.sort_values(by=['dividend_date'],ascending = False,ignore_index = True)



def Dividend_twse():

    ###公開資訊站API
    url = 'https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL'


    df_all = pd.DataFrame()
    #pd.set_option('display.max_rows', None)
    #pd.set_option('display.max_columns', None)

    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}


    dfs = pd.read_json(url)
    
    dfs['Date_set'] = dfs['Date'].apply(lambda x: str(x).replace(str(datetime.date.today().year -1911 ),'')  if pd.notnull(x)  else  x )
    dfs['years'] = dfs['Date'].apply(lambda x: str(datetime.date.today().year)  if pd.notnull(x)  else  x )
    dfs['Date'] = dfs['Date_set'].apply(lambda x: x[0:2] +'/' + x[2:4]  if pd.notnull(x)  else  x )
    
    match_row = dfs.iloc[:,[1,2,7,0,4,0,13]].copy()
    
    match_row.columns = ['code','code_name','cash_dividend','dividend_date','stock_dividend','dividend_stock_date','years']

    match_row['dividend_date'] = match_row.apply(lambda x : checked(x['cash_dividend'],x['dividend_date']) ,axis=1 )
    match_row['cash_dividend'] = match_row.apply(lambda x : dividend_checked(x['cash_dividend']) ,axis=1 )
    match_row['dividend_stock_date'] = match_row.apply(lambda x : checked(x['stock_dividend'],x['dividend_stock_date']) ,axis=1 )
    match_row['stock_dividend'] = match_row.apply(lambda x : dividend_checked(x['stock_dividend']) ,axis=1 )


    if not match_row.empty  : 

       records = match_row.copy() 
       records =records.to_dict(orient='records')

       com_list = list(match_row['code'])
       del_year = match_row.iloc[0,6]
       del_filter ={'years': del_year , 'code': {'$in' : com_list}}      

       delete_many_mongo_db('stock','Rep_Stock_dividind_Com',del_filter)
       insert_many_mongo_db('stock','Rep_Stock_dividind_Com',records)
  
   
    #return match_row.sort_values(by=['dividend_date'],ascending = False,ignore_index = True)
    #print('Dividend_twse:',match_row.info())


def checked(x,y) :

      try:
        if float(x) > 0 :
          return y
        else :
          return np.nan

      except:
        return np.nan

def dividend_checked(x) :

      try:
        if float(x) > 0 :
          return x
        else :
          return 0

      except:
        return 0





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

#goodinfo_data = Dividend_goodinfo()

#twse_data = Dividend_twse()

#print(goodinfo_data.info())

div_data.columns= llist(len(div_data.columns))
div_data = div_data.iloc[:,[0,2,3,4,5]]
div_data.rename(columns={0: 'code', 2:'cash_dividend',3: 'dividend_date',4: 'stock_dividend',5:'dividend_stock_date'}, inplace=True)

#div_data['cash_dividend'] = div_data.apply(lambda x : dividend_checked(x['cash_dividend']) ,axis=1 )
#div_data['stock_dividend'] = div_data.apply(lambda x : dividend_checked(x['stock_dividend']) ,axis=1 )

#div_data['cash_dividend'].fillna(value=0, inplace=True)

#print(div_data[div_data['code'] == "2449"])

## get com_list

dictt = {}
_columns= {"_id":0 , "code" : 1,"name" : 1}


com_mydoc = atlas_read_mongo_db('stock','com_lists',dictt,_columns)

com_df = pd.DataFrame(list(com_mydoc))

com_df.rename(columns={'name' : 'code_name'}, inplace=True)


div_doc = com_df.merge(div_data,how='left' ,on=['code'])

#print(div_doc[div_doc['code'] == "2449"])


### cash_dividend NaN is 0 for history data
#if yy < today.year :

div_doc['cash_dividend']=div_doc['cash_dividend'].fillna(0)
div_doc['stock_dividend']=div_doc['stock_dividend'].fillna(0)


match_row = div_doc.copy()

if not match_row.empty  :

      records = match_row.copy()
      #records.columns =["code","code_name","the_year", "last_year" , "EPS_g%"]
      #records.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA']

      #records["season"] = str(season)
      records["years"]  = str(yy)
      #print(str(yy))
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
      
      ### doublecheck from twse

      Dividend_twse()


