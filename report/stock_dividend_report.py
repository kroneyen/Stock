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
import os
import pandas_table as pd_table
from io import StringIO
import urllib3
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 改style要在改font之前
plt.style.use("seaborn-v0_8")
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')


options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")
file_path = os.getcwd() + '/Dividend_file'
#prefs = {"download.default_directory":"./KD_file" ,"savefile.default_directory" : "./KD_file","download.directory_upgrade": True,} ###selenium 4.X
prefs = {"download.default_directory":file_path ,"savefile.default_directory" : file_path,"download.directory_upgrade": True,} ###selenium 4.X
#prefs = {"download.default_directory":"./Dividend_file"}
options.add_experimental_option("prefs",prefs)
web = webdriver.Chrome(options=options)

#del_png.del_files()



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

def read_mongo_db_sort_limit(_db,_collection,dicct,_columns,_sort):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns).sort(_sort)



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
    ## https://goodinfo.tw/tw/StockDividendPolicyList.asp?MARKET_CAT=%E4%B8%8A%E6%AB%83&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024
    ## https://goodinfo.tw/tw/StockDividendScheduleList.asp?MARKET_CAT=%E5%85%A8%E9%83%A8&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR=2024
    ##    url_1 = 'https://mops.twse.com.tw/mops/web/ajax_t05st09_2?encodeURIComponent=1&step=1&firstin=1&off=1&isnew=0&co_id='
    ##    url_2 = '&date1=109&date2=109&qryType=2'
 

    #print('url:',url)

    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/ 89.0.4389.82 Safari/537.36'}

    r = requests.get(url, headers= headers)

    dfs = pd.read_html(StringIO(r.text))[0]



    return dfs





def Dividend_goodinfo(years) :

    #url = 'https://goodinfo.tw/tw/StockDividendScheduleList.asp?MARKET_CAT=%E5%85%A8%E9%83%A8&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR='+str(years)
    url_future = 'https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=%E6%99%BA%E6%85%A7%E9%81%B8%E8%82%A1&INDUSTRY_CAT=%E5%8D%B3%E5%B0%87%E9%99%A4%E6%AC%8A%E6%88%96%E9%99%A4%E6%81%AF'
    url_done = 'https://goodinfo.tw/tw2/StockList.asp?RPT_TIME=&MARKET_CAT=%E6%99%BA%E6%85%A7%E9%81%B8%E8%82%A1&INDUSTRY_CAT=%E5%B7%B2%E9%99%A4%E6%AC%8A%E6%88%96%E9%99%A4%E6%81%AF'
    	
    url_idx = 0
    df_concat =pd.DataFrame()
    	
    for url in  [url_done,url_future] :
         ### save Dividend_file path
         
         #old = './Dividend_file/StockList.html'
         old = file_path +'/StockList.html'

         if url_idx == 0 : 
           
            #file_html = './Dividend_file/'+str(years)+'_done_StockList.html'
            file_html = file_path + '/'+str(years)+'_done_StockList.html'
            #columns_list=[0,1,7,6,16,15,9]
         
         else : 
            	 
            #file_html = './Dividend_file/'+str(years)+'_future_StockList.html'
            file_html = file_path +'/'+str(years)+'_future_StockList.html'
            #columns_list=[0,1,7,6,16,15] 
         
         
         start = time.time()

         download_times = 3
         for check in range(0 ,download_times) : 
             web.get(url)
             time.sleep(random.randrange(10 ,20, 1))
         
             html_d ='/html/body/table[2]/tbody/tr[2]/td[3]/main/section/table/tbody/tr[5]/td[2]/input[3]'
             ads_button = "ats-interstitial-button"
             ### download file

             try :

                 ads_iframe_close =  WebDriverWait(web, 5).until(EC.element_to_be_clickable((By.ID,ads_button)))
                 ads_iframe_close.click()
                 time.sleep(random.randrange(3, 5, 1))
                 print('have ads')
             except : 
                continue 
 
             finally  : 
                file_download = WebDriverWait(web, 15).until(EC.element_to_be_clickable((By.XPATH,html_d)))
                #time.sleep(random.randrange(3, 5, 1))
                file_download.click()

                time.sleep(random.randrange(5, 10, 1))
        
                if  os.path.exists(old) :
                    break
         
                else : 
                    web.close()  
         os.rename(old, file_html)
         df = pd.read_html(file_html)[0] ## []list to pandas
         
         ### Filter columns
         
         """
         dfs = df.iloc[:,[1,2,15,4,18,9]]
         dfs.columns = ['code','code_name','cash_dividend','dividend_date','stock_dividend','dividend_stock_date']
         """
         #dfs = df.iloc[:,[1,2,15,4,18,9,5]]
         ### change web version 
         dfs = df.iloc[:,[0,1,7,6,16,15,9]]
                  	  	
         dfs.columns = ['code','code_name','cash_dividend','dividend_date','stock_dividend','dividend_stock_date','price']
         
         
         #year = "'"+ url_html.split("_")[0][2:4] +"/"
         year = "'"+ str(years)[2:4] +"/"
         
         ### avoid chaining SettingWithCopyWarning
         #dfs = dfs.copy()
         dfs = dfs[dfs['dividend_date'].str.contains(year, na=False)] ## filter not match years
         
         #dfs['dividend_date'] = dfs['dividend_date'].apply(lambda x: x.replace(' 即將除息','').replace(' 今日除息','').replace(year,"")  if pd.notnull(x) and re.match(year, x)  else np.NAN  )
         dfs['dividend_date'] = dfs['dividend_date'].apply(lambda x: x.replace('即將除息','').replace('今日除息','').replace(year,"").replace(r'\s+','')  if pd.notnull(x) and re.match(year, x)  else np.NAN  )
         #dfs['dividend_stock_date'] = dfs.apply(lambda x: x['dividend_stock_date'].replace(year,"")  if pd.notnull(x['dividend_stock_date']) and re.match(year, x['dividend_stock_date']) else np.NAN if x['stock_dividend'] == 0  else x['dividend_date'] ,axis=1 )
         dfs['dividend_stock_date'] = dfs.apply(lambda x: x['dividend_stock_date'].replace('即將除權','').replace('今日除權','').replace(year,"").replace(r'\s+','')  if pd.notnull(x['dividend_stock_date']) and re.match(year, x['dividend_stock_date']) else np.NAN if x['stock_dividend'] == 0  else x['dividend_date'] ,axis=1 )
         
         dfs['dividend_date'] = dfs.apply(lambda x : checked(x['cash_dividend'],x['dividend_date']) ,axis=1 )
         dfs['cash_dividend'] = dfs.apply(lambda x : dividend_checked(x['cash_dividend']) ,axis=1 )
         dfs['dividend_stock_date'] = dfs.apply(lambda x : checked(x['stock_dividend'],x['dividend_stock_date']) ,axis=1 )
         dfs['stock_dividend'] = dfs.apply(lambda x : dividend_checked(x['stock_dividend']) ,axis=1 )
         
         dfs['years'] = dfs['code'].apply(lambda x: str(years)  if pd.notnull(x)  else np.NAN )
         
         
         ### get mongo data
         _dictt = { "years" : str(years)}
         
         _values = { "_id" :0 , "code" :1 }
         
         mydoc = read_mongo_db('stock','Rep_Stock_dividend_Com',_dictt,_values)
         
         df_mydoc = pd.DataFrame(list(mydoc))
         
         if not df_mydoc.empty :
         
           #df_merge = df_mydoc.merge(dfs,how='left', on='code')
           df_merge = df_mydoc.merge(dfs,how='inner', on='code')
         
         
         else :
         
           df_merge = dfs.copy()
         
         #df_merge = dfs.copy()
         ##remove duplicate code 
         df_merge.dropna(subset=['cash_dividend'],inplace = True)
         
         #com_list = df_merge['code'].drop_duplicates().tolist()
         #d_dictt = { "years" : str(years) ,"code" : {"$in" : com_list } }
         
         #print('d_dictt:',com_list)
         """ 
         delete_many_mongo_db('stock','Rep_Stock_dividend_Com',d_dictt)
         records = df_merge.copy()
         records =records.to_dict(orient='records')
         insert_many_mongo_db('stock','Rep_Stock_dividend_Com',records)
         """
         #return dfs.sort_values(by=['dividend_date'],ascending = False,ignore_index = True)
         df_concat = pd.concat([df_concat, df_merge])
         
        
         
         url_idx += 1
         
         
    df_concat = df_concat.drop_duplicates(subset=['code', 'cash_dividend','dividend_date'])
    web.quit()

    #return df_merge[df_merge["code"]== "2449"]
    return df_concat




def Dividend_twse():

    ###公開資訊站API 僅提供最新資訊
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

       delete_many_mongo_db('stock','Rep_Stock_dividend_Com',del_filter)
       insert_many_mongo_db('stock','Rep_Stock_dividend_Com',records)
  
   
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



### main code

#year=2024
#yy = year


key_yy=str(today.year)
#key_yy='2024'

#div_data = Dividend(yy,com_lists)

div_data = Dividend_goodinfo(key_yy)

#twse_data = Dividend_twse()

#print(goodinfo_data.info())
#print(div_data.info())

div_data = div_data.astype({'cash_dividend':'float' ,'stock_dividend':'float' , 'price':'float'})
div_data.columns= llist(len(div_data.columns))
div_data = div_data.iloc[:,[0,2,3,4,5,6]].copy() ## adding price
merge_data =div_data.iloc[:,[1,5]].copy() ### cash_dividend , price
merge_data_name = list(merge_data.columns)
div_data[6] = merge_data.apply(lambda  x:   round((x[merge_data_name[0]] + x[merge_data_name[1]]),2)  if   x[merge_data_name[0]]  > 0 else 0 ,axis=1) ### price = cash_dividend + price
div_data[7] = merge_data.apply(lambda  x:   round(x[merge_data_name[0]] / (x[merge_data_name[0]] + x[merge_data_name[1]])*100 ,2)  if   ( x[merge_data_name[0]]  > 0 and x[merge_data_name[1]] > 0 ) else 0 ,axis=1) ### price = cash_dividend + price

div_data.rename(columns={0: 'code', 2:'cash_dividend',3: 'dividend_date',4: 'stock_dividend',5:'dividend_stock_date',6:'price',7 : 'yield'}, inplace=True)


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
#div_doc = div_doc.dropna(inplace = True)
#print(div_data.info())
### cash_dividend NaN is 0 for history data
#if yy < today.year :

div_doc['cash_dividend']=div_doc['cash_dividend'].fillna(0)
div_doc['stock_dividend']=div_doc['stock_dividend'].fillna(0)

match_row = div_doc.drop_duplicates(subset=['code', 'cash_dividend','dividend_date']).copy()
#match_row = div_data.drop_duplicates(subset=['code', 'cash_dividend','dividend_date']).copy()
#match_row["years"]=str(key_yy)

#print(div_data.info())

if not match_row.empty  :

      records = match_row.copy()
      #records_coms_list = list(div_data['code'])

      records["years"]  = str(key_yy)
      ### rename for mongodb
      records =records.to_dict(orient='records')
      ## coding  bec not include ROE ROA
      #del_filter = {"years":str(yy),"season":str(season)}
      #del_filter = {"years":str(key_yy),'code': {"$in" : records_coms_list},"price" :  float("NaN")}
      del_filter = {"years":str(key_yy)}
     
      #print("del_filter:",del_filter)
      delete_many_mongo_db('stock','Rep_Stock_dividend_Com',del_filter)
      insert_many_mongo_db('stock','Rep_Stock_dividend_Com',records)

      time.sleep(1)
      
      ### doublecheck from twse

      #Dividend_twse()


### Mail Report Dividind Coming 

_dividend_date = today.strftime('%m/%d')
mail_date = today.strftime('%Y%m%d')

_dictt = { "years" : str(key_yy),"dividend_date" : {"$gte": _dividend_date}}

_values = { "_id" :0 ,"price":0, "yield":0}

_sort=[("dividend_date",1)]

mydoc = read_mongo_db_sort_limit('stock','Rep_Stock_dividend_Com',_dictt,_values,_sort)

match_row = pd.DataFrame(list(mydoc))


### get mongodata Stock_Eps_Yield_PE_Daily 

_dicct= {}
_columns={"_id" :0 , "years" :0 }

mydoc = read_mongo_db('stock','Stock_Eps_Yield_PE_Daily',_dicct,_columns) 
mydoc_eps_daily = pd.DataFrame(list(mydoc))
match_row = match_row.merge(mydoc_eps_daily,how='left', on='code')
match_row = match_row.drop('years',axis=1)

#print(match_row.info())


#if not match_row.empty and time.strftime("%H:%M:%S", time.localtime()) > mail_time :
if not match_row.empty :
       
       
       for  idx in [0,2,4] :

            match_row[match_row.columns[idx]] = match_row[match_row.columns[idx]].astype(object)

            if idx in [2,4]:

               #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % round(x,3)   if x < 5  else f'<font color="red">+%s</font>' % round(x,3)  if x >=10 else  round(x,3))
               match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % round(x,3)   if x < 5  else f'<p style="background-color:Aqua;">%s</p>' % round(x,3)  if x >=10 else  f'<font color="red">+%s</font>' % round(x,3))


            elif idx == 0 :

               match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<a href="https://www.wantgoo.com/stock/%s/dividend-policy/ex-dividend" target="_blank">%s</a>' %( x , x )  if pd.notnull(x)  else  x)
      

       match_row = pd_table.add_columns_into_row(match_row,20) 
       body = match_row.to_html(classes='table table-striped',escape=False)
       send_mail.send_email('Stock_Dividind_Coming_{today}'.format(today=mail_date),body)


       #print(body)
