# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
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


# 改style要在改font之前
plt.style.use("seaborn-v0_8")
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")
options.add_argument("--disable-site-isolation-trials")
options.add_argument("--renderer-process-limit=4")
file_path =  os.getcwd()+ '/KD_file'
#prefs = {"download.default_directory":"./KD_file" ,"savefile.default_directory" : "./KD_file","download.directory_upgrade": True,} ###selenium 4.X 
prefs = {"download.default_directory":file_path ,"savefile.default_directory" : file_path,"download.directory_upgrade": True,} ###selenium 4.X 
#prefs = {"download.default_directory":'./KD_file', "download.directory_upgrade": True,} ###selenium 4.X 
options.add_experimental_option("prefs",prefs)
web = webdriver.Chrome(options=options)



#del_png.del_files()


### del images/*.png
del_png.del_images()

user_agent = UserAgent()

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


def KDJ_indicator_goodinfo(url,idx) :

    old = file_path +'/StockList.html'
    html_list = ['/Daily_StockList_L_20.html','/Daily_StockList_H_80.html','/Daily_StockList_L_20_M.html']

    web_times = 3
    web_check = 0


    while web_times > web_check: 

          download_times = 3
          check = 0
              	
          ### web check retry 
          try :
              web.get(url)
              time.sleep(random.randrange(5, 10, 1))
              html_d ='/html/body/table[2]/tbody/tr[2]/td[3]/main/section/table/tbody/tr[5]/td[2]/input[3]'
              ads_button = "ats-interstitial-button"
              #print('web_get is sucesses')

              
              #ads_button = "ats-interstitial-button"

              while download_times > check  :
                  ### download file
                  try :
                         if   check > 0 : 
                             web.get(url)
                             time.sleep(random.randrange(5, 10, 1))
                         else  :

                           ads_iframe_close =  WebDriverWait(web, 10).until(EC.element_to_be_clickable((By.ID,ads_button)))
                           ads_iframe_close.click()
                           time.sleep(random.randrange(1, 3, 1))
                           print('have ads')
                  except :
                     print('no ads')
                     continue
                     

                  finally :
                     file_download = WebDriverWait(web, 10).until(EC.element_to_be_clickable((By.XPATH,html_d)))
                     file_download.click()
                     time.sleep(random.randrange(5, 10, 1))
                     #print('old:',old)
              
              
                     if  os.path.exists(old) :
              
                       print('patch is exists')
                       check =3
              
              
                     else :
                        try :
                           web.navigate().refresh()
                           print('navigate')
              
                        except :
              
                          web.refresh()
                          print('refresh')
              
              
                        check +=1
              
                  print('check_download:',check)
                  time.sleep(random.randrange(1, 5, 1))
              ### web download sucesses 
              print('web download sucesses')
              web_check =3

          ### web check retry
          except :
              
             print('check_web:',web_check)
             ### close web driver restart
             time.sleep(random.randrange(1, 5, 1))
             web_check +=1
             continue        

    file_html = file_path + html_list[idx]

    os.rename(old, file_html)

    df = pd.read_html(file_html)[0] ## []list to pandas

    dfs = df.iloc[1:,[0,1,9,10,11,3]]
    dfs.columns = ['code','code_name','K_Day','D_Day','J_Day','price']

    return dfs



######## main code   ############


try :

  ### got mongo data from atlas
  dictt = {}
  _columns= {"code":1,"_id":0}
  com_lists = []
  com_list = []

  mydoc = atlas_read_mongo_db('stock','com_lists',dictt,_columns)

  for idx in mydoc :
    com_lists.append(idx.get('code'))

  
  mydoc = atlas_read_mongo_db('stock','com_list',dictt,_columns)

  for idx in mydoc :
    com_list.append(idx.get('code'))


except :
   ### got redis data from local
   com_lists = get_redis_data("com_lists","lrange",0,-1) ## get  redis data


   com_list = get_redis_data("com_list","lrange",0,-1) ## get  redis data


### get season data from mongo

last_modify=get_mongo_last_date(1)

url_L_20 = 'https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E6%99%BA%E6%85%A7%E9%81%B8%E8%82%A1&INDUSTRY_CAT=%E6%97%A5D%E5%80%BC%E4%BD%8E%E6%96%BC20%40%40%E6%97%A5KD%E8%90%BD%E9%BB%9E%40%40D%E5%80%BC%E4%BD%8E%E6%96%BC20' 
url_H_80 = 'https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E6%99%BA%E6%85%A7%E9%81%B8%E8%82%A1&INDUSTRY_CAT=%E6%97%A5D%E5%80%BC%E9%AB%98%E6%96%BC80%40%40%E6%97%A5KD%E8%90%BD%E9%BB%9E%40%40D%E5%80%BC%E9%AB%98%E6%96%BC80'  
url_L_20_M = 'https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=%E6%99%BA%E6%85%A7%E9%81%B8%E8%82%A1&INDUSTRY_CAT=%E6%9C%88D%E5%80%BC%E4%BD%8E%E6%96%BC20%40%40%E6%9C%88KD%E8%90%BD%E9%BB%9E%40%40D%E5%80%BC%E4%BD%8E%E6%96%BC20' 

mail_list = ['KD_L_20','KD_H_80','M_KD_L_20']
url_list = [url_L_20,url_H_80,url_L_20_M]


for url_idx in range(0,len(mail_list)) : 

    try : 

	
       kd_data = KDJ_indicator_goodinfo(url_list[url_idx],url_idx)
       
       match_row =kd_data[ kd_data['code'].isin(com_lists)]
       
       ### get 5 years data avg over 5%
       
       _yield_avg = 5
       
       dictt = {"yield_avg" :{"$gte" : _yield_avg}}
       
       _columns = {"_id":0 ,"price_avg" : 1, "yield_avg": 1 ,"code" :1}
       
       mydoc = read_mongo_db('stock','view_Rep_Stock_dividend_Com_5_yesrs_avg',dictt,_columns)
       
       mydoc_5y_avg = pd.DataFrame(list(mydoc))
       
       match_row = pd.merge(match_row,mydoc_5y_avg,on='code',how='left')
       
       ### adding seasonable price
       
       dictt = {}
       
       _columns = {"_id":0 ,"code":1,"cheap":1,"seasonable":1,"expensive":1}
       
       mydoc = read_mongo_db('stock','Stock_Eps_Yield_PE_Daily',dictt,_columns)
       
       mydoc_pe_daily = pd.DataFrame(list(mydoc))
       
       match_row = pd.merge(match_row,mydoc_pe_daily,on='code',how='left')
       
       
       
       #if not match_row.empty and time.strftime("%H:%M:%S", time.localtime()) > mail_time :
       if not match_row.empty :
       
       
              for  idx in [0,2,3,4] :
       
                   if idx in [2,3,4] :
       
                      match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x  if x.find('↘') >=0  else f'<font color="red">%s</font>' % x )
       
                   elif idx == 0 :
         
                      ### hightline code_name 
       
                     match_row.iloc[:,idx+1] = match_row.apply(lambda  x: f'<p style="background-color:Aqua;">%s</p>' %  x['code_name']  if  (x['code'] in com_list) else x['code_name'],axis=1 )
                     match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<a href="https://www.wantgoo.com/stock/%s/dividend-policy/ex-dividend" target="_blank">%s</a>' %( x , x )  if pd.notnull(x)  else  x)
       
            
              kd_data_row = match_row.iloc[:,[0,2,3,4]].copy()
              kd_data_name = list(kd_data_row.columns)
        
              ### code /K_day/D_day
              match_row['KDJ'] = match_row.apply(lambda x: "K:"+ x[kd_data_name[1]]+" "+"D:"+x[kd_data_name[2]] +" "+"J:"+x[kd_data_name[3]] if pd.notnull(x[kd_data_name[0]]) else "K:"+x[kd_data_name[1]]+" "+"D:"+x[kd_data_name[2]] +" "+"J:"+x[kd_data_name[3]], axis=1)
              match_row = match_row.iloc[:,[0,1,5,6,7,8,9,10,11]].copy()
              match_row.rename(columns={'price_avg': 'price_5_avg', 'yield_avg': 'yield_5_avg'}, inplace=True)
              ### price < price_5_avg to  hight line 
              match_row.iloc[:,[2]] =match_row.apply(lambda  x: f'<p style="background-color:#ffdead;">%s</p>' %  float(x['price'])  if  (float(x['price']) < x['price_5_avg']) else float(x['price']),axis=1 )
              match_row = match_row.sort_values(by='yield_5_avg',ascending=False,ignore_index = True)
              ### add coloumns into row
              match_row = pd_table.add_columns_into_row(match_row ,20)
        
              body = match_row.to_html(classes='table table-striped',escape=False)
              send_mail.send_email('Stock_KD_Report_Daily_{m_title}_{today}'.format(m_title=mail_list[url_idx],today=last_modify),body)
              print('mail_list:',mail_list[url_idx]) 
       
       time.sleep(random.randrange(5, 10, 1))

    except :   
        print('mail_list:',mail_list[url_idx] , 'is failed')
        continue 

###  close web
if web :
   web.quit()
