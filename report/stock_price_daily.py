# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
from pymongo import MongoClient
from fake_useragent import UserAgent
import del_png
from io import StringIO
import requests
import urllib3
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
import fetch_url


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

def Check_sys_date(i_date_sii , i_date_otc) :
    import sys

    o_date_sii = None
    o_date_otc = None
    try :
          i_date_sii = sys.argv[1]
          i_date_otc = sys.argv[2]

    except :
           print('please execute python with sys_argv')
           sys.exit(1)


    if i_date_sii == '0' :

          o_date_sii = datetime.date.today().strftime('%Y%m%d')

    else :

           o_date_sii = i_date_sii

    if i_date_otc  == '0' :

           o_date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')

    else :

           o_date_otc = i_date_otc

    return o_date_sii , o_date_otc


def fetch(url, retries=5):
    for i in range(retries):
        print("FETCH:", url, flush=True)
        try:
            r = requests.get(url, headers={'user-agent': user_agent.random},
                             verify=False, timeout=(10, 60))
            r.raise_for_status()
            time.sleep(3)          # 避免連續請求被限流
            return r
        except (ChunkedEncodingError, ConnectionError, Timeout) as e:
            wait = 10 * (2 ** i)   # 10, 20, 40, 80, 160 秒
            print(f"  失敗 [{i+1}/{retries}]：{e}，{wait} 秒後重試", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"重試 {retries} 次仍失敗：{url}")





def price_daily(date_sii,date_otc):

       ### get com_lists
       
       com_lists = []
       
       dictt = {}
       _columns= {"code":1,"_id":0}
       
       mydoc = atlas_read_mongo_db('stock','com_lists',dictt,_columns)
       for idx in mydoc :
           com_lists.append(idx.get('code'))
       
       
       ### get last price
       #last_modify=get_mongo_last_date(1)

       #date_sii =  datetime.date.today().strftime('%Y%m%d')
       #date_otc = str( int(datetime.datetime.strptime(date_sii, '%Y%m%d').date().strftime('%Y')) - 1911)  + datetime.datetime.strptime(date_sii, '%Y%m%d').date().strftime('/%m/%d')

       #date_sii,date_otc  = Check_sys_date(None,None)

       url_sii = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=' + date_sii + '&type=ALLBUT0999'
       #https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=20210910&type=ALL

       url_otc = 'https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=' + date_otc +'&o=htm&s=0,asc,0'
       #https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=110/09/10&o=htm&s=0,asc,0

       url_list = [url_sii,url_otc]

       u_index = 0
       dfs = pd.DataFrame()
       for url in url_list :
              
              #print("FETCH:", url, flush=True)

              #r = requests.get(url , headers={ 'user-agent': user_agent.random },verify=False)
              r = fetch_url.fetch(url)

              r.encoding = 'utf8'

              if u_index == 0 :
                df = pd.read_html(StringIO(r.text),thousands=",")[8] ## []list to pandas
              else :
                df = pd.read_html(StringIO(r.text),thousands=",")[0] ## []list to pandas

              df_len = len(df.columns)
              df.columns= llist(len(df.columns)) ##編列columns
              df = df[df[0].isin(com_lists)] ##比對

              #### 0,1,2,3/*sii(證券代號        證券名稱 收盤價 漲跌(+/-)       漲跌價差)*/
              if u_index == 0 :
                df[16] = df[9] + df[10].astype(str) ##合併 (漲跌(+/-)   漲跌價差)
                df = df.iloc[:,[0,1,8,16]] ## 取的欄位
                df.columns= llist(len(df.columns))


              #### 0,1,2,3/*otc代號     名稱    收盤    漲跌*/
              else :

                df = df.iloc[:,[0,1,2,3]] ##代號        名稱    收盤    漲跌
                df.columns= llist(len(df.columns)) ## define new column array

              ##df = df[df[0].isin(com_lists)] ##比對

              dfs = pd.concat([dfs,df],ignore_index=True) ##合併
              #dfs[3] = df[3].fillna(0)

              time.sleep(10)
              u_index += 1
              #print(df.info())
       dfs = dfs.fillna(0)

       dfs = dfs.iloc[:,[0,2]]
       dfs.iloc[:,1] = dfs.iloc[:,1].apply(lambda  x: None  if x =='--' else None if x=='---' else float(x))
       #dfs.iloc[:,1] = dfs.iloc[:,1].apply(lambda  x: None  if( x =='--' or x=='---' or x=='管理股票' ) else  float(x))
       dfs.columns = ['code', 'price']
       dfs = dfs.astype({dfs.columns[1]:'float'})
       records =  dfs.dropna().copy()
       records['last_modify'] = date_sii
       records =records.to_dict(orient='records')
       delete_many_mongo_db('stock','Stock_Price_Daily',{})
       insert_many_mongo_db('stock','Stock_Price_Daily',records)


       return dfs.dropna()




if __name__ == '__main__':

   ### call Rep_price


   date_sii,date_otc  = Check_sys_date(None,None)

   df_rep = price_daily(date_sii,date_otc)

   print(date_sii,date_otc)
   print(df_rep.head())
   
