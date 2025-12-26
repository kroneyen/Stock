#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
#from datetime import datetime
import datetime
from pymongo import MongoClient
import redis
import send_mail
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager
from fake_useragent import UserAgent
import del_png
import pandas_table as pd_table
from io import StringIO
import urllib3
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)




# 改style要在改font之前
plt.style.use("seaborn-v0_8")
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')



#date_sii = datetime.date.today().strftime('%Y%m%d')
#date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')


#date_sii = datetime.date.today().strftime('%Y%m%d')
#date_sii= '20250624'
#date_otc= '114/06/24'

mail_time = '09:00:00'
#mail_time = '18:00:00'

### redis connection & data 

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


def get_redis_data(_key,_type,_field_1,_field_2):

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


def insert_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert(_values)



def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)

  

def drop_mongo_db(_db,_collection):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.drop()


def read_aggregate_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)


def delete_many_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.delete_many(dicct)



def get_mongo_last_date(cal_day):
 ### mongo query for last ? days
 dictt_set = [ {"$group": { "_id" : { "$toInt" : "$last_modify" } }},{"$sort" : {"_id" :-1}} , { "$limit" : cal_day},{"$sort" : {"_id" :1}} , { "$limit" :1}]

 ### mongo dict data

 set_doc =  read_aggregate_mongo_db('stock','Rep_Stock_Exchange',dictt_set)

 ### for lists  get cal date 
 for idx in set_doc:

  idx_date = idx.get("_id")

 #set_date = str(idx_date)
 return str(idx_date)



### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)



def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def read_aggregate_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)




#### share function


def llist(df_len):

    llist =[]
    for i in range(df_len) :
      llist.append(i)
    return llist    

def Rep_Stock_Exchange(date_sii) : 

  ##### 20230317 web change version  abandoned  
  url_SLBNLB = 'https://www.twse.com.tw/exchangeReport/TWT72U?response=html&date='+ date_sii +'&selectType=SLBNLB'  ##借券統計  20220615
  url = 'https://www.twse.com.tw/exchangeReport/TWT93U?response=html&date='+ date_sii ##借券賣出 20220615 
  
  dfs = pd.DataFrame()
  ## conn pyload
  _db='stock'
  _collection ='com_list'
  _filed='code'
  dictt={}
  _columns= {_filed:1,"_id":0}

  mydoc = atlas_read_mongo_db(_db,_collection,dictt,_columns)
  ## mongo data to lists
  com_lists=[]
  for idx in mydoc : 
   com_lists.append(idx.get('code'))
  
  ### get report data 
  url_list = [url_SLBNLB,url]
  u_index = 0
  for url in url_list :
     
     r = requests.get(url)
     r.encoding = 'utf8'
     df = pd.read_html(r.text,thousands=",")[0]
   
     df_len = len(df.columns)
     df.columns= llist(len(df.columns)) ##編列columns

     ### 證券代號	證券名稱	前日借券餘額(1)股	 本日異動股借券(2)	本日異動股還券(3)	
     #本日借券餘額股(4)=(1)+(2)-(3)	本日收盤價(5)單位：元	借券餘額市值單位：元(6)=(4)*(5)	市場別
     if u_index == 0 : ##借券 

       df = df.iloc[:,[0,1,3,4,6]] ## 取的欄位 [證券代號  證券名稱  本日異動股借券(2)  本日異動股還券(3) 收盤價]
       df.columns= llist(len(df.columns))
       df_SLBNLB = df[df[0].isin(com_lists)] ##比對 
      
     #### 股票代號 股票名稱	  前日餘額	賣出	買進	現券	今日餘額	限額	      前日餘額	 
        #當日賣出	   當日還券(借券賣出還券)	  當日調整	當日餘額	 次一營業日可限額	備註
     else :
      
       df = df.iloc[:,[0,1,9,10,8,12]] ## 取的欄位  [代號 股票名稱 當日賣出 當日還券 前日餘額 當日餘額]
       df.columns= llist(len(df.columns)) ## define new column array
       df = df[df[0].isin(com_lists)] ##比對
      
     time.sleep(1)
     u_index += 1
  ##合併 
  dfs = pd.merge(df_SLBNLB,df,how='left',on=[0,1]).fillna(0)
  ### x_1 x_2 y_1 y_2 column_name be alias
  dfs.columns= llist(len(dfs.columns))  ## define new column array
  ### add 借券增減 
  dfs[9] = (dfs[2] - dfs[3]).copy()
  ### add 借券賣出餘額差額
  dfs[10] =(dfs[8] - dfs[7]).copy()
  ### rebuild dataframe 
  dfs = dfs.iloc[:,[0,1,2,3,9,5,6,7,8,10,4]]
  dfs.columns= llist(len(dfs.columns))  ## define new column array
  
  dfs = dfs.astype({2:'int64',3:'int64',4:'int64',5:'int64',6:'int64',7:'int64',8:'int64',9:'int64'}) ##change type
    
  for i in range(2,10,1) :
    dfs[i] =round((dfs[i]/1000),0).astype(int)

  """
  dfs[2] =round((dfs[2]/1000),0).astype(int)
  dfs[3] =round((dfs[3]/1000),0).astype(int)
  dfs[4] =round((dfs[4]/1000),0).astype(int)
  dfs[5] =round((dfs[5]/1000),0).astype(int)
  dfs[6] =round((dfs[6]/1000),0).astype(int)
  dfs[7] =round((dfs[7]/1000),0).astype(int)
  """
  #dfs = dfs.style.apply(color_negative_red)
  dfs.columns = ['公司代號', '公司簡稱', '借券', '還券','借券差', '借券賣出', '借券賣出還券','前日借券賣出餘額','借券賣出餘額','借券賣出餘額差','收盤價'] 
  
  return dfs.sort_values(by=['公司代號'])






def Alert_Stock_Exchange(date_sii):
   ### 20230317 web change version , abandoned function

   ### cal  continue  +/-5,10 days 
   match_row = pd.DataFrame()
   com_df = pd.DataFrame()
   #for cal_day in [3,5] :
   for cal_day in [3,5,10] :
   
      set_date=get_mongo_last_date(cal_day)
   
   
      dictt_gt = [ {"$match": { "last_modify" : { "$gte" : set_date}  , "$expr" : { "$gt": [ { "$toInt": "$selling_short_balance" }, {"$toInt": "$be_selling_short_balance"} ] }  }}  ,
              {"$group":{"_id": {"code" : "$code" , "name" : "$name"} ,"sum_coun" : { "$sum" :1} ,"diff_selling_short_balance" : {"$sum":{"$toInt":"$diff_selling_short_balance" } } }}  , 
              {"$match": {"sum_coun" : { "$gte": cal_day } } } ,  {"$sort" : {"diff_selling_short_balance" : -1}}]
   
      #dictt_gt = [ {"$match": { "last_modify" : { "$gte" : set_date}  , "$expr" : { "$gt": [ { "$toInt": "$total" }, 0 ] }   }}  ,{"$group":{"_id": {"code" : "$code" , "name" : "$name"} ,"total_values" : { "$sum" : { "$toInt": "$total"} } ,"sum_coun" : { "$sum" :1}  }}  , {"$match": {"sum_coun" : { "$gte": cal_day } } } ,  {"$sort" : {"total_values" : -1}}  ]
   
   
   
      idx_lt_list=[]
      idx_code_list=[]
      idx_name_list=[]
      total_lt_list=[]
      total_diff_selling_short_balance_list=[]
   
   
      ### get mongo data 
      #mydoc_lt = read_aggregate_mongo_db('stock','Rep_3_Investors',dictt_lt)
      mydoc_gt = read_aggregate_mongo_db('stock','Rep_Stock_Exchange',dictt_gt)
   
   
      ### lists coms code 

      for idx in mydoc_gt :
   
         idx_code_list.append(idx.get('_id').get('code'))
         idx_name_list.append(idx.get('_id').get('name'))
         
         #total_diff_selling_short_balance_list.append(idx.get('diff_selling_short_balance'))
         total_diff_selling_short_balance_list.append(idx.get('diff_selling_short_balance'))
         
      #print(total_diff_selling_short_balance_list)   
      key_day  =  str(cal_day) +'day'
      key_day_total = str(cal_day) +'day_balance'
      #key_day_p =  str(cal_day) +'day%'
   
      pyload_total = { '公司代號' : idx_code_list ,
                '公司簡稱' : idx_name_list,   
                key_day_total : total_diff_selling_short_balance_list }  
    
    
      com_toal = pd.DataFrame(pyload_total)
   
      #print("com_toal:",com_toal)   
      if com_df.empty  :
   
        com_df = com_toal
   
      else :
   
        com_df = pd.merge(com_df,com_toal,on = ['公司代號','公司簡稱'],how='left')
   
   
   #print(com_df.info())
   #com_df['continue_days'] = com_df['5day_balance'].apply(lambda  x: 3  if pd.isnull(x)  else 5 )
   com_df['continue_days'] = com_df.apply(lambda  x: 10  if pd.notnull(x['10day_balance'])  else 5 if pd.notnull(x['5day_balance'])  else 3  ,axis=1)
   ###instead  
   #com_df['continue_selling_short_balance_total'] = com_df.apply(lambda  x: x['3day_balance']  if pd.isnull(x['5day_balance'])  else x['5day_balance'] ,axis=1).astype('int64')
   com_df['continue_selling_short_balance_total'] = com_df.apply(lambda  x: x['10day_balance']  if pd.notnull(x['10day_balance'])  else x['5day_balance'] if pd.notnull(x['5day_balance']) else x['3day_balance'] ,axis=1)
   
   #print(com_df)
   last_code=[]
   last_selling_short_balance =[]
   last_modify_mydoc = read_mongo_db('stock','Rep_Stock_Exchange',{"last_modify":date_sii},{"code":1,"selling_short_balance":1,"_id":0})
   for idx in last_modify_mydoc : 
     last_code.append(idx.get('code'))
     last_selling_short_balance.append(idx.get('selling_short_balance'))
   
   pyload_total = { '公司代號' : last_code ,
                'total_balance' : last_selling_short_balance }
     
   last_selling_short_balance = pd.DataFrame(pyload_total)
   
   com_df = pd.merge(com_df,last_selling_short_balance,on = ['公司代號'],how='left')
   
   com_df = com_df.astype({'total_balance':'int64'})
   
   
 
   com_df['continue_selling_short_balance_total%'] =  round(round(com_df['continue_selling_short_balance_total']/com_df['total_balance'],4)*100 ,2)
   #com_df = com_df.astype({'continue_days':'int64','continue_selling_short_balance_total':'int64'})

   #com_df['continue_days'] = round(com_df['continue_days'],0).astype(int)
   #com_df['continue_selling_short_balance_total'] = round(com_df['continue_selling_short_balance_total'],0).astype(int)

   com_df = com_df.iloc[:,[0,1,5,6,7,8]]
   #for i in ['continue_days','continue_selling_short_balance_total'] :
   # com_df[i] =round((com_df[i]/1),0).astype(int)

   #print(com_df)
   #com_df = com_df[com_df['借券賣出餘額差'] != 0]
   return com_df.reset_index(drop=True)



def Rep_Stock_Exchange_v1(date_sii,date_otc,com_lists) :
  
  user_agent = UserAgent()
  
  url = 'https://www.twse.com.tw//rwd/zh/lending/TWT72U?response=html&date=' + date_sii ##證商/證金營業統計 SLBNLB 改版
  ### sii
  sii = 'https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?date='+ date_sii + '&response=html'##借券賣出 20230321 
  ### oct 
  otc = 'https://www.tpex.org.tw/web/stock/margin_trading/margin_sbl/margin_sbl_result.php?l=zh-tw&d='+ date_otc +'&s=0,asc,0&o=htm'

  dfs = pd.DataFrame()

  ### get report data 
  url_list = [url ,sii, otc ]
  u_index = 0
  dfs = pd.DataFrame()

  
  for url in url_list :

     r = requests.get(url , headers={ 'user-agent': user_agent.random },verify=False)
     r.encoding = 'utf8'
     df = pd.read_html(StringIO(r.text),thousands=",")[0]

     df_len = len(df.columns)
     df.columns= llist(len(df.columns)) ##編列columns

     if u_index == 0 : ##證券商 

       df = df.iloc[:,[0,1,3,4,5,6]] ## 取的欄位 [證券代號  證券名稱  本日異動股借券(2)  本日異動股還券(3) 本日借券餘額 收盤價]
       df.columns= llist(len(df.columns))
       df_SLBNLB = df[df[0].isin(com_lists)] ##比對   

     else :

       #df = df.iloc[:,[0,1,9,10,8,12]]  ## 台灣交易所
       df = df.iloc[:,[0,9,10,8,12]]  ## 台灣交易所

       df = df[df[0].isin(com_lists)]

       dfs = pd.concat([dfs,df],ignore_index=True) ##合併
      

     time.sleep(1)
     u_index += 1
  
  ### 合併 SLBNLB sii otc    
  dfs = pd.merge(df_SLBNLB,dfs,how='left',on=[0]).fillna(0)

  dfs = dfs.astype({2:'int64',3:'int64',4:'int64',9:'int64',10:'int64',8:'int64',12:'int64'}) ##change type

  for i in [2,3,4,9,10,8,12] :
    
    dfs[i] =round((dfs[i]/1000),0).astype(int)
  return dfs


def cal_con_days(_db,_collection):                                                                                                                                        
                                                                                                                                                            
    set_date=[]                                                                                                                                                 
    cal_day =1                                                                                                                                                  
                                                                                                                                                                
    limit_day = 60

    dictt_30day = [  {"$group": { "_id" : { "$toInt": "$last_modify" }, "sum_coun" : { "$sum" :1} }} , {"$sort" : {"_id" :-1}} , {"$limit" : limit_day},{"$match" : { "_id" : {"$ne": None }}}  	]
                                          
    mydoc_30day = read_aggregate_mongo_db(_db,_collection,dictt_30day)  

                                                                              
                                                                                                                                                                
    ### get 30 day date in mongo data                                                                                                                                                            
    for idx in mydoc_30day :                                                                                                                                    
                                                                                                                                                                
      set_date.append(str(idx.get('_id')))   

                                                                                                                                                                
    ### last days to check                                                                                                                                      
    dictt_gt = [ {"$match": { "last_modify" : { "$gte" : str(set_date[0]) }  , "$expr" : { "$gt": [ { "$toInt": "$diff_selling_short_balance" }, 0 ] }   }}  , 
              {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$diff_selling_short_balance"} } , "sum_coun" : { "$sum" :1}  }}  ,                                
              {"$match": {"sum_coun" : { "$gte": 1 } } } ,  {"$sort" : {"total_values" : -1}}  ] 


    dictt_lt = [ {"$match": { "last_modify" : { "$gte" : str(set_date[0]) }  , "$expr" : { "$lt": [ { "$toInt": "$diff_selling_short_balance" }, 0 ] }   }}  ,                       
              {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$diff_selling_short_balance"} } , "sum_coun" : { "$sum" : 1}  }}  ,                            
              {"$match": {"sum_coun" : { "$gte": 1 } } } ,  {"$sort" : {"total_values" : -1}}  ]                                                                
                                                                                                                                                                
                                                                                                                                                                
    mydoc_gt = read_aggregate_mongo_db(_db,_collection,dictt_gt)                                                                                      
    mydoc_lt = read_aggregate_mongo_db(_db,_collection,dictt_lt)                                                                                      
                                                                                                                                                                
    df_cal_gt = pd.DataFrame(list(mydoc_gt))                                                                                                                    
    df_cal_lt = pd.DataFrame(list(mydoc_lt))                                                                                                                    
                                                                                                                                                                
    cal_data_gt = pd.DataFrame()                                                                                                                                
    cal_data_lt = pd.DataFrame()                                                                                                                                
                                                                                                                                                                
                                                                                                                                                                
    ### last day data merge cal_data                                                                                                                            
    cal_data_gt = pd.concat([cal_data_gt,df_cal_gt ], axis=0)                                                                                                   
    cal_data_lt = pd.concat([cal_data_lt,df_cal_lt ], axis=0)                                                                                                   
                                                                                                                                                                
                                                                                                                                                                 
                                                                                                                                                                
    while ((not df_cal_gt.empty) or (not df_cal_lt.empty))  and cal_day < len(set_date) :                                                                                                      
                                                                                                                                                                
      dictt_gt = [ {"$match": { "last_modify" : { "$gte" : set_date[cal_day]}  , "$expr" : { "$gt": [ { "$toInt": "$diff_selling_short_balance" }, 0 ] }   }}  ,                     
              {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$diff_selling_short_balance"} } , "sum_coun" : { "$sum" :1}  }}  ,                                
              {"$match": {"sum_coun" : { "$gte": cal_day+1 } } } ,  {"$sort" : {"total_values" : -1}}  ]                                                        
                                                                                                                                                                
      dictt_lt = [ {"$match": { "last_modify" : { "$gte" : set_date[cal_day] }  , "$expr" : { "$lt": [ { "$toInt": "$diff_selling_short_balance" }, 0 ] }   }}  ,                    
                 {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$diff_selling_short_balance"} } , "sum_coun" : { "$sum" : 1}  }}  ,                            
              {"$match": {"sum_coun" : { "$gte": cal_day+1 } } } ,  {"$sort" : {"total_values" : -1}}  ]                                                        
                                                                                                                                                                
      mydoc_gt = read_aggregate_mongo_db(_db,_collection,dictt_gt)                                                                                    
      mydoc_lt = read_aggregate_mongo_db(_db,_collection,dictt_lt)                                                                                    
                                                                                                                                                                
                                                                                                                                                                
      df_cal_gt = pd.DataFrame(list(mydoc_gt))                                                                                                                  

      if  not df_cal_gt.empty :                                                                                                                                 
                                                                                                                                                                
            cal_data_gt = pd.merge(cal_data_gt,df_cal_gt,on = ['_id'],how='left')                                                                               
                                                                                                                                                                
            cal_data_gt['total_values'] = cal_data_gt.apply(lambda x: x['total_values_y'] if (pd.notnull(x['total_values_y'])) else x['total_values_x'],axis=1) 
            cal_data_gt['sum_coun'] = cal_data_gt.apply(lambda x: x['sum_coun_y'] if (pd.notnull(x['sum_coun_y'])) else x['sum_coun_x'],axis=1)                 
            cal_data_gt = cal_data_gt.drop(['total_values_x','sum_coun_x','total_values_y','sum_coun_y'],axis='columns')                                        


      df_cal_lt = pd.DataFrame(list(mydoc_lt))                                                                                                                  

      if  not df_cal_lt.empty :                                                                                                                                 
                                                                                                                                                                
           cal_data_lt = pd.merge(cal_data_lt,df_cal_lt,on = ['_id'],how='left')                                                                                
                                                                                                                                                                
           cal_data_lt['total_values'] = cal_data_lt.apply(lambda x: x['total_values_y'] if (pd.notnull(x['total_values_y'])) else x['total_values_x'],axis=1)  
           cal_data_lt['sum_coun'] = cal_data_lt.apply(lambda x: x['sum_coun_y'] if (pd.notnull(x['sum_coun_y'])) else x['sum_coun_x'],axis=1)                  
           cal_data_lt = cal_data_lt.drop(['total_values_x','sum_coun_x','total_values_y','sum_coun_y'],axis='columns')                                         
                                                                                                                                                                
                                                                                                                             
      cal_day = cal_day + 1                                                                                                                                     

    ### merge data                                                                                                                                         
    cal_dayy = pd.concat([cal_data_gt,cal_data_lt])                                                                                                             
                                                                                                                                                                
    cal_dayy['sum_coun'] = cal_dayy.apply(lambda  x: -x['sum_coun'] if x['total_values'] < 0  else x['sum_coun'],axis=1)                                        
                         
    cal_dayy.rename(columns={'_id':'code','sum_coun':'con_days'}, inplace=True)                                                                                                                                                        
    return cal_dayy




def plot_Rep_Stock_Exchange(match_row) :

   dfs = pd.DataFrame()
   
   ### match_row  code con_days
   cal_day = match_row.iloc[:,[1]].astype('int64').abs().max()
   #cal_day = match_row.iloc[0,1]   ###max con_days
   
   
   last_modify = get_mongo_last_date(int(cal_day.iloc[0]))

   ### dataframe to list 
   com_lists = match_row.iloc[:,[0]].reset_index(drop=True).squeeze()


   for idx in com_lists :

      altas_mydoc = read_mongo_db('stock','Rep_Stock_Exchange',{'code': idx ,'last_modify': {'$gte' : last_modify}},{'_id':0})

      df = pd.DataFrame(list(altas_mydoc))

      df = df.iloc[:,[0,1,9,12]]

      rest_df = pd.DataFrame(data = df['selling_short_balance'].values,columns=[df['code'][0]+'_'+ df['name'][0]] , index=df['last_modify'])

      dfs = pd.concat([dfs,rest_df],axis=1)


   ### data to int for plot  
   records = dfs.fillna(0).astype('int64').sort_index()
   

   ### 5 rows for each paint  
   for idx in range(0,len(records.columns),5):

     idx_records = records.iloc[ : , idx:idx+5 ]

     #idx_records.plot(y = idx_records.columns)
     ax = idx_records.plot(y = idx_records.columns)
     #idx_records.plot.line(y = idx_records.columns, figsize=(10,6))
     #idx_records.plot.bar(y = idx_records.columns, figsize=(10,6))
     ### del png with crontab jobs

     ## 顯示公司在尾端
     for line, name in zip(ax.lines, idx_records.columns):
          y = line.get_ydata()[-1]
          ax.annotate(name, xy=(1,y), xytext=(3,0), color=line.get_color(),
                xycoords = ax.get_yaxis_transform(), textcoords="offset points",
                size=10, va="center")

     plt.savefig('./images/image_'+ str(idx) +'_'+ str(idx+4) +'.png' )
     plt.clf()



def Bias_Rate(cal): 

    set_date = get_mongo_last_date(cal)

  
    dictt =  [ {"$match": { "last_modify" : { "$gte" : set_date } }}  ,
            {"$group":{"_id": "$code"  ,"avg_price" : { "$avg" : { "$toDecimal": "$price"} } ,"last_price" : {"$last":"$price"} }}
          ]
      
    mydoc = read_aggregate_mongo_db('stock','Rep_Stock_Exchange',dictt)

    df = pd.DataFrame(list(mydoc))
    ### decimal128 to float
    df = df.astype({"avg_price": "string"})
    df = df.astype({"avg_price": "float" ,"last_price": "float"})
    ### adding Bias_rate
    col_name = 'Bias_'+str(cal)
    df[col_name] = df.apply(lambda x : round(round((x['last_price'] - x['avg_price']) / x['avg_price'] ,4 ) * 100 , 2) if x['avg_price'] > 0 else x['avg_price'] ,axis=1 )
    #df.columns=["code","avg_price","last_price",'Bias'] 
    df = df.iloc[:,[0,3]]
    df.columns=["code",col_name]
    return df ,col_name


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






### del images/*.png
del_png.del_images()


#### call function

date_sii = None

date_otc = None

date_sii , date_otc = Check_sys_date(None,None)



df_Rep_Stock_Exchange = pd.DataFrame()
df_cal_con_days = pd.DataFrame()


mydoc = atlas_read_mongo_db('stock','com_list',{},{'code':1,'_id':0})
## mongo data to lists
com_lists=[]
for idx in mydoc :
 com_lists.append(idx.get('code'))

try :

    df_Rep_Stock_Exchange = Rep_Stock_Exchange_v1(date_sii,date_otc,com_lists)

except : 

    last_modify=get_mongo_last_date(1)
    date_sii = last_modify
    date_otc = str( int(datetime.datetime.strptime(last_modify, '%Y%m%d').date().strftime('%Y')) - 1911)  + datetime.datetime.strptime(last_modify, '%Y%m%d').date().strftime('/%m/%d')

    df_Rep_Stock_Exchange = Rep_Stock_Exchange_v1(date_sii,date_otc,com_lists)
    



### define columns name
df_Rep_Stock_Exchange.columns = ['code','name','lending','back','lending_balance','price','selling_short','selling_short_back','be_selling_short_balance','selling_short_balance']
### add 借券差/借券賣出餘額差額

df_Rep_Stock_Exchange['diff_lending'] =(df_Rep_Stock_Exchange['lending'] - df_Rep_Stock_Exchange['back']).copy()
df_Rep_Stock_Exchange['diff_selling_short_balance'] =(df_Rep_Stock_Exchange['selling_short_balance'] - df_Rep_Stock_Exchange['be_selling_short_balance']).copy()


### insert into local  mongodb 
records = pd.DataFrame()
records = df_Rep_Stock_Exchange.copy()
records["last_modify"]= date_sii
###get columns 

records = records.iloc[:,[0,1,2,3,10,4,6,7,8,9,11,5,12]]
#records.columns= ['code','name','selling_short','selling_short_back','be_selling_short_balance','selling_short_balance','diff_selling_short_balance','price','last_modify']


for idx in records.columns : 
    records[idx] = records[idx].astype('str')

### check data exist in mongo  
db_records = read_mongo_db('stock','Rep_Stock_Exchange',{"last_modify":date_sii},{"_id":0})
db_records_df =  pd.DataFrame(list(db_records))
### compare dataframe db data  and web data  avoid duplicate data  
chk = records.equals(db_records_df)
if chk == False :
   
   dicct = {"last_modify":date_sii}
   delete_many_mongo_db('stock','Rep_Stock_Exchange',dicct)
   
   records =records.to_dict(orient='records')
   insert_many_mongo_db('stock','Rep_Stock_Exchange',records)
time.sleep(1)




#### merge cal_con_day data

df_cal_con_days = cal_con_days('stock','Rep_Stock_Exchange')
match_row = df_Rep_Stock_Exchange.copy()
### filter diff_selling_short_balance 
match_row = match_row[match_row['diff_selling_short_balance'] != 0]
match_row = pd.merge(match_row,df_cal_con_days, on =['code']) ##dataframe join by columns

### add continue_selling_short_balance_total% = 連續total_values/借券賣出餘額  %
match_row['continue_selling_short_balance_total']= round(round(match_row['total_values']/match_row['selling_short_balance'],4)*100 ,2) 

###20250220 add Bias_Rate using 10 day SMA 

df_bias_10 , col_bias_10 = Bias_Rate(10)
df_bias_22 , col_bias_22 = Bias_Rate(22)
match_row = pd.merge(match_row,df_bias_10,how='left',on =['code'])
match_row = pd.merge(match_row,df_bias_22,how='left',on =['code'])
#match_row['Bias'] = match_row.apply(lambda x : round(round((x['price'] - float(str(x['avg_price']))) / float(str(x['avg_price'])) ,4 ) * 100 , 2) if float(str(x['avg_price'])) > 0 else x['avg_price'] ,axis=1 )


#print('match_row_661:',match_row.info())
##get column
#match_row = match_row.iloc[:,[0,1,2,3,10,4,6,7,11,9,13,12,14,5,15]] 
match_row = match_row.iloc[:,[0,1,2,3,10,4,6,7,11,9,13,12,14,5,15,16]] 
match_row.columns =['代號','名稱','借券','還券','借券差','借券差餘額','借券賣出','借券賣出還券','借券賣出餘額差','借券賣出餘額','con_days','total_values','連續天總額%(+)','收盤價',col_bias_10,col_bias_22]
match_row = match_row.sort_values(by=['連續天總額%(+)'],ascending = False ,ignore_index = True)


#print('match_row_667:',match_row.info())
### plot Rep_Stock_Exchange for line 
## code , con_days%
plot_Rep_Stock_Exchange(match_row.iloc[:,[0,10]])


match_row['con_days'] = match_row['con_days'].astype('int64')

### adding nenagive vlues to red
for  idx in [0,4,8,10,11,12,14,15] :

    match_row[match_row.columns[idx]] = match_row[match_row.columns[idx]].astype(object)


    if idx == 0 :

       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<a href="https://goodinfo.tw/tw/ShowMarginChart.asp?STOCK_ID=%s" target="_blank">%s</a>' %( x , x )  if int(x) >0  else  x)
    
    elif idx == 10 :    ##con_days

       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<p style="background-color:Tomato;">+%s</p>' % (x ) if x >= 10 else f'<font color="red">+%s</font>' % (x ) if x > 0 else  f'<p style="background-color:Lime;">%s</p>'  % (x) if x < -10 else  f'<font color="green">%s</font>' % (x ) )


    elif idx == 12 :    
       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s%s</font>' % (x ,' %') if x > 0 else  f'<font color="green">%s%s</font>' % (x,' %' ) )

    elif idx >= 14 : ## 14 Bias_10 / 15 Bias_22
       #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s%s</font>' % (str(x) ,' %') if float(str(x)) > 10 else  f'<font color="green">%s%s</font>' % (str(x),' %')  if float(str(x)) < -10  else  str(x)+' %'  )
       #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s%s</font>' % (str(x) ,' %') if float(str(x)) > 10 else  f'<font color="blue">%s%s</font>' % (str(x),' %')  if float(str(x)) >=5   else  f'<font color="green">%s%s</font>' % (str(x),' %') if  float(str(x)) < -10  else   f'<font color="brown">%s%s</font>' % (str(x),' %')  if  float(str(x)) <-5   else str(x)+' %'  )
       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<p style="background-color:Tomato;">+%s%s</p>' % (str(x) ,' %') if float(str(x)) > 10 else  f'<font color="red">+%s%s</font>' % (str(x),' %')  if float(str(x)) >=5   else  str(x)+' %' if float(str(x)) > 0  else   f'<p style="background-color:Lime;">%s%s</p>'  % (str(x),' %') if  float(str(x)) < -10  else   f'<p style="background-color:Aqua;">%s%s</p>' % (str(x),' %')  if  float(str(x)) <-5   else  str(x)+' %'  )
  
    else : 
       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s</font>' % (x ) if x > 0 else  f'<font color="green">%s</font>' % (x ) )



###send mail
if (time.strftime("%H:%M:%S", time.localtime()) > mail_time) and (chk == False)  and (not match_row.empty):

       match_row = pd_table.add_columns_into_row(match_row ,20)
       body = match_row.to_html(escape=False)
       send_mail.send_email('Rep_Stock_Exchange_%s' % date_sii ,body)

else :
    print('Rep_Stock_Exchange_%s' % date_sii )
    #print(match_row.to_html(escape=False))
