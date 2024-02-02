#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
#from datetime import date,datetime
import redis
import time
#import line_notify
import requests
import send_mail
from IPython.display import display, clear_output
from io import StringIO
from pymongo import MongoClient
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager
from fake_useragent import UserAgent
import del_png



# 改style要在改font之前
plt.style.use('seaborn')
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')





user_agent = UserAgent()

mail_time = "18:00:00"
#mail_time = "09:00:00"

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



#date_sii = datetime.date.today().strftime('%Y%m%d')
#date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')

#date_sii='20231116'
#date_otc='112/11/16'

match_row = pd.DataFrame()


def tableColor(val):
    if val > 0:
        color = 'red'
    elif val < 0:
        color = 'green'
    else:
        color = 'block'
    return 'color: %s' % color

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)


def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list


def insert_redis_data(_key,_values):

        r.rpush(_key,_values)



def delete_redis_data(key):

    if r.exists(key) : ### del yesterday key
       r.delete(key)




### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )


### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)



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



def read_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    #return collection.find({_code:_qq},{"code":1,"name":0,"_id":0,"last_modify":0})
    return collection.find(dicct,{"code": 1,"name": 1,"_id": 0})

def read_mongo_db(_db,_collection,dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)

def delete_many_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.delete_many(dicct)




def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def llist(df_len):       
    llist =[] 
    for i in range(df_len) : 
      llist.append(i)
    return llist


def read_aggregate_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)


def Rep_3_Investors(date_sii,date_otc,com_lists) : 



  #url_sii ='https://www.twse.com.tw/fund/T86?response=html&date='+ date_sii +'&selectType=ALL'
  url_sii ='https://www.twse.com.tw/rwd/zh/fund/T86?response=html&date='+ date_sii +'&selectType=ALL'
  url_otc ='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=htm&se=EW&t=D&d=' + date_otc + '&s=0,asc'
  url_list = [url_sii,url_otc]
  dfs = pd.DataFrame()
  u_index = 0
  
  #com_lists = get_redis_data("com_list") ## get  redis data
  
  for url in url_list :
     
     r = requests.get(url , headers={ 'user-agent': user_agent.random }) 
     r.encoding = 'utf8'
     df = pd.read_html(r.text,thousands=",")[0]
     df_len = len(df.columns)
     df.columns= llist(len(df.columns)) ##編列columns
     
     ### 0,1,4,10,11,18 /*sii*/
     if u_index == 0 :

       df = df.iloc[:,[0,1,4,10,11,18]] ## 取的欄位
       df.columns= llist(len(df.columns))
       df = df[df[0].isin(com_lists)] ##比對 
       
     
     #### 0,1,4,13,22,23/*otc*/
     else :
      
       df = df.iloc[:,[0,1,4,13,22,23]]
       df.columns= llist(len(df.columns)) ## define new column array
       df = df[df[0].isin(com_lists)] ##比對
     
     dfs = pd.concat([dfs,df],ignore_index=True) ##合併
          
     time.sleep(1)
     u_index += 1

  

  dfs = dfs.astype({2:'int64',3:'int64',4:'int64',5:'int64'}) ##change type

  dfs[2] =round((dfs[2]/1000),0).astype(int)
  dfs[3] =round((dfs[3]/1000),0).astype(int)
  dfs[4] =round((dfs[4]/1000),0).astype(int)
  dfs[5] =round((dfs[5]/1000),0).astype(int)
  dfs.columns = ['公司代號', '公司簡稱', '法人', '投信', '自營商','合計'] 


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
              r = requests.get(url ,  headers={ 'user-agent': user_agent.random })
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

#com_lists = get_redis_data("com_list") ## get  redis data

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




### del images/*.png
del_png.del_images()



### call function

date_sii = None

date_otc = None

date_sii,date_otc  = Check_sys_date(None,None)

df_Rep_3_Investors = Rep_3_Investors(date_sii,date_otc,com_lists)
df_Rep_price = Rep_price(date_sii,date_otc,com_lists)


df_s = pd.merge(df_Rep_3_Investors,df_Rep_price, on =['公司代號']) ##dataframe join by columns
df_s['漲跌(+/-)'] = df_s['漲跌(+/-)'].str.replace('X','').str.replace('除息','0').str.replace(' ---','0').str.replace('--- ','0').astype({'漲跌(+/-)':'float'}).fillna(0).round(2)
df_s = df_s.iloc[:,[0,1,2,3,4,5,7,8]]
match_row = df_s.sort_values(by=['漲跌(+/-)'],ascending=False,ignore_index= True).copy()

##insert local mongo
local_mongo=[]
local_redis=[]

local_mongo_doc = read_mongo_db('stock','com_list',{},{"code": 1,"name": 1,"_id": 0})

for idx in local_mongo_doc :
    local_mongo.append(idx.get('code'))

local_redis = get_redis_data('com_list','lrange',0,-1) 


#print('local_mongo:',local_mongo)
#print('local_redis:',local_redis)
     
if com_lists != local_mongo : 
   drop_mongo_db('stock','com_list')
   for com in com_lists :
     _values = { 'code' : com ,'last_modify': datetime.datetime.now() }
     insert_mongo_db('stock','com_list',_values)    




##insert local redis
if com_lists != local_redis : 
   delete_redis_data('com_list')
   for com in com_lists : 
     insert_redis_data('com_list',com)

 

### insert into local  mongodb 
records = pd.DataFrame()
records = match_row.copy()
#print(records.info())
records = records.iloc[:,[0,1,2,3,4,5,6]] ### adding price
records["last_modify"]= date_sii
records = records.iloc[:,[0,1,2,3,4,5,7,6]] ### select column 
records.columns= ['code','name','foreign','trust','dealer','total','last_modify','price']

for idx in records.columns :
    records[idx] = records[idx].astype('str')


### check data exist in mongo  
db_records = read_mongo_db('stock','Rep_3_Investors',{"last_modify":date_sii},{"_id":0})
db_records_df =  pd.DataFrame(list(db_records))
### compare dataframe db data  and web data  avoid duplicate data  
chk = records.equals(db_records_df)
if chk == False :

   dicct = {"last_modify":date_sii}
   delete_many_mongo_db('stock','Rep_3_Investors',dicct)

   records =records.to_dict(orient='records')
   insert_many_mongo_db('stock','Rep_3_Investors',records)

time.sleep(1)

#records =records.to_dict(orient='records')
#insert_many_mongo_db('stock','Rep_3_Investors',records)

### sync local mongo & redis data of com_list

                                                                                                                                                          
def cal_con_days(_db,_collection):                                                                                                                                        
                                                                                                                                                            
    set_date=[]                                                                                                                                                 
    cal_day =1                                                                                                                                                  
    limit_day = 60     
                                                                                                                                                            
    dictt_30day = [ {"$group": { "_id" : { "$toInt": "$last_modify" } }} , {"$sort" : {"_id" :-1}} , {"$limit" : limit_day}]                                            
    mydoc_30day = read_aggregate_mongo_db(_db,_collection,dictt_30day)                                                                                
                                                                                                                                                                
    ### get 30 day date in mongo data                                                                                                                                                            
    for idx in mydoc_30day :                                                                                                                                    
                                                                                                                                                                
      set_date.append(str(idx.get('_id')))                                                                                                                      
    
    ### last days to check                                                                                                                                      
    dictt_gt = [ {"$match": { "last_modify" : { "$gte" : str(set_date[0]) }  , "$expr" : { "$gt": [ { "$toInt": "$total" }, 0 ] }   }}  ,                       
              {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$total"} } , "sum_coun" : { "$sum" :1}  }}  ,                                
              {"$match": {"sum_coun" : { "$gte": 1 } } } ,  {"$sort" : {"total_values" : -1}}  ]                                                                
                                                                                                                                                                
    dictt_lt = [ {"$match": { "last_modify" : { "$gte" : str(set_date[0]) }  , "$expr" : { "$lt": [ { "$toInt": "$total" }, 0 ] }   }}  ,                       
                 {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$total"} } , "sum_coun" : { "$sum" : 1}  }}  ,                            
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
                                                                                                                                                                
                                                                                                                                                               
                                                                                                                                                            
    while ((not df_cal_gt.empty) or (not df_cal_lt.empty)) and cal_day < len(set_date):                                                                                                      
      
                                                                                                                                                                
      dictt_gt = [ {"$match": { "last_modify" : { "$gte" : set_date[cal_day] }  , "$expr" : { "$gt": [ { "$toInt": "$total" }, 0 ] }   }}  ,                     
              {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$total"} } , "sum_coun" : { "$sum" :1}  }}  ,                                
              {"$match": {"sum_coun" : { "$gte": cal_day+1 } } } ,  {"$sort" : {"total_values" : -1}}  ]                                                        
    
                                                                                                                                                            
      dictt_lt = [ {"$match": { "last_modify" : { "$gte" : set_date[cal_day] }  , "$expr" : { "$lt": [ { "$toInt": "$total" }, 0 ] }   }}  ,                    
                 {"$group":{"_id": "$code"  ,"total_values" : { "$sum" : { "$toInt": "$total"} } , "sum_coun" : { "$sum" : 1}  }}  ,                            
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
                         
    cal_dayy.columns=['code','total_values','con_days']                                                                                                                                                        

                                                                                                                                                            
    return cal_dayy.sort_values(by='code',ascending=True,ignore_index= True)                  


def  get_auth_stock(_db,_collection,com_lists): 
  
   ### mongo query com info 
   dicct_com = [ {"$match" : {"code": { "$in": com_lists } }} , { "$project" :{ "code":1 ,"auth_stock":1,"_id":0}} ]
   
   ### get mongo data
   mydoc_com_info = read_aggregate_mongo_db(_db,_collection,dicct_com) ## code,auth_stock
   df_auth_stock = pd.DataFrame(list(mydoc_com_info))  
   df_auth_stock = df_auth_stock.astype({'auth_stock':'int64'})
   
   return df_auth_stock



def get_mongo_last_date(cal_day):
   ### mongo query for last ? days
   dictt_set = [ {"$group": { "_id" : { "$toInt" : "$last_modify" } }},{"$sort" : {"_id" :-1}} , { "$limit" : cal_day},{"$sort" : {"_id" :1}} , { "$limit" :1}]

   ### mongo dict data

   set_doc =  read_aggregate_mongo_db('stock','Rep_3_Investors',dictt_set)

   ### for lists  get cal date 
   for idx in set_doc:

       idx_date = idx.get("_id")

   #set_date = str(idx_date)
   return str(idx_date)




def plot_Rep_3_Investors_price(match_row) :

   dfs = pd.DataFrame()
   
   ### match_row  code con_days
   cal_day = match_row.iloc[:,[1]].astype('int64').abs().max() ###max con_days

   last_modify = get_mongo_last_date(int(cal_day))
   ### dataframe to list 
   com_lists = match_row.iloc[:,[0]].reset_index(drop=True).squeeze()
   
   for idx in com_lists :

      altas_mydoc = read_mongo_db('stock','Rep_3_Investors',{'code': idx ,'last_modify': {'$gte' : last_modify}},{'_id':0})

      df = pd.DataFrame(list(altas_mydoc))

      df = df.iloc[:,[0,1,5,6]]

      rest_df = pd.DataFrame(data = df['total'].values,columns=[df['code'][0]+'_'+ df['name'][0]] , index=df['last_modify'])

      dfs = pd.concat([dfs,rest_df],axis=1)


   ### data to int for plot  
   records = dfs.fillna(0).astype('int64')

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





### change column name for merge
match_row.rename(columns={'公司代號':'code','合計':'total'}, inplace=True)

df_cal_day_conti = cal_con_days('stock','Rep_3_Investors')  ## code	total_values	con_days
df_auth_stock  = get_auth_stock('stock','com_lists',com_lists) ## "code" : , "auth_stock"

match_row =pd.merge( pd.merge(match_row,df_auth_stock,on = ['code'],how='left'),df_cal_day_conti,on = ['code'],how='left')     


### adding nenagive vlues to red 

match_row['1day%'] = round(round(match_row['total']/match_row['auth_stock'],4)*100 ,2)
match_row['con_days%'] = round(round(match_row['total_values']/match_row['auth_stock'],4)*100 ,2)
match_row = match_row.sort_values(by=['con_days%'],ascending = False ,ignore_index = True)

### select column
match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,11,10,12]].fillna(0)  
#code 公司簡稱 法人  投信 自營商 total  收盤價 漲跌(+/-)  total_values  auth_stock con_days 1day% con_days%

### plot Rep_3_Investors_price for line 
## code , con_days%
#print('match_row_0_9:' ,match_row.iloc[:,[0,9]])
plot_Rep_3_Investors_price(match_row.iloc[:,[0,9]])


match_row['con_days'] = match_row['con_days'].astype('int64')


for  idx in range(2,11,1) :

    
    #if idx ==9 :	
    if idx in [5,7,8,9,10]:
         #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">+%s</font>' % x  if x >0  else f'<font color="red">%s</font>' % x if x < 0 else 0 )
         match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x  if x <0  else f'<font color="red">+%s</font>' % x if x > 0 else 0)
         #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">+%s</font>' % x if x >=5  else f'<font color="red">%dday</font>' % x if x < 0 else 0 )
    #elif  idx == 10:
    #	   match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">+%s</font>' % x if x >= 10 else f'<font color="red">%dday</font>' % x if x < 0 else 0 )	
    elif idx != 6:
         #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">%s</font>' % x if x < 0 else str(x))
         match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x if x < 0 else str(x))




####match_row=Rep_3_Investors(url_list)

#if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
if (time.strftime("%H:%M:%S", time.localtime()) > mail_time) and chk == False :

    if not match_row.empty :
       body = match_row.to_html(escape=False)
       #body_color = body.replace('<td><font color="green">+5</font></td>','<td style="background-color:yellow;"><font color="green">+5</font></td>').replace('<td><font color="green">+10</font></td>','<td style="background-color:yellow;"><font color="green">+10</font></td>').replace('<td><font color="red">-5day</font></td>','<td style="background-color:#D3D3D3;"><font color="red">-5</font></td>').replace('<td><font color="red">-10day</font></td>','<td style="background-color:#D3D3D3;"><font color="red">-10</font></td>')
       #send_mail.send_email('Rep_3_Investors_%s' % date_sii ,body_color)
       send_mail.send_email('Rep_3_Investors_%s' % date_sii ,body)

else :
    print('Rep_3_Investors_%s' % date_sii )
    #print(match_row)
    print(match_row.to_html(escape=False))
    #display(match_row)

