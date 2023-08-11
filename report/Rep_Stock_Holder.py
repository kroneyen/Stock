#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
#from datetime import datetime
import datetime
from io import StringIO
from pymongo import MongoClient
import redis
import send_mail
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager
from fake_useragent import UserAgent
import del_png
import numpy as np


# 改style要在改font之前
plt.style.use('seaborn')
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')



mail_time = '10:00:00'

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


def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)


def read_aggregate_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)


def delete_many_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.delete_many(dicct)


### mongodb atlas connection
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)



def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



def get_Stock_Holder_Set(cal_date):
    my_doc = read_mongo_db('stock','Rep_Stock_Exchange',{'last_modify': cal_date},{"code":1,"price":1,"last_modify":1,"_id":0})
    df =pd.DataFrame(list(my_doc))
    
    return df.reset_index(drop=True)


def get_mongo_cal_date(cal_day,_collection):


 if  _collection  ==  'Rep_Stock_Holder' : 
    ### mongo query for last ? days
    dictt_set = [ {"$group": { "_id" : { "$toInt" : "$date" } }},{"$sort" : {"_id" :-1}} , { "$limit" : cal_day}]

 else : 
    
    dictt_set = [ {"$group": { "_id" : { "$toInt" : "$last_modify" } }},{"$sort" : {"_id" :-1}} , { "$limit" : cal_day}]




### mongo dict data

 set_doc =  read_aggregate_mongo_db('stock',_collection,dictt_set)

 idx_date =[]
 ### for lists  get cal date 
 for idx in set_doc:

  idx_date.append(str(idx.get("_id")))

 #set_date = str(idx_date)
 return idx_date



def get_Rep_Stock_Holder(com_num):

    user_agent = UserAgent()

    """    
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
    0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'} 
    """
    url ='https://opendata.tdcc.com.tw/getOD.ashx?id=1-5'

    #res = requests.get(url, headers=headers )
    res = requests.get(url,  headers={ 'user-agent': user_agent.random } )
    df = pd.read_csv(StringIO(res.text))
    df = df.astype(str)
    
    if com_num == 'nan' :
      com_list = read_mongo_db('stock','com_list',{},{'code':1,'_id':0})
      my_doc_list = pd.DataFrame(list(com_list)) ##cours to dric
          
      df = df[df['證券代號'].isin(list(my_doc_list['code']))]
    else : 
   
      df = df[df['證券代號'].isin([com_num])] 

    df = df.rename(columns={
        '證券代號': 'stock_id',
        '股數': '持有股數', '占集保庫存數比例%': '占集保庫存數比例'
    })
   
    
    # 官方有時會有不同格式誤傳，做例外處理
    if '占集保庫存數比例' not in df.columns:
        df = df.rename(columns={'佔集保庫存數比例%': '占集保庫存數比例'})
        
    # 持股分級=16時，資料都為0，要拿掉
    df = df[df['持股分級'] != '16']
    df['持股分級'] = df['持股分級'].replace(['17'],'16')
    
    # 資料轉數字
    float_cols = ['人數', '持有股數', '占集保庫存數比例']
    df[float_cols] = df[float_cols].apply(lambda s: pd.to_numeric(s, errors="coerce"))
    # 抓表格上的時間資料做處理
     
    #df['date'] = datetime.datetime.strptime(df.iloc[0,0],"%Y%m%d")
    
    #只要第二層欄位名稱
    #df = df.drop(columns=df.columns[0])
    
    # 索引設置 unique index
    #df = df.set_index(['stock_id', 'date', '持股分級'])
    df.columns = ['date','code','level','owner','stock','percentage']
    return df.reset_index(drop=True)



def cal_level_min(code ,cal_day) :                                                                                            
                                                                                                                              
                                                                                                                              
    dicct =  [ {'$match':  { 'last_modify' : { '$gte' : cal_day } ,'code' : code }},                                            
            {'$group': { '_id' : {'code' : '$code' , 'name':'$name'} ,'avg_price' : {'$avg' : { '$toDouble' :"$price" }   } }} ]
                                                                                                                                
    cal_mydoc = read_aggregate_mongo_db('stock','Rep_Stock_Exchange',dicct)                                                    
    ##{ "_id" : { "code" : "1326", "name" : "台化" }, "avg_price" : 68.70967741935483 }
                                                                                                                                
    df = pd.DataFrame(list(cal_mydoc))                                                                                          
                                                                                                                                
    stock = ['0-0.9','1-5','5.1-10','10.1-15','15.1-20',                                                                        
         '20.1-30','30.1-40','40.1-50','50.1-100','100.1-200',                                                                  
         '200.1-400','400.1-600','600.1-800','800.1-1,000','1,000.1up']                                                         
                                                                                                                                
    map_data = {"level": range(1,16,1) ,  "stock_num" : stock}                                                                  
    level_map = pd.DataFrame(data= map_data,columns=['level','stock_num'])                                                      
                                                                                                                                
                                                                                                                                
    """                                                                                                                         
    ## set level                                                                                                                
    #stock = ['1-999','1,000-5,000','5,001-10,000','10,001-15,000','15,001-20,000','20,001-30,000',                             
    #    '30,001-40,000','40,001-50,000','50,001-100,000','100,001-200,000','200,001-400,000',                                  
    #    '400,001-600,000','600,001-800,000','800,001-1,000,000','1,000,001以上','合　計']                                           
                                                                                                                                
    stock = ['0-0.9','1-5','5.1-10','10.1-15','15.1-20','20.1-30',                                                              
      '30.1-40','40.1-50','50.1-100','100.1-200','200.1-400',                                                                   
      '400.1-600','600.1-800','800.1-1,000','1,000.1張以上','合　計']                                                                 
    map_data = {"level": range(1,17,1) ,  "stock_num" : stock}                                                                  
                                                                                                                                
                                                                                                                                
    ## mapping & merge data                                                                                                     
                                                                                                                                
    level_map = pd.DataFrame(data= map_data,columns=['level','stock_num'])                                                      
    level_map['level'] =level_map['level'].astype(str)                                                                          
    """                               

                                                                                          
    #avgprice = round((df['avg_price'].values/1),2)                                                                                           
    avgprice = round((df.iloc[0,1]),2)                                                                                           
                                                                                                                            
    if avgprice > 30 and avgprice < 50 : ##800張以上                                                                               
      level_min = 14                                                                                                            
                                                                                                                                
    elif avgprice > 50 and avgprice < 100:  ##600張以上                                                                            
      level_min = 13                                                                                                            
                                                                                                                                
                                                                                                                                
    elif avgprice > 100 : ##400張以上                                                                                              
      level_min = 12                                                                                                            
                                                                                                                                
                                                                                                                                
    else : ## $30 以下                                                                                                            
      level_min = 15 ##1000張以上                                                                                                  
                                                                                                                                
    level_stock = level_map[level_map['level'] == level_min].iloc[0,1] ## get stock_num of mapping level                        
                                                                                                                                
    return level_min ,level_stock,avgprice                                           




def plot_Rep_Stock_Holder_code() : 

      dfs = pd.DataFrame()
   
      
      ### get cal_day of 6 week date on   Rep_Stock_Holder
      date_lists=[]

      date_lists = get_mongo_cal_date(6,'Rep_Stock_Holder')
      date_lists =list(date_lists)

      ## mongo last date from Rep_Stock_Holder to lists
      cal_day = date_lists[len(date_lists)-1]   ## get first day 
      last_day = date_lists[0] ## get last day

     # print('last_day:',last_day)
      ### get all code & name  of last day 
      
      last_day_dicct = [ {'$match':  { 'last_modify' : last_day } }, {'$group': { '_id' : { 'code' : '$code' , 'name' : '$name' } } } ]
   
      mydoc = read_aggregate_mongo_db('stock','Rep_Stock_Exchange',last_day_dicct)
  
      last_day_df = pd.DataFrame(list(mydoc))

      ### non mapping price of Rep_Stock_Exchange
      if last_day_df.empty : 

         ex_last_day = get_mongo_cal_date(1,'Rep_Stock_Exchange')

         last_day_dicct = [ {'$match':  { 'last_modify' : ex_last_day[0] } }, {'$group': { '_id' : { 'code' : '$code' , 'name' : '$name' } } } ]

         mydoc = read_aggregate_mongo_db('stock','Rep_Stock_Exchange',last_day_dicct)

         last_day_df = pd.DataFrame(list(mydoc))



              
      #print('last_day_df:',last_day_df.info())
       
      ### list of com & name 
      com_lists=[]
      name_lists=[]
      
      for idx in last_day_df['_id'] :
          com_lists.append(idx.get('code')) 
          name_lists.append(idx.get('name')) 
     
      
      name_idx = 0 
      plot_data_df = pd.DataFrame()
   
      for code in com_lists : 
         
         ### get level, stock , avg_price 
         level_min ,level_stock ,avg_price = cal_level_min(code,cal_day) 
   
         dicct = [ {'$match':  { 'date' : { '$gte' : cal_day } ,'code' : code ,  
             '$expr' :  { '$and' : [  { '$gte': [ { '$toInt': '$level' }, level_min ] } , {'$lt':  [ { '$toInt': '$level' }, 16 ] } ]  }}}  ,
             {'$group': { '_id' : { 'date' : {'$toInt': '$date'}  , 'code' : '$code' }, 
             'sum_owner' : { '$sum' : '$owner'} , 'sum_percentage' : {'$sum' : '$percentage'} }} ,  {'$sort' : {'_id' :1}} ]
   
      
         altas_mydoc = read_aggregate_mongo_db('stock','Rep_Stock_Holder',dicct)
           
         df = pd.DataFrame(list(altas_mydoc))
     
         ### for pivot prepare
         date_list = [] 
         code_list = [] 
         for idx in df['_id'] : 
           date_list.append(idx.get('date'))
           code_list.append(idx.get('code'))
   
         df['date'] = date_list
         df['code'] = code_list      
   
         
         
         #### df to pivot for mail table
         pivot_df = df.pivot(index='code', columns='date', values='sum_percentage')
         rest_df = pivot_df.copy()
         rest_df.insert(0,'code',code ,True)
         rest_df.insert(1,'name',name_lists[name_idx] ,True)
         rest_df.insert(2,'avg_price',avg_price ,True)
         rest_df.insert(3,'level_min',level_min ,True)
         rest_df.insert(4,'hold(up)',level_stock ,True)
         
         dfs = pd.concat([dfs,rest_df],axis=0,ignore_index=True) 
   
   
         ### df get plot data 
         df['date'] = df['date'].astype('str')      
         plot_data = pd.DataFrame(data = df['sum_percentage'].values,columns=[com_lists[name_idx]+'_'+name_lists[name_idx]] , index=df['date'])
         plot_data_df =  pd.concat([plot_data_df,plot_data],axis=1)      
         
   
         name_idx +=1
      
      ######## for email table #########

      ### sum_percentage data type trans to float
      for idx in range(5,len(dfs.columns),1) :
         dfs[dfs.columns[idx]] = dfs[dfs.columns[idx]].fillna(0).astype('float')
        
      ### add date% column of  cal growth rate :  round ( (next_date - base_date) / base_date * 100 ,2 )
      for idx in range(1,6,1) : 
        
         dfs[str(dfs.columns[idx+5]) + '%'] = round((dfs.iloc[:,idx+5] - dfs.iloc[:,idx+4])/ dfs.iloc[:,idx+4] *100 ,2)
   
      ## sort columns   
      dfs = dfs.iloc[:,[0,1,2,3,4,5,6,11,7,12,8,13,9,14,10,15]].fillna(0)
   
      ######### sum_percentage  for plot set ########
      records = plot_data_df.fillna(0).astype('float64')
   
      for idx in range(0,len(records.columns),5):
      
         idx_records = records.iloc[ : , idx:idx+5 ]
       
         idx_records.plot(y = idx_records.columns)
         plt.savefig('./images/image_'+ str(idx) +'_'+ str(idx+4) +'.png' )
   
   
      return dfs ,last_day



### get url  & insert into mongo 
match_row = get_Rep_Stock_Holder('nan')


records = match_row.copy()
### get records date 
records_date = records.iloc[0,0]

Holder_last_day = get_mongo_cal_date(1,'Rep_Stock_Holder')
Holder_last_day = Holder_last_day[0]

db_records = read_mongo_db('stock','Rep_Stock_Holder',{"date":Holder_last_day},{"_id":0,"last_modify":0})
db_records_df =  pd.DataFrame(list(db_records))
chk = records.equals(db_records_df)
if chk == False :

   dicct = {"date":records_date}
   delete_many_mongo_db('stock','Rep_Stock_Holder',dicct)

   records['last_modify']=datetime.datetime.now()
   records =records.to_dict(orient='records')
   insert_many_mongo_db('stock','Rep_Stock_Holder' , records )

time.sleep(1)


### del images/*.png
del_png.del_images()


#######  plot Rep_Stock_Hoder for 6 week
match_row , last_day =  plot_Rep_Stock_Holder_code()


### adding nenagive % vlues to red 
for  idx in [7,9,11,13,15] :

    match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s</font>' % x if x > 0 else  f'<font color="green">%s</font>' % x)




###send mail
#if (time.strftime("%H:%M:%S", time.localtime()) > mail_time) and chk == False :
if (time.strftime("%H:%M:%S", time.localtime()) > mail_time)  :

    if not match_row.empty :
       body = match_row.to_html(escape=False)
       send_mail.send_email('Rep_Stock_Holder_%s' % last_day ,body)

else :
    print('Rep_Stock_Holder_%s' % last_day )
    print(match_row.to_html(escape=False))
    #print(match_row.info())
