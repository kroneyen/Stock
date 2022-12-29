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


mail_time = "18:00:00"
#mail_time = "09:00:00"

date_sii = datetime.date.today().strftime('%Y%m%d')
date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')

#date_sii='20220906'
#date_otc='111/09/06'

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


  url_sii ='https://www.twse.com.tw/fund/T86?response=html&date='+ date_sii +'&selectType=ALL'
  url_otc ='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=htm&se=EW&t=D&d=' + date_otc + '&s=0,asc'
  url_list = [url_sii,url_otc]
  dfs = pd.DataFrame()
  u_index = 0
  
  #com_lists = get_redis_data("com_list") ## get  redis data
  
  for url in url_list :
     
     r = requests.get(url)
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

     ##df = df[df[0].isin(com_lists)] ##比對
     
     dfs = pd.concat([dfs,df],ignore_index=True) ##合併
          
     time.sleep(1)
     u_index += 1

  

  dfs = dfs.astype({2:'int64',3:'int64',4:'int64',5:'int64'}) ##change type

  dfs[2] =round((dfs[2]/1000),0).astype(int)
  dfs[3] =round((dfs[3]/1000),0).astype(int)
  dfs[4] =round((dfs[4]/1000),0).astype(int)
  dfs[5] =round((dfs[5]/1000),0).astype(int)
  #dfs = dfs.style.apply(color_negative_red)
  dfs.columns = ['公司代號', '公司簡稱', '法人', '投信', '自營商','合計'] 
  #dfs = dfs.sort_values(by=['公司代號'])    
  #dfs = dfs.style.applymap(tableColor, subset=['法人', '投信', '自營商'])


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
              r = requests.get(url)
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



df_Rep_3_Investors = Rep_3_Investors(date_sii,date_otc,com_lists)
df_Rep_price = Rep_price(date_sii,date_otc,com_lists)


df_s = pd.merge(df_Rep_3_Investors,df_Rep_price, on =['公司代號']) ##dataframe join by columns
df_s['漲跌(+/-)'] = df_s['漲跌(+/-)'].str.replace('X','').str.replace('除息','0').str.replace(' ---','0').str.replace('--- ','0').astype({'漲跌(+/-)':'float'}).fillna(0).round(2)
df_s = df_s.iloc[:,[0,1,2,3,4,5,7,8]]
#df_s = df_s.fillna(0)
#match_row = df_s.style.applymap(tableColor, subset=['法人', '投信', '自營商','漲跌(+/-)']).set_precision(2)
#match_row = df_s
match_row = df_s.sort_values(by=['漲跌(+/-)'],ascending=False,ignore_index= True).copy()

##insert local mongo
local_mongo=[]
local_redis=[]

local_mongo = read_mongo_db('stock','com_list',dictt)
local_redis = get_redis_data('com_list','lrange',0,-1) 
     
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

 
### insert into mongodb 
"""
for index in range(len(match_row)) :
   # print(str(df.iloc[index,0])+' "' +str(df.iloc[index,1])+'" ',str(df.iloc[index:0])+'_p "'+str(df.iloc[index:2])+'"')
   ### for dic {1234 : "aaa" , 1234_p : "otc"}

   _values = { 'code' : str(match_row.iloc[index,0]) ,'name': str(match_row.iloc[index,1]) , 'foreign' : str(match_row.iloc[index,2]),'trust' : str(match_row.iloc[index,3]),'dealer' : str(match_row.iloc[index,4]),'total' : str(match_row.iloc[index,5]),'last_modify':date_sii }

   insert_mongo_db('stock','Rep_3_Investors',_values)
"""

### insert into local  mongodb 
records = pd.DataFrame()
records = match_row.copy()
records = records.iloc[:,[0,1,2,3,4,5]]
records["last_modify"]= date_sii
records.columns= ['code','name','foreign','trust','dealer','total','last_modify']

for idx in records.columns :
    records[idx] = records[idx].astype('str')


records =records.to_dict(orient='records')

insert_many_mongo_db('stock','Rep_3_Investors',records)



time.sleep(1)

### sync local mongo & redis data of com_list




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


### cal  continue  +/-5,10 days 
 
for cal_day in [5,10] :
  
   set_date=get_mongo_last_date(cal_day) 
  

   dictt_lt = [ {"$match": { "last_modify" : { "$gte" : set_date}  , "$expr" : { "$lt": [ { "$toInt": "$total" }, 0 ] }   }}  ,{"$group":{"_id": {"code" : "$code" , "name" : "$name"} ,"total_values" : { "$sum" : { "$toInt": "$total"} } ,"sum_coun" : { "$sum" :1}  }}  , {"$match": {"sum_coun" : { "$gte": cal_day } } } ,  {"$sort" : {"total_values" : -1}}  ]
   
   dictt_gt = [ {"$match": { "last_modify" : { "$gte" : set_date}  , "$expr" : { "$gt": [ { "$toInt": "$total" }, 0 ] }   }}  ,{"$group":{"_id": {"code" : "$code" , "name" : "$name"} ,"total_values" : { "$sum" : { "$toInt": "$total"} } ,"sum_coun" : { "$sum" :1}  }}  , {"$match": {"sum_coun" : { "$gte": cal_day } } } ,  {"$sort" : {"total_values" : -1}}  ]
   

   
   idx_lt_list=[]
   idx_gt_list=[]
   total_lt_list=[]
   total_gt_list=[]
   
   
   ### get mongo data 
   mydoc_lt = read_aggregate_mongo_db('stock','Rep_3_Investors',dictt_lt)
   mydoc_gt = read_aggregate_mongo_db('stock','Rep_3_Investors',dictt_gt)

   
   ### lists coms code 
   
   for idx in mydoc_lt :
      idx_lt_list.append(idx.get('_id').get('code'))
      total_lt_list.append(idx.get('total_values'))
   
   for idx in mydoc_gt :
      idx_gt_list.append(idx.get('_id').get('code'))
      total_gt_list.append(idx.get('total_values'))
   

   ### get mongo data of auth_stock
   
   if  'auth_stock' not in match_row.columns :
      ### list search coms 
      auth_com_list = match_row['公司代號'].tolist()
      
      ### mongo query com info 
      dicct_com = [ {"$match" : {"code": { "$in": auth_com_list } }} , { "$project" :{ "code":1 ,"auth_stock":1}} ]
      
      ### get mongo data
      mydoc_com_info = read_aggregate_mongo_db('stock','com_lists',dicct_com) ## code,auth_stock
   
      auth_code_list=[]
      auth_stock_list=[]
   
      ### lists coms code       
      for idx in mydoc_com_info :
         auth_code_list.append(idx.get('code'))
         auth_stock_list.append(idx.get('auth_stock'))
      
      ### get auth_stock merge match_row
      pyload_auth_stock = { '公司代號' : auth_code_list ,
                        'auth_stock' : auth_stock_list}
      
      df_auth_stock = pd.DataFrame(pyload_auth_stock)
      
      df_auth_stock = df_auth_stock.astype({'auth_stock':'int64'})
      
      ### merge data check         
      
      match_row = pd.merge(match_row,df_auth_stock,on = ['公司代號'],how='left')  
  
   ###  compare coms_list change data
   
   key_day  =  str(cal_day) +'day' 
   key_day_total = str(cal_day) +'day_total' 
   key_day_p =  str(cal_day) +'day%'
   
   #match_row['5day'] = match_row['公司代號'].apply(lambda  x: -5 if x in idx_lt_list  else 5 if x in idx_gt_list else 0)
   match_row[key_day] = match_row['公司代號'].apply(lambda  x: -cal_day if x in idx_lt_list  else cal_day if x in idx_gt_list else 0)
   #id_total = idx_lt_list.extend(idx_gt_list)
   #total_list = total_lt_list.extend(total_gt_list)
   idx_lt_list.extend(idx_gt_list)
   total_lt_list.extend(total_gt_list)
   
   
   pyload_total = { '公司代號' : idx_lt_list ,
   	     key_day_total : total_lt_list }
        #'5day_total' : total_lt_list }
   
   
   com_toal = pd.DataFrame(pyload_total)
   
   #com_toal = com_toal.fillna(0).astype({'5day_total':'int64'})
   #com_toal = com_toal.fillna(0).astype({ key_day_total:'int64'})
   com_toal = com_toal.astype({ key_day_total:'int64'})



   
   #match_row = pd.merge(match_row,com_toal,on = ['公司代號'],how='left').fillna(0)
   match_row = pd.merge(match_row,com_toal,on = ['公司代號'],how='left')
   
   match_row[key_day_p] = round(round(match_row[key_day_total]/match_row['auth_stock'],4)*100 ,2)
   #match_row['5day%'] = round(round(match_row['5day_total']/match_row['auth_stock'],4)*100 ,2)
   
   #match_row['10day'] = match_row['公司代號'].apply(lambda  x: -10 if x in idx_lt_list  else 0 if x in idx_gt_list else 0)


### adding nenagive vlues to red 

#match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,9,10]] ## 公司代號       公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock      5day    5day%   for  hide ['auth_stock']
#match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,9,11]] ## 公司代號        公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock      5day    5day_total 5day%   for  hide ['auth_stock','5day_total']

match_row['1day%'] = round(round(match_row['合計']/match_row['auth_stock'],4)*100 ,2)

#match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,9,11,12,14]] ## 公司代號        公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock      5day    5day_total 5day%   10day    10day_total 10day%  for  hide ['auth_stock','5day_total','10day_total']
match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,15,9,11,12,14]] ## 公司代號        公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock       5day    5day_total 5day%   10day    10day_total 10day%  1day%  for  hide ['auth_stock','5day_total','10day_total']

match_row['con_days'] = match_row.apply(lambda  x: x['10day']  if x['10day']!= 0  else x['5day']  if x['5day']!= 0 else 0 ,axis =1)
#match_row['con_days'] = match_row['5day'].apply(lambda  x: 5  if pd.isnull(x)  else 10 )
###instead  
match_row['con_days%'] = match_row.apply(lambda  x: x['10day%']  if pd.notna(x['10day%'])  else x['5day%'] if pd.notna(x['5day%']) else 0 ,axis=1)

#match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,9,11,12,14]] ## 公司代號        公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock      5day    5day_total 5day%   10day    10day_total 10day%  for  hide ['auth_stock','5day_total','10day_total']
#match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,15,9,11,12,14,16,17]] ## 公司代號        公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock       5day(9)    5day_total 5day%   10day    10day_total 10day%  1day%(15),'con_days','con_days%  for  hide ['auth_stock','5day_total','10day_total']
match_row = match_row.iloc[:,[0,1,2,3,4,5,6,7,8,13,14]] ## 公司代號        公司簡稱        法人    投信    自營商  合計    收盤價  漲跌(+/-)       auth_stock       5day(9)    5day_total 5day%   10day    10day_total 10day%  1day%(15),'con_days','con_days%  for  hide ['auth_stock','5day_total','10day_total']


for  idx in [2,3,4,5,7,8,9,10] :
#for  idx in [2,3,4,5,7,8,9,10,11,12] :
#for  idx in [2,3,4,5,7,8,9,10,11] :
#for  idx in [2,3,4,5,7,8,9] :
#for  idx in range(2,10,1) :
    
    if idx ==9 :	
         match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">+%s</font>' % x  if x >=5  else f'<font color="red">%dday</font>' % x if x < 0 else 0 )
         #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">+%s</font>' % x if x >=5  else f'<font color="red">%dday</font>' % x if x < 0 else 0 )
    #elif  idx == 10:
    #	   match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">+%s</font>' % x if x >= 10 else f'<font color="red">%dday</font>' % x if x < 0 else 0 )	
    else :
         match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">%s</font>' % x if x < 0 else str(x))


####match_row=Rep_3_Investors(url_list)

if time.strftime("%H:%M:%S", time.localtime()) > mail_time :

    if not match_row.empty :
       body = match_row.to_html(escape=False)
       body_color = body.replace('<td><font color="green">+5</font></td>','<td style="background-color:yellow;"><font color="green">+5</font></td>').replace('<td><font color="green">+10</font></td>','<td style="background-color:yellow;"><font color="green">+10</font></td>').replace('<td><font color="red">-5day</font></td>','<td style="background-color:#D3D3D3;"><font color="red">-5</font></td>').replace('<td><font color="red">-10day</font></td>','<td style="background-color:#D3D3D3;"><font color="red">-10</font></td>')
       send_mail.send_email('Rep_3_Investors_%s' % date_sii ,body_color)

else :
    print('Rep_3_Investors_%s' % date_sii )
    #print(match_row)
    print(match_row.to_html(escape=False))
    #display(match_row)


