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


#date_sii = datetime.date.today().strftime('%Y%m%d')
#date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')

user_agent = UserAgent()

mail_time = "09:00:00"
#mail_time = "21:00:00"

def color_red(val):
    if val < 0:
        color = 'red'
    #elif val < 0:
    #    color = 'green'
    else:
        color = 'block'
    return 'color: %s' % color




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
    collection.delete_many(_values)


### mongodb atlas connection
user = get_redis_data('mongodb_user',"hget","user",'NULL')
pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

conn = MongoClient('mongodb+srv://'+user+':'+pwd+'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def get_mongo_last_date():
 ### mongo query for last ? days
 dictt_set = [ {"$group": { "_id" : { "$toInt" : "$last_modify" } }} , {"$sort" : {"_id" :-1}} , { "$limit" : 1},{"$sort" : {"_id" :1}} , { "$limit" :1}]

 ### mongo dict data

 set_doc =  read_aggregate_mongo_db('stock','Rep_3_Investors',dictt_set)

 ### for lists  get cal date 
 for idx in set_doc:

  idx_date = idx.get("_id")

 #set_date = str(idx_date)
 return str(idx_date)




def stock_season_report(year, season, yoy_up,yoy_low,com_lists):
    #pd.options.display.max_rows = 3000
    #pd.options.display.max_columns 
    #pd.options.display.max_colwidth = 200 

    url_sii = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year='+ str(year-1911)  +'&season='+ str(season)
    url_otc = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=otc&year='+ str(year-1911)  +'&season='+ str(season)    
    url_list =[url_sii,url_otc]
    #type_list=['sii','otc']    
     
    df_f = pd.DataFrame()

    #user_agent = UserAgent()

    #u_index = 0
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


    df_f[0]= df_f[0].astype('str')
    ### filter com_lists data
    df_f =df_f[df_f[0].isin(com_lists)]
    df_f = df_f.sort_values(by=[2],ascending = False,ignore_index = True) ## 排序
    sst = "Q%s累計盈餘" %(season)
    #df_f.columns = ['公司代號', '公司簡稱','基本每股盈餘（元）' ]  ##定義欄位
    df_f.columns = ['公司代號', '公司名稱',sst ]  ##定義欄位
    
    return df_f


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

              time.sleep(1)
              u_index += 1
              #print(df.info())
       dfs = dfs.fillna(0)
       #dfs.columns = ['公司代號', '證券名稱', '收盤價', '漲跌(+/-)']
       #dfs = dfs.iloc[:,[0,1,2,16]]
       dfs = dfs.iloc[:,[0,2]]
       dfs.iloc[:,1] = dfs.iloc[:,1].apply(lambda  x: None  if x =='--' else None if x=='---' else float(x))
       dfs.columns = ['code', 'price']
       dfs = dfs.astype({dfs.columns[1]:'float'}) 

       return dfs.dropna()






###  5/15,8/14,11/14,3/31

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

"""
#### Q4 of years  
if season == 4 :
     yy = today.year -1
     last_yy = today.year -2
else :
### last season replort
      yy = today.year
      last_yy = today.year -1
"""

### last season replort
yy = today.year
last_yy = today.year -1



### get atlas mongodb data 
com_lists = []


try :

  ### got mongo data from atlas
  dictt = {}
  _columns= {"code":1,"_id":0}

  mydoc = atlas_read_mongo_db('stock','com_lists',dictt,_columns)

  for idx in mydoc :
    com_lists.append(idx.get('code'))

except :
   ### got redis data from local
   redis_lists = get_redis_data("com_lists","hkeys",'NULL','NULL') ## get  redis data

   for  idx in redis_lists :
        if not re.match("(\w+_p$)", idx) :  
           com_lists.append(idx)





### get season data from mongo

mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0})
mydoc_code_lists=[]

for idx in mydoc_season :
   mydoc_code_lists.append(idx.get('code'))


mydoc_code_lists.sort()
com_lists.sort()


### compare mydoc_code_lists & com_lists 
if not mydoc_code_lists == com_lists :

   try :

        last_year = stock_season_report( last_yy ,season ,'undefined','undefined',com_lists)  ## last year season

        time.sleep(1)

        the_year = stock_season_report(yy ,season,'undefined','undefined',com_lists)  ## the year season


   except :  ### if no data  get last season

          if  season == 1   : ## crossover years

                season = 4
                last_yy  =   today.year -2
                yy =  today.year -1
                
          else :        
                 season = season -1

          
          last_year = stock_season_report( last_yy ,season ,'undefined','undefined',com_lists)  ## last year season
          
          time.sleep(1)
          
          the_year = stock_season_report( yy ,season ,'undefined','undefined',com_lists)  ## the year season 



   #s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司簡稱'],suffixes=('_%s' % str(today.year) , '_%s' % str(today.year -1))).copy() ## merge 2021_Q3 & 2020_Q3 
   s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司名稱'],suffixes=('_%s' % str(yy) , '_%s' % str(last_yy))).copy() ## merge 2021_Q3 & 2020_Q3 


   #### (累計EPS / 去年累計EPS - 1) * 100% 

   s_df['EPS_g%'] = (( (s_df.iloc[:,2] - (s_df.iloc[:,3]).abs() )/ (s_df.iloc[:,3]).abs() ) * 100 ).round(2) ## 計算成長% (2021- 2020) /2020 * 100%




   match_row = s_df.sort_values(by=['EPS_g%'],ascending = False,ignore_index = True).copy()
   #match_row.columns =["code","code_name","the_year", "last_year" , "EPS_g%" , "season" , "years"]  
   #match_row.columns =["code","code_name","the_year", "last_year" , "EPS_g%"]
   #code_list = list(match_row["code"])
   
   #print('match_row_info:',match_row.info())
   #if not match_row.empty and (len(match_row) != len(com_lists)) :
   #if not match_row.empty and not  code_list == com_lists  :
   if not match_row.empty  :

      records = match_row.copy()
      records.columns =["code","code_name","the_year", "last_year" , "EPS_g%"]
      records["season"] = str(season)
      records["years"]  = str(yy)

      records =records.to_dict(orient='records')

      del_filter = {"years":str(yy),"season":str(season)}
      delete_many_mongo_db('stock','Rep_Stock_Season_Com',del_filter)
      insert_many_mongo_db('stock','Rep_Stock_Season_Com',records)

      time.sleep(1)
   ### data empty & get last season report  
   else : 
      
   
      mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0})

      match_row= pd.DataFrame(list(mydoc_season))

 
###  (mydoc_season) == (com_lists)
else :   

   mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0})

   match_row= pd.DataFrame(list(mydoc_season))




### get last price
last_modify=get_mongo_last_date()
#20230807
#print('last_modify:',last_modify)
date_sii = last_modify
date_otc = str( int(datetime.datetime.strptime(last_modify, '%Y%m%d').date().strftime('%Y')) - 1911)  + datetime.datetime.strptime(last_modify, '%Y%m%d').date().strftime('/%m/%d')

#print(date_sii,date_otc)

mydoc = Rep_price(date_sii,date_otc,com_lists)


match_row.rename(columns={'公司代號': 'code', '公司名稱': 'code_name'}, inplace=True)

match_row_doc = pd.merge(match_row,mydoc, on =['code']) ##dataframe join by column

match_row_doc.dropna()


#print("match_row_doc_info:",match_row_doc.info())
match_row_doc = match_row_doc.astype({match_row_doc.columns[2]:'float',match_row_doc.columns[5]:'float'})

### 70% Dividend for earn_yield
div_per = 0.7 
match_row_doc['earn_yield']= round(round((match_row_doc.iloc[:,2]/match_row_doc['price']*0.7),4)*100 ,2)
match_row_doc['PE']= round(round(match_row_doc['price']/match_row_doc.iloc[:,2],4),2)
## PE & earn_yield filter
match_row=match_row_doc[(match_row_doc['PE']<35) & (match_row_doc['earn_yield']>0)] .copy()
#match_row=match_row_doc.copy()
#match_row=match_row.sort_values(by=['earn_yield'],ascending = False,ignore_index = True)
match_row=match_row.sort_values(by=['PE'],ascending = True,ignore_index = True)



#match_row = match_row.iloc[:,[0,1,2,3,4,5,7]]
for idx in list(range(2,8)) :
    if idx == 4 or idx == 6 or  idx == 7:
       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x  if x <0  else f'<font color="red">+%s</font>' % x if x > 0 else 0)
    else : 
       match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x if x < 0 else str(x))
       #match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">%s</font>' % str(x) if x < 0 else str(x))



if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
    #match_row.to_html('hot_news_link.log')
    if not match_row.empty :
       body = match_row.to_html(classes='table table-striped',escape=False)
       
       #body = match_row.to_html(escape=False)
       #log_date = datetime.date.today().strftime("%Y-%m-%d")
       #send_mail.send_email('{year}_stock_Q{season}_report' .format(year = today.year ,season=season) ,body)
       #send_mail.send_email('{year}_stock_Q{season}_report' .format(year = yy ,season=season) ,body)
       to_date = datetime.date.today().strftime("%Y%m%d")
       #send_mail.send_email('Stock_Eps_Earnning_Yield_{today}_com'.format(today=to_date),body)
       send_mail.send_email('Stock_Eps_Yield_PE_{today}_com'.format(today=to_date),body)
else :
    #print(match_row.to_string(index=False))
    #print(match_row.to_html(escape=False))
    print(match_row.head(100))
