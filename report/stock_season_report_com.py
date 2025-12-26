#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
import requests
#import line_notify
from bs4 import BeautifulSoup
import send_mail
from pymongo import MongoClient
from fake_useragent import UserAgent
import del_png
import re
from pymongo import MongoClient
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager
import pandas_table as pd_table
from io import StringIO
import urllib3
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# 改style要在改font之前
plt.style.use("seaborn-v0_8")
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')


### del images/*.png
del_png.del_images()




#mail_time = "18:00:00"
mail_time = "09:00:00"



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



### mongodb atlas connection
user = get_redis_data('mongodb_user',"hget","user",'NULL')
pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

conn = MongoClient('mongodb+srv://'+user+':'+pwd+'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')

def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )


def insert_many_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert_many(_values)

def delete_many_mongo_db(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.delete_many(dicct)

def read_mongo_db(_db,_collection,dicct,_columns):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)


def read_mongo_db_sort_limit(_db,_collection,dicct,_columns,_sort):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns).sort(_sort).limit(1)




def stock_season_report(year, season, yoy_up,yoy_low,com_lists):
    #pd.options.display.max_rows = 3000
    #pd.options.display.max_columns 
    #pd.options.display.max_colwidth = 200 

    user_agent = UserAgent()

    url_sii = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year='+ str(year-1911)  +'&season='+ str(season)
    url_otc = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=otc&year='+ str(year-1911)  +'&season='+ str(season)    
    url_list =[url_sii,url_otc]
    
    df_f = pd.DataFrame() ## sii & otc

    for url in url_list : 
       dfs=pd.DataFrame()
       r =  requests.post(url ,  headers={ 'user-agent': user_agent.random },verify=False)
       r.encoding = 'utf8'
       soup = BeautifulSoup(r.text, 'html.parser')
       tables = soup.find_all('table',attrs={"class": "hasBorder"})
       

       for i in range(1,len(tables)+1) : 

         df = pd.read_html(StringIO(r.text),thousands=",")[i] ## 多表格取的[i]為dataframe

         df.columns= llist(len(df.columns)) ##重新編列columns
         last_col_num = len(df.columns)-1  ## get last columns num (基本每股盈餘（元）) 17 or 21 or 29

         df = df.iloc[:,[0,1,last_col_num]] ## 每年欄位不同 
         df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
         dfs = pd.concat([dfs,df],ignore_index=True) ##合併

    
       df_f = pd.concat([df_f,dfs],ignore_index=True) ###合併

       time.sleep(1)
    """
    ### get atlas mongodb data 
    mydoc = atlas_read_mongo_db("stock","com_list",{},{"_id":0,"code":1})
    com_lists = []
    for idx in mydoc :
     com_lists.append(idx.get('code'))
    """
        
    #if not df_f.empty :

    df_f[0]= df_f[0].astype('str')
    ### filter com_lists data
    df_f =df_f[df_f[0].isin(com_lists)]
    df_f = df_f.sort_values(by=[2],ascending = False,ignore_index = True) ## 排序
    sst = "Q%s累計盈餘" %(season)
    df_f.columns = ['公司代號', '公司名稱',sst ]  ##定義欄位


    return df_f





def stock_season_roa_roe(year, season, com_lists):


    dfs =  pd.DataFrame()
    dfs_b =  pd.DataFrame()
    dfs_a =  pd.DataFrame()
    df_merge =  pd.DataFrame()

    url_list = ['https://mopsov.twse.com.tw/mops/web/ajax_t163sb04','https://mopsov.twse.com.tw/mops/web/ajax_t163sb05']


    typek=['sii','otc']

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
  0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'}

    #r =  requests.post(url ,  headers={ 'user-agent': user_agent.random })
    #print('url:',url)


    url_idx = 0 ## for 綜合損益表Statement of Comprehensive Income or financials
    for url in url_list :

      for ty_idx in typek :

       payload = {'encodeURIComponent': 1,
            'step': 1,
            'firstin': 1,
            'off': 1 ,
            'isQuery': 'Y' ,
            'TYPEK': ty_idx ,
            'year': str(year-1911) ,
            'season': str(season) }


       ##print('payload:',payload)
       dfs = pd.DataFrame()
       r =  requests.post(url ,params=payload ,  headers=headers ,verify=False)
       r.encoding = 'utf8'
       soup = BeautifulSoup(r.text, 'html.parser')
       tables = soup.find_all('table',attrs={"class": "hasBorder"})
       ##print(tables)

       if url_idx == 0 :

        for i in range(1,len(tables)+1) :

          df = pd.read_html(StringIO(r.text),thousands=",")[i] ## 多表格取的[i]為dataframe
          
          
          df_list = df.columns
          last_col_num=len(df_list) -1 
          

          if last_col_num == 21 :
          
             if re.search("停業單位損益",df_list[11]) :

                ## get  12  本期稅後淨利（淨損）/ 21  基本每股盈餘（元）
                #print(df_list[12] , df_list[21])
                df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
                df = df.iloc[:,[0,1,12,last_col_num]] ## 每年欄位不同

                df.columns= llist(len(df.columns))
                
             else :  
                ## get  11  本期稅後淨利（淨損）/ 21  基本每股盈餘（元）
                #print(df_list[11] , df_list[21])
                df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
                df = df.iloc[:,[0,1,11,last_col_num]] ## 每年欄位不同
                
                df.columns= llist(len(df.columns))
          
          if last_col_num == 29 :

             #print(df_list[19] , df_list[29])
             df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
             df = df.iloc[:,[0,1,19,last_col_num]] ## 每年欄位不同   

             df.columns= llist(len(df.columns))          

          if last_col_num == 22 :

             #print(df_list[12] , df_list[22])
             df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
             df = df.iloc[:,[0,1,12,last_col_num]] ## 每年欄位不同   

             df.columns= llist(len(df.columns))          


          if last_col_num == 17 :

             #print(df_list[8] , df_list[17]) 
             df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
             df = df.iloc[:,[0,1,8,last_col_num]] ## 每年欄位不同   

             df.columns= llist(len(df.columns))                             


          dfs_b = pd.concat([dfs_b,df],ignore_index=True) ##合併
         

       else : ### for 資產負債表Balance
        
        for i in range(1,len(tables)+1) :

          df = pd.read_html(StringIO(r.text),thousands=",")[i] ## 多表格取的[i]為dataframe
   

          df_list = df.columns
          last_col_num=len(df_list) -1 
          

          if last_col_num == 56 :
           
            if re.search("其他資產－淨額",df_list[23]) :

               ## get  12  本期稅後淨利（淨損）/ 21  基本每股盈餘（元）
             
               #print(df_list[24] , df_list[52])
               df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
               df = df.iloc[:,[0,1,24,52]] ## 每年欄位不同    

               df.columns= llist(len(df.columns))           

            else : 
               ## get  11  本期稅後淨利（淨損）/ 21  基本每股盈餘（元）
               #print(df_list[23] , df_list[52])
               df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
               df = df.iloc[:,[0,1,23,52]] ## 每年欄位不同  

               df.columns= llist(len(df.columns))                       

          
          if last_col_num == 22 :

             #print(df_list[4] , df_list[18])
             df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
             df = df.iloc[:,[0,1,4,18]] ## 每年欄位不同 

             df.columns= llist(len(df.columns))                  

          if last_col_num == 48 :

             #print(df_list[15] , df_list[44])
             df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
             df = df.iloc[:,[0,1,15,44]] ## 每年欄位不同  

             df.columns= llist(len(df.columns))            

          if last_col_num == 21 :

             #print(df_list[4] , df_list[17])
             df.columns= llist(len(df.columns)) ##重新編列columns  0 ,1 ,2
             df = df.iloc[:,[0,1,4,17]] ## 每年欄位不同   

             df.columns= llist(len(df.columns))              

          dfs_a = pd.concat([dfs_a,df],ignore_index=True) ##合併

       time.sleep(1)

      ### url_list 
      url_idx =1  

    dfs_b.columns =['code','code_name','Net_Income','Eps']
    dfs_a.columns =['code','code_name','Asset','Equity'] ## 資產/股東權益
     

    df_merge = dfs_b.merge(dfs_a,how='inner' ,on=['code','code_name']).copy()
    df_merge['ROE']= df_merge.apply(lambda x : round( x['Net_Income']/x['Equity']*100 ,2)  if pd.notnull(x['Net_Income']) else round( x['Net_Income']/x['Equity']*100 ,2) ,axis =1 )
    df_merge['ROA']= df_merge.apply(lambda x : round( x['Net_Income']/x['Asset']*100 ,2)  if pd.notnull(x['Net_Income']) else round( x['Net_Income']/x['Asset']*100 ,2) ,axis =1 )
   
    df_merge['code']=df_merge['code'].astype('str')

    ###remove EPS
    df_merge.drop(columns=['Eps'],inplace=True)
    #print(df_merge.info())
    return df_merge


def plot_Rep_Stock_Season(season,year,com_lists) :

      #print(season,year)
      dfs = pd.DataFrame()
      for idx in com_lists :


         #altas_mydoc = atlas_read_mongo_db('stock','Rep_Stock_Season_Com',{'code': idx ,'season': str(season) , "years" : str(year)  },{'_id':0})
         mydoc = read_mongo_db('stock','Rep_Stock_Season_Com',{"season": str(season) , "years" : str(year) ,"code": idx  },{"_id":0,"Net_Income":0,"Asset":0,"Equity":0})

         df = pd.DataFrame(list(mydoc))

         #print('before:',df.info())  
         if not df.empty :

           tmp_df = pd.DataFrame()
           ### add column roa &roe
           rest_the_year_df = df.iloc[:,[0,1,2,5,6,7,8]].copy()
           rest_last_year_df = df.iloc[:,[0,1,3,5,6,7,8]].copy()

           #rest_last_year_df.loc[:,'years'] = str(int(rest_last_year_df.iloc[0,4]) - 1)
           rest_last_year_df.loc[:,'years'] = str(int(rest_last_year_df.iloc[0,6]) - 1)
           #rest_last_year_df['years'] = str(int(last_years_values)-1)
           #rest_last_year_df['years'] = rest_the_year_df.apply(lambda x: str(int(x['years'])-1) if pd.notnull(x['years']) else x['years'],axis =1  )

           #new_last_years = rest_last_year_df.apply(lambda x: str(int(x['years']) -1)  if pd.notnull(x['years']) else str(x['years'] -1),axis =1  )
           #rest_last_year_df['years'] = new_last_years


           tmp_df = pd.concat([rest_last_year_df,rest_the_year_df],axis=0)


           #print('tmp_df:',tmp_df) 

           dfs = pd.concat([dfs,tmp_df],axis=0)


      #dfs['EPS_g%']= dfs.apply(lambda x: x['last_year'] if pd.notnull(x['last_year']) else x['the_year'],axis =1  )
      dfs['EPS']= dfs.apply(lambda x: x['last_year'] if pd.notnull(x['last_year']) else x['the_year'],axis =1  )
      dfs['x_code']= dfs.apply(lambda x: x['code']+'_'+ x['code_name'] if pd.notnull(x['code_name']) else  x['code']+'_'+ x['code_name'],axis =1  )
      dfs['years_s']= dfs.apply(lambda x: x['years']+'_Q'+ x['season'] if pd.notnull(x['years']) else   x['years']+'_Q'+ x['season'],axis =1  )
      dfs.dropna(axis='columns' ,inplace=True)

      #print(dfs.info()) 
      #records = dfs.iloc[:,[4,5,6]]
      records = dfs.iloc[:,[6,7,8]]

      #print(records.info()) 

      ### without ROE / ROA splot 

      for idx in range(0,len(records),10):

         idx_records = records.iloc[idx:idx+10]
         colors = ['tab:blue', 'tab:orange', 'tab:red', 'tab:green', 'tab:gray']
         idx_records.index = idx_records['x_code']
         #seaborn 0.11.2 sns.catplot(data=tips, kind="bar", x="day", y="total_bill", hue="smoker") 
         splot = sns.barplot( data=idx_records, x='x_code', y='EPS', hue='years_s',palette = ['tab:blue', 'tab:orange'])

         ## 顯示數據
         for g in splot.patches:
            splot.annotate(format(g.get_height(), '.2f'),
                      (g.get_x() + g.get_width() / 2., g.get_height()),
                      ha = 'center', va = 'center',
                      xytext = (0, 9),
                      textcoords = 'offset points')

         #plt.show()
         plt.savefig('./images/image_'+ str(idx) +'_'+ str(idx+4) +'.png' )
         ###  clear the figure 
         plt.clf()



def get_last_year_season(season):

  _sort = [("_id",-1)]

  if  season == 'undefined' : 

     mydoc_season = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{},{"_id":0,"years":1,"season":1},_sort)

  else : 

     mydoc_season = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{"season":str(season)},{"_id":0,"years":1,"season":1},_sort)

  for idx in mydoc_season :
      season = int(idx.get('season'))
      years = int(idx.get('years'))

  return years ,season 



###  5/15,8/14,11/14,3/31

today = datetime.date.today()
#today = datetime.datetime.strptime('20221101', '%Y%m%d')

if today.month >= 4 and today.month<= 6 :
  season = 1
elif  today.month >= 7 and today.month<= 9 :
  season = 2 

elif  today.month >= 10 and today.month<= 12 :
  season = 3
else :
  season = 4 
  ### month 1~3


yy = int(today.year)

last_yy =int(yy) -1


         
#### 累計EPS年增率

s_df = pd.DataFrame()


#### get com_lists

com_lists = []
com_list = []

try :

  ### got mongo data from atlas
  dictt = {}
  _columns= {"code":1,"_id":0}

  #mydoc = atlas_read_mongo_db('stock','com_list',dictt,_columns)
  mydoc = atlas_read_mongo_db('stock','com_lists',dictt,_columns)

  for idx in mydoc :
    com_lists.append(idx.get('code'))

  mydoc = atlas_read_mongo_db('stock','com_list',dictt,_columns)

  for idx in mydoc :
    com_list.append(idx.get('code'))


except :
   ### got redis data from local
   redis_lists = get_redis_data("com_lists","hkeys",'NULL','NULL') ## get  redis data
   for  idx in redis_lists : 
        #if not re.match("(\w+_p$)", idx) : 
        if  re.match(r"(\w+:code$)", idx) :
           com_lists.append(idx) 

   com_list = get_redis_data("com_list","lrange",0,-1) ## get  redis data

the_year =pd.DataFrame()


### stock_season_report

try :
        
        last_year = stock_season_report( last_yy ,season ,'undefined','undefined',com_lists)  ## last year season

        time.sleep(1)

        the_year = stock_season_report(yy ,season,'undefined','undefined',com_lists)  ## the year season
         

except :
        

        yy ,season = get_last_year_season('undefined')
       

        last_yy = yy - 1    
                

        last_year = stock_season_report( last_yy ,season  ,'undefined','undefined',com_lists)  ## last year season

        time.sleep(1)

        the_year = stock_season_report( yy ,season  ,'undefined','undefined',com_lists)  ## the year season
 



if not the_year.empty :

   s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司名稱'],suffixes=('_%s' % str(yy) , '_%s' % str(last_yy))).copy() ## merge 2021_Q3 & 2020_Q3 

   #### (累計EPS / 去年累計EPS - 1) * 100% 
   #s_df['EPS成長%'] = (( (s_df.iloc[:,2] - (s_df.iloc[:,3]).abs() )/ (s_df.iloc[:,3]).abs() ) * 100 ).round(2) ## 計算成長% (2021- 2020) /2020 * 100%
   s_df['EPS成長%'] = (( (s_df.iloc[:,2] - (s_df.iloc[:,3]) )/ (s_df.iloc[:,3]).abs() ) * 100 ).round(2) ## 計算成長% (2021- 2020) /2020 * 100%
   s_df.rename(columns={'公司代號': 'code', '公司名稱': 'code_name'}, inplace=True)


### main code  stock_season_roa_roe



try :

        match_row_roe = stock_season_roa_roe(yy, season, com_lists)

except :
 
        yy ,season = get_last_year_season('undefined')


        match_row_roe = stock_season_roa_roe( yy, season, com_lists)


match_row = s_df.merge(match_row_roe,how='left' ,on=['code','code_name'])

match_row = match_row.sort_values(by=['EPS成長%'],ascending = False,ignore_index = True).copy()



### insert to mongo 
records = match_row.copy()

records['season']= season
records['years']= yy

#records.columns =['code','code_name','the_year','last_year','EPS_g%','season','years']
records.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA','season','years']
records = records.astype({'season':'string','years':'string'})

#records = records.to_dict(orient='records')
dicct={"season" : str(season), "years" : str(yy)}

### check data is exist on  mongo 
db_records = read_mongo_db('stock','Rep_Stock_Season_Com',{"season" : str(season),"years" : str(yy)},{"_id":0})
db_records_df =  pd.DataFrame(list(db_records))


chk = records.equals(db_records_df)

if chk == False:
   
     dicct = {"season" : str(season),"years" : str(yy)}
     delete_many_mongo_db('stock','Rep_Stock_Season_Com',dicct)

     records =records.to_dict(orient='records')
     insert_many_mongo_db('stock','Rep_Stock_Season_Com',records)

#else:
#     print(match_row.to_html(escape=False)

##### season report  merge

"""
_sort=[("years",-1) ,("season" , -1)]

get_last_data = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{},{"_id":0,"years" : 1,"season":1},_sort)

for idx in get_last_data :
    s_season = str(idx.get('season'))
    s_year = str(idx.get('years'))
"""

s_season = str(season)
s_year =  str(yy)

#print(type(s_season),type(s_year))
#print(s_season,s_year)


for idx_com in [com_list, com_lists] :

     mydoc = read_mongo_db('stock','Rep_Stock_Season_Com',{'season': s_season , 'years' : s_year ,'code': {"$in" : idx_com}},{'_id':0,'Net_Income':0,'Asset':0,'Equity':0,'season':0,'years':0})

     match_row=pd.DataFrame(list(mydoc))
     #print('match_row_doc:',match_row.info())


     if not match_row.empty :
     
        match_row=match_row.sort_values(by=['EPS_g%'],ascending=False,ignore_index = True)
     
        if len(idx_com) == len(com_list): ### plot com_list only 

           try :
               #plot_Rep_Stock_Season(s_season,s_year,com_lists)
               plot_Rep_Stock_Season(s_season,s_year,match_row['code'])
     
           except BulkWriteError as e:
     
               print('plot_Rep_Stock_Season:',e.details)

        else : 
           ### del images/*.png 
           del_png.del_images()
 
     
     
        for idx in range(0,len(match_row.columns)) :
    
            match_row[match_row.columns[idx]] = match_row[match_row.columns[idx]].astype(object)

            if idx == 0 :
     
                match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<a href="https://goodinfo.tw/tw/StockBzPerformance.asp?STOCK_ID=%s&RPT_CAT=M_FYEA" target="_blank">%s</a>' %( x , x )  if int(x) >0  else  x)
     
     
            #elif idx == 4 or idx ==5 or idx == 6:
            elif idx >= 4 :
     
                match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s</font>' % x if x > 0 else  f'<font color="green">%s</font>' % x)
     
     
            else :
                if idx > 1 :
     
                    match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % str(x) if x < 0 else str(x))
     
     
        if  time.strftime("%H:%M:%S", time.localtime()) > mail_time :
     
            ## for email
            the_sst = "EPS_{}_Q{}".format(s_year,s_season)
            last_sst = "EPS_{}_Q{}".format(int(s_year)-1,s_season)
            m_today = datetime.date.today().strftime("%Y%m%d")

            match_row.rename(columns={'the_year': the_sst , 'last_year' : last_sst}, inplace=True)
            match_row = pd_table.add_columns_into_row(match_row ,20) 
            body = match_row.to_html(classes='table table-striped',escape=False)
            send_mail.send_email('{year}_Stock_Q{season}_Report_{today}' .format(year = s_year ,season=s_season,today=m_today ) ,body)
        else :
           print(match_row.to_html(escape=False))
