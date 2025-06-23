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
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager


# 改style要在改font之前
plt.style.use('seaborn')
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')





### del images/*.png
del_png.del_images()


#date_sii = datetime.date.today().strftime('%Y%m%d')
#date_otc = str(int(datetime.date.today().strftime('%Y')) - 1911)  +  datetime.date.today().strftime('/%m/%d')

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


def read_mongo_db_sort_limit(_db,_collection,dicct,_columns,_sort):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns).sort(_sort).limit(1)




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



def stock_season_report(year, season, yoy_up,yoy_low,com_lists):
    #pd.options.display.max_rows = 3000
    #pd.options.display.max_columns 
    #pd.options.display.max_colwidth = 200 

    #url_sii = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year='+ str(year-1911)  +'&season='+ str(season)
    #url_otc = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=otc&year='+ str(year-1911)  +'&season='+ str(season)    
    url_sii = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=sii&year='+ str(year-1911)  +'&season='+ str(season)
    url_otc = 'https://mopsov.twse.com.tw/mops/web/ajax_t163sb04?encodeURIComponent=1&step=1&firstin=1&off=1&isQuery=Y&TYPEK=otc&year='+ str(year-1911)  +'&season='+ str(season)    
    
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



def stock_season_roa_roe(year, season, com_lists):


    dfs =  pd.DataFrame()
    dfs_b =  pd.DataFrame()
    dfs_a =  pd.DataFrame()
    df_merge =  pd.DataFrame()

    url_list = ['https://mopsov.twse.com.tw/mops/web/ajax_t163sb04','https://mopsov.twse.com.tw/mops/web/ajax_t163sb05']


    typek=['sii','otc']
    
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_1\
  0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.5304.107 Safari/537.36'}
    """
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
       #r =  requests.post(url ,params=payload ,  headers=headers)
       r =  requests.post(url ,params=payload ,  headers={ 'user-agent': user_agent.random })
       r.encoding = 'utf8'
       soup = BeautifulSoup(r.text, 'html.parser')
       tables = soup.find_all('table',attrs={"class": "hasBorder"})
       ##print(tables)

       if url_idx == 0 :

        for i in range(1,len(tables)+1) :

          df = pd.read_html(r.text,thousands=",")[i] ## 多表格取的[i]為dataframe

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

          df = pd.read_html(r.text,thousands=",")[i] ## 多表格取的[i]為dataframe


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




def plot_Stock_Eps_Yield_PE_Season(season,year,com_list):

     dfs = pd.DataFrame()
     dfs_Investors = pd.DataFrame()
     df_cal_day_conti = cal_con_days('stock','Rep_3_Investors')  ## code     total_values    con_days
     cal_day = df_cal_day_conti.iloc[:,[2]].astype('int64').abs().max()
     last_modify = get_mongo_last_date(int(cal_day[0]))
     dfs_merge = pd.DataFrame()

     for idx in com_list :

         altas_mydoc_Season = read_mongo_db('stock','Rep_Stock_Season_Com',{'code': idx ,'season': str(season) , "years" : str(year)  },{'_id':0})
         ### get price
         altas_mydoc_Investors = read_mongo_db('stock','Rep_3_Investors',{'code': idx ,"last_modify" : {"$gte" : last_modify}},{'_id':0 ,'code':1 ,'price':1 , 'last_modify':1})


         df = pd.DataFrame(list(altas_mydoc_Season))

         df_Investors = pd.DataFrame(list(altas_mydoc_Investors))
         df_Investors.dropna()

         if not df.empty :

            rest_the_year_df = df.iloc[:,[0,1,2,5,6]].copy()

            dfs = pd.concat([dfs,rest_the_year_df],axis=0)

            dfs_Investors = pd.concat([dfs_Investors,df_Investors],axis=0)

     dfs['x_code']= dfs.apply(lambda x: x['code']+'_'+ x['code_name'] if pd.notnull(x['code_name']) else  x['code']+'_'+ x['code_name'],axis =1  )
     ### drop nan column
     dfs.dropna(axis='columns')
     ### merge   
     df_merge = pd.merge(dfs_Investors,dfs, on = ['code'],how='left')

     #df_merge['PE'] = df_merge.apply(lambda x: round(float(x['price']) / float(x['the_year']),2) if pd.notnull(x['price']) and x['the_year'] != 0 else  round(float(x['price']) / float(x['the_year']),2),axis =1  )
     df_merge['PE'] = df_merge.apply(lambda x: round(float(x['price']) / float(x['the_year']),2) if (x['price'] !=0 ) and (x['the_year'] != 0)  else  0 ,axis =1  )
     ### select column 
     df_merge = df_merge.iloc[:,[1,7,8]]
     df_merge=df_merge.sort_values(by=['PE'],ascending = True,ignore_index = True)
     #df_merge['PE'] = df_merge['PE'].fillna(0)

     ### povit table 
     #records = df_merge.pivot(index='last_modify', columns='x_code', values='PE',fill_value=0)
     records = pd.pivot_table(df_merge, index='last_modify', columns='x_code', values='PE',fill_value=0)


     for idx in range(0,len(records.columns),5):

          idx_records = records.iloc[ : , idx:idx+5 ]

          ax = idx_records.plot(y = idx_records.columns)
          ## 顯示數據
          for line, name in zip(ax.lines, idx_records.columns):
             y = line.get_ydata()[-1]
             ax.annotate(name, xy=(1,y), xytext=(4,0), color=line.get_color(),
                    xycoords = ax.get_yaxis_transform(), textcoords="offset points",
                    size=10, va="center")

          plt.title("Stock_PE_Rate")
          plt.xlabel("Date")
          plt.ylabel("PE")
          #plt.show()
          plt.savefig('./images/image_'+ str(idx) +'_'+ str(idx+4) +'.png' )
          plt.clf()




def Reasonable_Price(limit_y) :

   _dicct = [ {"$group" : { "_id" :"$years"}  } , {"$limit" : limit_y },{"$sort" :{"_id" : 1}}, {"$limit" : 1} ]



   last_5years_doc = read_aggregate_mongo_db('stock','Rep_Stock_dividend_Com',_dicct ) 

   last_5years = list(last_5years_doc)[0].get("_id")


   _dicct = [{"$match" :  { "years"  : { "$gte" : last_5years } } }
           ,{ "$group" : { "_id" : "$code"  , "cash_sum" : { "$sum": { "$cond": [{ "$eq":["$cash_dividend" , float("NaN") ]}, 0, "$cash_dividend"]} }  ,
                     "stock_sum" : { "$sum": { "$cond": [{ "$eq":["$stock_dividend" , float("NaN") ]}, 0, "$stock_dividend"]} }  }    }
           ,{ "$project" : { "code" : "$_id"  , "avg" : { "$divide": [ "$cash_sum", limit_y ] } 
                           #,"stock_avg" : { "$divide": [ "$stock_sum", limit_y ] } 
                           ,"_id" :0  } } ]


   mydoc_stock_season = read_aggregate_mongo_db('stock','Rep_Stock_dividend_Com',_dicct)

   df = pd.DataFrame(list(mydoc_stock_season))
   ### code	avg	stock_avg
   return df


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



def compare_row(row):
  
  rows=[]
  for idx in range(1,len(row)) :
    
    #print(row.iloc[idx] ,row.iloc[0] )
    if (float(row.iloc[idx]) > float(row.iloc[0])) :
      
      if  idx==1 :
        p_stype= f'<p style="background-color:Lime;">%s</p>'
                  
      elif idx==2 :  
        p_stype= f'<p style="background-color:Aqua;">%s</p>'
     
      elif idx==3 :  
        p_stype= f'<p style="background-color:Tomato;">%s</p>'

      
      idx_p_stype =  p_stype % row.iloc[0] 
      rows.append(idx_p_stype)
     
    
    elif idx==3 and rows ==[] : 
       idx_p_stype = row.iloc[0]
       rows.append(idx_p_stype)

  
    if not rows ==[] :
      break

  return rows[0]



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


###  season cal 
if season == 4  :
     yy = today.year -1
     last_yy = today.year -2
else :
### last season replort
      yy = today.year
      last_yy = today.year -1



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
        if  re.match("(\w+:code$)", idx) : 
           com_lists.append(idx)


   com_list = get_redis_data("com_list","lrange",0,-1) ## get  redis data


##20250103
##yy=2024,last_yy=2023,season=4


### get season data from mongo

mydoc_code_lists=[]

#print('yy:',yy , 'season:',season)

### compare season report & com_lists  count

mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0,"Net_Income":0,"Asset":0,"Equity":0})

if len(list(mydoc_season)) == 0  and season >1:
    
    ### get last_year_season
    yy ,season = get_last_year_season(season)
    last_yy = yy  - 1

    mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0,"Net_Income":0,"Asset":0,"Equity":0})


for idx in mydoc_season :
   mydoc_code_lists.append(idx.get('code'))


mydoc_code_lists.sort()
com_lists.sort()

### compare mydoc_code_lists & com_lists
the_year = pd.DataFrame()
last_year = pd.DataFrame()


### not get data again 

### Update season report
if not mydoc_code_lists == com_lists :
   #print('614_yy:',yy , 'season:',season)

   try :

        last_year = stock_season_report( last_yy ,season ,'undefined','undefined',com_lists)  ## last year season

        time.sleep(1)

        the_year = stock_season_report(yy ,season,'undefined','undefined',com_lists)  ## the year season
 

   except :  ### if no data  get last season
          """
          #read_mongo_db_limit(_db,_collection,dicct,_columns,sort_col,limit_cnt)
          _sort=[("years",-1) ,("season" , -1)]
  
          #get_last_data = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{"season":"4"},{"_id":0,"years" : 1,"season":1},_sort)
          get_last_data = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{},{"_id":0,"years" : 1,"season":1},_sort)
  
          for idx in get_last_data :
              season = int(idx.get('season'))
              yy = int(idx.get('years'))
          """
          yy ,season = get_last_year_season(season)
  

          last_yy = yy  - 1

          last_year = stock_season_report( last_yy ,season ,'undefined','undefined',com_lists)  ## last year season
          
          time.sleep(1)
          
          the_year = stock_season_report( yy ,season ,'undefined','undefined',com_lists)  ## the year season 

  # print('650_yy:',yy , 'season:',season)
   #s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司簡稱'],suffixes=('_%s' % str(today.year) , '_%s' % str(today.year -1))).copy() ## merge 2021_Q3 & 2020_Q3 
   s_df = the_year.merge(last_year, how='inner', on=['公司代號','公司名稱'],suffixes=('_%s' % str(yy) , '_%s' % str(last_yy))).copy() ## merge 2021_Q3 & 2020_Q3 

   #### (累計EPS / 去年累計EPS - 1) * 100% 

   s_df['EPS_g%'] = (( (s_df.iloc[:,2] - (s_df.iloc[:,3]))/ (s_df.iloc[:,3]).abs() ) * 100 ).round(2) ## 計算成長% (2021- 2020) /2020 * 100%
   s_df.rename(columns={'公司代號': 'code', '公司名稱': 'code_name'}, inplace=True)



   ### main code  stock_season_roa_roe

   try :

        match_row_roe = stock_season_roa_roe(yy, season, com_lists)

   except :
        """
        #read_mongo_db_limit(_db,_collection,dicct,_columns,sort_col,limit_cnt)
        _sort=[("years",-1) ,("season" , -1)]

        get_last_data = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{},{"_id":0,"years" : 1,"season":1},_sort)

        for idx in get_last_data :
            season = int(idx.get('season'))
            yy = int(idx.get('years'))
        """
        yy ,season = get_last_year_season(season)
  

        match_row_roe = stock_season_roa_roe( yy, season, com_lists)

   
   #print('688_yy:',yy , 'season:',season)
   match_row = s_df.merge(match_row_roe,how='left' ,on=['code','code_name'])

   match_row = match_row.sort_values(by=['EPS_g%'],ascending = False,ignore_index = True).copy()

   #match_row = s_df.sort_values(by=['EPS_g%'],ascending = False,ignore_index = True).copy()

   """
   if not match_row.empty  :

   """
   """
      records = match_row.copy()
      #records.columns =["code","code_name","the_year", "last_year" , "EPS_g%"]
      records.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA']

      records["season"] = str(season)
      records["years"]  = str(yy)
      
      #print('records:',records.info())
      ### rename for mongodb
      #records.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA','season','years']
      records =records.to_dict(orient='records')
      ## coding  bec not include ROE ROA
      del_filter = {"years":str(yy),"season":str(season)}
      delete_many_mongo_db('stock','Rep_Stock_Season_Com',del_filter)
      insert_many_mongo_db('stock','Rep_Stock_Season_Com',records)
      
      time.sleep(1)
   """
   """
      pass
   ### data empty & get last season report  
   else : 
      
   
      mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0,"Net_Income":0,"Asset":0,"Equity":0})

      match_row= pd.DataFrame(list(mydoc_season))

      #print('else_663:',yy,last_yy,season) 
   ###  (mydoc_season) == (com_lists)
   """
else :   
  
   yy ,season = get_last_year_season(season)

   last_yy = yy  - 1

   mydoc_season = read_mongo_db('stock','Rep_Stock_Season_Com',{"years":str(yy),"season":str(season)},{"season":0,"years":0,"_id":0,"Net_Income":0,"Asset":0,"Equity":0})

   match_row= pd.DataFrame(list(mydoc_season))
   

#print('744_yy:',yy , 'season:',season)

### get last price
last_modify=get_mongo_last_date(1)
#20230807
date_sii = last_modify
date_otc = str( int(datetime.datetime.strptime(last_modify, '%Y%m%d').date().strftime('%Y')) - 1911)  + datetime.datetime.strptime(last_modify, '%Y%m%d').date().strftime('/%m/%d')


### get  data from DB

mydoc = Rep_price(date_sii,date_otc,com_lists)



#match_row.rename(columns={'公司代號': 'code', '公司名稱': 'code_name'}, inplace=True)
### merge price
match_row_doc = pd.merge(match_row,mydoc, on =['code']) ##dataframe join by column

match_row_doc.dropna()


#print("match_row_doc_info:",match_row_doc.info())
match_row_doc = match_row_doc.astype({match_row_doc.columns[2]:'float',match_row_doc.columns[5]:'float'})

### 60% Dividend for yield
div_per = 0.6


##expect div = EPS * 60%
match_row_doc['yield_60%']= round(round((match_row_doc.iloc[:,2]/match_row_doc['price']* div_per),4)*100 ,2)
match_row_doc['PE']= round(round(match_row_doc['price']/match_row_doc.iloc[:,2],4),2)


#match_row_doc = pd.merge(match_row_doc,avgg, on =['code']) ##dataframe join by column



## PE & yield filter


for idx_com in [com_list, com_lists] :

    match_row = pd.DataFrame()

    if len(idx_com) == len(com_list) :

         match_row = match_row_doc[match_row_doc["code"].isin(com_list)]
         plot_Stock_Eps_Yield_PE_Season(season,yy,com_list)

    	 
    else : 	 

         ### del images/*.png
         del_png.del_images()
 
         if datetime.datetime.today().isoweekday() == 5 :  ## show all company on friday
       
             match_row = match_row_doc.copy()
         
       
         else :
                
             match_row_filter = match_row_doc[(match_row_doc['PE']<35) & (match_row_doc['yield_60%']>0)] .copy()
       
             ### avoid empty of filter
             if   match_row_filter.empty :
       
                  match_row = match_row_doc.copy()
             else  :
                  match_row = match_row_filter.copy()    	 
          
    ## columns order for mail                                                                                                                                                                                                        
    """                                                                                                                                                                                                                           
    the_sst = "累計盈餘{}_Q{}".format(yy,season)                                                                                                                                                                                      
    last_sst = "累計盈餘{}_Q{}".format(yy-1,season)                                                                                                                                                                                   
    """                                                                                                                                                                                                                           
    the_sst = "EPS_{}_Q{}".format(yy,season)                                                                                                                                                                                      
    last_sst = "EPS_{}_Q{}".format(yy-1,season)                                                                                                                                                                                   
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
    #print(match_row.info())                                                                                                                                                                                                                         
    ## defiend columns                                                                                                                                                                                                            
    match_row.columns =['code','code_name','the_year','last_year','EPS_g%','Net_Income','Asset','Equity','RoE','RoA','price','yield_60%','PE']                                                                                    
                                                                                                                                                                                                                             
    ## filter columns                                                                                                                                                                                                             
    match_row =match_row[['code','code_name','the_year','last_year','EPS_g%','price','yield_60%','PE','RoE','RoA']]                                                                                                               
                                                                                                                                                                                                                             
    ## for email                                                                                                                                                                                                                  
    match_row.rename(columns={'the_year': the_sst , 'last_year' : last_sst}, inplace=True)                                                                                                                                        
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
    ## divieded at seson 4                                                                                                                                                                                                        
    if today.month >=1 :                                                                                                                                                                                                          
                                                                                                                                                                                                                             
       df_merge=pd.DataFrame()                                                                                                                                                                                                    
                                                                                                                                                                                                                             
       """                                                                                                                                                                                                                        
       dict_stock_season_div={'years':str(today.year) }                                                                                                                                                                           
                                                                                                                                                                                                                             
       columns_stock_season_div={'_id':0,'years':0,'code_name':0}                                                                                                                                                                 
                                                                                                                                                                                                                             
       mydoc_stock_season_diviended = read_mongo_db('stock','Rep_Stock_dividend_Com',dict_stock_season_div ,columns_stock_season_div)                                                                                             
       """                                                                                                                                                                                                                        
       _dicct = [{"$match" :  { "years"  : { "$eq" : str(today.year) } } }                                                                                                                                                        
                   ,{ "$sort" : {"dividend_date" : 1 , "dividend_stock_date" :1}}                                                                                                                                                 
               ,{ "$group" : { "_id" : "$code"  , "cash_sum" : { "$sum": { "$cond": [{ "$eq":["$cash_dividend" , float("NaN") ]}, 0, "$cash_dividend"]} }  ,                                                                      
                         "stock_sum" : { "$sum": { "$cond": [{ "$eq":["$stock_dividend" , float("NaN") ]}, 0, "$stock_dividend"]} }  ,                                                                                            
                            "last_dividend_date" : {"$last" :"$dividend_date"} , "last_dividend_stock_date" : {"$last" :"$dividend_stock_date"}                                                                                   
                             }    }                                                                                                                                                                                               
               ,{ "$project" : { "code" : "$_id"  ,"cash_dividend" : "$cash_sum" , "dividend_date" : "$last_dividend_date" ,"stock_dividend" : "$stock_sum" , "dividend_stock_date" : "$last_dividend_stock_date"  ,"_id" :0  } }]
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
       mydoc_stock_season_diviended  = read_aggregate_mongo_db('stock','Rep_Stock_dividend_Com',_dicct )                                                                                                                          
                                                                                                                                                                                                                             
       df =  match_row.copy()                                                                                                                                                                                                     
                                                                                                                                                                                                                             
       dfs = pd.DataFrame(list(mydoc_stock_season_diviended))                                                                                                                                                                     
                                                                                                                                                                                                                             
       dfs =  dfs.astype({'cash_dividend':'float','stock_dividend':'float'})                                                                                                                                                      
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
       dfs['cash_dividend'] = dfs['cash_dividend'].round(2)                                                                                                                                                                       
       dfs['stock_dividend'] = dfs['stock_dividend'].round(2)                                                                                                                                                                     
       df_merge = df.merge(dfs,how='left', on='code')                                                                                                                                                                             
       df_merge.rename(columns={'cash_dividend':'cash_div','dividend_date': 'div_date','stock_dividend':'stock_div','dividend_stock_date':'div_stock_date'},inplace=True)                                                         
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
       df_merge['yield%'] = df_merge.apply(lambda x: round(x['cash_div']/x['price']*100,2) if (pd.notnull(x['cash_div'])) else x['cash_div'] ,axis=1)                                                                             
       match_row = df_merge[['code','code_name',the_sst,last_sst,'EPS_g%','price','yield%','PE','RoE','RoA','cash_div','div_date','stock_div','div_stock_date']]                                                                  
                                                                                                                                                                                                                             
       ## ref buy price level , 5years avg                                                                                                                                                                                        
                                                                                                                                                                                                                             
       r_price = Reasonable_Price(5)                                                                                                                                                                                              
       r_price["cheap"] = round(r_price['avg']/0.07,2)                                                                                                                                                                            
       r_price["seasonable"] = round(r_price['avg']/0.05,2)                                                                                                                                                                       
       r_price["expensive"] = round(r_price['avg']/0.03,2)                                                                                                                                                                        
       #r_price.rename(columns={"_id":"code"},inplace=True)                                                                                                                                                                       
       ### remove avg field                                                                                                                                                                                                       
       r_price = r_price.iloc[:,[0,2,3,4]]                                                                                                                                                                                        
                                                                                                                                                                                                                             
       match_row = match_row.merge(r_price,how='left', on='code')                                                                                                                                                                 
                                                                                                                                                                                                                             
       sort_column='yield%'                                                                                                                                                                                                       
       sort_column_1='PE'

       assc = False                                                                                                                                                                                                               
       range_s = 0                                                                                                                                                                                                                
       range_d= 17                                                                                                                                                                                                                

       check_sort_column=['cheap_check','seasonable_check',sort_column,sort_column_1,'cash_div']                                                                                                                               
       check_assc = [False,False,False,assc,False]                                                                                                                                                                             
       rename_col = {'cheap': 'cheap↓','seasonable':'seasonable↓',sort_column : sort_column+'↓',sort_column_1:sort_column_1+'↑','cash_div' : 'cash_div↓'}  
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
                                                                                                                                                                                                                             
    else :                                                                                                                                                                                                                        
                                                                                                                                                                                                                             
       sort_column='PE'                                                                                                                                                                                                           
       assc = True                                                                                                                                                                                                                
       range_s = 0                                                                                                                                                                                                                
       range_d= 10                                                                                                                                                                                                                
                                                                                                                                                                                                                             
       check_sort_column=[sort_column]                                                                                                                                                                                            
       check_assc = assc                                                                                                                                                                                                          
                                                                                                                                                                                                                             
       rename_col = {'PE':'PE↑'}                                                                                                                                                                                                  

    """                                                                                                                                                                                                                          
    for idx in check_sort_column :                                                                                                                                                                                             
        match_row[idx] = match_row[idx].astype(float)                                                                                                                                                                          
    """
    match_row['PE'] = match_row['PE'].astype(float)     
                                                                                                                                                                                                                             
    	
    compare_data_p_stype = pd.DataFrame()
    ### for mail thml
    for idx in list(range(range_s,range_d)) :
        ## code
        if idx == 0 :
           match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<a href="https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID=%s" target="_blank">%s</a>' %( x , x )  if int(x) >0  else  x)
    
        elif idx == 4 :
           match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x  if x <0  else f'<font color="red">+%s</font>' % x if x > 0 else 0)
        ## 'yield_60%' , 'yield%'
        elif idx == 6 :
    
           match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">%s</font>' % x  if x >6  else  f'<font color="green">%s</font>' % x  if pd.isnull(x) else str(x))
    
        #elif idx >= 11 and today.month >=3 :
        #elif idx >= 11 and idx <=13 and today.month >=3 :
        elif idx >= 11 and idx <=13  :
    
             ##stock_div 
             if idx == 12 :
                match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="red">+%s</font>' % x  if x >0  else str(x))
    
             ## div_date , div_stock_date
             else :
    
                  continue
    
        #elif idx >= 14 and idx <= 15 and today.month >=3:
        #elif idx >= 14 and idx <= 16 and today.month >=3:
        elif idx >= 14 and idx <= 16 :
    
             ## price compare / cheap ,seasonable,expensive
             compare_data = match_row.iloc[:,[5,10,idx,11,13]].copy()
             compare_name = list(compare_data.columns)
             #print(compare_data.info())
             #<p style="background-color:Tomato;">Lorem ipsum...</p>
             if idx == 14 :
                p_stype= f'<p style="background-color:Lime;">%s</p>'
                ##match_row.iloc[:,17]
                match_row['cheap_check'] =  compare_data.apply(lambda  x: 1   if float(x[compare_name[0]]) <= float(x[compare_name[2]])   else 0 ,axis=1)
    
             elif idx == 15 :
                p_stype= f'<p style="background-color:Aqua;">%s</p>'
                ##match_row.iloc[:,18]
                match_row['seasonable_check'] =  compare_data.apply(lambda  x: 1   if float(x[compare_name[0]]) <= float(x[compare_name[2]])   else 0 ,axis=1)
    
             elif idx == 16 :
    
               p_stype= f'<p style="background-color:Tomato;">%s</p>'
    
    

             compare_data_p_stype = pd.concat([compare_data_p_stype,compare_data.iloc[:,2]] ,axis=1)
   
             match_row.iloc[:,idx] =  compare_data.apply(lambda  x: p_stype  % x[compare_name[2]]  if float(x[compare_name[0]]) <= float(x[compare_name[2]])   else x[compare_name[2]],axis=1)
    
             ### color for price / cash_div
             #if idx == 15 : 
             if idx == 16 :
    
    
                match_row.iloc[:,10] =  compare_data.apply(lambda  x: f'<p style="background-color:LightSalmon;">%s</p>' % x[compare_name[1]]  if float(x[compare_name[0]]) <= float(x[compare_name[2]])   else x[compare_name[1]],axis=1)
              

                #compare_data_p_stype = match_row.iloc[:,[5,14,15,16]].copy()
                #compare_name_p_stype = list(compare_data_p_stype.columns) 
                compare_data_p_stype = pd.concat([compare_data_p_stype,match_row.iloc[:,5]] ,axis=1)  ## adding column[price]
                
                compare_data_p_stype = compare_data_p_stype.iloc[:,[3,0,1,2]] ### sort column for compare_row

                match_row.iloc[:,5] =  compare_data_p_stype.apply(compare_row,axis=1)

    
                ## div_date /div_stock_date            
                match_row.iloc[:,11] =  compare_data.apply(lambda  x: f'<del style="color:#aaa;">%s</del>' % x[compare_name[3]]  if pd.notnull(x[compare_name[3]])  and datetime.datetime.strptime(str(datetime.date.today().year) + '/' +x[compare_name[3]] ,  "%Y/%m/%d") <= datetime.datetime.now()   else x[compare_name[3]],axis=1)
    
                match_row.iloc[:,13] =  compare_data.apply(lambda  x: f'<del style="color:#aaa;">%s</del>' % x[compare_name[4]]  if pd.notnull(x[compare_name[4]])  and datetime.datetime.strptime(str(datetime.date.today().year) + '/' +x[compare_name[4]] ,  "%Y/%m/%d") <= datetime.datetime.now()   else x[compare_name[4]],axis=1)
    
    
    
        else :
    
             if idx > 1:
    
                match_row.iloc[:,idx] = match_row.iloc[:,idx].apply(lambda  x: f'<font color="green">%s</font>' % x if x < 0 else str(x))
    
    
    #check_sort_column=['cheap_check','seasonable_check',sort_column,'cash_div']
    #check_assc = [False,False,False,False]
    
    
    #match_row=match_row.sort_values(by=check_sort_column,ascending = check_assc ,ignore_index = True)
    #match_row=match_row.iloc[:,0:17].copy()
    #match_row=match_row.iloc[:,range_s:range_d].copy()
   
    """ 
    if range_d>=17 :
    
       match_row=match_row.sort_values(by=check_sort_column,ascending = check_assc ,ignore_index = True)
       match_row=match_row.iloc[:,0:17].copy()
    """

    match_row=match_row.sort_values(by=check_sort_column,ascending = check_assc ,ignore_index = True)
    match_row=match_row.iloc[:,range_s:range_d].copy()
    
    match_row =  match_row.rename(columns=rename_col)
    
    
    if time.strftime("%H:%M:%S", time.localtime()) > mail_time :
        #match_row.to_html('hot_news_link.log')
        if not match_row.empty :
           body = match_row.to_html(classes='table table-striped',escape=False)
    
           #send_mail.send_email('Stock_Eps_Yield_PE_{today}_com'.format(today=to_date),body)
           send_mail.send_email('Stock_Eps_Yield_PE_{today}_com_Q{s}'.format(today=date_sii,s=season),body)
    else :
        #print(match_row.to_string(index=False))
        #print(match_row.to_html(escape=False))
        print(match_row.head(100))

