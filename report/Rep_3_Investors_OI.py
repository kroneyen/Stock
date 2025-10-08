#! /usr/local/python-3.9.2/bin/python3.9
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
import redis
import time
#import line_notify
import requests
import send_mail
from IPython.display import display, clear_output
#from io import StringIO
from pymongo import MongoClient
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.font_manager import fontManager
#from fake_useragent import UserAgent
import del_png
import random
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import tg_bot 
import urllib3
### disable  certificate verification
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# 改style要在改font之前
plt.style.use("seaborn-v0_8")
fontManager.addfont('images/TaipeiSansTCBeta-Regular.ttf')
mpl.rc('font', family='Taipei Sans TC Beta')



def Check_sys_date(i_start_date , i_today) :
    import sys

    o_start_date = None
    o_today = None
    try :
          i_start_date = sys.argv[1]
          i_today = sys.argv[2]

    except :
           print('please execute python with sys_argv')
           sys.exit(1)


    if i_start_date == '0' :

          o_start_date = datetime.today()

    else :

           o_start_date = datetime.strptime(i_start_date, '%Y%m%d')

    if i_today  == '0' :

           o_today = datetime.today()

    else :

           o_today =  datetime.strptime(i_today, '%Y%m%d')

    return o_start_date ,o_today




match_row = pd.DataFrame()



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


def read_aggregate_mongo(_db,_collection,dicct):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.aggregate(dicct)


def send_line_notify(token,msg):

    requests.post(
    url='https://notify-api.line.me/api/notify',
    headers={"Authorization": "Bearer " + token},
    data={'message': msg}
    )




def send_tg_bot_msg(token,chat_id,msg):

  #url = f"https://api.telegram.org/bot{'+token+'}/sendMessage?chat_id={'+chat_id}&text={msg}&parse_mode=HTML"
  url = "https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={msg}&parse_mode=HTML".format(token = token ,chat_id=chat_id,msg=msg)
  requests.get(url)



def send_tg_bot_photo(token,chat_id,msg,img_url):
 
  #print(file_path)

  #url = f"https://api.telegram.org/bot{'+token+'}/sendMessage?chat_id={'+chat_id}&text={msg}&parse_mode=HTML"
  #files={'photo': open(file_path, 'rb')}
  #files = open(file_path,'rb') 
  #url = "https://api.telegram.org/bot{token}/sendPhoto?chat_id={chat_id}&caption={msg}&files={files}".format(token = token ,chat_id=chat_id,msg=msg,files=files)
  #img_url ='https://ibb.co/0q1cZWb'
  #url = "https://api.telegram.org/bot{token}/sendPhoto?chat_id={chat_id}&caption={msg}&photo={img_url}".format(token = token ,chat_id=chat_id,msg=msg,img_url=img_url)
  url = f"https://api.telegram.org/bot{token}/sendPhoto?chat_id={chat_id}&caption={msg}".format(token = token ,chat_id=chat_id,msg=msg)
  #print(url)
  requests.post(url,files={'photo':img_url})




def Rep_3_Investors_OI(date):

    user_agent = UserAgent()
  
    #print(type(date),date) 
    ### str > datetime obj > str format
    q_date=  date.strftime('%Y/%m/%d')
    #prin(type(q_date))
    #url = 'https://www.taifex.com.tw/cht/3/futContractsDate?queryDate={}%2F{}%2F{}'.format(date.year, date.month,date.day)
    url = 'https://www.taifex.com.tw/cht/3/futContractsDate?queryDate='+ q_date
    r = requests.get(url ,  headers={ 'user-agent': user_agent.random },verify=False)
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.text, 'html.parser')
    else:
        print('connection error')

    try:
        table = soup.find('table', class_='table_f')
        trs = table.find_all('tr')
    except AttributeError:
        return {}

    rows = trs[3:]
    data = {}

    try :

       for row in rows:
        tds = row.find_all('td')
        cells = [td.text.strip() for td in tds]

        if cells[0] == '期貨小計':
            break

        if len(cells) == 15:
            product = cells[1]
            row_data = cells[1:]
        else:
            row_data = [product] + cells

        converted = [int(d.replace(',', '')) for d in row_data[2:]]
        row_data = row_data[:2] + converted

        headers = ['商品', '身份別', '交易多方口數', '交易多方金額', '交易空方口數', '交易空方金額', '交易多空淨口數', '交易多空淨額',
                   '未平倉多方口數', '未平倉多方金額', '未平倉空方口數', '未平倉空方金額', '未平倉淨口數', '未平倉多空淨額']
    
        product = row_data[0]
        who = row_data[1]
        contents = {headers[i]: row_data[i] for i in range(2, len(headers))}
    
  
        if product not in data:
            data[product] = {who: contents}
        else:
            data[product][who] = contents

    except :
      pass 
    
    ### insert mongoDB

    if bool(data) : 
      plot_date =date.strftime('%Y%m%d')                                             
      OI_data_value = data['臺股期貨']['外資']['未平倉淨口數'] 
      records = [{'date' : plot_date,'open_interest': OI_data_value }] ##for insert_many
      delete_many_mongo_db('stock','Rep_3_Investors_OI',{'date':plot_date})
      insert_many_mongo_db('stock','Rep_3_Investors_OI',records)

    time.sleep(random.randrange(1, 3, 1))




def polt_3_Investors_OI(date,today_date): 
 
   #last_modify =start_date

   ### del images/*.png
   del_png.del_images()

   mydoc = read_mongo_db('stock','Rep_3_Investors_OI',{'date': {'$gte' : date}},{'_id':0})
 
   df = pd.DataFrame(list(mydoc))
   
   ### for plot  
   plot_df = pd.DataFrame(data = df['open_interest'].values,columns=['3_Investors_OI'] , index=df['date'])

   ### plot settng                                                                                            
   ax = plot_df.plot(y = plot_df.columns )                                    

   ### 顯示數據                                                                                     
   for line, name in zip(ax.lines, plot_df.columns):                                   
      y = line.get_ydata()[-1]                                                                 
      ax.annotate(name, xy=(1,y), xytext=(4,0), color=line.get_color(),                        
         xycoords = ax.get_yaxis_transform(), textcoords="offset points",                    
         size=10, va="center")                                                               
                                                                                             
   plt.savefig('./images/image_Rep_3_Investors_OI_'+ today_date +'.png' )
   plt.clf()

   return df


def get_mongo_last_date(cal_day):
 ### mongo query for last ? days
 dictt_set = [ {"$group": { "_id" : { "$toInt" : "$date" } }},{"$sort" : {"_id" :-1}} , { "$limit" : cal_day},{"$sort" : {"_id" :1}} , { "$limit" :1}]

 ### mongo dict data
 set_doc =  read_aggregate_mongo('stock','Rep_3_Investors_OI',dictt_set)

 ### for lists  get cal date 
 for idx in set_doc:

  idx_date = idx.get("_id")

 return str(idx_date)


def match_row_5 ( get_updatetime ,match_row,extend ) :
          
          """ 
          line_key_list =[]
          tg_key_list=[]
          tg_chat_id=[]

          line_key_list.append( get_redis_data('line_key_hset','hget','Stock_YoY','NULL')) ## for exchange_rate (Stock_YoY/Stock/rss_google/Exchange_Rate)
          tg_key_list.append(get_redis_data('tg_bot_hset','hget','@stock_broadcast_2024bot','NULL')) ## for tg of signle
          tg_chat_id.append(get_redis_data('tg_chat_id','hget','stock_broadcast','NULL')) ## for tg of signle
          """

          for match_row_index in range(0,len(match_row),5) :
              msg = get_updatetime + extend  + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
              tg_msg ="【3_Investors_OI】"+  msg

 
          ### for tg_msg
          tg_bot.send_tg_bot_msg(tg_msg)
          time.sleep(random.randrange(1, 3, 1))

          ### for  tg_photo
          tg_caption="【3_Investors_OI】" + get_updatetime
          tg_file='./images/image_Rep_3_Investors_OI_' + get_updatetime + '.png'
          tg_bot.send_tg_bot_photo(tg_caption,tg_file) 

    
          """
              ### for multiple line group
              for line_key in  line_key_list : ## 
                  send_line_notify(line_key, msg)
                  time.sleep(random.randrange(1, 3, 1))

              for tg_key in  tg_key_list : ## 
                  #send_tg_bot_msg(tg_key,tg_chat_id[0],tg_msg)
                  tg_bot.send_tg_bot_msg(tg_msg)
                  time.sleep(random.randrange(1, 3, 1))
                  
                  tg_caption="【3_Investors_OI】" + get_updatetime
                  #tg_file='./images/image_Rep_3_Investors_OI_' + get_updatetime + '.png'
                  #tg_file=open('./images/image_Rep_3_Investors_OI_20250515.png', 'rb')
                  #tg_file=open('./images/image_Rep_3_Investors_OI_' + get_updatetime + '.png', 'rb')
                  tg_file='./images/image_Rep_3_Investors_OI_' + get_updatetime + '.png'
                  #send_tg_bot_photo(tg_key,tg_chat_id[0],tg_caption,tg_file)  
                  #print('tg_caption:',tg_caption , 'tg_file:',tg_file)
                  tg_bot.send_tg_bot_photo(tg_caption,tg_file) 
          """     


### Main Code 


start_date = None

today = None

start_date,today  = Check_sys_date(None,None)


mail_date=today.strftime('%Y-%m-%d')

#start_date =  datetime.today()- timedelta(days=30)
#start_date =  today

### get data into mongo
while start_date <= today : 

  Rep_3_Investors_OI(start_date)

  start_date += timedelta(days=1)   


### for line & TG  of 5 days 
 

ddate = get_mongo_last_date(5)
today_date = get_mongo_last_date(1)

match_row = polt_3_Investors_OI(ddate,today_date)

match_row = match_row[::-1] ### for reverse data

#match_row_5("\n", match_row,"\n ")
match_row_5(today_date , match_row,"\n ")



### plot of 30 days

if datetime.today().isoweekday() == 5 :

     ### del images/*.png
     del_png.del_images()

     ### get last day 
     ddate = get_mongo_last_date(30)

     mail_match_row = polt_3_Investors_OI(ddate,today_date)

     match_row = match_row[::-1] ### for reverse data

     body = mail_match_row.to_html(classes='table table-striped',escape=False)

     send_mail.send_email('Rep_3_Investors_OI_{today}'.format(today=mail_date),body)

else  :
   #print("%s target_currency is not match buy_price" % get_updatetime)
   print(match_row)

