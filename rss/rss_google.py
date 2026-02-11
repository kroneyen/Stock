import pandas as pd
import datetime
import time
import requests
#from bs4 import BeautifulSoup
import json
import re
import random
import lxml
import xml.etree.ElementTree as ET
import redis
from pymongo import MongoClient
from collections import Counter


##### global parameter
pd.options.display.max_rows = 200
pd.options.display.max_columns = 200
pd.options.display.max_colwidth = 500

today = datetime.date.today()

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.StrictRedis(connection_pool=pool)

#notify_time = "23:00:00"


###local mongodb
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )



def insert_mongo_db(_db,_collection,_values):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    collection.insert(_values)




def get_redis_data(_key,_type,_field_1,_field_2):

    if _type == "lrange" :
       _list = r.lrange(_key,_field_1,_field_2)

    elif _type == "hget" :
       _list = r.hget(_key,_field_1)

    return _list





def time_change(pubDate ,ttype):
   format_from = "%a, %d %b %Y %H:%M:%S GMT" 
   gmt_8 = datetime.datetime.strptime(pubDate,format_from) + datetime.timedelta(hours=8)
   #format_to  = "%Y%m%d %H:%M:%S"
   if ttype == "tt" :
   
     format_to  = "%H:%M:%S"
   else :
     format_to  = "%Y%m%d" 
   
   return gmt_8.strftime(format_to)



def root_data(item): 
   title = item.find('title').text
   link = item.find('link').text
   pubDate = time_change(item.find('pubDate').text,'dd')
   pubTime = time_change(item.find('pubDate').text,'tt')
   #description = item.find('description').text
   source = item.find('source').get('url').replace("https://","")
  
   return ([title,link,pubDate,pubTime,source])



def reurl_API(link_list,_key):

     reurl_link_list = []
     reurl_api_key = "".join(get_redis_data('short_url_key','hget',_key,'NULL'))  ## list_to_str


     if _key  ==  "reurl_key" :


        s_reurl = 'https://api.reurl.cc/shorten'
        s_header = {"Content-Type": "application/json" , "reurl-api-key": reurl_api_key}

        for link in link_list :
            s_data = {"url": link}

            """
            r = requests.post(s_reurl, json= s_data , headers=s_header ).json()

            try :
                rerul_link = r["short_url"]
            except :
                rerul_link = link

            """
            try : 
                 r = requests.post(s_reurl, json= s_data , headers=s_header ).json()
                 rerul_link = r["short_url"]

            except : 
                 
                   time.sleep(round(random.uniform(0.5, 1.0), 20))
                   
                   """
                   reurl_api_key = "".join(get_redis_data('short_url_key','hget','reurl_hotmail_key','NULL'))  ## list_to_str
                   s_header = {"Content-Type": "application/json" , "reurl-api-key": reurl_api_key}

                   try : 
                         
                         r = requests.post(s_reurl, json= s_data , headers=s_header ).json()
                         rerul_link = r["short_url"]
                   """

                   reurl_api_key = "".join(get_redis_data('short_url_key','hget','ssur_key','NULL'))  ## list_to_str
                   s_reurl = 'https://ssur.cc/api.php?'
                   s_data = {"format": "json" , "appkey": reurl_api_key ,"longurl" : link}

                   r = requests.post(s_reurl, data= s_data  ).json()

                   try :
                       rerul_link = r["ae_url"]


                   except :
                         rerul_link = link


            reurl_link_list.append(rerul_link)


            time.sleep(round(random.uniform(0.5, 1.0), 20))


     elif   _key  ==  "ssur_key" :


        for link in link_list :

            s_reurl = 'https://ssur.cc/api.php?'
            s_data = {"format": "json" , "appkey": reurl_api_key ,"longurl" : link}

            r = requests.post(s_reurl, data= s_data  ).json()

            try :
                  rerul_link = r["ae_url"]
            except :
                  rerul_link = link

            reurl_link_list.append(rerul_link)


            time.sleep(round(random.uniform(0.5, 1.0), 20))


     return reurl_link_list









def send_tg_bot_msg(token,chat_id,msg):

  #url = f"https://api.telegram.org/bot{'+token+'}/sendMessage?chat_id={'+chat_id}&text={msg}&parse_mode=HTML"
  url = "https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={msg}&parse_mode=HTML".format(token = token ,chat_id=chat_id,msg=msg)

  try :
       requests.get(url)

  except :
       time.sleep(random.random()) ### 0~1 num   
       requests.get(url)





def delete_redis_data(today):

     dd = 10 ##days :10
     while  len(r.keys('*_reurl_lists')) >2 and dd > 1 :

       yy = today - datetime.timedelta(days=dd)
       yy_key = yy.strftime("%Y%m%d")+'_reurl_lists'

       if r.exists(yy_key) : ### del archive  key
          r.delete(yy_key)

       dd -=1

def insert_redis_data(_key,_values):
    line_display = []

    if r.exists(_key) :
           _list = r.lrange(_key,'0','-1')
           for i_value in _values :
               if i_value.replace("https://reurl.cc/","") not in _list :
                  line_display.append(i_value)
                  r.rpush(_key,i_value.replace("https://reurl.cc/",""))

    else :
       for i_value in _values :
           line_display.append(i_value)
           r.rpush(_key,i_value.replace("https://reurl.cc/",""))

    return line_display


def insert_com_list_redis_data(_key,_values):

     
     if r.exists(_key) : ### del archive  key
          r.delete(_key)

     for i in _values :
         r.rpush(str(_key),str(i))


### atlas mongodb conn info
conn_user = get_redis_data('mongodb_user',"hget","user",'NULL')
conn_pwd = get_redis_data('mongodb_user',"hget","pwd",'NULL')

mongourl = 'mongodb+srv://' + conn_user +':' + conn_pwd +'@cluster0.47rpi.gcp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
conn = MongoClient(mongourl)


def atlas_read_mongo_db(_db,_collection,dicct,_columns):
    db = conn[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns)



def rss_get(com_list_name,web_list,exclude_tag):


    dfs = pd.DataFrame()
    for com in com_list_name :

      url ="https://news.google.com/rss/search?q=allintitle:{com}+when:{hh}h&hl=zh-TW&gl=TW&ceid=TW%3Azh-Hant".format(com=com,hh=1) ## intitile:{key_word} + when:{1 hour} 
      #print('processing %d :' % com_list_name.index(com),com)

      r = requests.get(url)
      root = ET.fromstring(r.text) ## for XML
      items = root.iter('item')
      datas = []
      for item in items :
         rows = root_data(item)
         datas.append(rows) 
     
      df = pd.DataFrame(data=datas, columns=['title', 'link', 'pubDate','pubTime', 's_url'])
      dfs = pd.concat([dfs,df])
      time.sleep(random.randrange(1, 3, 1))
    ### filter duplicates news of titile 
    dfs_uni =dfs[dfs['s_url'].isin(web_list)].copy()
    dfs_uni_titile = dfs_uni.drop_duplicates(subset='title').copy()
    
    for idx in exclude_tag :

      p = re.compile(r'\w{idx}*'  )
      p1 = re.compile(r'\b{idx}*')

      dfs_uni_titile['title_None'] = dfs_uni_titile.apply(lambda  x: None  if  p.findall(x['title'])  else None if p1.findall(x['title']) else 1   ,axis=1)
      dfs_uni_titile = dfs_uni_titile.dropna()     

    match_row = dfs_uni_titile.iloc[:,[0,1,2,3,4]].copy()   
  
    #match_row = dfs_uni_titile[~dfs_uni_titile['title'].isin(exclude_tag)].copy()

    match_row['URL'] = reurl_API(match_row['link'].values , 'reurl_key') ## short url
    rediskeys = match_row.iloc[0,2] + '_reurl_lists'
    ### delete old redis key
    delete_redis_data(today)

    #insert_redis_data('skey_lists',match_row['skey_lists'].values) 
    line_display_list = insert_redis_data(rediskeys,match_row['URL'].values)

    #print('297_line_display_list:',line_display_list) 
    
    ## check new array 
    ###### wait
    match_row_line = match_row[match_row['URL'].isin(line_display_list)].copy() ### new arrary
    

    return match_row_line.sort_values(by=['pubTime'])
    #return match_row.sort_values(by=['pubDate'])

com_list_code = []
com_list_name = []
extra_tag=[]
exclude_tag=[]
web_list=[]
## get com_list code from redis data 
redis_com_list = get_redis_data('com_list','lrange',0,-1)

try :

  ### got mongo data from atlas
  dictt = {}
  #_columns= {"code":1,"_id":0}
  _columns= {"code":1,"name":1,"_id":0}
  _tag_columns= {"tag":1,"_id":0}
  _url_columns= {"url":1,"_id":0}


  mydoc = atlas_read_mongo_db('stock','view_com_list_name',dictt,_columns)
  mydoc_tag = atlas_read_mongo_db('stock','extra_tag',dictt,_tag_columns)
  time.sleep(round(random.uniform(0.5, 1.0), 10))

  mydoc_exclude_tag = atlas_read_mongo_db('stock','exclude_tag',dictt,_tag_columns)
  mydoc_web_url = atlas_read_mongo_db('stock','web_url',dictt,_url_columns)
  time.sleep(round(random.uniform(0.5, 1.0), 10))


  for idx in mydoc :
    com_list_code.append(idx.get('code'))
    com_list_name.append(idx.get('name'))
  for idx in mydoc_tag :
    extra_tag.append(idx.get('tag'))
  for idx in mydoc_exclude_tag :
    exclude_tag.append(idx.get('tag'))  
  for idx in  mydoc_web_url:
    web_list.append(idx.get('url'))


  ### compare redis com_list code from mongo sync to redis data 
  if (collections.Counter(com_list_code) != collections.Counter(redis_com_list)) and (len(com_list) > 0) :
     insert_com_list_redis_data('com_list',com_list_code)
     
     for com in com_list_code :
       _values = { 'code' : com ,'last_modify':datetime.datetime.now() }
       insert_mongo_db('stock','com_list',_values)
except :
   ### got redis data from local
   ## get com_list code 
   com_list = redis_com_list


#com_list = get_redis_data('com_list','lrange',0,-1)
#com_list_name = []

"""
## get mapping com_list_name
for com in com_list : 
   name =  get_redis_data('com_lists','hget',com,'NULL')
   com_list_name.append(name)
"""
## extra tag group
#extra_tag=['晶圓','特斯拉','財報','蘋果','Facebook','FB','Meta','谷歌','離岸風電','電動車','被動元件','車用晶片','AMD','Nvida','Apple','Mac','Mini LED','MicroLED','Google','自動駕駛','充電站','元宇宙','Omicron','國際油價','Peloton','健身器材','道瓊','ASML','Applied Materials','應材','MLCC','AR／VR','停工','烏克蘭','俄烏','低軌衛星','Starlink','SpaceX','infineon','英飛凌']


try:
    com_list_name.remove('世界')  ## remove '世界'
except :
    pass

com_list_name.extend(extra_tag) ## adding '世界先進'




#print('com_list_name:',com_list_name)
## discard url list
discard_list =['www.cool3c.com','wantrich.chinatimes.com','finance.sina.com.cn','news.sina.com.tw','news.cheshi.com','www.eprice.com.tw','m.eprice.com.tw','www.cnbeta.com']
## mapping web
web_list = ['www.cnyes.com','news.cnyes.com','www.moneydj.com','fund.megabank.com.tw','money.udn.com','www.cw.com.tw','www.ettoday.net','tw.stock.yahoo.com','ctee.com.tw','news.pchome.com.tw','ce.ltn.com.tw','cn.wsj.com','udn.com','finance.ettoday.net']
#web_list = ['www.cnyes.com','news.cnyes.com','www.moneydj.com','fund.megabank.com.tw','money.udn.com','www.cw.com.tw','www.ettoday.net','tw.stock.yahoo.com','ctee.com.tw','www.chinatimes.com','news.pchome.com.tw','news.ltn.com.tw']


#exclude_tag=list(mydoc_exclude_tag)
#print('com_list_name:',com_list_name)


#### get function 
match_row =pd.DataFrame()  ### is empty() 
#exclude_tag=list(mydoc_exclude_tag)

#print(exclude_tag)
try :
    match_row = rss_get(com_list_name,web_list,exclude_tag)
    #match_row = rss_get(com_list_name,web_list)
    ## choice column for line nofity 
    match_row = match_row.iloc[:,[3,0,5]] ## pub_Time/title/URL
    ### filter exclude with tag 世界 盤中速報

except : 
       #match_row =pd.DataFrame()  ### is empty()       
       pass

"""
鉅亨網
https://www.cnyes.com/
https://news.cnyes.com/

MoneyDJ理財網
www.moneydj.com

國際財經新聞
https://fund.megabank.com.tw/

經濟日報
https://money.udn.com/
https://udn.com/news

財經 - 天下雜誌
https://www.cw.com.tw/

ET today
https://www.ettoday.net/
https://finance.ettoday.net/news

yahoo 
https://tw.stock.yahoo.com/

工商時報
https://ctee.com.tw/

中時新聞網
https://www.chinatimes.com/

PChome
https://news.pchome.com.tw/

自由時報
https://news.ltn.com.tw/
https://ce.ltn.com.tw/


華爾街日報
https://cn.wsj.com/zh-hant
"""
#print('match_row:',match_row)

#if not match_row.empty and  time.strftime("%H:%M:%S", time.localtime()) < notify_time :
if not match_row.empty  :
          tg_key_list=[]
          tg_chat_id=[]
          tg_key_list.append(get_redis_data('tg_bot_hset','hget','@stock_broadcast_2024bot','NULL')) ## for rss_google of signle
          tg_chat_id.append(get_redis_data('tg_chat_id','hget','stock_broadcast','NULL')) ## for rss_google of signle
          
          for match_row_index in range(0,len(match_row),5) :
              
              msg = "\n " + match_row.iloc[match_row_index:match_row_index+5,:].to_string(index = False)  ## for line notify msg 1000  character limit 
              tg_msg ="【rss_google】  "+ "\n" + msg

              ### for multiple line group

              for tg_key in  tg_key_list : ## 
                  send_tg_bot_msg(tg_key,tg_chat_id[0],tg_msg)                  
                  time.sleep(random.randrange(1, 3, 1))



