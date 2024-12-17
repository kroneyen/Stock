#! /usr/bin/env python3.6
# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import redis
import time
import requests
from pymongo import MongoClient
import json
import Atlas_mongodb_insert
from dateutil.relativedelta import relativedelta
import re      


### sync 1 month data from local to Atlas mongodb
cal_date = datetime.date.today() + relativedelta(months=-1)

_date = datetime.datetime.strftime( cal_date,'%Y%m%d' )

iso_date_str = datetime.datetime.strftime( cal_date,'%Y-%m-%d' )+ "T00:00:00"
_iso_date  =  datetime.datetime.strptime(iso_date_str, '%Y-%m-%dT%H:%M:%S')



### try to instantiate a client instance for local
c = MongoClient(
        host = 'localhost',
        port = 27017,
        serverSelectionTimeoutMS = 3000, # 3 second timeout
        username = "dba",
        password = "1234",
    )




def read_mongo_db_sort_limit(_db,_collection,dicct,_columns,_sort):
    db = c[_db] ## database
    collection = db[_collection] ## collection 
    return collection.find(dicct,_columns).sort(_sort).limit(1)





### stock ###

_db= 'stock'

#_collection=['Rep_3_Investors','Rep_Stock_Exchange','Rep_Stock_Holder','Rep_Stock_Season','Rep_Stock_Season_Com']
_collection=['Rep_3_Investors','Rep_Stock_Exchange','Rep_Stock_Holder','Rep_Stock_Season_Com','Rep_Stock_dividind_Com']


### for season setting
today = datetime.date.today()


"""
if today.month >= 4 and today.month<= 6 :
  season = 1
elif  today.month >= 7 and today.month<= 9 :
  season = 2

elif  today.month >= 10 and today.month<= 12 :
  season = 3
else :
  season = 4

yy = today.year
"""


## get last row
_sort=[("_id",-1)]

get_last_data = read_mongo_db_sort_limit('stock','Rep_Stock_Season_Com',{},{"_id":0,"years" : 1,"season":1},_sort)

for idx in get_last_data :
    season = idx.get('season')
    yy = idx.get('years')

### for season setting

_collection_list= Atlas_mongodb_insert.Get_Collection_Names(_db)


for _collection in _collection_list :


    if _collection == 'Rep_Stock_Holder' :

       _dicct={"date" : {"$gte":  _date }}


    elif  _collection == 'Rep_Stock_Season_Com': 

       _dicct={"years" : str(yy) , "season" : str(season)}


    elif  _collection == 'Rep_Stock_dividind_Com':

       #_dicct={"years" : str(yy)}
       _dicct={}

    elif  re.search("^view",_collection) :

             view_data = Atlas_mongodb_insert.getCollectionInfos(_db,_collection)
             view_on = view_data.get('cursor').get('firstBatch')[0].get('options').get('viewOn')
             view_pipe = view_data.get('cursor').get('firstBatch')[0].get('options').get('pipeline')
             ### ceate view in atlas

             Atlas_mongodb_insert.create_view(_db,view_on,_collection,view_pipe)

    else :

       _dicct={"last_modify" : {"$gte":  _date }}
   
    
    if re.search("^Rep",_collection) : 


       Atlas_mongodb_insert.data_insert_mongo(_db, _collection, _dicct)


    if __name__ == '__main__': 

       print( _db, _collection, _dicct)
     

    time.sleep(1)





#### bankrate

_db_list = ['bankrate']

_collection_list = ['target_currency','daily_currency']

_dicct={"last_modify" : {"$gte":  _iso_date }}

for _db in _db_list :



    for _collection in _collection_list :


           Atlas_mongodb_insert.data_insert_mongo(_db, _collection, _dicct)


           if __name__ == '__main__':

              print( _db, _collection, _dicct)



    time.sleep(1)


"""
### rent ###


### sync 2 month data from local to Atlas mongodb 
cal_date = datetime.date.today() + relativedelta(months=-2)
iso_date_str = datetime.datetime.strftime( cal_date,'%Y-%m-%d' )+ "T00:00:00"
_iso_date  =  datetime.datetime.strptime(iso_date_str, '%Y-%m-%dT%H:%M:%S')



#_db_list= ['591','yungching','century21','sinyi','newbuild']
_db_list= ['591','yungching','century21','sinyi','newbuild','rent']

_dicct={"last_modify" : {"$gte":  _iso_date }}


## "last_modify" : ISODate("2023-09-26T17:58:59.659Z")
## {"last_modify" : {"$gte": "2023-10-05T00:00:00"}}

for _db in _db_list :


    if _db == '591':

        _collection_list= Atlas_mongodb_insert.Get_Collection_Names(_db)

        for _collection in _collection_list :
          
           if  re.search("^view",_collection) : 

                view_data = Atlas_mongodb_insert.getCollectionInfos(_db,_collection)
                view_on = view_data.get('cursor').get('firstBatch')[0].get('options').get('viewOn')
                view_pipe = view_data.get('cursor').get('firstBatch')[0].get('options').get('pipeline')

                ### ceate view in atlas
                
                Atlas_mongodb_insert.create_view(_db,view_on,_collection,view_pipe)


                if __name__ == '__main__':

                        print( _db, _collection, _dicct)

           elif re.search("house$",_collection) : 

                 Atlas_mongodb_insert.data_insert_mongo(_db, _collection, _dicct)

                 if __name__ == '__main__':

                        print( _db, _collection, _dicct)

              
           print('collection:{s}'.format(s=_collection))     


    elif  _db == 'newbuild' : 
          
          #_collection_list=['build_591','housetube'] 
          _collection_list= Atlas_mongodb_insert.Get_Collection_Names(_db)

          for _collection in _collection_list :




             Atlas_mongodb_insert.data_insert_mongo(_db, _collection, _dicct)


             if __name__ == '__main__':

               print( _db, _collection, _dicct)
 
    
    else : 

           _collection='sale_house'

           Atlas_mongodb_insert.data_insert_mongo(_db, _collection, _dicct)


           if __name__ == '__main__':

              print( _db, _collection, _dicct)


    time.sleep(1)
"""
