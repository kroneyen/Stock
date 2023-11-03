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
from dateutil.relativedelta import *
      


### sync 1 month data from local to Atlas mongodb
cal_date = datetime.date.today() + relativedelta(months=-1)

_date = datetime.datetime.strftime( cal_date,'%Y%m%d' )

iso_date_str = datetime.datetime.strftime( cal_date,'%Y-%m-%d' )+ "T00:00:00"
_iso_date  =  datetime.datetime.strptime(iso_date_str, '%Y-%m-%dT%H:%M:%S')



### stock ###

_db= 'stock'

#_collection=['Rep_3_Investors','Rep_Stock_Exchange','Rep_Stock_Holder','Rep_Stock_Season','Rep_Stock_Season_Com']
_collection=['Rep_3_Investors','Rep_Stock_Exchange','Rep_Stock_Holder']



for idx in _collection :


    if idx == 'Rep_Stock_Holder' :

       _dicct={"date" : {"$gte":  _date }}

    else :

      _dicct={"last_modify" : {"$gte":  _date }}



    Atlas_mongodb_insert.data_insert_mongo(_db, idx, _dicct)

    if __name__ == '__main__': 

       print( _db, idx, _dicct)
     

    time.sleep(1)

### rent ###
_db_list= ['591','yungching','century21','sinyi']


_dicct={"last_modify" : {"$gte":  _iso_date }}


## "last_modify" : ISODate("2023-09-26T17:58:59.659Z")
## {"last_modify" : {"$gte": "2023-10-05T00:00:00"}}

for _db in _db_list :



    if _db == '591':

        _collection_list=['sale_house','rent_house']        

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

    
