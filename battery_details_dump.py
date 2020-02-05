#!/usr/bin/env python
# coding: utf-8
#pandas is imported for managing table like data
#numpy is used by pandas and to make math operations with vectors
#the influxdb DataFrameClient enables to make queries to the server and save
#the response in a DataFrame
import pandas as pd
import numpy as np
from influxdb import DataFrameClient

min_volt_batt = 3200    
max_volt_batt = 3600

#the query is stored in the variables "select" and "were" 
select='select  "butler_id" as "bot", "1_int"  as "celda 1",  "2_int"  as "celda 2",  "3_int"  as "celda 3",  "4_int"  as "celda 4",  "5_int"  as "celda 5", "6_int"  as "celda 6", "7_int"  as "celda 7", "8_int"  as "celda 8", "9_int"  as "celda 9", "10_int" as "celda 10","11_int" as "celda 11","12_int" as "celda 12","13_int" as "celda 13","14_int" as "celda 14","15_int" as "celda 15","16_int" as "celda 16" '

were='''from battery_details_info where time > now() - 2d order by time desc limit 100000;'''

#http://10.115.43.24:8083/
#def get_query_from_server(host='10.113.95.45', port=8086,query=''):
def get_query_from_server(host='10.115.43.24', port=8086,query=''):
    """Instantiate a connection to the InfluxDB."""
    user = ''
    password = ''
    dbname = 'GreyOrange'
    
    client = DataFrameClient(host, port, user, password, dbname)

    result = client.query(query)
    
    return result


query = get_query_from_server(query=select+were)

batt = query['battery_details_info']


#AQUI HAY QUE TRANSFORMAR LOS SETS A STRING

from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime
engine = create_engine('postgresql://postgres:@10.113.95.45:5432/SistemoDB')
tpd = dict()
for i in range(1,17):
    tpd['celda {}'.format(i)] = Integer

tpd2 = {'bot':Text}
tpd2.update(tpd)

batt.to_sql("battery_details_info",
          engine,
          if_exists='replace',
          schema='public',
          index=True,
          chunksize=500,
          dtype=tpd2)

