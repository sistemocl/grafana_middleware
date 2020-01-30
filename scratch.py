#!/usr/bin/env python
# coding: utf-8

from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
from influxdb import DataFrameClient
    

select='select  "butler_id" as "bot", "1_int"  as "celda 1",  "2_int"  as "celda 2",  "3_int"  as "celda 3",  "4_int"  as "celda 4",  "5_int"  as "celda 5", "6_int"  as "celda 6", "7_int"  as "celda 7", "8_int"  as "celda 8", "9_int"  as "celda 9", "10_int" as "celda 10","11_int" as "celda 11","12_int" as "celda 12","13_int" as "celda 13","14_int" as "celda 14","15_int" as "celda 15","16_int" as "celda 16" '


were='''from battery_details_info order by time desc limit 8000;'''


def baterias(host='192.168.222.39', port=8086):
    """Instantiate a connection to the InfluxDB."""
    user = ''
    password = ''
    dbname = 'GreyOrange'
    dbuser = ''
    dbuser_password = ''
    bind_params = {'host': 'server01'}
    

    client = DataFrameClient(host, port, user, password, dbname)

    result = client.query(select+were)
    
    return result

test = baterias()

batt = test['battery_details_info']


conditions = []
for i in range(1,17):
    conditions.append(batt['celda {}'.format(i)] <=3200)
for i in range(1,17):
    conditions.append(batt['celda {}'.format(i)] >=3600)


con = conditions[0]
for m in conditions:
    con = con | m


new = batt[con]
new2 = new.copy()
new2['celdas malas'] = [set() for _ in range(len(new2))]
new2['celdas bajas'] = [set() for _ in range(len(new2))]
new2['celdas altas'] = [set() for _ in range(len(new2))]


butlers_malos = []
for i, row in new.iterrows():
    butlers_malos.append(row['bot'])
    celdas_bajas = set()
    for j in range(16):
        test = conditions[j]
        if test[i]:
            celdas_bajas.add(j+1)
    cellsb =  celdas_bajas
    new2['celdas bajas'][i] = cellsb
    celdas_altas = set()
    for j in range(16,32):
        test = conditions[j]
        if test[i]:
            celdas_altas.add((j-15))
    cellsa = celdas_altas
    new2['celdas altas'][i] = cellsa
    new2['celdas malas'][i] = cellsb.union(cellsa)


def duplicados(a):
    seen = {}
    dupes = []

    for x in a:
        if x not in seen:
            seen[x] = 1
        else:
            if seen[x] == 1:
                dupes.append(x)
            seen[x] += 1
    return dupes
    

butlers_dup = set(duplicados(butlers_malos))
butlers_todos = set(butlers_malos)


butlers_solos = butlers_todos.difference(butlers_dup)


d = {'butler_id':list(butlers_todos)}
df = pd.DataFrame(data=d)


df['celdas bajas'] = [set() for _ in range(len(df))]
df['celdas altas'] = [set() for _ in range(len(df))]
df['celdas malas'] = [set() for _ in range(len(df))]


for but in butlers_dup:
    tmp = new2[new2['bot'] == but]
    cb = set()
    ca = set()
    cm = set()
    for i, row in tmp.iterrows():
        cb = cb.union(row['celdas bajas'])
        ca = ca.union(row['celdas altas'])
        cm = cm.union(row['celdas malas'])
    df['celdas bajas'][df.index[df['butler_id']==but].to_list()[0]] = cb
    df['celdas altas'][df.index[df['butler_id']==but].to_list()[0]] = ca
    df['celdas malas'][df.index[df['butler_id']==but].to_list()[0]] = cm



for but in butlers_solos:
    cb = new2['celdas bajas'][new2.index[new2['bot']==but].to_list()[0]]
    ca = new2['celdas altas'][new2.index[new2['bot']==but].to_list()[0]]
    cm = new2['celdas malas'][new2.index[new2['bot']==but].to_list()[0]]
    df['celdas bajas'][df.index[df['butler_id']==but].to_list()[0]] = cb
    df['celdas altas'][df.index[df['butler_id']==but].to_list()[0]] = ca
    df['celdas malas'][df.index[df['butler_id']==but].to_list()[0]] = cm


select='select  "butler_id" as "bot", "1_int"  as "celda 1",  "2_int"  as "celda 2",  "3_int"  as "celda 3",  "4_int"  as "celda 4",  "5_int"  as "celda 5", "6_int"  as "celda 6", "7_int"  as "celda 7", "8_int"  as "celda 8", "9_int"  as "celda 9", "10_int" as "celda 10","11_int" as "celda 11","12_int" as "celda 12","13_int" as "celda 13","14_int" as "celda 14","15_int" as "celda 15","16_int" as "celda 16" '


were='''from battery_details_info order by time desc limit 8000;'''


def baterias(host='192.168.222.39', port=8086,query=''):
    """Instantiate a connection to the InfluxDB."""
    user = ''
    password = ''
    dbname = 'GreyOrange'
    dbuser = ''
    dbuser_password = ''
    bind_params = {'host': 'server01'}
    
    client = DataFrameClient(host, port, user, password, dbname)

    result = client.query(query)
    
    return result



select='select  "butler_id" as "bot", "1_int"  as "celda 1",  "2_int"  as "celda 2",  "3_int"  as "celda 3",  "4_int"  as "celda 4",  "5_int"  as "celda 5", "6_int"  as "celda 6", "7_int"  as "celda 7", "8_int"  as "celda 8", "9_int"  as "celda 9", "10_int" as "celda 10","11_int" as "celda 11","12_int" as "celda 12","13_int" as "celda 13","14_int" as "celda 14","15_int" as "celda 15","16_int" as "celda 16" '


scores = dict()
for i,row in df.iterrows():
    were='''from battery_details_info where butler_id='{}' order by time desc limit 800;'''.format(row['butler_id'])
    nq = baterias(query=select+were)
    celdas = nq['battery_details_info']
    values = celdas.to_numpy()
    values = values[:,1:]
    med = np.median(values,axis=1)
    med = [med]
    med = np.array(med)
    med = med.transpose()
    test = np.repeat(med,16,axis=1)
    dif = test - values
    dif = np.abs(dif)
    scores[row['butler_id']] = np.sum(dif,axis=0)/(dif.shape[0])
    
    
df = df.set_index('butler_id')

for i in range(1,17):
    df['score celda {}'.format(i)] = 0
df['score robot'] = 0
df['max score'] = 0

for key in scores:
    for i in range(len(scores[key])):
        df['score celda {}'.format(i+1)][key] = scores[key][i]
    df['score robot'][key] = np.sum(scores[key])
    df['max score'][key] = np.max(scores[key])


df.sort_values('max score')




