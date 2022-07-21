# 處理相關連結同名異物的情況

import re
import itertools
from conf.settings import env
import pymysql
import pandas as pd
import requests
from datetime import datetime
import json

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

conn = pymysql.connect(**db_settings)

query = """SELECT at.taxon_id, at.links, at.accepted_taxon_name_id, tn.name 
            FROM api_taxon at 
            JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id 
            WHERE json_length(at.links) > 0"""

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ['taxon_id', 'links', 'taxon_name_id', 'name']
    results['links'] = results['links'].apply(json.loads)


df = results.explode('links')
x = pd.json_normalize(df.links)

df = df.reset_index(drop=True)

df = df.join(x)

df = df.drop(['links'],axis=1)
df = df.drop_duplicates()


source_dict = {'wikispecies':1,
'discoverlife':2,
'taibif':3,
'inat':4,
'antwiki':5,
'mycobank':6,
'worms':7,
'powo':8,
'tropicos':9,
'lpsn':10,
'ncbi':11,
'irmng':12,
'col':13,
'amphibiansoftheworld':14,
'adw':15,
'fishbase_species':16,
'fishbase_family':17,
'fishbase_order':18,
'flow':19,
'orthoptera':20,
'taiherbarium':21,
'nc':22,
'wsc':23,
'gisd':24,
'algaebase_species':25,
'algaebase_hierarchy':26}


df['source_id'] = df['source'].apply(lambda x: source_dict[x])


# 拿掉IRMNG (改成name search)
df = df[df.source_id != 12]

# 忽略NCBI 
df = df[df.source_id != 11]

# 同名異物 - 思怡處理
df['is_edited'] = 0

df = df[(df.taxon_id != 't045194')|(df.taxon_id == 't045194')&(df.suffix != '1235429')]
df.loc[(df.taxon_id == 't045194')&(df.suffix == '1105351'),'is_edited'] = 1

df = df[(df.taxon_id != 't054326')|(df.taxon_id == 't054326')&(df.suffix != '1204055')]
df.loc[(df.taxon_id == 't054326')&(df.suffix == '1128819'),'is_edited'] = 1

df = df[(df.taxon_id != 't054959')|(df.taxon_id == 't054959')&(df.suffix != '18674')]
df.loc[(df.taxon_id == 't054959')&(df.suffix == '9945'),'is_edited'] = 1

df = df[(df.taxon_id != 't060258')|(df.taxon_id == 't060258')&(df.suffix != '/species/31001/Myrmarachne_formosana')]
df.loc[(df.taxon_id == 't060258')&(df.suffix == '/species/31002/Myrmarachne_formosana'),'is_edited'] = 1

df = df[(df.taxon_id != 't017277')|(df.taxon_id == 't017277')&(df.suffix != '1142767')]
df.loc[(df.taxon_id == 't017277')&(df.suffix == '1103073'),'is_edited'] = 1

# 再確認還有沒有其他同名異物
y = df.groupby(['taxon_id','source'], as_index=False).size()

z = pd.merge(df,y,on=['taxon_id', 'source'])
z = z[z.source != 'nc']
z = z[z['size']>1]

# 寫入資料庫
conn = pymysql.connect(**db_settings)

for i in df.index:
    if i % 1000 == 0:
        print(i)
    row = df.loc[i]
    if row.is_edited:
        query = f"""INSERT INTO api_taxon_links (taxon_id, source_id, suffix, is_edited) 
                    VALUES ('{row.taxon_id}',{row.source_id},"{row.suffix}",1)"""
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
    else:
        query = f"""INSERT INTO api_taxon_links (taxon_id, source_id, suffix) 
                    VALUES ('{row.taxon_id}',{row.source_id},"{row.suffix}")"""
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
