# prepare data for get_links
# 每季更新相關連結

import re
import itertools
from unicodedata import name
from unittest import result
from conf.settings import env
import pymysql
import pandas as pd
import requests
from datetime import datetime
import json
import string
from bs4 import BeautifulSoup


# fishbase 重新爬蟲
# 取得推薦連結資料

# FishBase
# species

# http://www.fishbase.us/listbyletter/ScientificNamesA.htm

df = pd.DataFrame()
for s in string.ascii_uppercase:
    print(s)
    url = f'https://fishbase.mnhn.fr/ListByLetter/ScientificNames{s}.htm'
    page = requests.get(url)
    text = page.text
    soup = BeautifulSoup(text)
    selector = "td a"
    hrefs = [i.get("href") for i in soup.select(selector)] 
    selector = "td a i"
    names = [i.text for i in soup.select(selector)] 
    for i in range(len(hrefs)):
        current_name = names[i]
        current_str = hrefs[i]
        current_id = current_str.split('id=')[-1]
        # print(current_name, current_id)
        df = df.append({'name': current_name, 'id': current_id, 'rank': 'species'}, ignore_index=True)

# families -> 爬蟲

url = 'https://fishbase.mnhn.fr/search.php'
page = requests.get(url)
text = page.text
soup = BeautifulSoup(text)
selector = "select[name='famcode1'] option"
hrefs = [i.get("value") for i in soup.select(selector)] 
hrefs = hrefs[1:]
names = [i.text for i in soup.select(selector)] 
names = names[1:]
# names = re.findall('[A-Z][^A-Z]*', names)

df = pd.DataFrame()
for i in range(len(hrefs)):
    current_name = names[i]
    current_id = hrefs[i]
    df = df.append({'name': current_name, 'id': current_id, 'rank': 'family'}, ignore_index=True)

df = df.rename(columns={'name': 'source_name'})
df.to_csv('/code/data/link/fishbase.csv', index=None)


# world spider catalog
# 先找出所有family name id
# https://wsc.nmbe.ch/families
page = requests.get('https://wsc.nmbe.ch/families')
text = page.text
soup = BeautifulSoup(text)

selector = "a[title='Classic view']"
hrefs = [i.get("href") for i in soup.select(selector)] 

df = pd.DataFrame()
for i in range(len(hrefs)):
    current_str_list = hrefs[i].split('/')
    current_id = current_str_list[2]
    current_name = current_str_list[3]
    df = df.append({'name': current_name, 'id': current_id, 'rank': 'family', 'suffix': hrefs[i]}, ignore_index=True)

#  直接存suffix (包含family/genus/species)
# https://wsc.nmbe.ch/genlist/1/Actinopodidae

for i in df.index:
    print(i)
    row = df.iloc[i]
    url = f"https://wsc.nmbe.ch/genlist/{row.id}/{row['name']}"
    page = requests.get(url)
    text = page.text
    soup = BeautifulSoup(text)
    selector = "a[title='Show species entries']"
    hrefs = [x.get("href") for x in soup.select(selector)] 
    for y in range(len(hrefs)):
        current_str_list = hrefs[y].split('/')
        current_id = current_str_list[2]
        current_name = current_str_list[3]
        df = df.append({'name': current_name, 'id': current_id, 'rank': 'genus', 'suffix': hrefs[y]}, ignore_index=True)

for i in df[df['rank']=='genus'].index:
    print(i)
    if i >= 3713:
        row = df.iloc[i]
        url = f"https://wsc.nmbe.ch/specieslist/{row.id}/{row['name']}"
        page = requests.get(url)
        text = page.text
        soup = BeautifulSoup(text)
        selector = "a[title='Genera list']"
        hrefs = [x.get("href") for x in soup.select(selector)] 
        for y in range(len(hrefs)):
            current_str_list = hrefs[y].split('/')
            current_id = current_str_list[2]
            current_name = current_str_list[3].replace('_', ' ')
            df = df.append({'name': current_name, 'id': current_id, 'rank': 'species', 'suffix': hrefs[y]}, ignore_index=True)

df.to_csv('/code/data/link/wsc.csv', index=None)

# FLOW
# families
page = requests.get('https://flow.hemiptera-databases.org/flow/?db=flow&page=explorer&card=families&lang=en')
text = page.text
soup = BeautifulSoup(text)

poster_selector = "td.cellAsLi a"
hrefs = [i.get("href") for i in soup.select(poster_selector)] # 37

poster_selector = "td.cellAsLi a i"
names = [i.text.replace('†', '') for i in soup.select(poster_selector)] # 37
# 抓出id
# ?page=explorer&db=flow&lang=en&card=taxon&rank=family&id=15213&loading=1
df = pd.DataFrame()
for i in range(len(hrefs)):
    current_name = names[i]
    current_str = hrefs[i]
    current_id = current_str.split('&')[-2].replace('id=','')
    df = df.append({'name': current_name, 'id': current_id}, ignore_index=True)
    
df['rank'] = 'family'

# genera
import string
# df = pd.DataFrame()

for s in string.ascii_uppercase:
    print(s)
    url = 'https://flow.hemiptera-databases.org/flow/?page=explorer&card=genera&db=flow&searchtable=noms_complets&lang=en&alph=' + s
    page = requests.get(url)
    text = page.text
    soup = BeautifulSoup(text)
    selector = "td.cellAsLi a"
    hrefs = [i.get("href") for i in soup.select(selector)] 
    selector = "td.cellAsLi a i"
    names = [i.text.replace('†', '') for i in soup.select(selector)] 
    for i in range(len(hrefs)):
        current_name = names[i]
        current_str = hrefs[i]
        current_id = current_str.split('&')[-1].replace('id=','')
        # print(current_name, current_id)
        df = df.append({'name': current_name, 'id': current_id, 'rank': 'genus'}, ignore_index=True)

df = df.drop_duplicates()

# species

for s in string.ascii_uppercase:
    print(s)
    url = 'https://flow.hemiptera-databases.org/flow/?page=explorer&card=speciess&searchtable=noms_complets&lang=en&db=flow&alph=' + s
    page = requests.get(url)
    text = page.text
    soup = BeautifulSoup(text)
    selector = "td.cellAsLi a"
    hrefs = [i.get("href") for i in soup.select(selector)] 
    selector = "td.cellAsLi a i"
    names = [i.text.replace('†', '') for i in soup.select(selector)] 
    for i in range(len(hrefs)):
        current_name = names[i]
        current_str = hrefs[i]
        current_id = current_str.split('&')[-1].replace('id=','')
        # print(current_name, current_id)
        df = df.append({'name': current_name, 'id': current_id, 'rank': 'species'}, ignore_index=True)

df.to_csv('/code/data/link/flow.csv', index=None)








# # 處理相關連結同名異物的情況

# import re
# import itertools
# from conf.settings import env
# import pymysql
# import pandas as pd
# import requests
# from datetime import datetime
# import json

# db_settings = {
#     "host": env('DB_HOST'),
#     "port": int(env('DB_PORT')),
#     "user": env('DB_USER'),
#     "password": env('DB_PASSWORD'),
#     "db": env('DB_DBNAME'),
# }

# conn = pymysql.connect(**db_settings)

# query = """SELECT at.taxon_id, at.links, at.accepted_taxon_name_id, tn.name 
#             FROM api_taxon at 
#             JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id 
#             WHERE json_length(at.links) > 0"""

# with conn.cursor() as cursor:
#     cursor.execute(query)
#     results = cursor.fetchall()
#     results = pd.DataFrame(results)
#     results.columns = ['taxon_id', 'links', 'taxon_name_id', 'name']
#     results['links'] = results['links'].apply(json.loads)


# df = results.explode('links')
# x = pd.json_normalize(df.links)

# df = df.reset_index(drop=True)

# df = df.join(x)

# df = df.drop(['links'],axis=1)
# df = df.drop_duplicates()


# source_dict = {'wikispecies':1,
# 'discoverlife':2,
# 'taibif':3,
# 'inat':4,
# 'antwiki':5,
# 'mycobank':6,
# 'worms':7,
# 'powo':8,
# 'tropicos':9,
# 'lpsn':10,
# 'ncbi':11,
# 'irmng':12,
# 'col':13,
# 'amphibiansoftheworld':14,
# 'adw':15,
# 'fishbase_species':16,
# 'fishbase_family':17,
# 'fishbase_order':18,
# 'flow':19,
# 'orthoptera':20,
# 'taiherbarium':21,
# 'nc':22,
# 'wsc':23,
# 'gisd':24,
# 'algaebase_species':25,
# 'algaebase_hierarchy':26}


# df['source_id'] = df['source'].apply(lambda x: source_dict[x])


# # 拿掉IRMNG (改成name search)
# df = df[df.source_id != 12]

# # 忽略NCBI 
# df = df[df.source_id != 11]

# # 同名異物 - 思怡處理
# df['is_edited'] = 0

# df = df[(df.taxon_id != 't045194')|(df.taxon_id == 't045194')&(df.suffix != '1235429')]
# df.loc[(df.taxon_id == 't045194')&(df.suffix == '1105351'),'is_edited'] = 1

# df = df[(df.taxon_id != 't054326')|(df.taxon_id == 't054326')&(df.suffix != '1204055')]
# df.loc[(df.taxon_id == 't054326')&(df.suffix == '1128819'),'is_edited'] = 1

# df = df[(df.taxon_id != 't054959')|(df.taxon_id == 't054959')&(df.suffix != '18674')]
# df.loc[(df.taxon_id == 't054959')&(df.suffix == '9945'),'is_edited'] = 1

# df = df[(df.taxon_id != 't060258')|(df.taxon_id == 't060258')&(df.suffix != '/species/31001/Myrmarachne_formosana')]
# df.loc[(df.taxon_id == 't060258')&(df.suffix == '/species/31002/Myrmarachne_formosana'),'is_edited'] = 1

# df = df[(df.taxon_id != 't017277')|(df.taxon_id == 't017277')&(df.suffix != '1142767')]
# df.loc[(df.taxon_id == 't017277')&(df.suffix == '1103073'),'is_edited'] = 1

# # 再確認還有沒有其他同名異物
# y = df.groupby(['taxon_id','source'], as_index=False).size()

# z = pd.merge(df,y,on=['taxon_id', 'source'])
# z = z[z.source != 'nc']
# z = z[z['size']>1]

# # 寫入資料庫
# conn = pymysql.connect(**db_settings)

# for i in df.index:
#     if i % 1000 == 0:
#         print(i)
#     row = df.loc[i]
#     if row.is_edited:
#         query = f"""INSERT INTO api_taxon_links (taxon_id, source_id, suffix, is_edited) 
#                     VALUES ('{row.taxon_id}',{row.source_id},"{row.suffix}",1)"""
#         with conn.cursor() as cursor:
#             cursor.execute(query)
#             conn.commit()
#     else:
#         query = f"""INSERT INTO api_taxon_links (taxon_id, source_id, suffix) 
#                     VALUES ('{row.taxon_id}',{row.source_id},"{row.suffix}")"""
#         with conn.cursor() as cursor:
#             cursor.execute(query)
#             conn.commit()
