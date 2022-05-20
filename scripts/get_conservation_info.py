from this import d
import requests
import re
import itertools
from conf.settings import env
import pymysql
import pandas as pd
import requests
from datetime import datetime
import json
import numpy as np


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

conn = pymysql.connect(**db_settings)

query = f'SELECT atu.id, tn.name, atu.taxon_id, atu.status, atu.is_latest FROM api_taxon_usages atu JOIN taxon_names tn ON atu.taxon_name_id = tn.id'

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'id', 1: 'name', 2: 'taxon_id', 3: 'status', 4: 'is_latest'})

#------- 法定保育類 -------#

df = pd.read_csv('protected.csv')
df[df.ScientificName.str.contains('\.', na=False)] # 全部都是spp.
df.ScientificName = df.ScientificName.apply(lambda x: x.replace('spp.', ''))
df.ScientificName = df.ScientificName.apply(lambda x: x.strip()) 

df = df[['ScientificName', 'CommonName', 'Category']]

final_df = df.merge(results, right_on='name', left_on='ScientificName')

# check names not in final_df

df[~df.ScientificName.isin(final_df.ScientificName.to_list())] # 用NomenMatch確認過都不在TaiCOL

# 確認一筆name對到一個taxon
len(final_df.name.unique())
len(final_df.taxon_id.unique())

# 寫回資料庫

conn = pymysql.connect(**db_settings)
for i in final_df.index:
    row = final_df.iloc[i]
    query = f"INSERT INTO api_conservation (taxon_id, scientific_name, protected_category) VALUES ('{row.taxon_id}', '{row.ScientificName}', '{row.Category}')"
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

#------- IUCN -------#

# taxon_id category	criteria	assessment_date

iucn_df = []
large_than_1 = []
for i in results.index: # 86643
    if i >= 0:
        print(i)
        row = results.iloc[i]
        url = f"https://apiv3.iucnredlist.org/api/v3/species/{row['name']}?token={env('IUCN_TOKEN')}"
        data = requests.get(url)
        if data.status_code == 200:
            r = data.json().get('result')
            if r:
                if len(r) > 1:
                    large_than_1.append(row['name'])
                iucn_taxon_id = r[0]['taxonid']
                assessment_date = r[0]['assessment_date']
                category = r[0]['category']
                criteria = r[0]['criteria']
                iucn_df.append({'index': i, 'taxon_id': row.taxon_id , 'name': row['name'],'iucn_taxon_id': iucn_taxon_id, 'assessment_date': assessment_date, 'category': category, 'criteria': criteria})

iucn_df = pd.DataFrame(iucn_df)

# 要用update的
# ALTER TABLE api_conservation ADD UNIQUE (taxon_id)

iucn_df = pd.read_csv('iucn.csv')
iucn_df = iucn_df.replace({np.nan: None})
query = f"""
        INSERT INTO api_conservation
        (taxon_id, scientific_name, iucn_taxon_id, iucn_category, iucn_criteria, iucn_assessment_date)
        VALUES
        (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        iucn_taxon_id = VALUES(iucn_taxon_id),
        iucn_category = VALUES(iucn_category),
        iucn_criteria = VALUES(iucn_criteria),
        iucn_assessment_date = VALUES(iucn_assessment_date)
        """

for i in iucn_df.index:
    print(i)
    row = iucn_df.iloc[i]
    with conn.cursor() as cursor:
        cursor.execute(query,(row.taxon_id, row['name'], row.iucn_taxon_id, row.category, row.criteria, row.assessment_date))
        conn.commit()

#------- CITES -------#

headers = {'X-Authentication-Token': env('CITES_TOKEN')}

cites_df = []
for i in results.index:
    if i >= 0:
        print(i)
        row = results.iloc[i]
        url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={row['name']}"
        data = requests.get(url, headers=headers)
        if data.status_code == 200:
            if r := data.json().get('taxon_concepts'):
                if r[0].get('cites_listing'):
                    cites_listing = r[0]['cites_listing']
                    cites_id = r[0]['id']
                    cites_df.append({'index': i, 'taxon_id': row.taxon_id, 'name': row['name'], 'cites_id': cites_id, 'cites_listing': cites_listing})
                elif r[0].get('accepted_names'):
                    url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={r[0].get('accepted_names')[0]['full_name']}"
                    data = requests.get(url, headers=headers)
                    if data.status_code == 200:
                        if r := data.json().get('taxon_concepts'):
                            cites_id = r[0]['id']
                            cites_listing = r[0]['cites_listing']
                            cites_df.append({'index': i, 'taxon_id': row.taxon_id , 'name': row['name'], 'cites_id': cites_id, 'cites_listing': cites_listing})

cites_df = pd.DataFrame(cites_df)

# import 
# 補上cites id
for i in cites_df.index:
    if i >= 0: #2686
        print(i)
        row = cites_df.iloc[i]
        url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={row['name']}"
        data = requests.get(url, headers=headers)
        if data.status_code == 200:
            if r := data.json().get('taxon_concepts'):
                if r[0].get('cites_listing'):
                    cites_id = r[0]['id']
                elif r[0].get('accepted_names'):
                    url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={r[0].get('accepted_names')[0]['full_name']}"
                    data = requests.get(url, headers=headers)
                    if data.status_code == 200:
                        if r := data.json().get('taxon_concepts'):
                            cites_id = r[0]['id']
        cites_df.loc[i, 'cites_id'] = int(cites_id)

# import


conn = pymysql.connect(**db_settings)

df = pd.read_csv('cites.csv')


# 先存只有一筆的
taxon_id_more_than_1 = df[df.taxon_id.duplicated()].taxon_id.unique() 

query = f"""
        INSERT INTO api_conservation
        (taxon_id, scientific_name, cites_id, cites_listing, cites_note)
        VALUES
        (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        cites_id = VALUES(cites_id),
        cites_listing = VALUES(cites_listing),
        cites_note = VALUES(cites_note)
        """

for i in df[~df['taxon_id'].isin(taxon_id_more_than_1)].index:
    print(i)
    row = df.iloc[i]
    cites_note = [{'name': row['name'], 'listing': row.cites_listing, 'id': int(row.cites_id), 'is_primary': True}]
    with conn.cursor() as cursor:
        cursor.execute(query,(row.taxon_id, row['name'], int(row.cites_id), row.cites_listing, json.dumps(cites_note)))
        conn.commit()


# 同一個taxon_id對到多個評估
# 優先順序
# 1. 和接受名完全一樣
# 2. 同模式自動名
# 3. 去掉雜交符號後相同 ×
# 4. 同模式（種小名相同），但有可能字尾陰陽性有差 -> 用original_taxon_name_id來判斷是不是同模異名

# 比對的時候要 split & 去掉空格

query = f'SELECT id, original_taxon_name_id, name FROM taxon_names WHERE original_taxon_name_id IS NOT NULL'

with conn.cursor() as cursor:
    cursor.execute(query)
    original_df = cursor.fetchall()
    original_df = pd.DataFrame(original_df)
    original_df = original_df.rename(columns={0: 'taxon_name_id', 1: 'original_taxon_name_id', 2: 'name'})

original_df = original_df.merge(df, left_on='name', right_on='name')

# merge accepted_name
df = df.merge(results[(results['is_latest']==1)&(results['status']=='accepted')], on='taxon_id')
df = df.rename(columns={'name_x': 'name', 'name_y': 'accepted_name'})

df[(df['taxon_id'].isin(taxon_id_more_than_1))&(~df['taxon_id'].isnull())].sort_values('taxon_id')


query = f"""
        INSERT INTO api_conservation
        (taxon_id, scientific_name, cites_id, cites_listing, cites_note)
        VALUES
        (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        cites_id = VALUES(cites_id),
        cites_listing = VALUES(cites_listing),
        cites_note = VALUES(cites_note)
        """

no_data = []
for t in taxon_id_more_than_1:
    if t: # taxon_id是None的先不處理，代表工具還有缺資料
        tmp = df[df['taxon_id']==t]
        accepted_name = tmp.accepted_name.values[0]
        accepted_name_list = tmp.accepted_name.values[0].split(' ')
        chosen_row = pd.DataFrame()
        if len(original_df[original_df['name'] == accepted_name]):
            accepted_original_name_id = original_df[original_df['name'] == accepted_name].original_taxon_name_id.values[0]
        else:
            accepted_original_name_id = None
        # 先確認有沒有完全一樣的
        if len(tmp[tmp['name']==accepted_name]):
            chosen_row = tmp[tmp['name']==accepted_name]
        # 再確認 去掉雜交符號相同
        elif len(tmp[tmp['name'].str.replace(' × ', ' ').str.strip()==accepted_name]):
            chosen_row = tmp[tmp['name'].str.replace(' × ', ' ').str.strip()==accepted_name]
        # 是否為自動名 (包含種小名相同的判斷)
        elif len(tmp[(tmp['name'].str.split(' ').str.get(0) == accepted_name_list[0]) & (tmp['name'].str.split(' ').str.get(-1) == accepted_name_list[-1])]):
            chosen_row = tmp[(tmp['name'].str.split(' ').str.get(0) == accepted_name_list[0]) & (tmp['name'].str.split(' ').str.get(-1) == accepted_name_list[-1])]
        # 種小名相同
        elif len(tmp[tmp['name'].str.split(' ').str.get(-1) == accepted_name_list[-1]]):
            chosen_row = tmp[tmp['name'].str.split(' ').str.get(-1) == accepted_name_list[-1]]
        # 是否有同一個original_name_id
        else:
            for tt in tmp.index:
                if len(original_df[original_df['name'] == tmp.loc[tt]['name']]):
                    original_name_id = original_df[original_df['name'] == tmp.loc[tt]['name']].original_taxon_name_id.values[0]
                    if original_name_id == accepted_original_name_id:
                        chosen_row = tmp.loc[tt]
                        print('hello', tt)
                        break
        if len(chosen_row):
            cites_note = [{'name': chosen_row['name'].values[0], 'listing': chosen_row.cites_listing.values[0], 'id': int(chosen_row.cites_id.values[0]), 'is_primary': True}]
            # 補上剩下的到note裡面
            for tt in tmp.index:
                if tmp.loc[tt]['name'] != chosen_row['name'].values[0]:
                    cites_note += [{'name': tmp.loc[tt]['name'], 'listing': tmp.loc[tt].cites_listing, 'id': int(tmp.loc[tt]['id']), 'is_primary': False}]
            # 存入資料庫
            with conn.cursor() as cursor:
                cursor.execute(query,(chosen_row.taxon_id.values[0], row['name'], int(chosen_row.cites_id.values[0]), chosen_row.cites_listing.values[0], json.dumps(cites_note)))
                conn.commit()
        else:
            no_data.append(t)




#------- 紅皮書 -------#

df = pd.read_csv("redlist.csv")

# 確定學名
for i in df.index:
    print(i)
    row = df.iloc[i]
    url = f"http://18.183.59.124/v1/nameMatch?name={row['TaiCOL-accepted_name']}"
    data = requests.get(url)
    if data.status_code == 200:
        if (r := data.json().get('data')) and data.json()['info']['total']==1: # 如果有比對到，且剛好比對到一筆
            taxon_id = r[0]['taxon_id']
            matched_name = r[0]['matched_name']
            accepted_name = r[0]['accepted_name']
            df.loc[i, 'taxon_id'] = taxon_id
            df.loc[i, 'matched_name'] = matched_name
            df.loc[i, 'accepted_name'] = accepted_name


for i in df.index:
    print(i)
    row = df.iloc[i]
    url = f"http://18.183.59.124/v1/nameMatch?name={row['scientific_name']}"
    data = requests.get(url)
    if data.status_code == 200:
        if (r := data.json().get('data')) and data.json()['info']['total']==1: # 如果有比對到，且剛好比對到一筆
            taxon_id = r[0]['taxon_id']
            matched_name = r[0]['matched_name']
            accepted_name = r[0]['accepted_name']
            df.loc[i, 'sci_taxon_id'] = taxon_id
            df.loc[i, 'sci_matched_name'] = matched_name
            df.loc[i, 'sci_accepted_name'] = accepted_name

df.to_csv('紅皮書.csv', index=None)



no = df[df['accepted_name'].isnull()]
no.to_csv('紅皮書_物種缺taxon_id.csv', index=None)


"""
紅皮書A學名評LC
紅皮書B學名評DD
TaiCOL A學名、B學名是同一物種，有效學名是學名A
評估取A學名的LC
紅皮書A學名評LC
紅皮書B學名評DD

TaiCOL A學名、B學名是同一物種，有效學名是學名C
看AB學名有沒有跟C學名是同模式異名(種小明相同aka依據同份模式)
如果B學名跟C學名是同模式異名，那評估取B學名的DD


note 裡面存每個名字 {'name': '', 'category': '', 'is_primary': true/false}
red_category, red_criteria, rea_adjustment就存主要學名的內容


"""

# 確認是不是有同一筆taxon_id對到兩筆紅皮書
df = pd.read_csv('紅皮書.csv')

df = df.replace({np.nan: None})

# category 空值是NA (not applicable)
df['category'] = df['category'].replace({None: 'NA'})

# 先存只有一筆的
taxon_id_more_than_1 = df[df.taxon_id.duplicated()].taxon_id.unique() 

query = f"""
        INSERT INTO api_conservation
        (taxon_id, scientific_name, red_category, red_criteria, red_adjustment, red_note)
        VALUES
        (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        red_category = VALUES(red_category),
        red_criteria = VALUES(red_criteria),
        red_adjustment = VALUES(red_adjustment),
        red_note = VALUES(red_note)
        """

for i in df[~df['taxon_id'].isin(taxon_id_more_than_1)].index:
    row = df.iloc[i]
    if row.taxon_id:
        red_category = row.category
        red_criteria = row.criteria
        red_adjustment = row.adjusting
        red_note = [{'name': row.scientific_name, 'red_category': row.category, 'is_primary': True}]
        with conn.cursor() as cursor:
            cursor.execute(query,(row.taxon_id, row['accepted_name'], red_category, red_criteria, red_adjustment, json.dumps(red_note)))
            conn.commit()


# 同一個taxon_id對到多個評估
# 優先順序
# 1. 和接受名完全一樣
# 2. 同模式自動名
# 3. 去掉雜交符號後相同 ×
# 4. 同模式（種小名相同），但有可能字尾陰陽性有差 -> 用original_taxon_name_id來判斷是不是同模異名

# 比對的時候要 split & 去掉空格

conn = pymysql.connect(**db_settings)

query = f'SELECT id, original_taxon_name_id, name FROM taxon_names WHERE original_taxon_name_id IS NOT NULL'

with conn.cursor() as cursor:
    cursor.execute(query)
    original_df = cursor.fetchall()
    original_df = pd.DataFrame(original_df)
    original_df = original_df.rename(columns={0: 'taxon_name_id', 1: 'original_taxon_name_id', 2: 'name'})

original_df = original_df.merge(df, left_on='name', right_on='scientific_name')

df[(df['taxon_id'].isin(taxon_id_more_than_1))&(~df['taxon_id'].isnull())].sort_values('taxon_id')

no_data = []
for t in taxon_id_more_than_1:
    if t: # taxon_id是None的先不處理，代表工具還有缺資料
        tmp = df[df['taxon_id']==t]
        accepted_name = tmp.accepted_name.values[0]
        accepted_name_list = tmp.accepted_name.values[0].split(' ')
        chosen_row = pd.DataFrame()
        if len(original_df[original_df['name'] == accepted_name]):
            accepted_original_name_id = original_df[original_df['name'] == accepted_name].original_taxon_name_id.values[0]
        else:
            accepted_original_name_id = None
        # 先確認有沒有完全一樣的
        if len(tmp[tmp['scientific_name']==accepted_name]):
            chosen_row = tmp[tmp['scientific_name']==accepted_name]
        # 再確認 去掉雜交符號相同
        elif len(tmp[tmp['scientific_name'].str.replace(' × ', ' ').str.strip()==accepted_name]):
            chosen_row = tmp[tmp['scientific_name'].str.replace(' × ', ' ').str.strip()==accepted_name]
        # 是否為自動名 (包含種小名相同的判斷)
        elif len(tmp[(tmp['scientific_name'].str.split(' ').str.get(0) == accepted_name_list[0]) & (tmp['scientific_name'].str.split(' ').str.get(-1) == accepted_name_list[-1])]):
            chosen_row = tmp[(tmp['scientific_name'].str.split(' ').str.get(0) == accepted_name_list[0]) & (tmp['scientific_name'].str.split(' ').str.get(-1) == accepted_name_list[-1])]
        # 種小名相同
        elif len(tmp[tmp['scientific_name'].str.split(' ').str.get(-1) == accepted_name_list[-1]]):
            chosen_row = tmp[tmp['scientific_name'].str.split(' ').str.get(-1) == accepted_name_list[-1]]
        # 是否有同一個original_name_id
        else:
            for tt in tmp.index:
                if len(original_df[original_df['name'] == tmp.loc[tt].scientific_name]):
                    original_name_id = original_df[original_df['name'] == tmp.loc[tt].scientific_name].original_taxon_name_id.values[0]
                    if original_name_id == accepted_original_name_id:
                        chosen_row = tmp.loc[tt]
                        print('hello', tt)
                        break
        if len(chosen_row):
            red_category = chosen_row.category.values[0]
            red_criteria = chosen_row.criteria.values[0]
            red_adjustment = chosen_row.adjusting.values[0]
            red_note = [{'name': chosen_row.scientific_name.values[0], 'red_category': red_category, 'is_primary': True}]
            # 補上剩下的到note裡面
            for tt in tmp.index:
                if tmp.loc[tt].scientific_name != chosen_row.scientific_name.values[0]:
                    red_note += [{'name': tmp.loc[tt].scientific_name, 'red_category': tmp.loc[tt].category, 'is_primary': False}]
            # 存入資料庫
            with conn.cursor() as cursor:
                cursor.execute(query,(t, chosen_row.accepted_name.values[0], red_category, red_criteria, red_adjustment, json.dumps(red_note)))
                conn.commit()
        else:
            no_data.append(t)


df[df['taxon_id'].isin(no_data)][['scientific_name','taxon_id','TaiCOL-accepted_name','accepted_name','matched_name']]


# ------ 敏感層級

import glob

files = glob.glob('敏感層級*')


df = pd.DataFrame()
for f in files:
    tmp = pd.read_csv(f,usecols=['學名','敏感層級_預設','敏感層級_建議'])
    df = df.append(tmp, ignore_index=True)

for i in df.index:
    print(i)
    row = df.iloc[i]
    url = f"http://18.183.59.124/v1/nameMatch?name={row['學名']}"
    data = requests.get(url)
    if data.status_code == 200:
        if (r := data.json().get('data')) and data.json()['info']['total']==1: # 如果有比對到，且剛好比對到一筆
            taxon_id = r[0]['taxon_id']
            matched_name = r[0]['matched_name']
            accepted_name = r[0]['accepted_name']
            df.loc[i, 'taxon_id'] = taxon_id
            df.loc[i, 'matched_name'] = matched_name
            df.loc[i, 'accepted_name'] = accepted_name


no = df[df['accepted_name'].isnull()]
no.to_csv('敏感物種_物種缺taxon_id.csv', index=None)

query = f"""
        INSERT INTO api_conservation
        (taxon_id, scientific_name, sensitive_default, sensitive_suggest)
        VALUES
        (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        sensitive_default = VALUES(sensitive_default),
        sensitive_suggest = VALUES(sensitive_suggest)
        """
df = df.replace({np.nan: None})

for i in df[~df['accepted_name'].isnull()].index:
    print(i)
    row = df.iloc[i]
    with conn.cursor() as cursor:
        cursor.execute(query,(row.taxon_id, row['accepted_name'], row.敏感層級_預設, row.敏感層級_建議))
        conn.commit()

