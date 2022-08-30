# 2022-08-30
# update taxon conservation
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
import glob


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}


cites_map = {
    'I/II/NC':'1/2/NC',
    'I/II':'1/2',
    'I':'1',
    'II':'2',
    'III':'3',
    'NC': 'NC',
    'II/NC':'2/NC',
    'I/NC':'1/NC',
    'I/II/III/NC':'1/2/3/NC',
    'III/NC':'3/NC',
    'I/III/NC':'1/3/NC',
    'II/III/NC':'2/3/NC',
    'II/III':'2/3',
    'I/III':'1/3'}

protected = pd.read_csv('/code/data/conservation/保育類名錄_merged.csv')
red = pd.read_csv('/code/data/conservation/redlist.csv')
sensitive = pd.read_csv('/code/data/conservation/sensitive.csv', usecols=['敏感層級_預設','敏感層級_建議','學名'])
sensitive = sensitive.rename(columns={'學名': 'source_name'})

def get_conservation_info(taxon_id, protected=protected, red=red, sensitive=sensitive):
    conn = pymysql.connect(**db_settings)
    query = f'SELECT atu.id, tn.name, atu.taxon_id, atu.status, atu.is_latest, atu.taxon_name_id \
                FROM api_taxon_usages atu JOIN taxon_names tn ON atu.taxon_name_id = tn.id \
                WHERE atu.status != "mispplied" and taxon_id = "{taxon_id}"'
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        results = pd.DataFrame(results)
        results = results.rename(columns={0: 'id', 1: 'name', 2: 'taxon_id', 3: 'status', 4: 'is_latest', 5:'taxon_name_id' })
    # 法定保育類 - 目前抓到的都是只有一個對應的name
    protected_df = results.merge(protected, left_on='name', right_on='ScientificName')
    # if len (protected_df) == 1:
    #     protected_category = protected_df.Category.values[0]
    #     protected_note = [{'name': protected_df.ScientificName.values[0]}]
    # else:
    #     protected_category = None
    #     protected_note = []
    protected_df = protected_df.rename(columns={'ScientificName': 'source_name'})
    protected_category = None
    protected_note = []
    if len(protected_df) > 1:
        protected_note, protected_row = determine_name(protected_df, taxon_id, 'protected', results)
        if len(protected_row):
            protected_category = protected_row.Category.values[0]
    elif len(protected_df) == 1:
        protected_category = protected_df.Category.values[0]
        protected_note = [{'name': protected_df.source_name.values[0],'protected_category': protected_df.Category.values[0], 'is_primary': True}]
    # IUCN - 目前抓到的都是只有一個對應的name
    iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note = None, None, None, None, []
    for i in results.index:
        name = results.iloc[i]['name']
        url = f"https://apiv3.iucnredlist.org/api/v3/species/{name}?token={env('IUCN_TOKEN')}"
        data = requests.get(url)
        if data.status_code == 200:
            r = data.json().get('result')
            if r:
                iucn_df = pd.DataFrame(r, columns=r[0].keys())
                iucn_df = iucn_df.rename(columns={'scientific_name': 'source_name'})
                if len(iucn_df) > 1:
                    iucn_note, iucn_row = determine_name(iucn_df, taxon_id, 'iucn', results)
                    if len(iucn_row):
                        iucn_taxon_id = iucn_row.taxonid.values[0]
                        iucn_assessment_date = iucn_row.assessment_date.values[0]
                        iucn_category = iucn_row.category.values[0]
                        iucn_criteria = iucn_row.criteria.values[0]
                elif len(iucn_df) == 1:
                    iucn_taxon_id = iucn_df.taxonid.values[0]
                    iucn_assessment_date = iucn_df.assessment_date.values[0]
                    iucn_category = iucn_df.category.values[0]
                    iucn_criteria = iucn_df.criteria.values[0]
                    iucn_note = [{'name': iucn_df.source_name.values[0],'taxon_id': int(iucn_df.taxonid.values[0]), 'category': iucn_df.category.values[0],'is_primary': True}]
                # iucn_note = []
                # iucn_taxon_id = r[0]['taxonid']
                # iucn_assessment_date = r[0]['assessment_date']
                # iucn_category = r[0]['category']
                # iucn_criteria = r[0]['criteria']
                # for rs in r:
                #     iucn_note += [{'name': rs['source_name'], 'taxon_id': int(rs['taxonid']), 'category': r[0]['category'] }]
    # CITES
    headers = {'X-Authentication-Token': env('CITES_TOKEN')} # t003006
    cites_df = pd.DataFrame(columns=['taxon_id','source_name','cites_id','cites_listing'])
    for i in results.index:
        name = results.iloc[i]['name']
        url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={name}"
        data = requests.get(url, headers=headers)
        if data.status_code == 200:
            if r := data.json().get('taxon_concepts'):
                if r[0].get('cites_listing'):
                    cites_listing = r[0]['cites_listing']
                    cites_name = r[0]['full_name']
                    cites_id = r[0]['id']
                    cites_df = cites_df.append({'taxon_id': taxon_id , 'source_name': cites_name, 'cites_id': cites_id, 'cites_listing': cites_listing},ignore_index=True)
                elif r[0].get('accepted_names'):
                    url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={r[0].get('accepted_names')[0]['full_name']}"
                    cites_name = r[0].get('accepted_names')[0]['full_name']
                    data = requests.get(url, headers=headers)
                    if data.status_code == 200:
                        if r := data.json().get('taxon_concepts'):
                            cites_id = r[0]['id']
                            cites_listing = r[0]['cites_listing']
                            cites_df = cites_df.append({'taxon_id': taxon_id , 'source_name': cites_name, 'cites_id': cites_id, 'cites_listing': cites_listing},ignore_index=True)
    cites_df = cites_df.drop_duplicates()
    cites_id, cites_listing, cites_note = None, None, []
    if len(cites_df) > 1:
        cites_note, cites_row = determine_name(cites_df, taxon_id, 'cites', results)
        if len(cites_row):
            cites_id = cites_row.cites_id.values[0]
            cites_listing = cites_row.cites_listing.values[0]
            cites_listing = cites_map[cites_listing] # 主要的那個改成1/2/3 
    elif len(cites_df) == 1:
        cites_id = cites_df.cites_id.values[0]
        cites_listing = cites_df.cites_listing.values[0]
        cites_note = [{'name': cites_df['source_name'].values[0], 'listing': cites_listing, 'id': cites_id, 'is_primary': True}]
        cites_listing = cites_map[cites_listing] # 主要的那個改成1/2/3 
    # 紅皮書
    red_df = results.merge(red, left_on='name', right_on='scientific_name')
    red_df = red_df.rename(columns={'scientific_name': 'source_name'})
    red_df = red_df.replace({np.nan: None})
    red_df['category'] = red_df['category'].replace({None: 'NA'}) # category 空值是NA (not applicable)
    red_category, red_criteria, red_adjustment, red_note = None, None, None, []
    if len(red_df) > 1:
        red_note, red_row = determine_name(red_df, taxon_id, 'red', results)
        if len(red_row):
            red_category = red_row.category.values[0]
            red_criteria = red_row.criteria.values[0]
            red_adjustment = red_row.adjusting.values[0]
    elif len(red_df) == 1:
        red_category = red_df.category.values[0]
        red_criteria = red_df.criteria.values[0]
        red_adjustment = red_df.adjusting.values[0]
        red_note = [{'name': red_df.source_name.values[0], 'red_category': red_category, 'is_primary': True}]
    # 敏感層級 - 目前抓到的都是只有一個對應的name
    sensitive_df = results.merge(sensitive, left_on='name', right_on='source_name')
    sensitive_default, sensitive_suggest,  sensitive_note = None, None, []
    if len(sensitive_df) > 1:
        sensitive_note, sensitive_row = determine_name(sensitive_df, taxon_id, 'sensitive', results)
        if len(sensitive_row):
            sensitive_default = sensitive_row.敏感層級_預設.values[0]
            sensitive_suggest = sensitive_row.敏感層級_建議.values[0]
    elif len(sensitive_df) == 1:
        sensitive_default = sensitive_df.敏感層級_預設.values[0]
        sensitive_suggest = sensitive_df.敏感層級_建議.values[0]
        sensitive_note = [{'name': sensitive_df.source_name.values[0],'suggest': sensitive_suggest, 'is_primary': True}]
    else:
        sensitive_default, sensitive_suggest,  sensitive_note = None, None, []
    if any((protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, sensitive_default, sensitive_suggest, sensitive_note)):
        return taxon_id, protected_category, json.dumps(protected_note), iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, json.dumps(iucn_note), cites_id, cites_listing, json.dumps(cites_note), red_category, red_criteria, red_adjustment, json.dumps(red_note), sensitive_default, sensitive_suggest,  json.dumps(sensitive_note)
    else:
        return None


## 決定誰是主要的學名

# merge accepted_name

def determine_name(df,taxon_id,source, results):
    no_data = []
    notes = []
    if source not in ['red','protected','sensitive']:
        df = df.merge(results[(results['is_latest']==1)&(results['status']=='accepted')], on='taxon_id')
    original_df = pd.DataFrame(columns=['taxon_name_id','original_taxon_name_id','name']) # 可能沒有資料
    query = f"SELECT id, original_taxon_name_id, name FROM taxon_names WHERE original_taxon_name_id IS NOT NULL \
                and `name` IN {str(list(results['name'].unique())).replace('[','(').replace(']',')')}"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        original_df = cursor.fetchall()
        original_df = pd.DataFrame(original_df)
        original_df = original_df.rename(columns={0: 'taxon_name_id', 1: 'original_taxon_name_id', 2: 'name'})
        conn.close()
    #
    accepted_name = df['name'].values[0]
    accepted_name_list = df['name'].values[0].split(' ')
    chosen_row = pd.DataFrame()
    accepted_original_name_id = None
    if len(original_df):
        if len(original_df[original_df['name'] == accepted_name]):
            accepted_original_name_id = original_df[original_df['name'] == accepted_name].original_taxon_name_id.values[0]
    # else:
    #     accepted_original_name_id = None
    # accepted_name本身的id
    accepted_name_id = results[results['name']==accepted_name].taxon_name_id.values[0]
    # 先確認有沒有完全一樣的
    if len(df[df['source_name']==accepted_name]):
        chosen_row = df[df['source_name']==accepted_name]
    # 再確認 去掉雜交符號相同
    elif len(df[df['source_name'].str.replace(' × ', ' ').str.strip()==accepted_name]):
        chosen_row = df[df['source_name'].str.replace(' × ', ' ').str.strip()==accepted_name]
    # 是否為自動名 (包含種小名相同的判斷)
    elif len(df[(df['source_name'].str.split(' ').str.get(0) == accepted_name_list[0]) & (df['source_name'].str.split(' ').str.get(-1) == accepted_name_list[-1])]):
        chosen_row = df[(df['source_name'].str.split(' ').str.get(0) == accepted_name_list[0]) & (df['source_name'].str.split(' ').str.get(-1) == accepted_name_list[-1])]
    # 種小名相同
    elif len(df[df['source_name'].str.split(' ').str.get(-1) == accepted_name_list[-1]]):
        chosen_row = df[df['source_name'].str.split(' ').str.get(-1) == accepted_name_list[-1]]
    else:
        for tt in df.index:
            # 是否有同一個original_name_id or 設定對方id為original_name_id
            if len(original_df[original_df['name'] == df.loc[tt]['source_name']]):
                original_name_id = original_df[original_df['name'] == df.loc[tt]['source_name']].original_taxon_name_id.values[0]
                if original_name_id == accepted_original_name_id or original_name_id == accepted_name_id:
                    chosen_row = df.loc[tt]
                    chosen_row = chosen_row.to_frame().transpose()
                    # TODO 如果是這邊的話 不能用chosen_row .values[0]
                    break
    if len(chosen_row):
        if source == 'cites':
            notes = [{'name': chosen_row['source_name'].values[0], 'listing': chosen_row.cites_listing.values[0], 'id': int(chosen_row.cites_id.values[0]), 'is_primary': True}]
            for tt in df.index:
                if df.loc[tt]['source_name'] != chosen_row['source_name'].values[0]:
                    notes += [{'name': df.loc[tt]['source_name'], 'listing': df.loc[tt].cites_listing, 'id': int(df.loc[tt]['id']), 'is_primary': False}]
        elif source == 'red':
            notes = [{'name': chosen_row.source_name.values[0], 'red_category': chosen_row.category.values[0], 'is_primary': True}]
            for tt in df.index:
                if df.loc[tt].source_name != chosen_row.source_name.values[0]:
                    notes += [{'name': df.loc[tt].source_name, 'red_category': df.loc[tt].category, 'is_primary': False}]
        elif source == 'protected':
            notes = [{'name': chosen_row['source_name'].values[0],'protected_category': chosen_row.Category.values[0], 'is_primary': True}]
            for tt in df.index:
                if df.loc[tt]['source_name'] != chosen_row['source_name'].values[0]:
                    notes += [{'name': chosen_row['source_name'].values[0],'protected_category': chosen_row.Category.values[0], 'is_primary': False}]
        elif source == 'sensitive':
            notes = [{'name': chosen_row['source_name'].values[0],'suggest': chosen_row.敏感層級_建議.values[0], 'is_primary': True}]
            for tt in df.index:
                if df.loc[tt]['source_name'] != chosen_row['source_name'].values[0]:
                    notes += [{'name': chosen_row['source_name'].values[0],'suggest': chosen_row.敏感層級_建議.values[0], 'is_primary': False}]
        elif source == 'iucn':
            notes = [{'name': chosen_row['source_name'].values[0],'taxonid': chosen_row.taxonid.values[0], 'category': chosen_row.category.values[0], 'is_primary': True}]
            for tt in df.index:
                if df.loc[tt]['source_name'] != chosen_row['source_name'].values[0]:
                    notes += [{'name': chosen_row['source_name'].values[0],'taxonid': chosen_row.taxonid.values[0], 'category': chosen_row.category.values[0], 'is_primary': False}]
    else:
        no_data.append(taxon_id)
    return notes, chosen_row


conn = pymysql.connect(**db_settings)
query = f"select taxon_id from api_taxon;"
with conn.cursor() as cursor:
    cursor.execute(query)
    taxon_list = cursor.fetchall()
    taxon_list = [t[0] for t in  taxon_list]
    conn.close()


for t in taxon_list:
    print(t)
    c = get_conservation_info(t)
    if c: # 如果有任何保育資訊的資料，新增/更新
        query = f"""
                INSERT INTO api_conservation
                (taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                    cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                    sensitive_default, sensitive_suggest,  sensitive_note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        conn = pymysql.connect(**db_settings)        
        with conn.cursor() as cursor:
            cursor.execute(query,c)
            conn.commit()
            conn.close()
    else:
        query = f"""
                INSERT INTO api_conservation
                (taxon_id)
                VALUES (%s)
                """
        conn = pymysql.connect(**db_settings)        
        with conn.cursor() as cursor:
            cursor.execute(query,(t))
            conn.commit()
            conn.close()



