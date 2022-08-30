# update taxon information
import re
import itertools
from unicodedata import name
from conf.settings import env
import pymysql
import pandas as pd
import requests
from datetime import datetime
import json
import glob
import numpy as np

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



db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

conn = pymysql.connect(**db_settings)



# --------- 相關連結

# 自然攝影中心 v
# COL -> 直接用nomenmatch比對, 裡面有全部資料 v
# Orthoptera -> 直接比對GBIF抓下來的source file v
# FLOW -> 爬蟲取得資料後先存成csv，後面用來比對 v
# FishBase -> 爬蟲取得資料後先存成csv，後面用來比對 (如果是orderk的話另外) v 
# world spider catalog -> 爬蟲取得資料後先存成csv，後面用來比對 v
# NCBI -> 比對csv v
# IRMNG -> 直接用name search api 會自動導向id v -> 改成比對csv v
# fishbase order v 
# 
# 以下只需要判斷是不是在所屬類群中
# GISD v
# antwiki v
# mycobank v
# tropicos v
# POWO v
# Amphibian Species of the World  v
# ADW - Animal Diversity Web v
# worms v

#  先讀csv
ncbi = pd.read_csv('/code/data/link/ncbi.csv')
irmng = pd.read_csv('/code/data/link/irmng.csv')
fishbase = pd.read_csv('/code/data/link/fishbase.csv')
nc = pd.read_csv('/code/data/link/NC-fixurls.csv')
orthoptera = pd.read_table('/code/data/link/orthoptera_taxon.txt',usecols=['scientificName','acceptedNameUsageID'])
orthoptera = orthoptera.drop_duplicates()
flow = pd.read_csv('/code/data/link/flow.csv')
wsc = pd.read_csv('/code/data/link/wsc.csv')


def get_links(taxon_id, updated=False):
    # TODO 如果是更新的話，先把舊的抓回來，然後只更新下列五個來源
    # 需要更新的：nc, irmng, orthoptera, gisd, Amphibian Species of the World
    links = []
    conn = pymysql.connect(**db_settings)
    query = f'SELECT atu.id, tn.name, atu.taxon_id, atu.status, atu.is_latest, atu.taxon_name_id, tn.rank_id \
                FROM api_taxon_usages atu JOIN taxon_names tn ON atu.taxon_name_id = tn.id \
                WHERE atu.status != "mispplied" and taxon_id = "{taxon_id}"'
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        results = pd.DataFrame(results)
        results = results.rename(columns={0: 'id', 1: 'name', 2: 'taxon_id', 3: 'status', 4: 'is_latest', 5:'taxon_name_id', 6:'rank_id' })
    accepted_name = results[(results['is_latest']==1)&(results['status']=='accepted')]['name'].values[0]
    rank_id = results[(results['is_latest']==1)&(results['status']=='accepted')]['rank_id'].values[0]
    query = f"SELECT path FROM api_taxon_tree WHERE taxon_id = '{taxon_id}'"
    with conn.cursor() as cursor:
        cursor.execute(query)
        path = cursor.fetchall()
        if path:
            path = path[0][0]    
    # 自然攝影中心
    x = results.merge(nc, right_on='accepted_name', left_on='name')
    y = results.merge(nc, right_on='source_name', left_on='name')
    nc_df = x.append(y).drop_duplicates()
    for i in nc_df.linkurl:
        links += [{'source': 'nc', 'suffix': i.split('?')[1]}]
    for n in results['name']:
        url = f"http://35.77.221.186/api.php?names={n}&source=col&format=json"
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()
            if len(data['data'][0][0]) == 1:
                if len(data['data'][0][0]['results']) ==1:
                    l = {'source': 'col', 'suffix': data['data'][0][0]['results'][0]['accepted_namecode']}
                    if l not in links:
                        links.append(l)
    # Orthoptera
    # 先確定是不是直翅目 t010004
    if 't010004' in path:
        orthoptera_df = results.merge(orthoptera, right_on='scientificName', left_on='name')
        for i in orthoptera_df.acceptedNameUsageID:
            links += [{'source': 'orthoptera', 'suffix': int(i)}]
    # IRMNG 改用 name_search
    # irmng_df = results[['name']].merge(irmng, left_on='name', right_on='source_name')
    # for i in irmng_df.taxonID:
    #     links += [{'source': 'irmng', 'suffix': int(i)}]
    # GISD 確認是不是invasive
    query = f"select id from api_taxon where alien_type='invasive' and taxon_id = '{taxon_id}' ;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        exists = cursor.fetchall()
        if exists:
            links += [{'source': 'gisd', 'suffix': True}]
    # Amphibian Species of the World
    if 't024204' in path:
        path_list = path.split('>')
        query = f"SELECT tn.name \
        FROM api_taxon t \
        JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
        WHERE t.taxon_id IN ({str(path_list).replace('[','').replace(']','')}) and t.rank_id >= 18 \
        ORDER BY t.rank_id ASC"
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            hie_str = ''
            for d in data:
                hie_str += d[0] + '/'
            if hie_str:
                links += [{'source':'amphibiansoftheworld', 'suffix':hie_str}]
    if updated:
        query = f"SELECT links FROM api_taxon WHERE taxon_id = '{taxon_id}'"
        with conn.cursor() as cursor:
            cursor.execute(query)
            old_links = cursor.fetchone()
            old_links = json.loads(old_links[0])
            # 上述五個先移除，再重新加上去
            old_links = [o for o in old_links if not (o['source'] in ['nc', 'irmng', 'orthoptera', 'gisd', 'amphibiansoftheworld'])]
            links += old_links
    else:
        # FLOW
        # 先確認是不是半翅目 Hemiptera t009943
        if 't009943' in path:
            flow_df = results[['name']].merge(flow, left_on='name', right_on='source_name')
            for i in flow_df.id:
                links += [{'source': 'flow', 'suffix': int(i)}]
        # POWO & tropicos
        if 't024278' in path:
            links += [{'source': 'powo', 'suffix': True}, {'source': 'tropicos', 'suffix': True}]
        # ADW 
        if 't024279' in path:
            links += [{'source': 'adw', 'suffix': True}]
        # antwiki  Formicidae (蟻科) t008402
        if 't008402' in path:
            links += [{'source': 'antwiki', 'suffix': True}]
        # mycobank Fungi (真菌界) t074122
        if 't074122' in path:
            links += [{'source': 'mycobank', 'suffix': True}]
        # worms is_marine, is_brackish
        query = f"select id from api_taxon where (is_marine=1 or is_brackish=1) and taxon_id = '{taxon_id}' ;"
        with conn.cursor() as cursor:
            cursor.execute(query)
            exists = cursor.fetchall()
            if exists:
                links += [{'source': 'worms', 'suffix': True}]
        # fishbase
        # 先確認是不是Myxini (盲鰻綱) t024239, Chondrichthyes (軟骨魚綱) t024216, Actinopterygii(條鰭魚綱) t024203
        if 't024239' in path or 't024216' in path or 't024203' in path:
            if rank_id == 22:
            # fishbase order
                url = 'https://fishbase.mnhn.fr/summary/OrdersSummary.php?order=' + accepted_name
                page = requests.get(url)
                if page.text != '</body>\r\n</html>\r\n':
                    links += [{'source': 'fishbase_order', 'suffix': True}]        
            else:
                fishbase_df = results[['name']].merge(fishbase, left_on='name', right_on='source_name')
                for i in fishbase_df.index:
                    fish_row = fishbase_df.iloc[i]
                    links += [{'source': 'fishbase_' + fish_row['rank'], 'suffix': int(fish_row.id)}]
        # WSC
        # 先確認是不是Araneae t009845
        if 't009845' in path:
            wsc_df = results[['name']].merge(wsc, left_on='name', right_on='source_name')
            for i in wsc_df.id:
                links += [{'source': 'wsc', 'suffix': int(i)}]
        # NCBI
        ncbi_df = results[['name']].merge(ncbi, left_on='name', right_on='source_name')
        for i in ncbi_df.id:
            links += [{'source': 'ncbi', 'suffix': int(i)}]
    return links

# iucn v
# cites v
# protected v
# Redlist v
# Sensitive

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
    conn = pymysql.connect(**db_settings)
    # query = f'SELECT atu.id, tn.name, atu.taxon_id, atu.status, atu.is_latest, atu.taxon_name_id \
    # FROM api_taxon_usages atu JOIN taxon_names tn ON atu.taxon_name_id = tn.id \
    # WHERE atu.status != "mispplied" and atu.taxon_id="{taxon_id}"'
    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    #     results = cursor.fetchall()
    #     results = pd.DataFrame(results)
    #     results = results.rename(columns={0: 'id', 1: 'name', 2: 'taxon_id', 3: 'status', 4: 'is_latest', 5:'taxon_name_id' })
    #
    if source not in ['red','protected','sensitive']:
        df = df.merge(results[(results['is_latest']==1)&(results['status']=='accepted')], on='taxon_id')
    # df = df.rename(columns={'name_x': 'name', 'name_y': 'accepted_name'})
    #
    original_df = pd.DataFrame(columns=['taxon_name_id','original_taxon_name_id','name']) # 可能沒有資料
    query = f"SELECT id, original_taxon_name_id, name FROM taxon_names WHERE original_taxon_name_id IS NOT NULL \
                and `name` IN {str(list(results['name'].unique())).replace('[','(').replace(']',')')}"
    with conn.cursor() as cursor:
        cursor.execute(query)
        original_df = cursor.fetchall()
        original_df = pd.DataFrame(original_df)
        original_df = original_df.rename(columns={0: 'taxon_name_id', 1: 'original_taxon_name_id', 2: 'name'})
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
        # 存入資料庫
        # with conn.cursor() as cursor:
        #     cursor.execute(query,(chosen_row.taxon_id.values[0], row['name'], int(chosen_row.cites_id.values[0]), chosen_row.cites_listing.values[0], json.dumps(cites_note)))
        #     conn.commit()
    else:
        no_data.append(t)
    return notes, chosen_row





# TODO api_taxon_usages中如果有被刪除的要拿掉

# 1 api_citations

def to_firstname_abbr(string):
    s_list = re.split(r'[\s|\-]', string)
    for i in range(len(s_list)):
        if len(s_list[i]) == 1 or re.match(r"(\w[\.]).*", s_list[i]):  # 本身只有一個字母或本身是縮寫
            c = s_list[i]
        else:
            c = re.sub(r"(\w).*", r'\1.', s_list[i])
        if i == 0:
            full_abbr = c
        else:
            full_abbr += '-' + c
    return full_abbr

def to_middlename_abbr(content):
    if re.match(r"(\w[\.]).*", content):  # 本身是縮寫
        return re.sub(r"(\w[\.]).*", r"\1", content)
    elif len(content) == 1:  # 本身只有一個字
        return content
    else:
        return re.sub(r"(\w).*", r"\1.", content)

def create_citations(id_list):
    query = f"SELECT p.last_name, p.first_name, p.middle_name, pr.reference_id, pr.order, r.publish_year \
            FROM person_reference pr \
            JOIN persons p ON pr.person_id = p.id \
            JOIN `references` r on pr.reference_id = r.id \
            WHERE r.id IN ({','.join(id_list)});"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = pd.DataFrame(cursor.fetchall(), columns=['last_name', 'first_name', 'middle_name', 'reference_id', 'order', 'year'])
    # author, shourt autor
    citation_df = []
    for g in results.reference_id.unique():
        rows = results[results['reference_id'] == g].sort_values('order')
        author_list = []
        short_author_list = []
        for i, r in rows.iterrows():
            last_name = r['last_name']
            first_name = to_firstname_abbr(r['first_name'])
            middle_name = to_middlename_abbr(r['middle_name'])
            full_name = f"{last_name}, {middle_name}{first_name}"
            author_list.append(full_name)
            short_author_list.append(last_name)
        if len(author_list) == 1:
            authors = author_list[0]
            short_authors = short_author_list[0]
        elif len(author_list) == 2:
            authors = ' & '.join(author_list)
            short_authors = ' & '.join(short_author_list)
        else:  # 三人或以上
            authors = ', '.join(author_list[:-1]) + ' & ' + author_list[-1]
            short_authors = short_author_list[0] + ' et al.'
        citation_df.append((g, f'{authors} ({rows.year.unique()[0]})', f'{short_authors}, {rows.year.unique()[0]}'))
    citation_df = pd.DataFrame(citation_df, columns=['reference_id','author', 'short_author'])
    # content
    query = f"SELECT r.id, r.type, r.title, r.properties FROM `references` r WHERE r.id IN ({','.join(id_list)});"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = pd.DataFrame(cursor.fetchall(), columns=['id', 'type', 'title', 'properties'])
    for i in results.index:
        row = results.iloc[i]
        prop = json.loads(row.properties)
        # 書籍
        if row.type == 3:
            content = row.title
            if content[-1] != '.':
                content += '.'
        # 名錄
        elif row.type == 4:
            content = row.title
        # 期刊文章
        elif row.type == 1:
            content = f"{prop.get('article_title')}. <i>{prop.get('book_title')}</i> {prop.get('volume')}"
            if prop.get('issue'):
                content += f"({prop.get('issue')})"
            content += f": {prop.get('pages_range')}."
        # 書籍篇章
        elif row.type == 2:
            content = f"{prop.get('article_title')}. In: {prop.get('book_title')},"
            if prop.get('edition'):
                content += f" {prop.get('edition')} ed.,"
            if prop.get('volume'):
                content += f" vol. {prop.get('volume')}."
            elif prop.get('chapter'):
                content += f" ch. {prop.get('chapter')}."
            content += f" {prop.get('pages_range')}."
        citation_df.loc[citation_df['reference_id']==row.id, 'content'] = content
    return citation_df

# 對應references, 抓更新時間or建立時間大於api_citations的最後更新時間

conn = pymysql.connect(**db_settings)

query = "select id from `references` where created_at > (select max(updated_at) from api_citations) or updated_at > (select max(updated_at) from api_citations);"
# query = "select id from `references`"
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

id_list = [str(r[0]) for r in results]

# 1-1 新增的文獻 -> 直接寫入api_citations
# 1-2 修改的文獻
if id_list:
    citation_df = create_citations(id_list)
    conn = pymysql.connect(**db_settings)
    for i in citation_df.index:
        row = citation_df.iloc[i]
        with conn.cursor() as cursor:
            query = "INSERT INTO api_citations (reference_id, author, short_author, content, updated_at) VALUES(%s, %s, %s, %s, CURRENT_TIMESTAMP) \
                    ON DUPLICATE KEY UPDATE author=%s, short_author=%s, content=%s, updated_at = CURRENT_TIMESTAMP; "        
            cursor.execute(query, (row.reference_id, row.author, row.short_author, row.content, row.author, row.short_author, row.content))
            conn.commit()


# 2 api_names
def create_names(name_list, hybrid_name_list):
    rows = []
    if name_list:
        query = f"SELECT rank_id, nomenclature_id, properties, id, `name` FROM taxon_names WHERE id IN ({','.join(name_list)})"
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        for r in results:
            pp = json.loads(r[2])
            if r[0] < 30:  # rank 為屬以上
                formatted_name = pp.get('latin_name')
            elif r[0] in [30,31,32,33]:  # rank 為屬 / 亞屬 /組 /亞組
                if r[1] == 2 and pp.get('is_hybrid_formula'):  # 命名規約為植物且為雜交
                    formatted_name = f"× <i>{pp.get('latin_name')}</i>"
                else:
                    formatted_name = f"<i>{pp.get('latin_name')}</i>"
            elif r[0] == 34:  # rank 為種
                if r[1] == 2 and pp.get('is_hybrid_formula'):  # 命名規約為植物且為雜交
                    formatted_name = f"<i>{pp.get('latin_genus')}</i> × <i>{pp.get('latin_s1')}</i>"
                else:
                    formatted_name = f"<i>{pp.get('latin_genus')} {pp.get('latin_s1')}</i>"
            elif r[0] > 34:  # 種下 & 種下下
                if r[1] == 1:  # 命名規約為動物
                    count = 0
                    for l in pp.get('species_layers'):
                        if count == 0:  # 種下rank不顯示
                            # latin genus 可能是空的 & latin s1
                            if pp.get('latin_genus') and pp.get('latin_s1'):
                                formatted_name = f"<i>{pp.get('latin_genus')} {pp.get('latin_s1')} {l.get('latin_name')}</i>"
                            else:
                                query = f"SELECT properties FROM taxon_names WHERE id = {pp.get('species_id')}"
                                conn = pymysql.connect(**db_settings)
                                with conn.cursor() as cursor:
                                    cursor.execute(query)
                                    n = cursor.fetchall()
                                    np = json.loads(n[0][0])
                                    formatted_name = f"<i>{np.get('latin_genus')} {np.get('latin_s1')} {l.get('latin_name')}</i>"
                        else:  # 種下下rank需顯示
                            formatted_name += f" {l.get('rank_abbreviation')} <i>{l.get('latin_name')}</i>"
                        count += 1
                else:  # 命名規約為植物
                    # latin genus 可能是空的 & latin s1
                    if pp.get('latin_genus') and pp.get('latin_s1'):
                        formatted_name = f"<i>{pp.get('latin_genus')} {pp.get('latin_s1')}</i>"
                    else:
                        query = f"SELECT properties FROM taxon_names WHERE id = {pp.get('species_id')}"
                        conn = pymysql.connect(**db_settings)
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            n = cursor.fetchall()
                            np = json.loads(n[0][0])
                            formatted_name = f"<i>{np.get('latin_genus')} {np.get('latin_s1')}</i>"
                    for l in pp.get('species_layers'):
                        formatted_name += f" {l.get('rank_abbreviation')} <i>{l.get('latin_name')}</i>"
            else:
                formatted_name = r[4]
            rows.append([r[3], formatted_name])
    # 雜交組合最後處理（要等學名已經建立）
    if hybrid_name_list:
        query = f"WITH view as (SELECT tnhp.taxon_name_id, an.formatted_name FROM taxon_name_hybrid_parent tnhp \
                JOIN api_names an ON tnhp.parent_taxon_name_id = an.taxon_name_id \
                WHERE tnhp.taxon_name_id IN ({','.join(hybrid_name_list)}) \
                ORDER BY tnhp.order) \
                SELECT taxon_name_id, group_concat(formatted_name SEPARATOR ' × ') FROM view \
                GROUP BY taxon_name_id \
                "
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        for r in results:
            rows.append([r[0], r[1]])
    return rows


query = "SELECT id FROM taxon_names WHERE rank_id <> 47 AND (created_at > (select max(updated_at) from api_names) or updated_at > (select max(updated_at) from api_names))"
# query = "SELECT id FROM taxon_names WHERE rank_id <> 47 AND (created_at > '2022-08-10' or updated_at > '2022-08-10')"
# query = "SELECT taxon_name_id FROM api_names WHERE name_author is null and taxon_name_id not in (select id FROM taxon_names WHERE rank_id = 47)"
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

name_list = [str(r[0]) for r in results]



# 如果parent name有修改再修改
# 原本這邊會有bug 有可能後來才把雜交親代加進去，但本身的name沒有更新，所以改成全選
query = "SELECT tnhp.taxon_name_id FROM taxon_name_hybrid_parent tnhp \
    JOIN taxon_names tn ON tnhp.parent_taxon_name_id = tn.id"
 # WHERE tn.created_at > (select max(updated_at) from api_names) or tn.updated_at > (select max(updated_at) from api_names) \

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

hybrid_name_list = [str(r[0]) for r in results]


rows = create_names(name_list, hybrid_name_list)

conn = pymysql.connect(**db_settings)

count = 0
for r in rows:
    count += 1
    if count % 100 == 0:
        print(count)
    with conn.cursor() as cursor:
        query = "INSERT INTO api_names (taxon_name_id, formatted_name, updated_at) VALUES(%s, %s, CURRENT_TIMESTAMP) \
                ON DUPLICATE KEY UPDATE formatted_name=%s, updated_at = CURRENT_TIMESTAMP; "        
        cursor.execute(query, (r[0], r[1], r[1]))
        conn.commit()

# name_author另外處理
# TODO 這邊要修改formatted_authors 可能為空
if name_list:
    query = f"select id, nomenclature_id, rank_id, name, original_taxon_name_id, formatted_authors, publish_year from \
            taxon_names where id in ({','.join(name_list)});"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetchall()
        df = pd.DataFrame(df)
        df.columns = ['taxon_name_id', 'nomenclature_id', 'rank_id', 'name', 'original_taxon_name_id', 'formatted_authors', 'publish_year']
    # 取得作者資訊
    query = """SELECT p.last_name, p.abbreviation_name, ptn.taxon_name_id, ptn.order, ptn.role FROM person_taxon_name ptn
                LEFT JOIN persons p ON ptn.person_id = p.id """
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        author = cursor.fetchall()
        author = pd.DataFrame(author)
        author.columns = ['last_name', 'name_abbr', 'taxon_name_id', 'order', 'role']
    df = df.replace({np.nan: None})
    for i in df.index:
        print(i)
        row = df.iloc[i]
        original_taxon_name_id = None
        author_str = ''
        p_year = None
        # 動物
        if row.nomenclature_id == 1:
            # 原始組合名
            if original_taxon_name_id := row.original_taxon_name_id:
                tmp = author[author.taxon_name_id==row.original_taxon_name_id].sort_values('order')
                names = [t for t in tmp.last_name]
                if len(df[df.taxon_name_id==row.original_taxon_name_id]):
                    p_year = df[df.taxon_name_id==row.original_taxon_name_id].publish_year.values[0]
            else:
                tmp = author[author.taxon_name_id==row.taxon_name_id].sort_values('order')
                names = [t for t in tmp.last_name]
                p_year = row.publish_year
            # 也有可能沒有名字
            if len(names) == 0:
                if p_year not in [None, 0, '0', '']:
                    author_str = p_year
            elif len(names) == 1:
                if p_year not in [None, 0, '0', '']:
                    author_str = f'{names[0]}, {p_year}'
                else:
                    author_str = f'{names[0]}'
            elif len(names) == 2:
                if p_year not in [None, 0, '0', '']:
                    author_str = f'{names[0]} & {names[1]}, {p_year}'
                else:
                    author_str = f'{names[0]} & {names[1]}'
            elif len(names) > 2:
                count = 0
                for n in names:
                    count += 1
                    if count == 1:
                        author_str = n
                    elif count != len(names):
                        author_str += ', ' + n
                    else: # 最後一個學名
                        if p_year not in [None, 0, '']:
                            author_str += f" & {n}, {p_year}"
                        else:
                            author_str += f" & {n}"
            # 原始組合名
            if row.original_taxon_name_id:
                if len(df[df.taxon_name_id==row.original_taxon_name_id]):
                    # 如果同屬，不加括號
                    if row['name'].split(' ')[0] == df[df.taxon_name_id==row.original_taxon_name_id]['name'].values[0].split(' ')[0]:
                        pass
                    else:
                        author_str = '(' + author_str + ')'
        # 植物
        elif row.nomenclature_id == 2:
            # 基礎名
            if original_taxon_name_id := row.original_taxon_name_id:
                tmp = author[(author.taxon_name_id==row.taxon_name_id)&(author.role!=1)].sort_values('order')
                names = [t for t in tmp.name_abbr]
                tmp = author[(author.taxon_name_id==row.taxon_name_id)&(author.role==1)].sort_values('order')
                ex_names = [t for t in tmp.name_abbr]
                tmp = author[(author.taxon_name_id==row.original_taxon_name_id)&(author.role==1)].sort_values('order')
                ori_ex_names = [t for t in tmp.name_abbr]
                tmp = author[(author.taxon_name_id==row.original_taxon_name_id)&(author.role!=1)].sort_values('order')
                ori_names = [t for t in tmp.name_abbr]
            else:
                tmp = author[(author.taxon_name_id==row.taxon_name_id)&(author.role!=1)].sort_values('order')
                names = [t for t in tmp.name_abbr]
                tmp = author[(author.taxon_name_id==row.taxon_name_id)&(author.role==1)].sort_values('order')
                ex_names = [t for t in tmp.name_abbr]
            if len(names) == 1:
                author_str = names[0]
            elif len(names) == 2:
                author_str = f'{names[0]} & {names[1]}'
            elif len(names) > 2:
                count = 0
                for n in names:
                    count += 1
                    if count == 1:
                        author_str = n
                    elif count != len(names):
                        author_str += ', ' + n
                    else: # 最後一個學名
                        author_str += f" & {n}"
            # 前述名
            ex_author_str = None
            if len(ex_names) == 1:
                ex_author_str = ex_names[0]
            elif len(ex_names) == 2:
                ex_author_str = f'{ex_names[0]} & {ex_names[1]}'
            elif len(ex_names) > 2:
                count = 0
                for n in ex_names:
                    count += 1
                    if count == 1:
                        ex_author_str = n
                    elif count != len(ex_names):
                        ex_author_str += ', ' + n
                    else: # 最後一個學名
                        ex_author_str += f" & {n}"
            if ex_author_str:
                author_str = ex_author_str + ' ex ' + author_str
            # 基礎名
            ori_ex_author_str = None
            if row.original_taxon_name_id:    
                if len(ori_ex_names) == 1:
                    ori_ex_author_str = ori_ex_names[0]
                elif len(ori_ex_names) == 2:
                    ori_ex_author_str = f'{ori_ex_names[0]} & {ori_ex_names[1]}'
                elif len(ori_ex_names) > 2:
                    count = 0
                    for n in ori_ex_names:
                        count += 1
                        if count == 1:
                            ori_ex_author_str = n
                        elif count != len(ori_ex_names):
                            ori_ex_author_str += ', ' + n
                        else: # 最後一個學名
                            ori_ex_author_str += f" & {n}"
                if len(ori_names) == 1:
                    ori_author_str = ori_names[0]
                elif len(ori_names) == 2:
                    ori_author_str = f'{ori_names[0]} & {ori_names[1]}'
                elif len(ori_names) > 2:
                    count = 0
                    for n in ori_names:
                        count += 1
                        if count == 1:
                            ori_author_str = n
                        elif count != len(ori_names):
                            ori_author_str += ', ' + n
                        else: # 最後一個學名
                            ori_author_str += f" & {n}"
                if ori_ex_author_str:
                    author_str = f"({ori_ex_author_str} ex {ori_author_str}) {author_str}"
                else:
                    author_str = f"({ori_author_str}) {author_str}"
        df.loc[i,'formatted_author'] = author_str.strip()
    df['formatted_author'] = df['formatted_author'].apply(str.strip)
    conn = pymysql.connect(**db_settings)
    df = df[df.formatted_author!=''].reset_index(drop=True)
    for i in df.index:
        row = df.loc[i]
        print(i)
        query = f'UPDATE api_names SET name_author = "{row.formatted_author}", updated_at = CURRENT_TIMESTAMP WHERE taxon_name_id = {row.taxon_name_id}'
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()




# 3 api_taxon_usages
# 取得所有相關的學名
def get_related_names(taxon_name_id, df, new_names):
    new_names.remove(taxon_name_id)  # remove current taxon_name_id
    query = f'''SELECT ru.reference_id, ru.`group`, ru.id, tn.rank_id, ru.status FROM reference_usages ru
                JOIN taxon_names tn ON ru.taxon_name_id = tn.id
                WHERE ru.is_title != 1 AND ru.taxon_name_id = {taxon_name_id} AND ru.status NOT IN ("", "undetermined")'''
    with conn.cursor() as cursor:
        cursor.execute(query)
        ref_group_pair = cursor.fetchall()
    query = f'SELECT ru.taxon_name_id, ru.status FROM reference_usages ru \
                INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                WHERE ru.status NOT IN ("", "undetermined") AND ru.is_title != 1 '
    p_query = ''
    for p in range(len(ref_group_pair)):
        df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 
                    'group': ref_group_pair[p][1], 'taxon_name_id': taxon_name_id, 'rank_id': ref_group_pair[p][3], 'status': ref_group_pair[p][4]}, ignore_index=True)
        if p < max(range(len(ref_group_pair))):
            p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) OR'
        else:
            p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) '
    if p_query:
        query += f'AND ({p_query})'
    with conn.cursor() as cursor:
        cursor.execute(query)
        names = cursor.fetchall()
        # names = [l[0] for l in names]
        new_names += [n[0] for n in names if n[0] not in list(df.taxon_name_id) and n[1] == 'accepted']
    return new_names, df

# 選出新增的reference_usages
# 新增的reference_usages不一定是最新的
# 抓出相關的names
conn = pymysql.connect(**db_settings)
results = pd.DataFrame()

query = 'select ru.id, ru.reference_id, ru.taxon_name_id, ru.`group`, tn.rank_id, "new" from reference_usages ru \
         join taxon_names tn ON ru.taxon_name_id = tn.id \
         where ru.is_title != 1 and ru.created_at > (select max(updated_at) from api_taxon_usages) \
         and JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 and ru.status NOT IN ("", "undetermined");'
query = 'select ru.id, ru.reference_id, ru.taxon_name_id, ru.`group`, tn.rank_id, "new", ru.status from reference_usages ru \
         join taxon_names tn ON ru.taxon_name_id = tn.id \
         where ru.is_title != 1 and ru.created_at > "2022-08-11" \
         and JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 and ru.status NOT IN ("", "undetermined");'
query = 'select ru.id, ru.reference_id, ru.taxon_name_id, ru.`group`, tn.rank_id, "new", ru.status from reference_usages ru \
         join taxon_names tn ON ru.taxon_name_id = tn.id \
         where ru.is_title != 1 and ru.id = 88844 \
         and JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 and ru.status NOT IN ("", "undetermined");'
with conn.cursor() as cursor:
    cursor.execute(query)
    tmp = cursor.fetchall()
    tmp = pd.DataFrame(tmp, columns=['id','reference_id','taxon_name_id','group', 'rank_id', 'ru_status', "status"])
    results = results.append(tmp, ignore_index=True)

query = 'select ru.id, ru.reference_id, ru.taxon_name_id, ru.`group`, tn.rank_id, "updated" from reference_usages ru \
         join taxon_names tn ON ru.taxon_name_id = tn.id \
         where ru.is_title != 1 and ru.created_at <= (select max(updated_at) from api_taxon_usages) and ru.updated_at > (select max(updated_at) from api_taxon_usages) \
         and JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 and ru.status NOT IN ("", "undetermined");'
with conn.cursor() as cursor:
    cursor.execute(query)
    tmp = cursor.fetchall()
    tmp = pd.DataFrame(tmp, columns=['id','reference_id','taxon_name_id','group', 'rank_id', 'ru_status'])
    results = results.append(tmp, ignore_index=True)


checked_name_id = []
count = 0
total_df = pd.DataFrame()
conn = pymysql.connect(**db_settings)

for i in results.index:
    if i % 100 == 0:
        print(i)
    row = results.iloc[i]
    if row.taxon_name_id not in checked_name_id:
        count += 1
        tmp_taxon_id = count
        checked_name_id += [row.taxon_name_id]
        name_list = [row.taxon_name_id]
        new_names = []
        df = pd.DataFrame()
        # get all reference_id & group
        # TODO 這邊可以改成query一次就好嗎
        # 1 找到所有的reference & group
        query = f'''SELECT ru.reference_id, ru.`group`, ru.id, tn.rank_id, ru.status FROM reference_usages ru
                    JOIN taxon_names tn ON ru.taxon_name_id = tn.id
                    WHERE ru.is_title != 1 AND ru.taxon_name_id = {row.taxon_name_id} AND ru.status NOT IN ("", "undetermined")'''
        with conn.cursor() as cursor:
            cursor.execute(query)
            ref_group_pair = cursor.fetchall()
        # 根據有的 reference_id & group 再去抓抓看有沒有別的name_id (需排除status為空值或未決的資料)
        # ??? 不確定先移除此處理 -> 如果有其他name_id的話，就有可能是不同rank，需要指定rank
        # TODO 這邊可以改成query一次就好嗎
        # 2 找到所有reference & group裡有的學名
        query = f'SELECT ru.taxon_name_id, ru.status FROM reference_usages ru \
                    INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                    WHERE ru.status NOT IN ("", "undetermined") and ru.is_title != 1 '
        p_query = ''
        for p in range(len(ref_group_pair)):
            df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 'group': ref_group_pair[p]
                        [1], 'taxon_name_id': row.taxon_name_id, 'rank_id': ref_group_pair[p][3], 'status': ref_group_pair[p][4]}, ignore_index=True)
            if p < max(range(len(ref_group_pair))):
                p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) OR'
            else:
                p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) '
        if p_query:
            query += f'AND ({p_query})'
        with conn.cursor() as cursor:
            cursor.execute(query)
            names = cursor.fetchall()
            # names = [l[0] for l in names]
            # 如果reference & group中有新的學名，且為accepted，則在找他對應的所有reference & group
            new_names += [n[0] for n in names if n[0] not in name_list and n[1] == 'accepted']  # 用來確定是不是還有name需要跑
        while len(new_names) > 0:
            for nn in new_names:
                checked_name_id += [nn]
                # TODO 只抓status是accepted的new_names
                new_names, df = get_related_names(nn, df, new_names)
        # df = df.astype('int32')
        # 排除掉related_name中 status是not-accepted的name
        df = df.drop_duplicates().reset_index()
        # 目前有些ref group會缺資料，再抓回來
        final_ref_group_pair = []
        for f in df[['reference_id','group']].drop_duplicates().index:
            final_ref_group_pair += [(df.iloc[f].reference_id, df.iloc[f].group)]
        query = f'SELECT ru.id, ru.reference_id, ru.group, ru.taxon_name_id, tn.rank_id, ru.status FROM reference_usages ru \
                    INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                    WHERE ru.status NOT IN ("", "undetermined") and ru.is_title != 1 '
        p_query = ''
        for p in range(len(final_ref_group_pair)):
            if p < max(range(len(final_ref_group_pair))):
                p_query += f' (ru.reference_id = {final_ref_group_pair[p][0]} AND ru.`group` = {final_ref_group_pair[p][1]}) OR'
            else:
                p_query += f' (ru.reference_id = {final_ref_group_pair[p][0]} AND ru.`group` = {final_ref_group_pair[p][1]}) '
        if p_query:
            query += f'AND ({p_query})'
        with conn.cursor() as cursor:
            cursor.execute(query)
            final_df = cursor.fetchall()
            final_df = pd.DataFrame(final_df, columns=['ru_id', 'reference_id', 'group', 'taxon_name_id', 'rank_id', 'status'])
            # TODO 根據 reference / group 分組
            # TODO 檢查如果沒有misapplied的情況
            # 同一個taxon_id的條件 -> 1 同一組 ref & group, 2 有一樣的name?
        final_df['tmp_taxon_id'] = tmp_taxon_id
        total_df = total_df.append(final_df, ignore_index=True)

total_df = total_df.drop_duplicates() # 155
# total_df = total_df.astype('int32')


# 取最新接受名，其他為同物異名或誤用名
# reference_id, group, taxon_name_id
# 抓status, publish_year
conn = pymysql.connect(**db_settings)

results = pd.DataFrame()
query = f"""SELECT id, publish_year, JSON_EXTRACT(properties, "$.doi") FROM `references` 
            WHERE id IN ({str(total_df.reference_id.to_list()).replace('[','').replace(']','')})"""

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'reference_id', 1: 'publish_year', 2: 'doi'})

total_df = total_df.merge(results)


# str(list(total_df.ru_id.unique())).replace('[','').replace(']','')

query = f"""SELECT id, status, taxon_name_id, parent_taxon_name_id FROM reference_usages 
          WHERE id IN ({str(list(total_df.ru_id.to_list())).replace('[','').replace(']','')})"""

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'ru_id', 1: 'status', 2: 'taxon_name_id', 3: 'parent_taxon_name_id'})

total_df = total_df.merge(results)


# 如果ru已存在則更新, 若不存在則新增
# 決定誰是接受學名
taxon_list = total_df.tmp_taxon_id.unique()

total_df['taxon_status'] = ''
total_df['is_latest'] = False
total_df['publish_date'] = ''

cannot_decide = []
for t in taxon_list:
    print(t)
    temp = total_df[total_df['tmp_taxon_id'] == t]
    # 如果有大於一個reference_id, 比較年份
    yr = temp[['reference_id', 'publish_year']].drop_duplicates()
    max_yr = yr.publish_year.max()
    if len(yr[yr['publish_year'] == max_yr]) > 1:
        ref_list = yr[yr['publish_year'] == max_yr].reference_id.to_list()
        if len(yr[yr['publish_year'] == max_yr]) == 2 and 153 in ref_list:
            # 代表和taicol backbone同年份，優先選擇非taicol backbone的文獻
            ref_list.remove(153)
            chosen_ref_id = ref_list[0]
            total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['reference_id'] == chosen_ref_id), 'is_latest'] = True
        else:
            for d in temp.doi.unique():
                if d:
                    d_str = d.replace('"', '').replace("'", '')
                    url = f'https://api.crossref.org/works/{d_str}'
                    result = requests.get(url)
                    if result.status_code == 200:
                        result = result.json()
                        if result.get('message'):
                            try:
                                date_list = result.get('message').get('published-print').get('date-parts')[0]
                            except:
                                date_list = result.get('message').get('published-online').get('date-parts')[0]
                            try:
                                total_df.loc[total_df['doi'] == d, 'publish_date'] = datetime(date_list[0], date_list[1], date_list[2]).strftime("%Y-%m-%d")
                            except:
                                pass
            temp = total_df[total_df['tmp_taxon_id'] == t]
            dt = temp[['reference_id', 'publish_date']].drop_duplicates()
            max_dt = dt.publish_date.max()
            if len(dt[dt['publish_date'] == max_dt]) > 1:
                ref_list = dt[dt['publish_date'] == max_dt].reference_id.to_list()
                if len(dt[dt['publish_date'] == max_dt]) == 2 and 153 in ref_list:
                    # 代表和taicol backbone同年份，優先選擇非taicol backbone的文獻
                    ref_list.remove(153)
                    chosen_ref_id = ref_list[0]
                    total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['reference_id'] == chosen_ref_id), 'is_latest'] = True
                else:
                    cannot_decide += [t]
            else:
                total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_date'] == max_dt), 'is_latest'] = True
            # TODO 尚未完成
            # 如果年份一樣，比對publish_date，但如果無法取得publish_date?
            # 也排除ref=153的情況？
    else:
        total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_year'] == max_yr), 'is_latest'] = True


# 有misapplied的情況下，同一個taxon_name_id會對到不同taxon

for i in total_df.index:
    if i % 1000 == 0:
        print(i)
    row = total_df.iloc[i]
    if row.is_latest:
        if row.status == 'not-accepted':
            total_df.loc[i, 'taxon_status'] = 'not-accepted'
        else:
            total_df.loc[i, 'taxon_status'] = row.status
    else:  # 不是最新的文獻
        accepted_name_id = total_df.loc[(total_df['tmp_taxon_id'] == row.tmp_taxon_id) & (total_df['status'] == 'accepted')]['taxon_name_id'].to_list()[0]
        if row.taxon_name_id == accepted_name_id:
            # 如果和accpeted一樣的name, 設成accepted
            total_df.loc[i, 'taxon_status'] = 'accepted'
        else:
            if row.status == 'misapplied':
                # 如果不一樣，且是misapplied, 設成misapplied
                total_df.loc[i, 'taxon_status'] = row.status
            else:
                # 如果不一樣，且不是misapplied, 設成not-accepted
                total_df.loc[i, 'taxon_status'] = 'not-accepted'

# 先看目前的name有沒有對應的taxon_id
# 一個name只會對應到一個taxon -> 不會!!
conn = pymysql.connect(**db_settings)

query = f"SELECT reference_usage_id, taxon_id FROM api_taxon_usages WHERE reference_usage_id IN {str(total_df.ru_id.to_list()).replace('[','(').replace(']',')')}"
with conn.cursor() as cursor:
    cursor.execute(query)
    db = cursor.fetchall()
    db = pd.DataFrame(db, columns=['ru_id', 'taxon_id'])

total_df = total_df.merge(db, how='left')
# total_df.to_csv('test.csv', index=None)

# 更新 api_taxon_usages & api_taxon & api_taxon_history & api_taxon_tree


# 1 原本沒有taxon_id
# 1-1 新增taxon (api_taxon_usages & api_taxon & api_taxon_history & api_taxon_tree & api_conservation)
old_tmp_taxon_list = total_df[total_df['taxon_id'].notnull()].tmp_taxon_id.unique()
new_taxon_list = total_df[~total_df.tmp_taxon_id.isin(old_tmp_taxon_list)].tmp_taxon_id.unique() # 22

# 寫入taxon table

total_df = total_df.replace({np.nan: None})

for nt in new_taxon_list:
    print(nt)
    conn = pymysql.connect(**db_settings)
    rows = total_df[total_df['tmp_taxon_id']==nt]
    i = rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row
    # for i in rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index:
    row = total_df.iloc[i] # 接受的row
    rank_id = row.rank_id
    accepted_taxon_name_id = row.taxon_name_id
    ru_list = total_df[(total_df['tmp_taxon_id'] == nt) & (total_df['status'] == 'accepted')].ru_id.to_list()
    if len(ru_list) == 1:
        query = f'SELECT JSON_EXTRACT(properties, "$.common_names") FROM reference_usages WHERE id = {ru_list[0]}'
    else:
        query = f'SELECT JSON_EXTRACT(properties, "$.common_names")  FROM reference_usages WHERE id IN {tuple(ru_list)}'
    # common_names
    common_names = []
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        n_list = []
        for n in results:
            if n[0]:
                n_list += json.loads(n[0])
        for nn in n_list:
            if nn.get('language') == 'zh-tw' and nn.get('name') not in common_names:
                common_names.append(nn.get('name'))
    if common_names:
        common_name_c = common_names[0]
        if len(common_names) > 1:
            alternative_name_c = ', '.join(common_names[1:])
        else:
            alternative_name_c = None
    else:
        common_name_c, alternative_name_c = None, None
    # is_hybrid
    ru_list = total_df[total_df['tmp_taxon_id'] == nt].ru_id.to_list()
    if len(ru_list) == 1:
        query = f'SELECT JSON_EXTRACT(properties, "$.is_hybrid_formula") FROM reference_usages WHERE id = {ru_list[0]}'
    else:
        query = f'SELECT JSON_EXTRACT(properties, "$.is_hybrid_formula")  FROM reference_usages WHERE id IN {tuple(ru_list)}'
    is_hybrid = False
    is_hybrid_list = []
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        for r in results:
            is_hybrid_list.append(r[0])
    if any(t for t in is_hybrid_list):
        is_hybrid = True
    # other properties based on latest reference usages
    is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine = None, None, None, None, None, None, None
    ru = total_df[(total_df['tmp_taxon_id'] == nt) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].ru_id.to_list()[0]
    # ref = total_df[(total_df['tmp_taxon_id'] == nt) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].reference_id.to_list()[0]
    query = f'SELECT properties FROM reference_usages WHERE id = {ru} AND properties IS NOT NULL'
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        if results:
            results = json.loads(results[0][0])
            is_endemic = results.get('is_endemic')
            alien_type = results.get('alien_type')
            is_fossil = results.get('is_fossil')
            is_terrestrial = results.get('is_terrestrial')
            is_freshwater = results.get('is_freshwater')
            is_brackish = results.get('is_brackish')
            is_marine = results.get('is_marine')
        else:
            is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine = None, None, None, None, None, None, None
    # 當前的taxon_id
    query = "SELECT max(taxon_id) FROM api_taxon"
    with conn.cursor() as cursor:
        cursor.execute(query)
        current_taxon_id = cursor.fetchone()[0]
    taxon_id = 't' + format(int(current_taxon_id.split('t')[-1]) + 1, '06d') # t085247
    print(taxon_id)
    # 寫入 api_taxon table 
    query = f"INSERT INTO api_taxon (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c,  \
                is_hybrid, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine ) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c, is_hybrid,
                                is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine))
        conn.commit()
    # 寫入 api_taxon_history table
    query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id ) \
                VALUES (%s, %s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (5, taxon_id, '新增Taxon', ru))
        conn.commit()
    # 寫入 api_taxon_usages table
    for i in rows.index:
        current_row = total_df.iloc[i]
        is_latest = 1 if current_row.is_latest else 0
        # update or insert
        query = f"""INSERT INTO api_taxon_usages (reference_usage_id, taxon_id, status, is_latest, taxon_name_id, parent_taxon_name_id) \
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    is_latest = VALUES(is_latest),
                    taxon_id = VALUES(taxon_id),
                    parent_taxon_name_id = VALUES(parent_taxon_name_id),
                    updated_at = CURRENT_TIMESTAMP
                    """
        with conn.cursor() as cursor:
            cursor.execute(query, (current_row.ru_id, taxon_id, current_row.taxon_status, is_latest, current_row.taxon_name_id, current_row.parent_taxon_name_id))
            conn.commit()
    # api_conservation -> 要先建立taxon
    # 要用同一個taxon裡面所有的names去抓
    print('conserv')
    conservation_info = get_conservation_info(taxon_id)
    if conservation_info: # 如果有任何保育資訊的資料，新增/更新
        query = f"""
                INSERT INTO api_conservation
                (taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                    cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                    sensitive_default, sensitive_suggest,  sensitive_note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        with conn.cursor() as cursor:
            cursor.execute(query,conservation_info)
            conn.commit()
    # api_taxon_tree
    query = f"""
            INSERT INTO api_taxon_tree (taxon_id, path, parent_taxon_id)
            WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id, parent_taxon_id) AS
                (
                    SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id, b.taxon_id
                    FROM api_taxon_usages c
                    JOIN taxon_names t on c.taxon_name_id = t.id 
                            LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                    WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
                    UNION ALL
                    SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id, b.taxon_id
                    FROM find_ancestor cp
                    JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
                    JOIN taxon_names t on c.taxon_name_id = t.id 
                        LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                    WHERE c.is_latest = 1 and c.status = 'accepted' 
                )
            SELECT taxon_id, path, parent_taxon_id
            FROM find_ancestor WHERE taxon_id = '{taxon_id}';
            """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
    # 1-2 確認有沒有修改到階層 -> 也有可能修改到其他相關taxon的階層
    # 所有有該taxon_id為上階層的階層都要跟著改變
    query = f"""
        WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id, parent_taxon_id) AS
            (
                SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id, b.taxon_id
                FROM api_taxon_usages c
                JOIN taxon_names t on c.taxon_name_id = t.id 
                        LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
                UNION ALL
                SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id, b.taxon_id
                FROM find_ancestor cp
                JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
                JOIN taxon_names t on c.taxon_name_id = t.id 
                    LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                WHERE c.is_latest = 1 and c.status = 'accepted' 
            )
        SELECT taxon_id, path, parent_taxon_id
        FROM find_ancestor WHERE path LIKE '%{taxon_id}%' AND taxon_id != '{taxon_id}';
        """
    highers = []
    h_path = ''
    with conn.cursor() as cursor:
        cursor.execute(query)
        highers = cursor.fetchall()
    # TODO 這邊會有原本有path後來沒有的情況嗎
    for h in highers:
        query = f"SELECT path FROM api_taxon_tree WHERE taxon_id = '{h[0]}'"
        with conn.cursor() as cursor:
            cursor.execute(query)
            h_path = cursor.fetchone()
        if h_path:
            if h_path[0] != h[1]:
                # 修改taxon_tree
                query = f"UPDATE api_taxon_tree SET path = '{h[1]}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = '{h[2]}' WHERE taxon_id = '{h[0]}'"
                with conn.cursor() as cursor:
                        cursor.execute(query)
                        conn.commit()
                # insert api_taxon_history
                old_path_str_name = ''
                if h_path[0]:
                    o_path_list = h_path[0].split('>')
                    if len(o_path_list) > 1:
                        query = f"""SELECT an.formatted_name
                                    FROM api_taxon at
                                    JOIN api_names an ON an.taxon_name_id = at.accepted_taxon_name_id
                                    WHERE at.taxon_id IN ({str(o_path_list).replace('[','').replace(']','')})
                                    ORDER BY at.rank_id ASC
                                """
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            ops = cursor.fetchall()
                            ops = [o[0] for o in ops]
                            old_path_str_name = ('>').join(ops)
                            if old_path_str_name:
                                old_path_str_name = '原階層：'+old_path_str_name
                query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                    VALUES (%s, %s, %s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (4, h[0], json.dumps({'old': h_path[0], 'new': h[1] }), ru, old_path_str_name))
                    conn.commit()
    # links -> 會需要先知道階層
    print('link')
    if links := get_links(taxon_id):
        query =  f"""UPDATE api_taxon
                    SET links=%s
                    WHERE taxon_id = '{taxon_id}'
                    """
        with conn.cursor() as cursor:
            cursor.execute(query, (json.dumps(links)))
            conn.commit()



# 檢查是不是所有都有寫入Taxon tree -> 有缺是因為少了reference_usages





# 2 原本有taxon
# 先確認是不是有物種拆分 & 物種合併的情況
# * 物種合併：新文獻中，兩筆(或以上)Taxon id 出現在同一個分類群裡
# * 物種拆分：新文獻中，同一筆Taxon id 同時出現在兩個(或以上)分類群裡

check = total_df[(total_df.tmp_taxon_id.isin(old_tmp_taxon_list))&total_df.taxon_id.notnull()][['tmp_taxon_id', 'taxon_id']]
check = check.drop_duplicates()
# * 物種合併：新文獻中，兩筆(或以上)Taxon id 出現在同一個分類群裡
check_1_count = check.groupby('tmp_taxon_id').count()
# * 物種拆分：新文獻中，同一筆Taxon id 同時出現在兩個(或以上)分類群裡
check_2_count = check.groupby('taxon_id').count()




# TODO t034658 <--> t034891
# t001119 有usage但沒有taxon -> fix

# total_df = total_df.replace({'t034658':'t034891'})

if all(check_1_count==1) and all(check_2_count==1):
    # 2-1 確認文獻是不是已存在在其taxon，若沒有，api_taxon_history新增文獻
    conn = pymysql.connect(**db_settings)
    for t in old_tmp_taxon_list:
        # 抓原本已存在的
        taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
        current_ref = []
        query = f"SELECT DISTINCT(ru.reference_id) FROM api_taxon_usages atu\
                JOIN reference_usages ru ON atu.reference_usage_id = ru.id \
                WHERE atu.taxon_id = '{taxon_id}' and ru.is_title != 1"
        with conn.cursor() as cursor:
            cursor.execute(query)
            current_ref= cursor.fetchall()
            current_ref = [r[0] for r in current_ref]
        if len(total_df[(total_df['tmp_taxon_id']==t)&(~total_df['reference_id'].isin(current_ref))]):
            # 代表有新的文獻
            for i in total_df[(total_df['tmp_taxon_id']==t)&(~total_df['reference_id'].isin(current_ref))].reference_id:
                # TODO 這邊一個reference_id可能會多到多個reference_usage_id 不確定會不會有問題
                if i != 153: # backbone不算
                    ru = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['reference_id'] == i)].ru_id.to_list()[0]
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id ) \
                            VALUES (%s, %s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (2, taxon_id, json.dumps({'reference_id': int(i)}), ru))
                        conn.commit()






    # 寫入api_taxon_usages
    # 2022 08 10 t041903 t=1059 
    # 5 t006678
if all(check_1_count==1) and all(check_2_count==1):
    # 2-1 確認文獻是不是已存在在其taxon，若沒有，api_taxon_history新增文獻
    conn = pymysql.connect(**db_settings)
    for t in old_tmp_taxon_list[7:]:
        if t % 100 == 0:
            conn = pymysql.connect(**db_settings)
        rows = total_df[total_df['tmp_taxon_id']==t]
        taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
        print(t, taxon_id)
        # 是否有新增的taxon_name_id -> 新增同物異名
        # 也有可能是刪除嗎？ 暫不處理
        if any(rows.taxon_status == 'not-accepted'):
            query = f"""SELECT DISTINCT(atu.taxon_name_id) FROM api_taxon_usages atu 
                        JOIN api_names an ON atu.taxon_name_id = an.taxon_name_id
                        WHERE atu.taxon_id = '{taxon_id}' and atu.`status`='not-accepted'"""
            with conn.cursor() as cursor:
                cursor.execute(query)
                syns = cursor.fetchall()
                syns = [s[0] for s in syns]
            for new_syn in rows[(rows.taxon_status=='not-accepted')&~rows.taxon_name_id.isin(syns)].taxon_name_id.unique():
                ru = rows[(rows.taxon_status=='not-accepted')&(rows.taxon_name_id==new_syn)].ru_id.values[0]
                # 寫入api_taxon_history
                query = f"""SELECT formatted_name FROM api_names 
                            WHERE taxon_name_id = {new_syn}"""
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    syn_name = cursor.fetchone()
                query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                          VALUES (%s, %s, %s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (1, taxon_id, json.dumps({'taxon_name_id':int(new_syn)}), ru, syn_name[0]))
                    conn.commit()
        # 寫入api_taxon
        i = rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row index
        # for i in rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index:
        row = total_df.iloc[i] # 接受的row
        rank_id = row.rank_id
        accepted_taxon_name_id = row.taxon_name_id
        ru_list = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['status'] == 'accepted')].ru_id.to_list()
        ru = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].ru_id.to_list()[0]
        if len(ru_list) == 1:
            query = f'SELECT id, JSON_EXTRACT(properties, "$.common_names") FROM reference_usages WHERE id = {ru_list[0]}'
        else:
            query = f'SELECT id, JSON_EXTRACT(properties, "$.common_names")  FROM reference_usages WHERE id IN {tuple(ru_list)}'
        # common_names
        common_names = {'data':[], 'ru_id': []}
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            n_list = []
            for n in results:
                if n[1]:
                    n_list += [[n[0],json.loads(n[1])]]
            for nn in n_list:
                for nnn in nn[1]:
                    if nnn.get('language') == 'zh-tw' and nnn.get('name') not in common_names['data']:
                        common_names['data'].append(nnn.get('name'))
                        common_names['ru_id'].append(nn[0])
        if common_names['data']:
            common_name_c = common_names['data'][0]
            if len(common_names['data']) > 1:
                alternative_name_c = ', '.join(common_names['data'][1:])
            else:
                alternative_name_c = None
        else:
            common_name_c, alternative_name_c = None, None
        # 判斷中文名是不是有新增
        query = f"""SELECT concat_ws(',',at.common_name_c, at.alternative_name_c)
                    FROM api_taxon at 
                    WHERE at.taxon_id = '{taxon_id}'"""
        with conn.cursor() as cursor:
            cursor.execute(query)
            old_common_name_data = cursor.fetchone()
            if old_common_name_data:
                old_common_name_data = old_common_name_data[0].split(',')
                c_index = 0
                for new_c in common_names['data']:
                    if new_c not in old_common_name_data:
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                                    VALUES (%s, %s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            cursor.execute(query, (7, taxon_id, new_c, common_names['ru_id'][c_index], new_c))
                            conn.commit()
                    c_index += 1
                # 刪除不處理
        # is_hybrid
        # ru_list = total_df[total_df['tmp_taxon_id'] == t].ru_id.to_list()
        if len(ru_list) == 1:
            query = f'SELECT id, JSON_EXTRACT(properties, "$.is_hybrid_formula") FROM reference_usages WHERE id = {ru_list[0]}'
        else:
            query = f'SELECT id, JSON_EXTRACT(properties, "$.is_hybrid_formula")  FROM reference_usages WHERE id IN {tuple(ru_list)}'
        is_hybrid = 0
        is_hybrid_list = {'data':[],'ru_id':[]}
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            for r in results:
                is_hybrid_list['data'].append(r[1])
                is_hybrid_list['ru_id'].append(r[0])
        if any(is_hybrid_list['data']):
            is_hybrid = 1
        # 判斷is_hybrid有沒有改變
        query = f"SELECT is_hybrid FROM api_taxon WHERE taxon_id = '{taxon_id}'"
        with conn.cursor() as cursor:
            cursor.execute(query)
            old_is_hybird = cursor.fetchone()
        if old_is_hybird:
            old_is_hybird = old_is_hybird[0]
        if not old_is_hybird and is_hybrid: # 新增屬性
            ih_index = 0
            for ih in is_hybrid_list['data']:
                if ih:
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                                VALUES (%s, %s, %s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (8, taxon_id, 'is_hybrid', is_hybrid_list['ru_id'][ih_index],'雜交'))
                        conn.commit()
                ih_index += 1
        elif old_is_hybird and not is_hybrid: # 移除屬性 -> 用最新學名使用的代表
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                                            VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (9, taxon_id, 'is_hybrid', ru,'雜交'))
                conn.commit()
        # other properties based on latest reference usages
        is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine = None, None, None, None, None, None, None
        # ref = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].reference_id.to_list()[0]
        query = f'SELECT properties FROM reference_usages WHERE id = {ru}'
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            results = json.loads(results[0][0])
            is_endemic = results.get('is_endemic')
            alien_type = results.get('alien_type')
            is_fossil = results.get('is_fossil')
            is_terrestrial = results.get('is_terrestrial')
            is_freshwater = results.get('is_freshwater')
            is_brackish = results.get('is_brackish')
            is_marine = results.get('is_marine')
        # common_names_str = (',').join(common_names)
        new_taxon_data = (accepted_taxon_name_id, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine)
        # 比對和原本的是否相同
        query = f"""SELECT at.accepted_taxon_name_id, at.is_endemic, at.alien_type, at.is_fossil, at.is_terrestrial, 
                            at.is_freshwater, at.is_brackish, at.is_marine, an.formatted_name
                    FROM api_taxon at 
                    JOIN api_names an ON an.taxon_name_id = at.accepted_taxon_name_id
                    WHERE at.taxon_id = '{taxon_id}'"""
        with conn.cursor() as cursor:
            cursor.execute(query)
            taxon_data = cursor.fetchone()
        # if new_taxon_data != taxon_data:
            # 接受名改變
        if accepted_taxon_name_id != taxon_data[0]:
            # 
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
            VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (0, taxon_id, json.dumps({'old_taxon_name_id': taxon_data[0], 'new_taxon_name_id': accepted_taxon_name_id}), ru, "原有效名："+taxon_data[8]))
                conn.commit()
            # 如果接受名改變的話，相關連結&保育資訊也跟著修改 -> 一起寫在下面
        # 物種資訊更新 
        # 2 is_hybrid, 3 is_endemic, 5 is_fossil, 
        # 6 is_terrestrial, 7 is_freshwater, 8 is_brackish, 9 is_marine, 
        prop_dict = {1: 'is_endemic', 3: 'is_fossil', 4: 'is_terrestrial', 5: 'is_freshwater', 6: 'is_brackish', 7: 'is_marine'}
        prop_dict_c = {1: '臺灣特有', 3: '化石種', 4: '陸生', 5: '淡水', 6: '半鹹水', 7: '海水'}
        for p in prop_dict.keys():
            if not taxon_data[p] and new_taxon_data[p]: # 新增屬性
                query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                            VALUES (%s, %s, %s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (8, taxon_id, prop_dict[p], ru, prop_dict_c[p]))
                    conn.commit()
            elif taxon_data[p] and not new_taxon_data[p]: # 移除屬性
                query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                                                VALUES (%s, %s, %s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (9, taxon_id, prop_dict[p], ru, prop_dict_c[p]))
                    conn.commit()
        # 原生 / 外來
        # 4 alien_type 
        alien_map_c = {'native': '原生','naturalized':'歸化','invasive':'入侵','cultured':'栽培豢養'}
        if not taxon_data[2] and new_taxon_data[2]: # 新增屬性
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content) \
                        VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (14, taxon_id, new_taxon_data[2], ru, alien_map_c[new_taxon_data[2]]))
                conn.commit()
        elif taxon_data[p] and not new_taxon_data[p]: # 移除屬性
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content) \
                                            VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (15, taxon_id, taxon_data[p], ru, alien_map_c[new_taxon_data[2]]))
                conn.commit()
        elif taxon_data[2] and new_taxon_data[2] and new_taxon_data[2] != taxon_data[2]:
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content) \
                                                VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (10, taxon_id, json.dumps({'old':taxon_data[4], 'new': new_taxon_data[4], 'type': 'alien_type'}), ru, alien_map_c[taxon_data[4]]+"改為"+alien_map_c[new_taxon_data[4]]))
                conn.commit()
        # 階層更新 -> 也有可能是新增或是移除？
        new_path = []
        old_path = []
        query = f"""
                WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id, parent_taxon_id) AS
                    (
                        SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id, b.taxon_id
                        FROM api_taxon_usages c
                        JOIN taxon_names t on c.taxon_name_id = t.id 
                                LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                        WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
                        UNION ALL
                        SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id, b.taxon_id
                        FROM find_ancestor cp
                        JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
                        JOIN taxon_names t on c.taxon_name_id = t.id 
                            LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                        WHERE c.is_latest = 1 and c.status = 'accepted' 
                    )
                SELECT taxon_id, path, parent_taxon_id
                FROM find_ancestor WHERE taxon_id = '{taxon_id}';
                """
        with conn.cursor() as cursor:
            cursor.execute(query)
            new_path = cursor.fetchone()
        query = f"SELECT taxon_id, path, parent_taxon_id FROM api_taxon_tree WHERE taxon_id = '{taxon_id}'"
        with conn.cursor() as cursor:
            cursor.execute(query)
            old_path = cursor.fetchone()
        if new_path != old_path:
            # path_list = []
            # parent_taxon_id = ''
            if old_path:
                old_path_str =  old_path[1]
            else:
                old_path_str = None
            if new_path:
                new_path_str =  new_path[1]
                parent_taxon_id = new_path[2]
            else:
                new_path_str = None
            old_path_str_name = ''
            if old_path_str:
                o_path_list = old_path_str.split('>')
                if len(o_path_list) > 1:
                    query = f"""SELECT an.formatted_name
                                FROM api_taxon at
                                JOIN api_names an ON an.taxon_name_id = at.accepted_taxon_name_id
                                WHERE at.taxon_id IN ({str(o_path_list).replace('[','').replace(']','')})
                                ORDER BY at.rank_id ASC
                            """
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        ops = cursor.fetchall()
                        ops = [o[0] for o in ops]
                        old_path_str_name = ('>').join(ops)
                        if old_path_str_name:
                            old_path_str_name = '原階層：'+old_path_str_name
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                        VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (4, taxon_id, json.dumps({'old': old_path_str, 'new': new_path_str }), ru, old_path_str_name))
                conn.commit()
            if new_path and old_path: # 更新
                if parent_taxon_id:
                    query = f"UPDATE api_taxon_tree SET path = '{new_path_str}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = '{parent_taxon_id}' WHERE taxon_id = '{taxon_id}'"
                else:
                    query = f"UPDATE api_taxon_tree SET path = '{new_path_str}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = NULL WHERE taxon_id = '{taxon_id}'"
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()
            elif not new_path and old_path: # 移除
                query = f"DELETE FROM api_taxon_tree WHERE taxon_id = '{taxon_id}'"
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()
                # 從 api_taxon_tree 移除
            elif new_path and not old_path: # 新增
                # 從 api_taxon_tree 新增
                query = f"""INSERT INTO api_taxon_tree (taxon_id, path, parent_taxon_id) VALUES (%s, %s, %s)"""
                with conn.cursor() as cursor:
                    cursor.execute(query, (taxon_id, new_path_str, parent_taxon_id))
                    conn.commit()
            # 其他相關的階層
            query = f"""SELECT path, taxon_id, parent_taxon_id FROM api_taxon_tree 
                        WHERE path LIKE '%{taxon_id}%' AND taxon_id != '{taxon_id}';
                        """
            highers = []
            h_path = ''
            with conn.cursor() as cursor:
                cursor.execute(query)
                highers = cursor.fetchall()
            # TODO 這邊會有原本有path後來沒有的情況嗎
            for h in highers:
                # query 當前的path
                query = f"""
                        WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id, parent_taxon_id) AS
                            (
                                SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id, b.taxon_id
                                FROM api_taxon_usages c
                                JOIN taxon_names t on c.taxon_name_id = t.id 
                                        LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                                WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
                                UNION ALL
                                SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id, b.taxon_id
                                FROM find_ancestor cp
                                JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
                                JOIN taxon_names t on c.taxon_name_id = t.id 
                                    LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
                                WHERE c.is_latest = 1 and c.status = 'accepted' 
                            )
                        SELECT taxon_id, path, parent_taxon_id
                        FROM find_ancestor WHERE taxon_id = '{h[1]}';
                        """
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    new_h_path = cursor.fetchone()
                    old_path_str_name = ''
                    if h[0]:
                        o_path_list = h[0].split('>')
                        if len(o_path_list) > 1:
                            query = f"""SELECT an.formatted_name
                                        FROM api_taxon at
                                        JOIN api_names an ON an.taxon_name_id = at.accepted_taxon_name_id
                                        WHERE at.taxon_id IN ({str(o_path_list).replace('[','').replace(']','')})
                                        ORDER BY at.rank_id ASC
                                    """
                            with conn.cursor() as cursor:
                                cursor.execute(query)
                                ops = cursor.fetchall()
                                ops = [o[0] for o in ops]
                                old_path_str_name = ('>').join(ops)
                                if old_path_str_name:
                                    old_path_str_name = '原階層：'+old_path_str_name
                    # 更新
                    if new_h_path:
                        if new_h_path[0] != h[0]:
                            # 修改taxon_tree
                            query = f"UPDATE api_taxon_tree SET path = '{h[0]}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = '{h[2]}' WHERE taxon_id = '{h[1]}'"
                            with conn.cursor() as cursor:
                                    cursor.execute(query)
                                    conn.commit()
                            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                                    VALUES (%s, %s, %s, %s, %s)"
                            with conn.cursor() as cursor:
                                cursor.execute(query, (4, h[1], json.dumps({'old': h[0], 'new': new_h_path[0] }), ru, old_path_str_name))
                                conn.commit()
                    # 刪除
                    else:
                        query = f"DELETE FROM api_taxon_tree WHERE taxon_id = '{h[1]}'"
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            conn.commit()
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                                    VALUES (%s, %s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            cursor.execute(query, (4, h[1], json.dumps({'old': h[0], 'new': None }), ru, old_path_str_name))
                            conn.commit()
        # update api_taxon
        # accepted_taxon_name_id, common_name_c, alternative_name_c, is_hybrid, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine
        query = f"UPDATE api_taxon SET accepted_taxon_name_id=%s, common_name_c=%s, alternative_name_c=%s, is_hybrid=%s, is_endemic=%s, alien_type=%s, \
                    is_fossil=%s, is_terrestrial=%s, is_freshwater=%s, is_brackish=%s, is_marine=%s, updated_at=CURRENT_TIMESTAMP WHERE taxon_id=%s"
        with conn.cursor() as cursor:
            cursor.execute(query, (accepted_taxon_name_id, common_name_c, alternative_name_c, is_hybrid, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine, taxon_id))
            conn.commit()
        # 寫入api_taxon_usages
        # 就算沒有改變，也修改更新時間
        for i in rows.index:
            current_row = total_df.iloc[i]
            is_latest = 1 if current_row.is_latest else 0
            # update or insert
            query = f"""INSERT INTO api_taxon_usages (reference_usage_id, taxon_id, status, is_latest, taxon_name_id, parent_taxon_name_id) \
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        status = VALUES(status),
                        is_latest = VALUES(is_latest),
                        taxon_id = VALUES(taxon_id),
                        parent_taxon_name_id = VALUES(parent_taxon_name_id),
                        updated_at = CURRENT_TIMESTAMP
                        """
            with conn.cursor() as cursor:
                cursor.execute(query, (current_row.ru_id, taxon_id, current_row.taxon_status, is_latest, current_row.taxon_name_id, current_row.parent_taxon_name_id))
                conn.commit()

# TODO else 物種拆分&合併待處理
# 1898 t073204
# 階層更新
# conn = pymysql.connect(**db_settings)
# query = "SELECT taxon_id, path FROM api_taxon_tree"
# with conn.cursor() as cursor:
#     cursor.execute(query)
#     trees = cursor.fetchall()
#     trees = pd.DataFrame(trees, columns=['taxon_id', 'path'])

# query = """
#         WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id) AS
#             (
#                 SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id
#                 FROM api_taxon_usages c
#                 JOIN taxon_names t on c.taxon_name_id = t.id 
#                 WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
#                 UNION ALL
#                 SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id
#                 FROM find_ancestor cp
#                 JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
#                 JOIN taxon_names t on c.taxon_name_id = t.id 
#                 WHERE c.is_latest = 1 and c.status = 'accepted' 
#             )
#         SELECT taxon_id, path
#         FROM find_ancestor;
#         """
# with conn.cursor() as cursor:
#     cursor.execute(query)
#     new_trees = cursor.fetchall()
#     new_trees = pd.DataFrame(new_trees, columns=['taxon_id', 'new_path'])

# tree_merged = trees.merge(new_trees)


# for t in tree_merged[tree_merged.new_path != tree_merged.path].index:
#     tree_row = tree_merged.iloc[t]
#     path_list = tree_row.new_path
#     path_list = path_list.split('>')
#     if len(path_list) > 1:
#         parent_taxon_id = path_list[1]
#     # 寫入taxon_history
#     query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
#                 VALUES (%s, %s, %s)"
#     with conn.cursor() as cursor:
#         cursor.execute(query, (4, taxon_id, json.dumps({'old': tree_row.path, 'new': tree_row.new_path })))
#         conn.commit()
#     # 更新taxon_tree
#     if len(path_list) > 1:
#         query = f"UPDATE api_taxon_tree SET path = '{tree_row.new_path}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = '{parent_taxon_id}' WHERE taxon_id = '{tree_row.taxon_id}'"
#     else:
#         query = f"UPDATE api_taxon_tree SET path = '{tree_row.new_path}', updated_at = CURRENT_TIMESTAMP WHERE taxon_id = '{tree_row.taxon_id}'"
#     with conn.cursor() as cursor:
#         cursor.execute(query)
#         conn.commit()


# Taxon刪除 -> 最新文獻指出原本有分佈在台灣改成沒有分佈
conn = pymysql.connect(**db_settings)
for t in old_tmp_taxon_list:
    ru = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].ru_id.values[0]
    query = f'SELECT id FROM reference_usages WHERE JSON_EXTRACT(properties, "$.is_in_taiwan") = 0 and id = {ru}'
    with conn.cursor() as cursor:
        cursor.execute(query)
        not_taiwan= cursor.fetchone()
        if not_taiwan:
            taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
            # 寫入api_taxon_history
            query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
                        VALUES (%s, %s, %s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (6, taxon_id, '因台灣無分佈而刪除Taxon', ru,'因台灣無分佈而刪除Taxon'))
                conn.commit()
            # 修改api_taxon
            query = f"UPDATE api_taxon SET is_deleted = 1 WHERE taxon_id = '{taxon_id}'"
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()


# TODO 保育資訊更新 -> 可能要做成ppt 包含來源資料更新 / 手動更新 / 自動更新
conn = pymysql.connect(**db_settings)
conserv_dict = {1: 'protected', 5: 'iucn', 9: 'cites', 11: 'red', 16: 'sensitive'}
conserv_dict_display = {1: '保育類', 5: 'IUCN', 9: 'CITES', 11: '紅皮書', 16: '敏感物種'}







# protected 1 protected_category
# IUCN 5 iucn_category
# CITES 9 cites_listing
# RED 11 red_category
# sensitive 16 sensitive_suggest    
# 
# 

conn = pymysql.connect(**db_settings)
for t in old_tmp_taxon_list:
    taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
    print(t,taxon_id)
    new_conservation_info = get_conservation_info(taxon_id) # 空值
    # 寫入conservation_info
    if not new_conservation_info: # 如果是空值，全部都塞null
        new_conservation_info = (taxon_id, None, '[]', None, None, None, None, '[]', None, None, '[]', None, None, None, '[]', None, None, '[]')
    query = f"""INSERT INTO api_conservation (taxon_id, protected_category, protected_note, iucn_taxon_id, 
            iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
            cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
            sensitive_default, sensitive_suggest,  sensitive_note, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE
            protected_category = VALUES(protected_category),
            protected_note = VALUES(protected_note),
            iucn_taxon_id = VALUES(iucn_taxon_id),
            iucn_assessment_date = VALUES(iucn_assessment_date),
            iucn_category = VALUES(iucn_category),
            iucn_criteria = VALUES(iucn_criteria),
            iucn_note = VALUES(iucn_note),
            cites_id = VALUES(cites_id),
            cites_listing = VALUES(cites_listing),
            cites_note = VALUES(cites_note),
            red_category = VALUES(red_category),
            red_criteria = VALUES(red_criteria),
            red_adjustment = VALUES(red_adjustment),
            red_note = VALUES(red_note),
            sensitive_default = VALUES(sensitive_default),
            sensitive_suggest = VALUES(sensitive_suggest),
            sensitive_note = VALUES(sensitive_note),
            updated_at = CURRENT_TIMESTAMP
            """
    with conn.cursor() as cursor:
        cursor.execute(query, new_conservation_info)
        conn.commit()
    query = f"""SELECT taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                    cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                    sensitive_default, sensitive_suggest,  sensitive_note FROM api_conservation WHERE taxon_id = '{taxon_id}'"""
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        conservation_info = cursor.fetchone() 
        if not conservation_info or not conservation_info[1:]: # 空值代表完全沒有任何保育資訊 
            if new_conservation_info: # 有新的要新增
                # 寫入conservation_info
                # query = f"""
                #         INSERT INTO api_conservation
                #         (taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                #             cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                #             sensitive_default, sensitive_suggest,  sensitive_note)
                #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                #         """
                # with conn.cursor() as cursor:
                #     cursor.execute(query,new_conservation_info)
                #     conn.commit()
                # 寫入api_taxon_history
                for c in conserv_dict.keys():
                    if new_conservation_info[c]: # 新增保育資訊
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, content ) \
                                    VALUES (%s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            cursor.execute(query, (11, taxon_id, json.dumps({'category': new_conservation_info[c],'type': conserv_dict[c]}), conserv_dict_display[c]+"："+new_conservation_info[c]))
                            conn.commit()
                        # update conservation
        else:
            # 空 -> 有
            # 有 -> 空
            # 有 -> 有
            # new_conservation_info可能是空的
            # TODO 這邊沒有寫到更新 紅皮書的比對可能要改成用scientific_name而不是TaiCOL accepted_name
            if new_conservation_info:
                for c in conserv_dict.keys():
                    if not conservation_info[c] and new_conservation_info[c]: # 新增保育資訊
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, content ) \
                                    VALUES (%s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            display_str = conserv_dict_display[c]+"："+new_conservation_info[c]
                            if c == 9:
                                display_str = display_str.replace('1','I').replace('2','II').replace('3','III')
                            cursor.execute(query, (11, taxon_id, json.dumps({'category': new_conservation_info[c],'type': conserv_dict[c]}), display_str))
                            conn.commit()
                    elif conservation_info[c] and not new_conservation_info[c]: # 移除保育資訊
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, content ) \
                                                        VALUES (%s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            display_str = conserv_dict_display[c]+"："+conservation_info[c]
                            if c == 9:
                                display_str = display_str.replace('1','I').replace('2','II').replace('3','III')
                            cursor.execute(query, (12, taxon_id, json.dumps({'category': conservation_info[c],'type': conserv_dict[c]}), display_str))
                            conn.commit()
                    elif conservation_info[c] and new_conservation_info[c] and new_conservation_info[c] != conservation_info[c]:
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, content ) \
                                                            VALUES (%s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            display_str = conserv_dict_display[c]+"："+conservation_info[c]+"改為"+new_conservation_info[c]
                            if c == 9:
                                display_str = display_str.replace('1','I').replace('2','II').replace('3','III')
                            cursor.execute(query, (13, taxon_id, json.dumps({'old':conservation_info[4], 'new': new_conservation_info[4], 'type': conserv_dict[c]}), display_str))
                            conn.commit()
            else: # 本來有變成沒有
                for c in conserv_dict.keys():
                    if conservation_info[c]:
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, content ) \
                                VALUES (%s, %s, %s, %s)"
                        with conn.cursor() as cursor:
                            display_str = conserv_dict_display[c]+"："+conservation_info[c]
                            if c == 9:
                                display_str = display_str.replace('1','I').replace('2','II').replace('3','III')
                            cursor.execute(query, (12, taxon_id, json.dumps({'category': conservation_info[c],'type': conserv_dict[c]}), display_str))
                            conn.commit()





conn = pymysql.connect(**db_settings)

# TODO 相關連結更新 -> 不用寫在history裡, 也可能會有手動更新的情況, 要避免自動更新覆蓋掉手動更新
for t in old_tmp_taxon_list:
    taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
    print(t, taxon_id)
    links = get_links(taxon_id, updated=True)
    query =  f"""UPDATE api_taxon
                SET links=%s
                WHERE taxon_id = '{taxon_id}'
                """
    with conn.cursor() as cursor:
        cursor.execute(query, json.dumps(links))
        conn.commit()



# misapplied taxon
query = "SELECT id, `group`, reference_id FROM reference_usages WHERE id IN (SELECT reference_usage_id FROM api_taxon_usages WHERE `status` = 'misapplied')"
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

for r in results:
    print(r[0])
    query = f"""SELECT DISTINCT(taxon_id) FROM api_taxon_usages WHERE 
                taxon_name_id = (SELECT taxon_name_id FROM reference_usages 
                where `group` = {r[1]} and reference_id = {r[2]} and `status` = 'accepted')
                AND `status` = 'accepted';"""
    with conn.cursor() as cursor:
        cursor.execute(query)
        t = cursor.fetchone()
        print(t)
        if t:
            query = f"UPDATE api_taxon_usages SET correct_taxon_id = '{t[0]}' WHERE reference_usage_id = {r[0]}"
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()

