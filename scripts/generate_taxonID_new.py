# 2022-08-25
# 因考慮誤用名的情況修改taxon_id分組判斷
# 重新產生taxonID

# update taxon information
from msilib.schema import Condition
import re
import itertools
from tabnanny import check
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
    "host": 'host.docker.internal',
    "port": 3306,
    "user": 'root',
    "password": 'example',
    "db": 'taicol',
}

conn = pymysql.connect(**db_settings)



# 取得所有相關的學名

def get_related_names(taxon_name_id, df, new_names):
    new_names.remove(taxon_name_id)  
    c_ref_group_pair = all_usages[all_usages.taxon_name_id==taxon_name_id]
    df = df.append(c_ref_group_pair)
    c_ref_group_pair = c_ref_group_pair.reset_index(drop=True)
    c_new_pair = all_usages.merge(c_ref_group_pair[['reference_id','group']],on=['reference_id','group'])
    new_names += [n[1].taxon_name_id for n in c_new_pair.iterrows() if n[1].taxon_name_id not in list(df.taxon_name_id) and n[1].status == 'accepted']  # 用來確定是不是還有name需要跑
    return new_names, df

# ------ 根據 reference_usage 產生 taxon ID ------ #
conn = pymysql.connect(**db_settings)

query = 'SELECT ru.id, ru.reference_id, ru.`group`, ru.taxon_name_id, tn.rank_id  FROM reference_usages ru \
         INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
         WHERE JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 AND ru.status NOT IN ("", "undetermined") and ru.is_title != 1 '
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'id', 1: 'reference_id', 2: 'group', 3: 'taxon_name_id', 4: 'rank_id'})
    conn.close()


# 先把全部抓回來就不用每次都一直query了
conn = pymysql.connect(**db_settings)
query = f'''SELECT tn.id, ru.reference_id, ru.`group`, ru.id, tn.rank_id, ru.status FROM reference_usages ru
            JOIN taxon_names tn ON ru.taxon_name_id = tn.id
            WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined")'''
with conn.cursor() as cursor:
    cursor.execute(query)
    all_usages = cursor.fetchall()
    all_usages = pd.DataFrame(all_usages, columns = ['taxon_name_id','reference_id','group','ru_id','rank_id','status'])
    conn.close()


checked_name_id = []
count = 0
total_df = pd.DataFrame()

for i in results.index:
    if i % 100 == 0:
        print(i)
    row = results.iloc[i]
    if row.taxon_name_id not in checked_name_id:
        checked_name_id += [row.taxon_name_id]
        name_list = [row.taxon_name_id]
        new_names = []
        # 1 找到所有的reference & group
        df = all_usages[all_usages.taxon_name_id==row.taxon_name_id]
        # 根據有的 reference_id & group 再去抓抓看有沒有別的name_id (需排除status不是accepted的資料)
        # ??? 不確定先移除此處理 -> 如果有其他name_id的話，就有可能是不同rank，需要指定rank
        # 2 找到所有reference & group裡有的學名
        # 根據 reference / group 分組
        # 檢查如果沒有misapplied的情況
        # 同一個taxon_id的條件 -> 1 同一組 ref & group, 2 有一樣的name?
        df = df.reset_index(drop=True)
        check_new_name = all_usages.merge(df[['reference_id','group']],on=['reference_id','group'])
        # 如果reference & group中有新的學名，且為accepted，則在找他對應的所有reference & group
        new_names += [n[1].taxon_name_id for n in check_new_name.iterrows() if n[1].taxon_name_id not in name_list and n[1].status == 'accepted']  # 用來確定是不是還有name需要跑
        while len(new_names) > 0:
            for nn in new_names:
                checked_name_id += [nn]
                # 只抓status是accepted的new_names
                new_names, df = get_related_names(nn, df, new_names)
        df = df.drop_duplicates().reset_index()
        # 目前有些ref group會缺資料，再抓回來
        final_df = all_usages.merge(df[['reference_id','group']].drop_duplicates(),on=['reference_id','group'])
        # 如果ref & group 已存在 則直接給對應的taxon_id
        if len(total_df) and len(total_df[['reference_id','group','taxon_id']].merge(final_df[['reference_id','group']],on=['reference_id','group'])):
            taxon_id = total_df[['reference_id','group','taxon_id']].merge(final_df[['reference_id','group']],on=['reference_id','group']).taxon_id.values[0]
        else:
            count += 1
            taxon_id = 't' + format(count, '06d')
        final_df['taxon_id'] = taxon_id
        total_df = total_df.append(final_df, ignore_index=True)
            

# 確認有沒有重複的ru
total_df = total_df.drop_duplicates()

# 取最新接受名，其他為同物異名或誤用名
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


query = f"""SELECT id, status, taxon_name_id, parent_taxon_name_id FROM reference_usages 
          WHERE id IN ({str(list(total_df.ru_id.to_list())).replace('[','').replace(']','')})"""

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'ru_id', 1: 'status', 2: 'taxon_name_id', 3: 'parent_taxon_name_id'})

total_df = total_df.merge(results)



# 決定誰是接受學名
taxon_list = total_df.taxon_id.unique() # 85283

total_df['taxon_status'] = ''
total_df['is_latest'] = False
total_df['publish_date'] = ''

cannot_decide = []
for t in taxon_list:
    print(t)
    temp = total_df[total_df['taxon_id'] == t]
    # 如果有大於一個reference_id, 比較年份
    yr = temp[['reference_id', 'publish_year']].drop_duplicates()
    max_yr = yr.publish_year.max()
    if len(yr[yr['publish_year'] == max_yr]) > 1:
        ref_list = yr[yr['publish_year'] == max_yr].reference_id.to_list()
        if len(yr[yr['publish_year'] == max_yr]) == 2 and 153 in ref_list:
            # 代表和taicol backbone同年份，優先選擇非taicol backbone的文獻
            ref_list.remove(153)
            chosen_ref_id = ref_list[0]
            total_df.loc[(total_df['taxon_id'] == t) & (total_df['reference_id'] == chosen_ref_id), 'is_latest'] = True
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
            temp = total_df[total_df['taxon_id'] == t]
            dt = temp[['reference_id', 'publish_date']].drop_duplicates()
            max_dt = dt.publish_date.max()
            if len(dt[dt['publish_date'] == max_dt]) > 1:
                ref_list = dt[dt['publish_date'] == max_dt].reference_id.to_list()
                if len(dt[dt['publish_date'] == max_dt]) == 2 and 153 in ref_list:
                    # 代表和taicol backbone同年份，優先選擇非taicol backbone的文獻
                    ref_list.remove(153)
                    chosen_ref_id = ref_list[0]
                    total_df.loc[(total_df['taxon_id'] == t) & (total_df['reference_id'] == chosen_ref_id), 'is_latest'] = True
                else:
                    cannot_decide += [t]
            else:
                total_df.loc[(total_df['taxon_id'] == t) & (total_df['publish_date'] == max_dt), 'is_latest'] = True
            # TODO 尚未完成
            # 如果年份一樣，比對publish_date，但如果無法取得publish_date?
            # 也排除ref=153的情況？
    else:
        total_df.loc[(total_df['taxon_id'] == t) & (total_df['publish_year'] == max_yr), 'is_latest'] = True


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
        accepted_name_id = total_df.loc[(total_df['taxon_id'] == row.taxon_id) & (total_df['status'] == 'accepted')]['taxon_name_id'].to_list()[0]
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

# # 有misapplied的情況下，要多給correct_taxon_id
# mis_group = total_df[total_df.taxon_status=='misapplied'][['ru_id','group','status','reference_id']]

# for m in mis_group.index:
#     row = mis_group.loc[m]
#     c_taxon = total_df[(total_df.group==row.group)&(total_df.reference_id==row.reference_id)&(total_df.taxon_status=='accepted')]
#     c_taxon = c_taxon.taxon_id.values[0]
#     total_df.loc[m,'correct_taxon_id'] = c_taxon

total_df.to_csv('total_df.csv', index=None)

total_df = pd.read_csv('total_df.csv')



# 寫入taxon table
# 先寫到local


total_df = total_df.replace({np.nan: None}) 



# total_df[total_df.taxon_status=='accepted'][total_df[total_df.taxon_status=='accepted'][['taxon_status','ru_id']].duplicated()]
# t000273 t001120 t002026 t085247 t085285

for nt in taxon_list: # 85287
    print(nt)
    conn = pymysql.connect(**db_settings)
    rows = total_df[total_df['taxon_id']==nt]
    i = rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row
    row = total_df.iloc[i] # 接受的row
    rank_id = row.rank_id
    accepted_taxon_name_id = row.taxon_name_id
    ru_list = total_df[(total_df['taxon_id'] == nt) & (total_df['status'] == 'accepted')].ru_id.to_list()
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
    ru_list = total_df[total_df['taxon_id'] == nt].ru_id.to_list()
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
    ru = total_df[(total_df['taxon_id'] == nt) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].ru_id.to_list()[0]
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
    # 寫入 api_taxon table 
    query = f"INSERT INTO api_taxon (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c,  \
                is_hybrid, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine ) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (nt, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c, is_hybrid,
                                is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine))
        conn.commit()
    # 寫入 api_taxon_usages table
    for i in rows.index:
        current_row = total_df.loc[i]
        current_row = current_row.replace({np.nan:None})
        is_latest = 1 if current_row.is_latest else 0
        # update or insert
        query = f"""INSERT INTO api_taxon_usages (reference_usage_id, taxon_id, status, is_latest, taxon_name_id, parent_taxon_name_id) \
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
        with conn.cursor() as cursor:
            cursor.execute(query, (current_row.ru_id, nt, current_row.taxon_status, is_latest, current_row.taxon_name_id, current_row.parent_taxon_name_id))
            conn.commit()


# 建立 api_taxon_tree
conn = pymysql.connect(**db_settings)
query = """
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
FROM find_ancestor;
"""
with conn.cursor() as cursor:
    cursor.execute(query)
    conn.commit()

# 新增相關連結 & 保育資訊在其他script
