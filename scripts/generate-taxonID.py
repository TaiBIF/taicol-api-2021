# generate taxonID for the first time

from sys import dllhandle
from unittest import result
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


def get_related_names(taxon_name_id, rank_id, df, new_names):
    new_names.remove(taxon_name_id)  # remove current taxon_name_id
    query = f'SELECT reference_id, `group`, id FROM reference_usages WHERE taxon_name_id = {taxon_name_id} AND status NOT IN ("", "undetermined")'
    with conn.cursor() as cursor:
        cursor.execute(query)
        ref_group_pair = cursor.fetchall()
    query = f'SELECT DISTINCT(ru.taxon_name_id) FROM reference_usages ru \
                INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                WHERE ru.status NOT IN ("", "undetermined") AND tn.rank_id = {rank_id} '
    p_query = ''
    for p in range(len(ref_group_pair)):
        df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 'group': ref_group_pair[p][1], 'taxon_name_id': taxon_name_id, 'rank_id': rank_id}, ignore_index=True)
        if p < max(range(len(ref_group_pair))):
            p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) OR'
        else:
            p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) '
    if p_query:
        query += f'AND ({p_query})'
    with conn.cursor() as cursor:
        cursor.execute(query)
        names = cursor.fetchall()
        names = [l[0] for l in names]
        new_names += [n for n in names if n not in list(df.taxon_name_id)]
    return new_names, df


conn = pymysql.connect(**db_settings)


# add publish_date to references table
query = f'SELECT id, publish_year, JSON_EXTRACT(properties, "$.doi") FROM `references`'

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'reference_id', 1: 'publish_year', 2: 'doi'})

results['publish_date'] = ''
for d in results.doi.unique():
    if d:
        d_str = d.replace('"', '').replace("'", '')
        if d_str:
            print(d_str)
            url = f'https://api.crossref.org/works/{d_str}'
            result = requests.get(url)
            if result.status_code == 200:
                result = result.json()
                if result.get('message'):
                    print('hello')
                    try:
                        date_list = result.get('message').get('published-print').get('date-parts')[0]
                    except:
                        date_list = result.get('message').get('published-online').get('date-parts')[0]
                    try:
                        results.loc[results['doi'] == d, 'publish_date'] = datetime.datetime(date_list[0], date_list[1], date_list[2]).strftime("%Y-%m-%d")
                    except:
                        pass


# update
for r in results[results['publish_date'] != ''].index:
    row = results.iloc[r]
    print(row)
    query = f"UPDATE `references` SET publish_date = '{row.publish_date}' WHERE id = {row.reference_id}"
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


# query = 'SELECT ru.id, ru.reference_id, ru.`group`, ru.taxon_name_id, \
#          tn.rank_id FROM reference_usages ru \
#          JOIN taxon_names tn ON tn.id = ru.taxon_name_id \
#          WHERE JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 AND ru.status NOT IN ("", "undetermined")'
query = 'SELECT ru.id, ru.reference_id, ru.`group`, ru.taxon_name_id, tn.rank_id  FROM reference_usages ru \
         INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
         WHERE JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 AND ru.status NOT IN ("", "undetermined")'
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'id', 1: 'reference_id', 2: 'group', 3: 'taxon_name_id', 4: 'rank_id'})


checked_name_id = []
count = 0
total_df = pd.DataFrame()
for i in results.index:
    print(i)
    row = results.iloc[i]
    if row.taxon_name_id not in checked_name_id:
        count += 1
        taxon_id = 't' + format(count, '06d')
        checked_name_id += [row.taxon_name_id]
        name_list = [row.taxon_name_id]
        new_names = []
        df = pd.DataFrame()
        # get all reference_id & group
        query = f'SELECT reference_id, `group`, id FROM reference_usages WHERE taxon_name_id = {row.taxon_name_id} AND status NOT IN ("", "undetermined")'
        with conn.cursor() as cursor:
            cursor.execute(query)
            ref_group_pair = cursor.fetchall()
        # 根據有的 reference_id & group 再去抓抓看有沒有別的name_id (需排除status為空值或未決的資料)
        # 如果有其他name_id的話，就有可能是不同rank，需要指定rank
        query = f'SELECT DISTINCT(ru.taxon_name_id) FROM reference_usages ru \
                    INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                    WHERE ru.status NOT IN ("", "undetermined") AND tn.rank_id = {row.rank_id} '
        p_query = ''
        for p in range(len(ref_group_pair)):
            df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 'group': ref_group_pair[p]
                           [1], 'taxon_name_id': row.taxon_name_id, 'rank_id': row.rank_id}, ignore_index=True)
            if p < max(range(len(ref_group_pair))):
                p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) OR'
            else:
                p_query += f' (ru.reference_id = {ref_group_pair[p][0]} AND ru.`group` = {ref_group_pair[p][1]}) '
        if p_query:
            query += f'AND ({p_query})'
        with conn.cursor() as cursor:
            cursor.execute(query)
            names = cursor.fetchall()
            names = [l[0] for l in names]
            # 如果有新名字的話要再重複抓
            new_names += [n for n in names if n not in name_list]  # 用來確定是不是還有name需要跑
        while len(new_names) > 0:
            for nn in new_names:
                checked_name_id += [nn]
                new_names, df = get_related_names(nn, row.rank_id, df, new_names)
        df = df.astype('int32')
        df['taxon_id'] = taxon_id
        total_df = total_df.append(df)

total_df.to_csv('taxonID_first_try.csv', index=False)

# 取最新接受名，其他為同物異名或誤用名
# reference_id, group, taxon_name_id
# 抓status, publish_year


query = f'SELECT id, publish_year, JSON_EXTRACT(properties, "$.doi"), publish_date FROM `references` \
          WHERE id IN {tuple(total_df.reference_id.unique())}'

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'reference_id', 1: 'publish_year', 2: 'doi', 3: 'publish_date'})

total_df = total_df.merge(results)

query = f'SELECT id, status FROM reference_usages \
          WHERE id IN {tuple(total_df.ru_id.unique())}'

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'ru_id', 1: 'status'})

total_df = total_df.merge(results)

# 決定誰是接受學名
taxon_list = total_df.taxon_id.unique()

total_df['taxon_status'] = ''
total_df['is_latest'] = False
total_df['publish_date'] = ''

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
            pass
            # TODO 尚未完成
            # 如果年份一樣，比對publish_date
            # 也排除ref=153的情況？
    else:
        total_df.loc[(total_df['taxon_id'] == t) & (total_df['publish_year'] == max_yr), 'is_latest'] = True


total_df.to_csv('taxonID_first_try.csv', index=False)

# check
# x = total_df[total_df['is_latest']==True][['reference_id','taxon_id','is_latest']].drop_duplicates().groupby(['taxon_id','is_latest']).count()
# y = total_df[total_df['is_latest']==False][['reference_id','taxon_id','is_latest']].drop_duplicates().groupby(['taxon_id','is_latest']).count()

# TODO 目前沒有誤用名的例子，有的話要再確認
for i in total_df.index:
    print(i)
    row = total_df.iloc[i]
    if row.is_latest:
        if row.status == 'not-accepted':
            total_df.loc[i, 'taxon_status'] = 'synonyms'
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
                # 如果不一樣，且不是misapplied, 設成synonyms
                total_df.loc[i, 'taxon_status'] = 'synonyms'

# 寫入taxon table
# 86624
for i in total_df[(total_df['is_latest']) & (total_df['taxon_status'] == 'accepted')].index:
    if i > 86623:
        print('now', i)
        row = total_df.iloc[i]
        taxon_id = row.taxon_id
        rank_id = row.rank_id
        accepted_taxon_name_id = row.taxon_name_id
        ru_list = total_df[(total_df['taxon_id'] == taxon_id) & (total_df['status'] == 'accepted')].ru_id.to_list()
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
            for n in results[0]:
                n_list += json.loads(n)
            for nn in n_list:
                if nn.get('language') == 'zh-tw' and nn.get('name') not in common_names:
                    common_names.append(nn.get('name'))
        if common_names:
            common_name_c = common_names[0]
            alternative_name_c = ', '.join(common_names[1:])
        else:
            common_name_c, alternative_name_c = None, None
        # is_hybrid
        ru_list = total_df[total_df['taxon_id'] == taxon_id].ru_id.to_list()
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
        ru = total_df[(total_df['taxon_id'] == taxon_id) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].ru_id.to_list()[0]
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
        # 寫入table
        query = f"INSERT INTO taxon (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c,  \
                    is_hybrid, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine ) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        with conn.cursor() as cursor:
            cursor.execute(query, (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c, is_hybrid,
                                   is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine))
            conn.commit()

# 寫入 taxon_usages table
for i in total_df.index:
    print('now', i)
    row = total_df.iloc[i]
    is_latest = 1 if row.is_latest else 0
    query = f"INSERT INTO taxon_usages (reference_usage_id, reference_id, taxon_id, status, is_latest) \
                VALUES (%s, %s, %s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (row.ru_id, row.reference_id, row.taxon_id, row.taxon_status, is_latest))
        conn.commit()

# 寫入taxon_hierarchy table
# 從reference_usages一層一層抓parent
h_df = pd.DataFrame()
for ru in total_df[(total_df['is_latest']) & (total_df['taxon_status'] == 'accepted')].ru_id:

    # 會有上層對到的name不是taxon的accepted name嗎?
