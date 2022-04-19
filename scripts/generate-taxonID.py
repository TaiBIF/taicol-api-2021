# generate taxonID for the first time

import re
import itertools
from importlib.abc import TraversableResources
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

conn = pymysql.connect(**db_settings)


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
# 沒有在台灣 & 沒有在reference_usages裡面的資料不給taxonID
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

query = f'SELECT id, status, taxon_name_id, parent_taxon_name_id FROM reference_usages \
          WHERE id IN {tuple(total_df.ru_id.unique())}'

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'ru_id', 1: 'status', 2: 'taxon_name_id', 3: 'parent_taxon_name_id'})

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
        query = f"INSERT INTO api_taxon (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c,  \
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
    query = f"INSERT INTO api_taxon_usages (reference_usage_id, reference_id, taxon_id, status, is_latest, taxon_name_id, parent_taxon_name_id) \
                VALUES (%s, %s, %s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (row.ru_id, row.reference_id, row.taxon_id, row.taxon_status, is_latest, taxon_name_id, parent_taxon_name_id))
        conn.commit()


# 寫入taxon_hierarchy table
# 1. 從 taxon_usages 裡面抓 is_latest = 1 & status = accepted
# 2. 將該taxon_id存入hierarchy table (parent & child都是自己)
# 3. 抓對應到reference_usages裡面的parent_taxon_name_id
# 4. 抓該parent_taxon_name_id對應到的taxon_id存進hierarchy table(parent是parent_taxon_name_id對到的, child是自己) -> 這樣好像只會存到length=1的組合

# sandy 寫法
# 此寫法是串name，要改成串taxon
# WITH RECURSIVE find_ancestor (taxon_name_id, usage_id, reference_id, path, root_taxon_name_id) AS
#     (
#         SELECT taxon_name_id, usage_id, reference_id, cast(taxon_name_id as CHAR(50)) as path, taxon_name_id as root_taxon_name_id
#         FROM accepted_usages
#         WHERE parent_taxon_name_id IS NULL
#         UNION ALL
#         SELECT c.taxon_name_id, c.usage_id, c.reference_id, concat(cast(c.taxon_name_id as CHAR(50)) , '>',  path), root_taxon_name_id
#         FROM find_ancestor AS cp
#         JOIN accepted_usages AS c
#             ON cp.taxon_name_id = c.parent_taxon_name_id
#     )
#     SELECT find_ancestor.*, taxon_names.name, taxon_names.rank_id
#     FROM find_ancestor
#     left join taxon_names on taxon_names.id = find_ancestor.root_taxon_name_id
#     where taxon_name_id in ({$taxonNamesIdString}) and taxon_names.rank_id = 3

# ------

query = f"""
WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, reference_usage_id, reference_id, path, root_taxon_name_id, path2) AS
    (
        SELECT t.rank_id, c.taxon_name_id, c.reference_usage_id, c.reference_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_name_id as root_taxon_name_id , cast(c.taxon_name_id as CHAR(1000)) as path2 
        FROM api_taxon_usages c
        JOIN taxon_names AS t
            on c.taxon_name_id = t.id 
        WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.`status` = 'accepted' and t.rank_id <= 34
        UNION ALL
        SELECT t.rank_id, c.taxon_name_id, c.reference_usage_id, c.reference_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), root_taxon_name_id, concat(cast(c.taxon_name_id as CHAR(1000)) , '>',  path2) 
        FROM find_ancestor AS cp
        JOIN api_taxon_usages AS c
            ON cp.taxon_name_id = c.parent_taxon_name_id
        JOIN taxon_names AS t
            on c.taxon_name_id = t.id 
        WHERE c.is_latest = 1 and c.status = 'accepted' and t.rank_id <= 34
    )
    SELECT *
    FROM find_ancestor
"""

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

df = pd.DataFrame()
for r in results:
    print(r[2])
    t_list = r[4].split('>')
    for i in itertools.combinations_with_replacement(t_list, 2):
        if not(i[0] == i[1] and i[0] != t_list[0]):
            length = t_list.index(i[1]) - t_list.index(i[0])
            df = df.append({'parent_taxon_id': i[1], 'child_taxon_id': i[0], 'length': length}, ignore_index=True)
    df = df.drop_duplicates()  # 移除重複的組合
    # 先確認組合是不是已經存在
    # conn = pymysql.connect(**db_settings)
    #         query = f"INSERT INTO taxon_hierarchy (parent_taxon_id, child_taxon_id, length) \
    #                     SELECT * FROM (SELECT %s as parent_taxon_id, %s as child_taxon_id, %s) AS tmp \
    #                     WHERE NOT EXISTS ( \
    #                         SELECT id FROM taxon_hierarchy WHERE parent_taxon_id=%s and child_taxon_id=%s and length=%s \
    #                     ) LIMIT 1;"
    #         with conn.cursor() as cursor:
    #             cursor.execute(query, (i[1], i[0], length, i[1], i[0], length))
    #             conn.commit()


# TODO 種下＆種下下階層另外處理
# 如果有種下的種階層是無效名，那該種下就是「單獨有Taxon_id 但沒有上階層」
# 確認種下的上階層是否有效

query = "WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, reference_usage_id, reference_id, path, root_taxon_name_id, path2) AS \
    ( \
        SELECT t.rank_id, c.taxon_name_id, c.reference_usage_id, c.reference_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_name_id as root_taxon_name_id , cast(c.taxon_name_id as CHAR(1000)) as path2 \
        FROM api_taxon_usages c \
        JOIN taxon_names AS t \
            on c.taxon_name_id = t.id \
        WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.`status` = 'accepted' \
        UNION ALL \
        SELECT t.rank_id, c.taxon_name_id, c.reference_usage_id, c.reference_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), root_taxon_name_id, concat(cast(c.taxon_name_id as CHAR(1000)) , '>',  path2) \
        FROM find_ancestor AS cp \
        JOIN api_taxon_usages AS c \
            ON cp.taxon_name_id = c.parent_taxon_name_id \
        JOIN taxon_names AS t \
            on c.taxon_name_id = t.id \
        WHERE c.is_latest = 1 and c.status = 'accepted' \
    ) \
    SELECT * \
    FROM find_ancestor \
    WHERE taxon_name_id IN ( \
        SELECT t1.taxon_name_id FROM api_taxon_usages t1, api_taxon_usages t2 \
        JOIN taxon_names t ON t.id = t2.taxon_name_id \
        WHERE t1.parent_taxon_name_id = t2.taxon_name_id and t2.status = 'accepted' and t.rank_id >= 34);"

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

for r in results:
    print(r[2])
    conn = pymysql.connect(**db_settings)
    t_list = r[4].split('>')
    for i in itertools.combinations_with_replacement(t_list, 2):
        if not(i[0] == i[1] and i[0] != t_list[0]):
            length = t_list.index(i[1]) - t_list.index(i[0])
            # 先確認組合是不是已經存在
            query = f"INSERT INTO api_taxon_hierarchy (parent_taxon_id, child_taxon_id, length) \
                        SELECT * FROM (SELECT %s as parent_taxon_id, %s as child_taxon_id, %s) AS tmp \
                        WHERE NOT EXISTS ( \
                            SELECT id FROM api_taxon_hierarchy WHERE parent_taxon_id=%s and child_taxon_id=%s and length=%s \
                        ) LIMIT 1;"
            with conn.cursor() as cursor:
                cursor.execute(query, (i[1], i[0], length, i[1], i[0], length))
                conn.commit()

query = "SELECT rank_id, nomenclature_id, properties, id FROM taxon_names WHERE rank_id <> 47"
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

# for r in results:
for r in results:
    print(r[3])
    pp = json.loads(r[2])
    if r[0] < 30:  # rank 為屬以上
        formatted_name = pp.get('latin_name')
    elif r[0] == 30:  # rank 為屬
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
                    formatted_name = f"<i>{pp.get('latin_genus')} {pp.get('latin_s1')} {l.get('latin_name')}</i>"
                else:  # 種下下rank需顯示
                    formatted_name += f" {l.get('rank_abbreviation')} <i>{l.get('latin_name')}</i>"
                count += 1
        else:  # 命名規約為植物
            formatted_name = f"<i>{pp.get('latin_genus')} {pp.get('latin_s1')}</i>"
            for l in pp.get('species_layers'):
                formatted_name += f" {l.get('rank_abbreviation')} <i>{l.get('latin_name')}</i>"
    query = f"INSERT INTO api_names (taxon_name_id, name_with_tag) VALUES (%s, %s);"
    with conn.cursor() as cursor:
        cursor.execute(query, (r[3], formatted_name))
        conn.commit()

# 雜交組合最後處理（要等學名已經建立）
query = "WITH view as (SELECT tnhp.taxon_name_id, an.name_with_tag FROM taxon_name_hybrid_parent tnhp \
         JOIN api_names an ON tnhp.parent_taxon_name_id = an.taxon_name_id \
         ORDER BY tnhp.order) \
         SELECT taxon_name_id, group_concat(name_with_tag SEPARATOR ' × ') FROM view \
         GROUP BY taxon_name_id"

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
for r in results:
    query = f"INSERT INTO api_names (taxon_name_id, name_with_tag) VALUES (%s, %s);"
    with conn.cursor() as cursor:
        cursor.execute(query, (r[0], r[1]))
        conn.commit()

# name with HTML tag
# 動物命名規約
# 1. 屬以上抓latin name 且不用斜體
# 2. 屬抓latin name且斜體
# 3. 種抓 latin genus & latin s1 且斜體
# 4. 種下 latin genus & latin s1 & latin s2 且斜體
# 5. 種下下 latin genus & latin s1 & latin s2 & s3_rank & latin s3 **rank不斜體
# 6. 雜交組合 hybrid parent1 × hybrid parent2


# 植物命名規約
# 1. 屬以上抓latin name 且不用斜體
# 2. 屬抓latin name且斜體
# 2-1. 屬且雜交 × latin name且斜體
# 3. 種抓 latin genus & latin s1 且斜體
# 3-1. 種且雜交 latin genus × latin s1
# 4. 種下 latin genus & latin s1 & s2_rank  & latin s2 且斜體 **rank不斜體
# 5. 種下下 latin genus & latin s1 & s2_rank & latin s2 & s3_rank & latin s3 **rank不斜體
# 6. 雜交組合 hybrid parent1 × hybrid parent2


# TODO name with tag 的更新流程 -> 在每次更新taxon的時候處理

# TODO 如果未來原本是種下的上階層是有效的種，但改為無效，要怎麼更新？

# TODO 這樣存資料表會不會太大？


# references

# 產生引用內的作者格式

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


query = "SELECT p.last_name, p.first_name, p.middle_name, pr.reference_id, pr.order, r.publish_year \
         FROM person_reference pr \
         JOIN persons p ON pr.person_id = p.id \
         JOIN `references` r on pr.reference_id = r.id "
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = pd.DataFrame(cursor.fetchall(), columns=['last_name', 'first_name', 'middle_name', 'reference_id', 'order', 'year'])


# author

citation_df = []
for g in results.reference_id.unique():
    rows = results[results['reference_id'] == g].sort_values('order')
    author_list = []
    for i, r in rows.iterrows():
        last_name = r['last_name']
        first_name = to_firstname_abbr(r['first_name'])
        middle_name = to_middlename_abbr(r['middle_name'])
        full_name = f"{last_name}, {middle_name}{first_name}"
        author_list.append(full_name)
    if len(author_list) == 1:
        authors = author_list[0]
    elif len(author_list) == 2:
        authors = ', '.join(author_list)
    else:  # 三人或以上
        authors = ', '.join(author_list[:-1]) + ' & ' + author_list[-1]
    citation_df.append((g, authors + f' ({rows.year.unique()[0]})'))


conn = pymysql.connect(**db_settings)
for c in citation_df:
    with conn.cursor() as cursor:
        query = "INSERT INTO api_citations (reference_id, author) VALUES (%s, %s)"
        cursor.execute(query, (c))
        conn.commit()


# content

query = "SELECT id, type, title, properties FROM `references`"
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = pd.DataFrame(cursor.fetchall(), columns=['id', 'type', 'title', 'properties'])

citation_df = []
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
    citation_df.append((content, row.id))

conn = pymysql.connect(**db_settings)
for c in citation_df:
    with conn.cursor() as cursor:
        query = "UPDATE api_citations SET content = %s WHERE reference_id = %s"
        cursor.execute(query, (c))
        conn.commit()


# TODO API的時候要把<i></i>拿掉
