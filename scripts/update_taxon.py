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
            authors = ' & '.join(author_list)
        else:  # 三人或以上
            authors = ', '.join(author_list[:-1]) + ' & ' + author_list[-1]
        citation_df.append((g, authors + f' ({rows.year.unique()[0]})'))
    citation_df = pd.DataFrame(citation_df, columns=['reference_id','author'])
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
            query = "INSERT INTO api_citations (reference_id, author, content, updated_at) VALUES(%s, %s, %s, CURRENT_TIMESTAMP) \
                    ON DUPLICATE KEY UPDATE author=%s, content=%s, updated_at = CURRENT_TIMESTAMP; "        
            cursor.execute(query, (row.reference_id, row.author, row.content, row.author, row.content))
            conn.commit()


# 2 api_names
def create_names(name_list, hybrid_name_list):
    rows = []
    if name_list:
        query = f"SELECT rank_id, nomenclature_id, properties, id FROM taxon_names WHERE id IN ({','.join(name_list)})"
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        for r in results:
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
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

name_list = [str(r[0]) for r in results]



# 如果parent name有修改再修改
query = "SELECT tnhp.taxon_name_id FROM taxon_name_hybrid_parent tnhp \
    JOIN taxon_names tn ON tnhp.parent_taxon_name_id = tn.id \
    WHERE tn.created_at > (select max(updated_at) from api_names) or tn.updated_at > (select max(updated_at) from api_names) \
        "
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

hybrid_name_list = [str(r[0]) for r in results]


rows = create_names(name_list, hybrid_name_list)

conn = pymysql.connect(**db_settings)

for r in rows:
    with conn.cursor() as cursor:
        query = "INSERT INTO api_names (taxon_name_id, formatted_name, updated_at) VALUES(%s, %s, CURRENT_TIMESTAMP) \
                ON DUPLICATE KEY UPDATE formatted_name=%s, updated_at = CURRENT_TIMESTAMP; "        
        cursor.execute(query, (r[0], r[1], r[1]))
        conn.commit()


# 3 api_taxon_usages
# 取得所有相關的學名
def get_related_names(taxon_name_id, df, new_names):
    new_names.remove(taxon_name_id)  # remove current taxon_name_id
    query = f'''SELECT ru.reference_id, ru.`group`, ru.id, tn.rank_id FROM reference_usages ru
                JOIN taxon_names tn ON ru.taxon_name_id = tn.id
                WHERE ru.taxon_name_id = {taxon_name_id} AND ru.status NOT IN ("", "undetermined")'''
    with conn.cursor() as cursor:
        cursor.execute(query)
        ref_group_pair = cursor.fetchall()
    query = f'SELECT DISTINCT(ru.taxon_name_id) FROM reference_usages ru \
                INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                WHERE ru.status NOT IN ("", "undetermined")'
    p_query = ''
    for p in range(len(ref_group_pair)):
        df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 'group': ref_group_pair[p][1], 'taxon_name_id': taxon_name_id, 'rank_id': ref_group_pair[p][3]}, ignore_index=True)
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

# 選出新增的reference_usages
# 新增的reference_usages不一定是最新的
# 抓出相關的names

results = pd.DataFrame()

query = 'select ru.id, ru.reference_id, ru.taxon_name_id, ru.`group`, tn.rank_id, "new" from reference_usages ru \
         join taxon_names tn ON ru.taxon_name_id = tn.id \
         where ru.created_at > (select max(updated_at) from api_taxon_usages) \
         and JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 and ru.status NOT IN ("", "undetermined");'
with conn.cursor() as cursor:
    cursor.execute(query)
    tmp = cursor.fetchall()
    tmp = pd.DataFrame(tmp, columns=['id','reference_id','taxon_name_id','group', 'rank_id', 'ru_status'])
    results = results.append(tmp, ignore_index=True)

query = 'select ru.id, ru.reference_id, ru.taxon_name_id, ru.`group`, tn.rank_id, "updated" from reference_usages ru \
         join taxon_names tn ON ru.taxon_name_id = tn.id \
         where ru.created_at <= (select max(updated_at) from api_taxon_usages) and ru.updated_at > (select max(updated_at) from api_taxon_usages) \
         and JSON_EXTRACT(ru.properties, "$.is_in_taiwan") = 1 and ru.status NOT IN ("", "undetermined");'
with conn.cursor() as cursor:
    cursor.execute(query)
    tmp = cursor.fetchall()
    tmp = pd.DataFrame(tmp, columns=['id','reference_id','taxon_name_id','group', 'rank_id', 'ru_status'])
    results = results.append(tmp, ignore_index=True)


checked_name_id = []
count = 0
total_df = pd.DataFrame()
for i in results.index:
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
        query = f'''SELECT ru.reference_id, ru.`group`, ru.id, tn.rank_id FROM reference_usages ru
                    JOIN taxon_names tn ON ru.taxon_name_id = tn.id
                    WHERE ru.taxon_name_id = {row.taxon_name_id} AND ru.status NOT IN ("", "undetermined")'''
        with conn.cursor() as cursor:
            cursor.execute(query)
            ref_group_pair = cursor.fetchall()
        # 根據有的 reference_id & group 再去抓抓看有沒有別的name_id (需排除status為空值或未決的資料)
        # ??? 不確定先移除此處理 -> 如果有其他name_id的話，就有可能是不同rank，需要指定rank
        query = f'SELECT DISTINCT(ru.taxon_name_id) FROM reference_usages ru \
                    INNER JOIN taxon_names tn ON ru.taxon_name_id = tn.id  \
                    WHERE ru.status NOT IN ("", "undetermined")'
        p_query = ''
        for p in range(len(ref_group_pair)):
            df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 'group': ref_group_pair[p]
                           [1], 'taxon_name_id': row.taxon_name_id, 'rank_id': ref_group_pair[p][3]}, ignore_index=True)
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
                new_names, df = get_related_names(nn, df, new_names)
        # df = df.astype('int32')
        df['tmp_taxon_id'] = tmp_taxon_id
        total_df = total_df.append(df, ignore_index=True)

total_df = total_df.drop_duplicates()
total_df = total_df.astype('int32')

# 取最新接受名，其他為同物異名或誤用名
# reference_id, group, taxon_name_id
# 抓status, publish_year

query = f"""SELECT id, publish_year, JSON_EXTRACT(properties, "$.doi") FROM `references` 
            WHERE id IN ({str(total_df.reference_id.unique()).replace('[','').replace(']','')})"""

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results = results.rename(columns={0: 'reference_id', 1: 'publish_year', 2: 'doi'})

total_df = total_df.merge(results)


str(list(total_df.ru_id.unique())).replace('[','').replace(']','')

query = f"""SELECT id, status, taxon_name_id, parent_taxon_name_id FROM reference_usages 
          WHERE id IN ({str(list(total_df.ru_id.unique())).replace('[','').replace(']','')})"""

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
            print('hello')
            # TODO 尚未完成
            # 如果年份一樣，比對publish_date
            # 也排除ref=153的情況？
    else:
        total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_year'] == max_yr), 'is_latest'] = True


for i in total_df.index:
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
# 一個name只會對應到一個taxon?
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
        for n in results[0]:
            if n:
                n_list += json.loads(n)
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
    query = f'SELECT properties FROM reference_usages WHERE id = {ru}'
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        results = json.loads(results[0][0])
        if results:
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
    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                VALUES (%s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (5, taxon_id, '新增Taxon'))
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
    # links -> 會需要先知道階層
    print('link')
    if links := get_links(taxon_id):
        for rl in links:
            query =  f"""INSERT INTO api_taxon_link
                        (taxon_id, source_id, suffix)
                        VALUES(%s, %s, %s)
                        """
            with conn.cursor() as cursor:
                cursor.execute(query, (taxon_id, source_dict[rl['source']], rl['suffix']))
                conn.commit()

# 檢查是不是所有都有寫入Taxon tree -> 有缺是因為少了reference_usages


# 1-2 確認有沒有修改到階層



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


if all(check_1_count==1) and all(check_2_count==1):
    # 2-1 確認文獻是不是已存在在其taxon，若沒有，api_taxon_history新增文獻
    conn = pymysql.connect(**db_settings)
    for t in old_tmp_taxon_list:
        # 抓原本已存在的
        taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
        current_ref = []
        query = f"SELECT DISTINCT(ru.reference_id) FROM api_taxon_usages atu\
                JOIN reference_usages ru ON atu.reference_usage_id = ru.id \
                WHERE atu.taxon_id = '{taxon_id}'"
        with conn.cursor() as cursor:
            cursor.execute(query)
            current_ref= cursor.fetchall()
            current_ref = [r[0] for r in current_ref]
        if len(total_df[(total_df['tmp_taxon_id']==t)&(~total_df['reference_id'].isin(current_ref))]):
            # 代表有新的文獻
            for i in total_df[(total_df['tmp_taxon_id']==t)&(~total_df['reference_id'].isin(current_ref))].reference_id:
                if i != 153: # backbone不算
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                            VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (2, taxon_id, json.dumps({'reference_id': int(i)})))
                        conn.commit()
    # 寫入api_taxon_usages
    for t in old_tmp_taxon_list:
        rows = total_df[total_df['tmp_taxon_id']==t]
        taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
        print(taxon_id)
        # 是否有新增的taxon_name_id -> 新增同物異名
        # TODO 也有可能是刪除?
        if any(rows.taxon_status == 'not-accepted'):
            query = f"SELECT DISTINCT(taxon_name_id) FROM api_taxon_usages WHERE taxon_id = '{taxon_id}' and `status`='not-accepted'"
            with conn.cursor() as cursor:
                cursor.execute(query)
                syns = cursor.fetchall()
                syns = [s[0] for s in syns]
            for new_syn in rows[(rows.taxon_status=='not-accepted')&~rows.taxon_name_id.isin(syns)].taxon_name_id.unique():
                # 寫入api_taxon_history
                query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                            VALUES (%s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (1, taxon_id, json.dumps({'taxon_name_id':int(new_syn)})))
                    conn.commit()
        # 寫入api_taxon
        i = rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row index
        # for i in rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index:
        row = total_df.iloc[i] # 接受的row
        rank_id = row.rank_id
        accepted_taxon_name_id = row.taxon_name_id
        ru_list = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['status'] == 'accepted')].ru_id.to_list()
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
            if len(common_names) > 1:
                alternative_name_c = ', '.join(common_names[1:])
            else:
                alternative_name_c = None
        else:
            common_name_c, alternative_name_c = None, None
        # is_hybrid
        ru_list = total_df[total_df['tmp_taxon_id'] == t].ru_id.to_list()
        if len(ru_list) == 1:
            query = f'SELECT JSON_EXTRACT(properties, "$.is_hybrid_formula") FROM reference_usages WHERE id = {ru_list[0]}'
        else:
            query = f'SELECT JSON_EXTRACT(properties, "$.is_hybrid_formula")  FROM reference_usages WHERE id IN {tuple(ru_list)}'
        is_hybrid = 0
        is_hybrid_list = []
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            for r in results:
                is_hybrid_list.append(r[0])
        if any(is_hybrid_list):
            is_hybrid = 1
        # other properties based on latest reference usages
        is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine = None, None, None, None, None, None, None
        ru = total_df[(total_df['tmp_taxon_id'] == t) & (total_df['taxon_status'] == 'accepted') & (total_df['is_latest'])].ru_id.to_list()[0]
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
        common_names_str = (',').join(common_names)
        new_taxon_data = (accepted_taxon_name_id, common_names_str, is_hybrid, is_endemic, alien_type, is_fossil, 
                            is_terrestrial, is_freshwater, is_brackish, is_marine)
        # 比對和原本的是否相同
        query = f"""SELECT at.accepted_taxon_name_id, concat_ws(',',at.common_name_c, at.alternative_name_c), at.is_hybrid,
                    at.is_endemic, at.alien_type, at.is_fossil, at.is_terrestrial, at.is_freshwater, at.is_brackish, at.is_marine
                    FROM api_taxon at 
                    WHERE at.taxon_id = '{taxon_id}'"""
        with conn.cursor() as cursor:
            cursor.execute(query)
            taxon_data = cursor.fetchone()
        if new_taxon_data != taxon_data:
            # 接受名改變
            if new_taxon_data[0] != taxon_data[0]:
                query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                VALUES (%s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (0, taxon_id, json.dumps({'old_taxon_name_id': taxon_data[0], 'new_taxon_name_id': new_taxon_data[0]})))
                    conn.commit()
            # 新增中文名 -> 也可能是刪除
            if new_taxon_data[1] != taxon_data[1]:
                if new_common_names := [n for n in new_taxon_data[1].split(',') if n not in taxon_data[1].split(',')]:
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (7, taxon_id, '、'.join(new_common_names)))
                        conn.commit()
            # 物種資訊更新 
            # 2 is_hybrid, 3 is_endemic, 5 is_fossil, 
            # 6 is_terrestrial, 7 is_freshwater, 8 is_brackish, 9 is_marine, 
            prop_dict = {2: 'is_hybrid', 4: 'alien_type', 3: 'is_endemic', 5: 'is_fossil', 6: 'is_terrestrial',7: 'is_freshwater',8: 'is_brackish',9: 'is_marine'}
            for p in prop_dict.keys():
                if not taxon_data[p] and new_taxon_data[p]: # 新增屬性
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (8, taxon_id, prop_dict[p]))
                        conn.commit()
                elif taxon_data[p] and not new_taxon_data[p]: # 移除屬性
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                                    VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (9, taxon_id, prop_dict[p]))
                        conn.commit()
            # 原生 / 外來
            # 4 alien_type (新增和移除跟上方一起)
            if taxon_data[4] and new_taxon_data[4] and new_taxon_data[4] != taxon_data[4]:
                query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                                    VALUES (%s, %s, %s)"
                with conn.cursor() as cursor:
                    cursor.execute(query, (10, taxon_id, json.dumps({'old':taxon_data[4], 'new': new_taxon_data[4], 'type': 'alien_type'})))
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

# 階層更新
conn = pymysql.connect(**db_settings)
query = "SELECT taxon_id, path FROM api_taxon_tree"
with conn.cursor() as cursor:
    cursor.execute(query)
    trees = cursor.fetchall()
    trees = pd.DataFrame(trees, columns=['taxon_id', 'path'])

query = """
        WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id) AS
            (
                SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id
                FROM api_taxon_usages c
                JOIN taxon_names t on c.taxon_name_id = t.id 
                WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
                UNION ALL
                SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id
                FROM find_ancestor cp
                JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
                JOIN taxon_names t on c.taxon_name_id = t.id 
                WHERE c.is_latest = 1 and c.status = 'accepted' 
            )
        SELECT taxon_id, path
        FROM find_ancestor;
        """
with conn.cursor() as cursor:
    cursor.execute(query)
    new_trees = cursor.fetchall()
    new_trees = pd.DataFrame(new_trees, columns=['taxon_id', 'new_path'])

tree_merged = trees.merge(new_trees)


for t in tree_merged[tree_merged.new_path != tree_merged.path].index:
    tree_row = tree_merged.iloc[t]
    path_list = tree_row.new_path
    path_list = path_list.split('>')
    if len(path_list) > 1:
        parent_taxon_id = path_list[1]
    # 寫入taxon_history
    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                VALUES (%s, %s, %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (4, taxon_id, json.dumps({'old': tree_row.path, 'new': tree_row.new_path })))
        conn.commit()
    # 更新taxon_tree
    if len(path_list) > 1:
        query = f"UPDATE api_taxon_tree SET path = '{tree_row.new_path}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = '{parent_taxon_id}' WHERE taxon_id = '{tree_row.taxon_id}'"
    else:
        query = f"UPDATE api_taxon_tree SET path = '{tree_row.new_path}', updated_at = CURRENT_TIMESTAMP WHERE taxon_id = '{tree_row.taxon_id}'"
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


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
            query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                        VALUES (%s, %s, %s)"
            with conn.cursor() as cursor:
                cursor.execute(query, (6, taxon_id, '因台灣無分佈而刪除Taxon'))
                conn.commit()
            # 修改api_taxon
            query = f"UPDATE api_taxon SET is_deleted = 1 WHERE taxon_id = '{taxon_id}'"
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()


# TODO 保育資訊更新 -> 可能要做成ppt 包含來源資料更新 / 手動更新 / 自動更新
conn = pymysql.connect(**db_settings)
conserv_dict = {1: 'protected', 5: 'iucn', 9: 'cites', 11: 'red', 16: 'sensitive'}
# protected 1 protected_category
# IUCN 5 iucn_category
# CITES 9 cites_listing
# RED 11 red_category
# sensitive 16 sensitive_suggest            
for t in old_tmp_taxon_list:
    taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
    new_conservation_info = get_conservation_info(taxon_id) # 空值
    query = f"""SELECT taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                    cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                    sensitive_default, sensitive_suggest,  sensitive_note FROM api_conservation WHERE taxon_id = '{taxon_id}'"""
    with conn.cursor() as cursor:
        cursor.execute(query)
        conservation_info = cursor.fetchone() 
        if not conservation_info: # 空值代表完全沒有任何保育資訊
            if new_conservation_info: # 有新的要新增
                # 寫入conservation_info
                query = f"""
                        INSERT INTO api_conservation
                        (taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                            cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                            sensitive_default, sensitive_suggest,  sensitive_note)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                with conn.cursor() as cursor:
                    cursor.execute(query,new_conservation_info)
                    conn.commit()
                # 寫入api_taxon_history
                for c in conserv_dict.keys():
                    if new_conservation_info[c]: # 新增保育資訊
                        query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                    VALUES (%s, %s, %s)"
                        with conn.cursor() as cursor:
                            cursor.execute(query, (11, taxon_id, json.dumps({'category': new_conservation_info[c],'type': conserv_dict[c]})))
                            conn.commit()
        else:
            # 空 -> 有
            # 有 -> 空
            # 有 -> 有
            for c in conserv_dict.keys():
                if not conservation_info[c] and new_conservation_info[c]: # 新增保育資訊
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (11, taxon_id, json.dumps({'category': new_conservation_info[c],'type': conserv_dict[c]})))
                        conn.commit()
                elif conservation_info[c] and not new_conservation_info[c]: # 移除保育資訊
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                                    VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (12, taxon_id, json.dumps({'category': conservation_info[c],'type': conserv_dict[c]})))
                        conn.commit()
                elif conservation_info[c] and new_conservation_info[c] and new_conservation_info[c] != conservation_info[c]:
                    query = f"INSERT INTO api_taxon_history (type, taxon_id, content ) \
                                                        VALUES (%s, %s, %s)"
                    with conn.cursor() as cursor:
                        cursor.execute(query, (13, taxon_id, json.dumps({'old':conservation_info[4], 'new': new_conservation_info[4], 'type': conserv_dict[c]})))
                        conn.commit()


# TODO 相關連結更新 -> 不用寫在history裡, 也可能會有手動更新的情況, 要避免自動更新覆蓋掉手動更新
for t in old_tmp_taxon_list:
    taxon_id = total_df[(total_df['tmp_taxon_id']==t)&(total_df.taxon_id.notnull())].taxon_id.values[0]
    links = get_links(taxon_id, updated=True)
    query =  f"""UPDATE api_taxon
                SET links=%s
                WHERE taxon_id = '{taxon_id}'
                """
    with conn.cursor() as cursor:
        cursor.execute(query, json.dumps(links))
        conn.commit()


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
    # 先確定是不是直翅目 t010005
    if 't010005' in path:
        orthoptera_df = results.merge(orthoptera, right_on='scientificName', left_on='name')
        for i in orthoptera_df.acceptedNameUsageID:
            links += [{'source': 'orthoptera', 'suffix': int(i)}]
    # IRMNG
    irmng_df = results[['name']].merge(irmng, left_on='name', right_on='source_name')
    for i in irmng_df.taxonID:
        links += [{'source': 'irmng', 'suffix': int(i)}]
    # GISD 確認是不是invasive
    query = f"select id from api_taxon where alien_type='invasive' and taxon_id = '{taxon_id}' ;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        exists = cursor.fetchall()
        if exists:
            links += [{'source': 'gisd', 'suffix': True}]
    # Amphibian Species of the World
    if 't024205' in path:
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
        # 先確認是不是半翅目 Hemiptera t009944
        if 't009944' in path:
            flow_df = results[['name']].merge(flow, left_on='name', right_on='source_name')
            for i in flow_df.id:
                links += [{'source': 'flow', 'suffix': int(i)}]
        # POWO & tropicos
        if 't024279' in path:
            links += [{'source': 'powo', 'suffix': True}, {'source': 'tropicos', 'suffix': True}]
        # ADW 
        if 't024280' in path:
            links += [{'source': 'adw', 'suffix': True}]
        # antwiki  Formicidae (蟻科) t008403
        if 't008403' in path:
            links += [{'source': 'antwiki', 'suffix': True}]
        # mycobank Fungi (真菌界) t074124
        if 't074124' in path:
            links += [{'source': 'mycobank', 'suffix': True}]
        # worms is_marine, is_brackish
        query = f"select id from api_taxon where (is_marine=1 or is_brackish=1) and taxon_id = '{taxon_id}' ;"
        with conn.cursor() as cursor:
            cursor.execute(query)
            exists = cursor.fetchall()
            if exists:
                links += [{'source': 'worms', 'suffix': True}]
        # fishbase
        # 先確認是不是Myxini (盲鰻綱) t024240, Chondrichthyes (軟骨魚綱) t024217, Actinopterygii(條鰭魚綱) t024204
        if 't024240' in path or 't024217' in path or 't024204' in path:
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
        # 先確認是不是Araneae t009846
        if 't009846' in path:
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
    if len (protected_df) == 1:
        protected_category = protected_df.Category.values[0]
        protected_note = [{'name': protected_df.ScientificName.values[0]}]
    else:
        protected_category = None
        protected_note = []
    # IUCN - 目前抓到的都是只有一個對應的name
    iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note = None, None, None, None, []
    for i in results.index:
        name = results.iloc[i]['name']
        url = f"https://apiv3.iucnredlist.org/api/v3/species/{name}?token={env('IUCN_TOKEN')}"
        data = requests.get(url)
        if data.status_code == 200:
            r = data.json().get('result')
            if r:
                iucn_note = []
                iucn_taxon_id = r[0]['taxonid']
                iucn_assessment_date = r[0]['assessment_date']
                iucn_category = r[0]['category']
                iucn_criteria = r[0]['criteria']
                for rs in r:
                    iucn_note += [{'name': rs['scientific_name'], 'taxon_id': int(rs['taxonid']), 'category': r[0]['category'] }]
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
    elif len(cites_df) == 1:
        cites_id = cites_df.cites_id.values[0]
        cites_listing = cites_df.cites_listing.values[0]
        cites_note = [{'name': cites_df['source_name'].values[0], 'listing': cites_listing, 'id': cites_id, 'is_primary': True}]
    # 紅皮書
    red_df = results.merge(red, left_on='name', right_on='TaiCOL-accepted_name')
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
    if len(sensitive_df) == 1:
        sensitive_default = sensitive_df.敏感層級_預設.values[0]
        sensitive_suggest = sensitive_df.敏感層級_建議.values[0]
        sensitive_note = [{'name': sensitive_df.source_name.values[0],'suggest': sensitive_suggest}]
    else:
        sensitive_default, sensitive_suggest,  sensitive_note = None, None, []
    if any((protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, sensitive_default, sensitive_suggest, sensitive_note)):
        return taxon_id, protected_category, json.dumps(protected_note), iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, json.dumps(iucn_note), cites_id, cites_listing, json.dumps(cites_note), red_category, red_criteria, red_adjustment, json.dumps(red_note), sensitive_default, sensitive_suggest,  json.dumps(sensitive_note)
    else:
        return None


## 決定誰是主要的學名

tmp = pd.DataFrame(tmp, columns=['iucn_taxon_id','iucn_assessment_date','iucn_category','iucn_criteria','name','taxon_id'])
df = pd.DataFrame(tmp, columns=['iucn_taxon_id','iucn_assessment_date','iucn_category','iucn_criteria','name','taxon_id'])

# merge accepted_name

def determine_name(df,source, results):
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
    if source !='red':
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
    if len(original_df[original_df['name'] == accepted_name]):
        accepted_original_name_id = original_df[original_df['name'] == accepted_name].original_taxon_name_id.values[0]
    else:
        accepted_original_name_id = None
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
        # 存入資料庫
        # with conn.cursor() as cursor:
        #     cursor.execute(query,(chosen_row.taxon_id.values[0], row['name'], int(chosen_row.cites_id.values[0]), chosen_row.cites_listing.values[0], json.dumps(cites_note)))
        #     conn.commit()
    else:
        no_data.append(t)
    return notes, chosen_row






#--------------test
test = pd.read_csv('/code/data/conservation/cites.csv')


headers = {'X-Authentication-Token': env('CITES_TOKEN')}
cites_df = pd.DataFrame(columns=['taxon_id','cites_name','cites_id','cites_listing'])
t_list = test[test['taxon_id'].duplicated()].taxon_id.to_list()
for i in test[test.taxon_id.isin(t_list)].index:
    print(i)
    name = test.iloc[i]['name']
    taxon_id = test.iloc[i]['taxon_id']
    url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={name}"
    data = requests.get(url, headers=headers)
    if data.status_code == 200:
        if r := data.json().get('taxon_concepts'):
            if r[0].get('cites_listing'):
                cites_listing = r[0]['cites_listing']
                cites_name = r[0]['full_name']
                cites_id = r[0]['id']
                cites_df = cites_df.append({'taxon_id': taxon_id , 'cites_name': cites_name, 'cites_id': cites_id, 'cites_listing': cites_listing},ignore_index=True)
            elif r[0].get('accepted_names'):
                url = f"https://api.speciesplus.net/api/v1/taxon_concepts?name={r[0].get('accepted_names')[0]['full_name']}"
                cites_name = r[0].get('accepted_names')[0]['full_name']
                data = requests.get(url, headers=headers)
                if data.status_code == 200:
                    if r := data.json().get('taxon_concepts'):
                        cites_id = r[0]['id']
                        cites_listing = r[0]['cites_listing']
                        cites_df = cites_df.append({'taxon_id': taxon_id , 'cites_name': cites_name, 'cites_id': cites_id, 'cites_listing': cites_listing},ignore_index=True)

cites_df = cites_df.drop_duplicates()





query = f"""
        INSERT INTO api_conservation
        (taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
            cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
            sensitive_default, sensitive_suggest,  sensitive_note)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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