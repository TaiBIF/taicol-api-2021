# 2023-03-01
# final version for new TaiCOL

# reference_usage_id 欄位先留著，但這個id有可能是會變動的，不能使用
# 改用taxon_name_id, accepted_taxon_name_id, reference_id組合為唯一值
# 實務上仍有可能會重複，若有重複情況則取集中一筆


# table list
# api_citations
# api_conservation
# api_names
# api_taxon
# api_taxon_history
# api_taxon_tree
# api_taxon_usages
# api_web_stat
# api_web_table


# 不用修改的
# api_links
# api_namecode

# update taxon information
import re
import itertools
from conf.settings import env
import pymysql
import pandas as pd
import requests
from datetime import datetime
import json
import glob
from numpy import nan
import numpy as np


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


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

# 1-1 新增文獻 -> 直接寫入api_citations

query = "SELECT max(updated_at) FROM `references`"

with conn.cursor() as cursor:
    cursor.execute(query)
    citation_last_updated = cursor.fetchone()[0]

query = "SELECT p.last_name, p.first_name, p.middle_name, pr.reference_id, pr.order, r.publish_year, \
        r.type, r.title, r.properties \
        FROM person_reference pr \
        JOIN persons p ON pr.person_id = p.id \
        JOIN `references` r on pr.reference_id = r.id;"

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = pd.DataFrame(cursor.fetchall(), columns=['last_name', 'first_name', 'middle_name', 'reference_id', 'order', 'year', 'type', 'title', 'properties'])

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

for i in results.index:
    row = results.iloc[i]
    prop = json.loads(row.properties)
    content = row.title
    # 書籍
    if row.type == 3:
        # content = row.title
        if content[-1] != '.':
            content += '.'
    # 名錄
    elif row.type == 4:
        pass
        # content = row.title
    # 期刊文章
    elif row.type == 1:
        if prop:
            content = f"{prop.get('article_title')}. <i>{prop.get('book_title')}</i> {prop.get('volume')}"
            if prop.get('issue'):
                content += f"({prop.get('issue')})"
            content += f": {prop.get('pages_range')}."
    # 書籍篇章
    elif row.type == 2:
        if prop:
            content = f"{prop.get('article_title')}. In: {prop.get('book_title')},"
            if prop.get('edition'):
                content += f" {prop.get('edition')} ed.,"
            if prop.get('volume'):
                content += f" vol. {prop.get('volume')}."
            elif prop.get('chapter'):
                content += f" ch. {prop.get('chapter')}."
            content += f" {prop.get('pages_range')}."
    citation_df.loc[citation_df['reference_id']==row.reference_id, 'content'] = content

citation_df = citation_df.drop_duplicates()
citation_df = citation_df.reset_index(drop=True)

conn = pymysql.connect(**db_settings)
for i in citation_df.index:
    row = citation_df.iloc[i]
    with conn.cursor() as cursor:
        query = "INSERT INTO api_citations (reference_id, author, short_author, content, updated_at, created_at) VALUES(%s, %s, %s, %s, %s, %s)"        
        cursor.execute(query, (row.reference_id, row.author, row.short_author, row.content, citation_last_updated, citation_last_updated))
        conn.commit()

# 2 api_names
# def create_names(name_list, hybrid_name_list):

query = "SELECT max(updated_at) FROM `taxon_names`"

with conn.cursor() as cursor:
    cursor.execute(query)
    name_last_updated = cursor.fetchone()[0]

rows = []
query = f"SELECT rank_id, nomenclature_id, properties, id, `name` FROM taxon_names WHERE rank_id <> 47"
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()

for r in results:
    pp = json.loads(r[2])
    if r[0] < 30:  # rank 為屬以上
        if r[1] in [3,4]: # 命名規約為細菌、古菌或病毒
            formatted_name = f"<i>{pp.get('latin_name')}</i>"
        else:
            formatted_name = pp.get('latin_name')
    elif r[0] in [30,31,32,33]:  # rank 為屬 / 亞屬 /組 /亞組
        if r[1] == 2 and pp.get('is_hybrid'):  # 命名規約為植物且為雜交
            formatted_name = f"× <i>{pp.get('latin_name')}</i>"
        else:
            formatted_name = f"<i>{pp.get('latin_name')}</i>"
    elif r[0] == 34:  # rank 為種
        if r[1] == 2 and pp.get('is_hybrid'):  # 命名規約為植物且為雜交
            formatted_name = f"<i>{pp.get('latin_genus')}</i> × <i>{pp.get('latin_s1')}</i>"
        elif r[1] == 4: # 命名規約為病毒
            formatted_name = f"<i>{pp.get('latin_s1')}</i>"
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
                    elif pp.get('species_id'):
                        query = f"SELECT properties FROM taxon_names WHERE id = {pp.get('species_id')}"
                        conn = pymysql.connect(**db_settings)
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            n = cursor.fetchall()
                            npp = json.loads(n[0][0])
                            formatted_name = f"<i>{npp.get('latin_genus')} {npp.get('latin_s1')} {l.get('latin_name')}</i>"
                    else:
                        formatted_name = r[4]
                else:  # 種下下rank需顯示
                    formatted_name += f" {l.get('rank_abbreviation')} <i>{l.get('latin_name')}</i>"
                count += 1
        elif r[1] in [2,3]:  # 命名規約為植物 或菌&病毒
            # latin genus 可能是空的 & latin s1
            if pp.get('latin_genus') and pp.get('latin_s1'):
                formatted_name = f"<i>{pp.get('latin_genus')} {pp.get('latin_s1')}</i>"
            elif pp.get('species_id'):
                query = f"SELECT properties FROM taxon_names WHERE id = {pp.get('species_id')}"
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    n = cursor.fetchall()
                    npp = json.loads(n[0][0])
                    formatted_name = f"<i>{npp.get('latin_genus')} {npp.get('latin_s1')}</i>"
            else:
                formatted_name = r[4]
            for l in pp.get('species_layers'):
                formatted_name += f" {l.get('rank_abbreviation')} <i>{l.get('latin_name')}</i>"
        # # 命名規約為病毒
        # elif r[1] == 4: 
    else:
        formatted_name = r[4]
    rows.append([r[3], formatted_name])



conn = pymysql.connect(**db_settings)

count = 0
for r in rows:
    count += 1
    if count % 100 == 0:
        print(count)
    with conn.cursor() as cursor:
        query = "INSERT INTO api_names (taxon_name_id, formatted_name, updated_at, created_at) VALUES(%s, %s, %s, %s);"        
        cursor.execute(query, (r[0], r[1], name_last_updated, name_last_updated))
        conn.commit()

# 雜交組合最後處理（要等學名已經建立）
# if hybrid_name_list:
rows = []
query = f"WITH view as (SELECT tnhp.taxon_name_id, an.formatted_name FROM taxon_name_hybrid_parent tnhp \
        JOIN api_names an ON tnhp.parent_taxon_name_id = an.taxon_name_id \
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
# return rows

count = 0
for r in rows:
    count += 1
    if count % 100 == 0:
        print(count)
    with conn.cursor() as cursor:
        query = "INSERT INTO api_names (taxon_name_id, formatted_name, updated_at, created_at) VALUES(%s, %s, %s, %s) \
                ON DUPLICATE KEY UPDATE formatted_name=%s, updated_at = %s, created_at=%s; "        
        cursor.execute(query, (r[0], r[1], name_last_updated, name_last_updated, r[1], name_last_updated, name_last_updated))
        conn.commit()


# query = "SELECT id FROM taxon_names WHERE rank_id <> 47"
# conn = pymysql.connect(**db_settings)
# with conn.cursor() as cursor:
#     cursor.execute(query)
#     results = cursor.fetchall()

# name_list = [str(r[0]) for r in results if r]

# # 如果parent name有修改再修改
# # 原本這邊會有bug 有可能後來才把雜交親代加進去，但本身的name沒有更新，所以改成全選
# query = "SELECT tnhp.taxon_name_id FROM taxon_name_hybrid_parent tnhp"

# conn = pymysql.connect(**db_settings)
# with conn.cursor() as cursor:
#     cursor.execute(query)
#     results = cursor.fetchall()

# hybrid_name_list = [str(r[0]) for r in results if r]

# rows = create_names(name_list, hybrid_name_list)


# name_author另外處理

# 取得作者資訊
query = """SELECT p.last_name, p.abbreviation_name, ptn.taxon_name_id, ptn.order, ptn.role FROM person_taxon_name ptn
            LEFT JOIN persons p ON ptn.person_id = p.id """
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    author = cursor.fetchall()
    author = pd.DataFrame(author)
    author.columns = ['last_name', 'name_abbr', 'taxon_name_id', 'order', 'role']


# 這邊要修改formatted_authors 可能為空
# if name_list:
query = f"SELECT id, nomenclature_id, rank_id, name, original_taxon_name_id, formatted_authors, publish_year FROM taxon_names"
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    df = cursor.fetchall()
    df = pd.DataFrame(df)
    df.columns = ['taxon_name_id', 'nomenclature_id', 'rank_id', 'name', 'original_taxon_name_id', 'formatted_authors', 'publish_year']

df = df.replace({nan: None})

for i in df.index:
    if i % 1000 == 0:
        print(i)
    row = df.iloc[i]
    original_taxon_name_id = None
    author_str = ''
    ori_author_str = ''
    p_year = None
    ori_p_year = None
    ori_names = []
    names = []
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
                    if p_year not in [None, 0, '0', '']:
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
    elif row.nomenclature_id == 3: # 細菌
        # 原始組合名
        if original_taxon_name_id := row.original_taxon_name_id:
            tmp = author[author.taxon_name_id==row.original_taxon_name_id].sort_values('order')
            ori_names = [t for t in tmp.last_name]
            if len(df[df.taxon_name_id==row.original_taxon_name_id]):
                ori_p_year = df[df.taxon_name_id==row.original_taxon_name_id].publish_year.values[0]
        # else:
        tmp = author[author.taxon_name_id==row.taxon_name_id].sort_values('order')
        names = [t for t in tmp.last_name]
        p_year = row.publish_year
        # 也有可能沒有名字
        if len(names) == 0:
            if p_year not in [None, 0, '0', '']:
                author_str = p_year
        elif len(names) == 1:
            if p_year not in [None, 0, '0', '']:
                author_str = f'{names[0]} {p_year}'
            else:
                author_str = f'{names[0]}'
        elif len(names) == 2:
            if p_year not in [None, 0, '0', '']:
                author_str = f'{names[0]} & {names[1]} {p_year}'
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
                    if p_year not in [None, 0, '0', '']:
                        author_str += f" & {n} {p_year}"
                    else:
                        author_str += f" & {n}"
        if len(ori_names) == 0:
            if ori_p_year not in [None, 0, '0', '']:
                ori_author_str = ori_p_year
        elif len(ori_names) == 1:
            if ori_p_year not in [None, 0, '0', '']:
                ori_author_str = f'{ori_names[0]} {ori_p_year}'
            else:
                ori_author_str = f'{ori_names[0]}'
        elif len(ori_names) == 2:
            if ori_p_year not in [None, 0, '0', '']:
                ori_author_str = f'{ori_names[0]} & {ori_names[1]} {ori_p_year}'
            else:
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
                    if ori_p_year not in [None, 0, '0', '']:
                        ori_author_str += f" & {n} {ori_p_year}"
                    else:
                        ori_author_str += f" & {n}"
        # 原始組合名
        if ori_author_str:
            author_str = '(' + ori_author_str + ') ' + author_str
    if author_str:
        df.loc[i,'formatted_author'] = author_str.strip()

df['formatted_author'] = df['formatted_author'].replace({nan: ''})
df['formatted_author'] = df['formatted_author'].apply(str.strip)

conn = pymysql.connect(**db_settings)
for i in df.index:
    row = df.loc[i]
    if i % 1000 == 0:
        print(i)
    if row.formatted_author:
        query = f'UPDATE api_names SET name_author = %s, updated_at = %s, created_at = %s WHERE taxon_name_id = %s'
        with conn.cursor() as cursor:
            cursor.execute(query, (row.formatted_author, name_last_updated, name_last_updated, row.taxon_name_id))
            conn.commit()


# --------------------- Taxon更新 --------------------- #
# --------------------- 學名使用 --------------------- #

# group 改用 accepted_taxon_name_id 判斷

# 3 api_taxon_usages
# 取得所有相關的學名
def get_related_names(taxon_name_id, df, new_names):
    new_names.remove(taxon_name_id)  # remove current taxon_name_id
    ref_group_pair = ref_group_pair_total[(ref_group_pair_total.taxon_name_id==taxon_name_id)&(ref_group_pair_total.ru_status!='misapplied')]
    ref_group_pair = ref_group_pair_total.merge(ref_group_pair[['reference_id','accepted_taxon_name_id']])
    ref_group_pair = list(ref_group_pair.itertuples(index=False))
    names = []
    for p in range(len(ref_group_pair)):
        # TODO 應該要只用accepted嗎
        # if not len(df[(df.reference_id==ref_group_pair[p][0])&(df.accepted_taxon_name_id!=taxon_name_id)]):
        df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 
                    'accepted_taxon_name_id': ref_group_pair[p][1], 'taxon_name_id': ref_group_pair[p][5], 'rank_id': ref_group_pair[p][3], 'status': ref_group_pair[p][4]}, ignore_index=True)
        p_row = ref_group_pair_total[(ref_group_pair_total.reference_id==ref_group_pair[p][0])&(ref_group_pair_total.accepted_taxon_name_id==ref_group_pair[p][1])]
        p_row = p_row[['taxon_name_id','ru_status']]
        names.append(list(p_row.values[0]))
    new_names += [n[0] for n in names if n[0] not in list(df.taxon_name_id) and n[1] != 'misapplied']
    return new_names, df

# 抓出相關的names
conn = pymysql.connect(**db_settings)


query = 'SELECT max(updated_at) \
         FROM reference_usages ru \
         WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined") AND ru.deleted_at IS NULL AND ru.accepted_taxon_name_id IS NOT NULL;'

with conn.cursor() as cursor:
    cursor.execute(query)
    last_updated = cursor.fetchone()[0]


results = pd.DataFrame()
# 排除刪除的reference_usages
# TODO 未來有可能 一個文獻的某個學名只有一筆學名使用 且被設定成is_title=1 這種情況應該要保留那筆不得排除（若有設定status的話）
query = 'SELECT ru.id, ru.reference_id, ru.taxon_name_id, ru.accepted_taxon_name_id, tn.rank_id, ru.status \
         FROM reference_usages ru JOIN taxon_names tn ON ru.taxon_name_id = tn.id \
         WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined") AND ru.deleted_at IS NULL AND ru.accepted_taxon_name_id IS NOT NULL ORDER BY tn.rank_id;'

with conn.cursor() as cursor:
    cursor.execute(query)
    tmp = cursor.fetchall()
    tmp = pd.DataFrame(tmp, columns=['ru_id','reference_id','taxon_name_id','accepted_taxon_name_id', 'rank_id', 'ru_status'])
    results = results.append(tmp, ignore_index=True)

# 156120

# conn = pymysql.connect(**db_settings)

# query = f'''SELECT ru.reference_id, ru.accepted_taxon_name_id, ru.id, tn.rank_id, ru.status, tn.id FROM reference_usages ru
#             JOIN taxon_names tn ON ru.taxon_name_id = tn.id
#             WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined") AND ru.deleted_at IS NULL AND ru.accepted_taxon_name_id IS NOT NULL;'''
# with conn.cursor() as cursor:
#     cursor.execute(query)
#     ref_group_pair_total = cursor.fetchall()
ref_group_pair_total = results
ref_group_pair_total = results[['reference_id','accepted_taxon_name_id','ru_id','rank_id','ru_status','taxon_name_id']]
# pd.DataFrame(ref_group_pair_total, columns=['reference_id','accepted_taxon_name_id','ru_id','rank_id','ru_status','taxon_name_id'] )

# 比較taxon_name_id & ru_id跑出來的結果

cannot_decide_taxon = []
count = 0
total_df = pd.DataFrame(columns=['ru_id','reference_id','accepted_taxon_name_id'])

# 排除掉已經處理過的reference group
import time


for i in results.index:
    if i % 100 == 0:
        print(i)
    # s = time.time()
    row = results.iloc[i]
    if not len(total_df[total_df.ru_id==row.ru_id]):
        name_list = [row.taxon_name_id]
        new_names = []
        df = pd.DataFrame(columns=['reference_id','accepted_taxon_name_id'])
        # 1 找到所有的reference & accepted_taxon_name_id
        ref_group_pair = ref_group_pair_total[(ref_group_pair_total.taxon_name_id==row.taxon_name_id)&(ref_group_pair_total.ru_status!='misapplied')]
        ref_group_pair = ref_group_pair_total.merge(ref_group_pair[['reference_id','accepted_taxon_name_id']]).drop_duplicates()
        # time_a = time.time() -s 
        # s = time.time()
        # if not len(total_df[['reference_id','accepted_taxon_name_id']].merge(ref_group_pair[['reference_id','accepted_taxon_name_id']].drop_duplicates())):
        ref_group_pair = list(ref_group_pair.itertuples(index=False))
        # 2 根據有的 reference_id & accepted_taxon_name_id 再去抓抓看有沒有別的name_id (需排除status為空值或未決的資料)
        names = []
        for p in range(len(ref_group_pair)):
            df = df.append({'ru_id': ref_group_pair[p][2], 'reference_id': ref_group_pair[p][0], 'accepted_taxon_name_id': ref_group_pair[p][1],
                'taxon_name_id': ref_group_pair[p][5], 'rank_id': ref_group_pair[p][3], 'status': ref_group_pair[p][4]}, ignore_index=True)
            p_row = ref_group_pair_total[(ref_group_pair_total.reference_id==ref_group_pair[p][0])&(ref_group_pair_total.accepted_taxon_name_id==ref_group_pair[p][1])&(ref_group_pair_total.taxon_name_id==ref_group_pair[p][5])]
            p_row = p_row[['taxon_name_id','ru_status']]
            names.append(list(p_row.values[0]))
        # 如果reference & group中有新的學名，且為accepted，則在找他對應的所有reference & accepted_taxon_name_id
        new_names += [n[0] for n in names if n[0] not in name_list and n[1] != 'misapplied']  # 用來確定是不是還有name需要跑
        new_names = list(dict.fromkeys(new_names)) # drop duplicates
        while len(new_names) > 0:
            for nn in new_names:
                # 只抓status不是misapplied的new_names
                new_names, df = get_related_names(nn, df, new_names)
        # 排除掉related_name中 status是misapplied的name
        # time_b = time.time()-s
        # s = time.time()
        df = df.drop_duplicates().reset_index()
        # 目前有些ref group會缺資料，再抓回來
        final_ref_group_pair = []
        if len(df):
            for f in df[['reference_id','accepted_taxon_name_id']].drop_duplicates().index:
                final_ref_group_pair += [(df.iloc[f].reference_id, df.iloc[f].accepted_taxon_name_id)]
        final_df = pd.DataFrame()
        for p in range(len(final_ref_group_pair)):
            p_row = ref_group_pair_total[(ref_group_pair_total.reference_id==final_ref_group_pair[p][0])&(ref_group_pair_total.accepted_taxon_name_id==final_ref_group_pair[p][1])]
            p_row = p_row[['ru_id','reference_id','accepted_taxon_name_id','taxon_name_id','rank_id','ru_status']]
            final_df = final_df.append(p_row)
        final_df = final_df.drop_duplicates().reset_index()
        # time_c = time.time() -s
        # 如果ref & group已存在在其他tmp_taxon_id，則納入該tmp_taxon_id分類群 
        if len(final_df):
            check_if_taxon_id = pd.DataFrame()
            if len(total_df):
                check_if_taxon_id = total_df.merge(final_df)
            if len(check_if_taxon_id):
                if len(check_if_taxon_id.tmp_taxon_id.unique()) == 1:
                    final_df['tmp_taxon_id'] = check_if_taxon_id.tmp_taxon_id.unique()[0]
                else:
                    # print('noooooo')
                    cannot_decide_taxon.append(check_if_taxon_id.tmp_taxon_id.to_list())
                    break
            else:
                count += 1
                tmp_taxon_id = count
                final_df['tmp_taxon_id'] = tmp_taxon_id
                # final_df['time_a'] = time_a
                # final_df['time_b'] = time_b
                # final_df['time_c'] = time_c
            total_df = total_df.append(final_df, ignore_index=True)

total_df = total_df.drop_duplicates()

test = total_df

test.to_csv('test.csv', index=None)
# total_df = test

# 取最新接受名，其他為同物異名或誤用名
# reference_id, group, taxon_name_id
# 抓status, publish_year
conn = pymysql.connect(**db_settings)

query = f'SELECT id, publish_year, JSON_EXTRACT(properties, "$.doi"), `type` FROM `references`'

with conn.cursor() as cursor:
    cursor.execute(query)
    refs = cursor.fetchall()
    refs = pd.DataFrame(refs)
    refs = refs.rename(columns={0: 'reference_id', 1: 'publish_year', 2: 'doi', 3: 'type'})

total_df = total_df.merge(refs)

query = f"SELECT `status`, parent_taxon_name_id, properties, id FROM reference_usages"

with conn.cursor() as cursor:
    cursor.execute(query)
    rus = cursor.fetchall()
    rus = pd.DataFrame(rus)
    rus = rus.rename(columns={ 0: 'status', 1: 'parent_taxon_name_id', 2: 'properties', 3: 'ru_id'})

total_df = total_df.merge(rus, on=['ru_id'])
total_df = total_df.drop(['index'],axis=1)

# 決定誰是接受學名
taxon_list = total_df.tmp_taxon_id.unique()

total_df['taxon_status'] = ''
total_df['is_latest'] = False
total_df['publish_date'] = ''

total_df.loc[total_df.reference_id==328,'publish_date'] = '2021-01-06'
total_df.loc[total_df.reference_id==336,'publish_date'] = '2021-09-03'
total_df.loc[total_df.reference_id==612,'publish_date'] = '2015-12-01'
total_df.loc[total_df.reference_id==674,'publish_date'] = '2015-05-15'
total_df.loc[total_df.reference_id==90,'publish_date'] = '2019-10-25'
total_df.loc[total_df.reference_id==87,'publish_date'] = '2019-02-28'
total_df.loc[total_df.reference_id==269,'publish_date'] = '1999-01-13'

cannot_decide = []


def check_latest(t):
    temp = total_df[total_df['tmp_taxon_id'] == t]
    # 如果有文獻的話就忽略backbone
    ignore_backbone = False
    if not all(temp['type']==4):
        temp = temp[temp['type']!=4]
        ignore_backbone = True
    # 如果都是backbone就直接比, 如果有大於一個reference_id, 比較年份
    yr = temp[['reference_id', 'publish_year']].drop_duplicates()
    max_yr = yr.publish_year.max()
    if len(yr[yr['publish_year'] == max_yr]) > 1:
        ref_list = yr[yr['publish_year'] == max_yr].reference_id.to_list()
        for d in temp[temp.publish_date!=''].doi.unique():
            date_list = []
            if d:
                d_str = d.replace('"', '').replace("'", '')
                if d_str:
                    url = f'https://api.crossref.org/works/{d_str}'
                    result = requests.get(url)
                    if result.status_code == 200:
                        result = result.json()
                        if result:
                            if result.get('message'):
                                try:
                                    date_list = result.get('message').get('published-print').get('date-parts')[0]
                                except:
                                    pass
                                try:
                                    total_df.loc[total_df['doi'] == d, 'publish_date'] = datetime(date_list[0], date_list[1], date_list[2]).strftime("%Y-%m-%d")
                                except:
                                    pass
            temp = total_df[total_df['tmp_taxon_id'] == t]
            dt = temp[['reference_id', 'publish_date']].drop_duplicates()
            max_dt = dt.publish_date.max()
            if len(dt[dt['publish_date'] == max_dt]) > 1:
                cannot_decide += [t]
            else:
                total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_date'] == max_dt), 'is_latest'] = True
    else:
        # 這裡也要排除backbone
        if ignore_backbone:
            total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_year'] == max_yr) & (total_df['type'] != 4), 'is_latest'] = True
        else:
            total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_year'] == max_yr), 'is_latest'] = True


for t in taxon_list:
    if t % 1000 == 0:
        print(t)
    check_latest(t)
    # temp = total_df[total_df['tmp_taxon_id'] == t]
    # # 如果有文獻的話就忽略backbone
    # ignore_backbone = False
    # if not all(temp['type']==4):
    #     temp = temp[temp['type']!=4]
    #     ignore_backbone = True
    # # 如果都是backbone就直接比, 如果有大於一個reference_id, 比較年份
    # yr = temp[['reference_id', 'publish_year']].drop_duplicates()
    # max_yr = yr.publish_year.max()
    # if len(yr[yr['publish_year'] == max_yr]) > 1:
    #     ref_list = yr[yr['publish_year'] == max_yr].reference_id.to_list()
    #     for d in temp[temp.publish_date!=''].doi.unique():
    #         date_list = []
    #         if d:
    #             d_str = d.replace('"', '').replace("'", '')
    #             if d_str:
    #                 url = f'https://api.crossref.org/works/{d_str}'
    #                 result = requests.get(url)
    #                 if result.status_code == 200:
    #                     result = result.json()
    #                     if result:
    #                         if result.get('message'):
    #                             try:
    #                                 date_list = result.get('message').get('published-print').get('date-parts')[0]
    #                             except:
    #                                 pass
    #                             try:
    #                                 total_df.loc[total_df['doi'] == d, 'publish_date'] = datetime(date_list[0], date_list[1], date_list[2]).strftime("%Y-%m-%d")
    #                             except:
    #                                 pass
    #         temp = total_df[total_df['tmp_taxon_id'] == t]
    #         dt = temp[['reference_id', 'publish_date']].drop_duplicates()
    #         max_dt = dt.publish_date.max()
    #         if len(dt[dt['publish_date'] == max_dt]) > 1:
    #             cannot_decide += [t]
    #         else:
    #             total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_date'] == max_dt), 'is_latest'] = True
    # else:
    #     # 這裡也要排除backbone
    #     if ignore_backbone:
    #         total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_year'] == max_yr) & (total_df['type'] != 4), 'is_latest'] = True
    #     else:
    #         total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['publish_year'] == max_yr), 'is_latest'] = True

print(cannot_decide)


# total_df.to_csv('version_accepted.csv', index=None)
# TODO 如果在一個tmp_taxon_id裡，最新文獻中有兩個有效分類群，則代表可能有物種拆分的情況產生

check_tmp = total_df[total_df.is_latest==1][['tmp_taxon_id','reference_id','accepted_taxon_name_id']].drop_duplicates().groupby(['tmp_taxon_id','reference_id'], as_index=False).count()
# check_tmp = check.groupby(['reference_id','accepted_taxon_name_id','taxon_name_id']).tmp_taxon_id.nunique()
check_tmp_taxon_id = check_tmp[check_tmp.accepted_taxon_name_id>1].tmp_taxon_id.to_list()


test3 = total_df

test3.to_csv('test3.csv',index=False)

# a = pd.read_csv('test3.csv')

# total_df = test3

total_df['divided'] = False

reset_is_latest_list = []

for ctt in check_tmp_taxon_id:
    print(ctt)
    rows = total_df[total_df.tmp_taxon_id==ctt]
    total_df.loc[total_df.tmp_taxon_id==ctt, 'divided'] = True
    rows_latest = rows[rows.is_latest]
    names_latest = rows_latest[rows_latest.ru_status!='misapplied'].taxon_name_id.to_list() # 最新文獻的同物異名
    rows_group = rows_latest.accepted_taxon_name_id.unique()
    # 如果在最新文獻中並沒有以其為接受名的分類群，且也沒出現最新文獻中的同物異名中，則給他一個獨立的tmp_taxon_id（但底下的同物異名可能會被搶走）
    no_group = rows[(~rows.accepted_taxon_name_id.isin(rows_group))&(~rows.accepted_taxon_name_id.isin(names_latest))].accepted_taxon_name_id.unique()
    # print(no_group)
    # 先把這邊改掉
    for ng in no_group:
        ng_new_taxon_id = total_df.tmp_taxon_id.max() + 1
        print(ng_new_taxon_id)
        reset_is_latest_list.append(ng_new_taxon_id)
        total_df.loc[(total_df.tmp_taxon_id==ctt)&(total_df.accepted_taxon_name_id==ng)&(~total_df.taxon_name_id.isin(rows_latest[rows_latest.ru_status!='misapplied'].taxon_name_id.to_list())),'tmp_taxon_id'] = ng_new_taxon_id
        # 移除掉有最新文獻的名字
        total_df = total_df[~((total_df.tmp_taxon_id==ctt)&(total_df.accepted_taxon_name_id==ng)&(total_df.taxon_name_id.isin(rows_latest[rows_latest.ru_status!='misapplied'].taxon_name_id.to_list())))]
    # 這些應該要被拆開來
    c = 0
    for rg in rows_group:
        new_tmp_taxon_id = ctt
        # 如果最新分類群中的某同物異名過去出現在其他分類群中，要忽略該筆usage
        syn_list = rows[(rows.accepted_taxon_name_id==rg)&(rows.is_latest==False)&(rows.ru_status=='not-accepted')].taxon_name_id.to_list()
        syn_list = list(dict.fromkeys(syn_list))
        syn_exclude = []
        for syn in syn_list:
            if syn in rows[(rows.accepted_taxon_name_id!=rg)&(rows.is_latest==True)].taxon_name_id.to_list() and syn not in syn_exclude:
                syn_exclude.append(syn)
        # 移除掉過去同物異名的usage
        total_df = total_df[~((total_df.tmp_taxon_id==ctt)&(total_df.accepted_taxon_name_id==rg)&(total_df.is_latest==False)&(total_df.taxon_name_id.isin(syn_exclude)))]
        # 修改新的taxon_id
        if c > 0:
            new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
            total_df.loc[(total_df.tmp_taxon_id==ctt)&(total_df.accepted_taxon_name_id==rg),'tmp_taxon_id'] = new_tmp_taxon_id
            tmp_name_list = total_df[(total_df.tmp_taxon_id==new_tmp_taxon_id)&(total_df.ru_status!='misapplied')].taxon_name_id.to_list()
            if len(tmp_name_list):
                total_df.loc[(total_df.tmp_taxon_id==ctt)&(total_df.accepted_taxon_name_id!=rg)&(total_df.taxon_name_id.isin(tmp_name_list))&(total_df.is_latest==False),'tmp_taxon_id'] = new_tmp_taxon_id
                total_df.loc[(total_df.tmp_taxon_id==ctt)&(total_df.accepted_taxon_name_id!=rg)&(total_df.accepted_taxon_name_id.isin(tmp_name_list))&(total_df.is_latest==False),'tmp_taxon_id'] = new_tmp_taxon_id
        c+=1
        # 如果有名字在no_group裡面 也要把他改成目前的tmp_taxon_id
        # total_df[(total_df.tmp_taxon_id.isin(no_group))&(total_df.taxon_name_id.isin(total_df[total_df.tmp_taxon_id==new_tmp_taxon_id].taxon_name_id.to_list()))&(total_df['status']!='misapplied')] = new_tmp_taxon_id

for t in reset_is_latest_list:
    check_latest(t)


total_df[total_df.divided==True].to_csv('check_divide.csv')

total_df = total_df.reset_index(drop=True)

# 再檢查一次

check_tmp = total_df[total_df.is_latest==1][['tmp_taxon_id','reference_id','accepted_taxon_name_id']].drop_duplicates().groupby(['tmp_taxon_id','reference_id'], as_index=False).count()
check_tmp_taxon_id = check_tmp[check_tmp.accepted_taxon_name_id>1].tmp_taxon_id.to_list()
print(check_tmp_taxon_id)

taxon_error = []

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
        accepted_name_id = total_df.loc[(total_df['tmp_taxon_id'] == row.tmp_taxon_id) & (total_df['status'] == 'accepted') & (total_df['is_latest'] == 1)]['taxon_name_id'].to_list()[0]
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


#===================

# query = """SELECT atu.id, atu.taxon_id, atu.taxon_name_id, atu.is_latest, ru.status  FROM api_taxon_usages atu
#             JOIN reference_usages ru ON ru.id = atu.reference_usage_id"""

# with conn.cursor() as cursor:
#     cursor.execute(query)
#     total_df = cursor.fetchall()
#     total_df = pd.DataFrame(total_df, columns=['id','taxon_id','taxon_name_id','is_latest','status'])

# total_df['taxon_status'] = ''
# for i in total_df.index:
#     if i % 1000 == 0:
#         print(i)
#     row = total_df.iloc[i]
#     if row.is_latest:
#         if row.status == 'not-accepted':
#             total_df.loc[i, 'taxon_status'] = 'not-accepted'
#         else:
#             total_df.loc[i, 'taxon_status'] = row.status
#     else:  # 不是最新的文獻
#         accepted_name_id = total_df.loc[(total_df['taxon_id'] == row.taxon_id) & (total_df['status'] == 'accepted') & (total_df['is_latest'] == 1)]['taxon_name_id'].to_list()[0]
#         if row.taxon_name_id == accepted_name_id:
#             # 如果和accpeted一樣的name, 設成accepted
#             total_df.loc[i, 'taxon_status'] = 'accepted'
#         else:
#             if row.status == 'misapplied':
#                 # 如果不一樣，且是misapplied, 設成misapplied
#                 total_df.loc[i, 'taxon_status'] = row.status
#             else:
#                 # 如果不一樣，且不是misapplied, 設成not-accepted
#                 total_df.loc[i, 'taxon_status'] = 'not-accepted'



# for i in total_df.index:
#     if i % 1000 == 0:
#         print(i)
#     row = total_df.iloc[i]
#     query = """
#             UPDATE api_taxon_usages SET `status` = %s WHERE id = %s;
#             """
#     with conn.cursor() as cursor:
#         cursor.execute(query, (row.taxon_status, row.id ))
#         conn.commit()


#===================

# 確定一個ru_id是不是只對到一個tmp_taxon_id
check = total_df[['reference_id','accepted_taxon_name_id','taxon_name_id','tmp_taxon_id']].drop_duplicates()
# check_tmp = check.groupby(['reference_id','accepted_taxon_name_id','taxon_name_id']).tmp_taxon_id.nunique()
# check_tmp[check_tmp>1]
all(check.groupby(['reference_id','accepted_taxon_name_id','taxon_name_id']).tmp_taxon_id.nunique() == 1)

# 1-1 新增taxon (api_taxon_usages & api_taxon & api_taxon_history & api_taxon_tree & api_conservation)
# new_taxon_list = total_df.tmp_taxon_id.unique() 

# 寫入taxon table
total_df = total_df.replace({nan: None})


query = f"SELECT id, properties FROM taxon_names"

with conn.cursor() as cursor:
    cursor.execute(query)
    name_prop = cursor.fetchall()
    name_prop = pd.DataFrame(name_prop)
    name_prop = name_prop.rename(columns={0: 'taxon_name_id', 1: 'properties'})


test2 = total_df
# test2[test2.tmp_taxon_id==nt]


# ===============================

# query = """SELECT ru.id, atu.taxon_id, atu.taxon_name_id, atu.is_latest, atu.status, ru.status,
#             atu.reference_id, atu.accepted_taxon_name_id, r.publish_year
#             FROM api_taxon_usages atu
#             JOIN reference_usages ru ON ru.id = atu.reference_usage_id
#             JOIN `references` r on r.id = atu.reference_id """

# with conn.cursor() as cursor:
#     cursor.execute(query)
#     total_df = cursor.fetchall()
#     total_df = pd.DataFrame(total_df, columns=['ru_id','taxon_id','taxon_name_id','is_latest','taxon_status', 'status',
#     'reference_id', 'accepted_taxon_name_id', 'publish_year'])

# query = f"SELECT `status`, parent_taxon_name_id, properties, id FROM reference_usages"

# with conn.cursor() as cursor:
#     cursor.execute(query)
#     rus = cursor.fetchall()
#     rus = pd.DataFrame(rus)
#     rus = rus.rename(columns={ 0: 'status', 1: 'parent_taxon_name_id', 2: 'properties', 3: 'ru_id'})


# count = 0
# for nt in total_df.taxon_id.unique():
#     count += 1
#     if count % 1000 == 0:
#         print(count)
#     conn = pymysql.connect(**db_settings)
#     rows = total_df[total_df['taxon_id']==nt]
#     i = rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row
#     row = total_df.iloc[i] # 接受的row
#     accepted_taxon_name_id = row.taxon_name_id
#     # 有效才會有common_names
#     ru_list = total_df[(total_df['taxon_id'] == nt) & (total_df['status'] == 'accepted')][['reference_id','accepted_taxon_name_id','taxon_name_id','publish_year','ru_id']]
#     prop_acp_list = []
#     tmp_ru_df = rus.merge(ru_list,left_on=['ru_id'],right_on=['ru_id'])
#     for pacp_i in tmp_ru_df.index:
#         pacp_i_row = tmp_ru_df.iloc[pacp_i]
#         pacp = pacp_i_row.properties
#         if json.loads(pacp):
#             prop_acp_list.append({'properties': json.loads(pacp),'publish_year': pacp_i_row.publish_year})
#     # other properties based on latest reference usages
#     # 有資料的最新 忽略null 每個屬性要分開來看
#     is_dict = {
#         'is_in_taiwan' : 0,
#         'is_endemic': 0, 
#         'is_fossil': 0, 
#         'is_terrestrial': 0, 
#         'is_freshwater': 0, 
#         'is_brackish': 0, 
#         'is_marine': 0
#     }
#     n_list = []
#     for n in prop_acp_list:
#         for current_is in is_dict.keys():
#             if n.get('properties').get(current_is) in [0,1]:
#                 # print(n.get('properties').get(current_is), current_is)
#                 n_list.append({'is': current_is,'value': n.get('properties').get(current_is),'publish_year': n.get('publish_year')})
#     n_list = pd.DataFrame(n_list)
#     if len(n_list):
#         n_list = n_list.sort_values(by=['is','publish_year'], ascending=False).reset_index()
#         for current_is in is_dict.keys():
#             if len(n_list[n_list['is']==current_is]):
#                 is_dict[current_is] = n_list[n_list['is']==current_is]['value'].values[0]
#     # 寫入 api_taxon table 
#     query = """
#             UPDATE api_taxon SET `is_endemic` = %s, is_fossil = %s, is_terrestrial = %s, is_freshwater = %s, is_brackish = %s, is_marine = %s, is_in_taiwan = %s  WHERE taxon_id = %s;
#             """
#     with conn.cursor() as cursor:
#         cursor.execute(query, (is_dict['is_endemic'], is_dict['is_fossil'], is_dict['is_terrestrial'], is_dict['is_freshwater'], is_dict['is_brackish'], is_dict['is_marine'], is_dict['is_in_taiwan'], nt))
#         conn.commit()



# ===============================


# total_df.groupby('tmp_taxon_id',as_index=False).ru_id.count().sort_values('ru_id')

# now_count = 0
# 100365
for nt in total_df.tmp_taxon_id.unique():
    # now_count += 1
    if nt % 1000 == 0:
        print(nt)
    conn = pymysql.connect(**db_settings)
    rows = total_df[total_df['tmp_taxon_id']==nt]
    if len(rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')][['reference_id', 'accepted_taxon_name_id', 'taxon_name_id']].drop_duplicates()) == 1:
    # try:
        i = rows[(rows['is_latest']) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row
        row = total_df.iloc[i] # 接受的row
        rank_id = row.rank_id
        accepted_taxon_name_id = row.taxon_name_id
        # 有效才會有common_names
        ru_list = total_df[(total_df['tmp_taxon_id'] == nt) & (total_df['status'] == 'accepted')][['reference_id','accepted_taxon_name_id','taxon_name_id','publish_year','ru_id']]
        common_names = []
        prop_acp_list = []
        tmp_ru_df = rus.merge(ru_list,left_on=['ru_id'],right_on=['ru_id'])
        for pacp_i in tmp_ru_df.index:
            pacp_i_row = tmp_ru_df.iloc[pacp_i]
            pacp = pacp_i_row.properties
            if json.loads(pacp):
                prop_acp_list.append({'properties': json.loads(pacp),'publish_year': pacp_i_row.publish_year})
        n_list = []
        for n in prop_acp_list:
            if n.get('properties').get('common_names'):
                n_list.append({'common_names': n.get('properties').get('common_names'),'publish_year': n.get('publish_year')})
        for nn in n_list:
            tmp_common_names = []
            for nnn in nn.get('common_names'):
                if nnn.get('language') == 'zh-tw':
                    tmp_common_names.append(nnn.get('name'))
            if len(tmp_common_names):
                common_names.append({'common_names': ','.join(tmp_common_names), 'publish_year': nn.get('publish_year')})
        # 要取有資料的最新的第一個common_name作為主要中文名
        common_names = pd.DataFrame(common_names)
        common_name_c, alternative_name_c = None, None
        if len(common_names):
            common_names = common_names.sort_values(by=['publish_year'], ascending=False).reset_index()
            cc_list = ','.join(common_names.common_names.to_list())
            cc_list = list(dict.fromkeys(cc_list.split(',')))
            if len(cc_list):
                common_name_c = cc_list[0]            
                if len(cc_list) > 1:
                    alternative_name_c = ', '.join(cc_list[1:])
            #     else:
            #         alternative_name_c = None
            # else:
            #     common_name_c, alternative_name_c = None, None
        # other properties based on latest reference usages
        # 有資料的最新 忽略null 每個屬性要分開來看
        is_dict = {
            'is_in_taiwan' : 0,
            'is_endemic': 0, 
            'is_fossil': 0, 
            'is_terrestrial': 0, 
            'is_freshwater': 0, 
            'is_brackish': 0, 
            'is_marine': 0
        }
        n_list = []
        for n in prop_acp_list:
            for current_is in is_dict.keys():
                if n.get('properties').get(current_is) in [0,1]:
                    # print(n.get('properties').get(current_is), current_is)
                    n_list.append({'is': current_is,'value': n.get('properties').get(current_is),'publish_year': n.get('publish_year')})
        n_list = pd.DataFrame(n_list)
        if len(n_list):
            n_list = n_list.sort_values(by=['is','publish_year'], ascending=False).reset_index()
            for current_is in is_dict.keys():
                if len(n_list[n_list['is']==current_is]):
                    is_dict[current_is] = n_list[n_list['is']==current_is]['value'].values[0]
        # if prop_latest: 
        #     is_in_taiwan = prop_latest.get('is_in_taiwan')
        #     is_endemic = prop_latest.get('is_endemic')
        #     is_fossil = prop_latest.get('is_fossil')
        #     is_terrestrial = prop_latest.get('is_terrestrial')
        #     is_freshwater = prop_latest.get('is_freshwater')
        #     is_brackish = prop_latest.get('is_brackish')
        #     is_marine = prop_latest.get('is_marine')
        # is_hybrid
        name_list = total_df[(total_df['tmp_taxon_id'] == nt)&(total_df['ru_status'] != 'misapplied')].taxon_name_id.unique()
        name_prop_all_list = []
        for pall in name_prop[name_prop.taxon_name_id.isin(name_list)].properties.to_list():
            if json.loads(pall):
                name_prop_all_list.append(json.loads(pall))
        is_hybrid = False
        is_hybrid_list = []
        for r in name_prop_all_list:
            is_hybrid_list.append(r.get('is_hybrid'))
        if any(t for t in is_hybrid_list):
            is_hybrid = True
        # alien_type
        ru_list = total_df[(total_df['tmp_taxon_id'] == nt)&(total_df['ru_status'] != 'misapplied')][['reference_id','accepted_taxon_name_id','taxon_name_id','ru_id']]
        alien_type = []
        is_cultured = 0
        tmp_ru_df = rus.merge(ru_list)
        for pall_i in tmp_ru_df.index:
            pall_ru_id = tmp_ru_df.loc[pall_i].ru_id
            pall_prop = tmp_ru_df.loc[pall_i].properties
            if pjson := json.loads(pall_prop):
                if current_alien_type := pjson.get('alien_type'):
                    if current_alien_type == 'cultured':
                        is_cultured = 1
                    alien_type.append({'reference_usage_id': pall_ru_id,
                                        'reference_id': total_df[total_df.ru_id==pall_ru_id].reference_id.to_list()[0],
                                        'alien_type': current_alien_type,
                                        'is_latest': total_df[total_df.ru_id==pall_ru_id].is_latest.to_list()[0],
                                        'status': total_df[total_df.ru_id==pall_ru_id].taxon_status.to_list()[0],
                                        'taxon_name_id': int(total_df[total_df.ru_id==pall_ru_id].taxon_name_id.to_list()[0]),
                                        'accepted_taxon_name_id': total_df[total_df.ru_id==pall_ru_id].accepted_taxon_name_id.to_list()[0],
                                        'reference_type': total_df[total_df.ru_id==pall_ru_id].type.to_list()[0]})
        # 當前的taxon_id
        taxon_id = 't' + format(int(nt), '07d') 
        print(taxon_id)
        # 寫入 api_taxon table 
        query = f"INSERT INTO api_taxon (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c,  \
                    is_hybrid, is_endemic, alien_type, is_fossil, is_terrestrial, is_freshwater, is_brackish, is_marine, is_in_taiwan, is_cultured ) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        with conn.cursor() as cursor:
            cursor.execute(query, (taxon_id, rank_id, accepted_taxon_name_id, common_name_c, alternative_name_c, is_hybrid,
                                    is_dict['is_endemic'], json.dumps(alien_type, cls=NpEncoder), is_dict['is_fossil'], is_dict['is_terrestrial'], is_dict['is_freshwater'], is_dict['is_brackish'], is_dict['is_marine'], is_dict['is_in_taiwan'], is_cultured))
            conn.commit()
        # 寫入 api_taxon_history table
        query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, reference_id, taxon_name_id, accepted_taxon_name_id, created_at, updated_at ) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        with conn.cursor() as cursor:
            cursor.execute(query, (5, taxon_id, json.dumps({'taxon_name_id': accepted_taxon_name_id}, cls=NpEncoder), row.ru_id, row.reference_id, row.taxon_name_id, row.accepted_taxon_name_id, last_updated, last_updated))
            conn.commit()
        # 寫入 api_taxon_usages table
        for i in rows.index:
            current_row = total_df.iloc[i]
            is_latest = 1 if current_row.is_latest else 0
            # update or insert
            query = f"""INSERT INTO api_taxon_usages (reference_usage_id, taxon_id, status, is_latest, taxon_name_id, parent_taxon_name_id, reference_id, accepted_taxon_name_id, created_at, updated_at) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        status = VALUES(status),
                        is_latest = VALUES(is_latest),
                        taxon_id = VALUES(taxon_id),
                        parent_taxon_name_id = VALUES(parent_taxon_name_id),
                        updated_at = CURRENT_TIMESTAMP
                        """
            with conn.cursor() as cursor:
                cursor.execute(query, (current_row.ru_id, taxon_id, current_row.taxon_status, is_latest, current_row.taxon_name_id, current_row.parent_taxon_name_id, 
                current_row.reference_id, current_row.accepted_taxon_name_id, last_updated, last_updated))
                conn.commit()
    else:
        taxon_error.append(nt)


# 階層
# api_taxon_tree
# 建立taxon_name_id與taxon_id的對應表

query = """
        select distinct taxon_name_id, taxon_id FROM api_taxon_usages where `status` !=  'misapplied';
        """
with conn.cursor() as cursor:
    cursor.execute(query)
    name_taxon = cursor.fetchall()
    name_taxon = pd.DataFrame(name_taxon, columns=['taxon_name_id','taxon_id'])

query = """
        SELECT atu.taxon_id, atu.parent_taxon_name_id, at.rank_id FROM api_taxon_usages atu
        JOIN api_taxon at ON at.taxon_id = atu.taxon_id 
        WHERE atu.`status` =  'accepted' and atu.is_latest = 1;
        """

with conn.cursor() as cursor:
    cursor.execute(query)
    name_p = cursor.fetchall()
    name_p = pd.DataFrame(name_p, columns=['taxon_id','parent_taxon_name_id','rank_id'])

name_final = name_p.merge(name_taxon, how='left').drop_duplicates()

name_final = name_final.merge(name_taxon, left_on='parent_taxon_name_id', right_on='taxon_name_id', how='left')

# name_final = name_final.merge(name_taxon, left_on='parent_taxon_name_id', right_on='taxon_name_id', how='right')
name_final = name_final[['taxon_id_x','taxon_id_y','rank_id']].drop_duplicates()
name_final = name_final.rename(columns={'taxon_id_x': 'taxon_id', 'taxon_id_y': 'parent_taxon_id'})

name_final = name_final.replace({np.nan: None})
name_final['path'] = ''

name_final = name_final.sort_values(by=["rank_id"]).reset_index(drop=True)

# 應該從高階先串比較快
# deprecated
# for n in name_final.index:
# # for n in [0]:
#     row = name_final.loc[n]
#     current_taxon_id = row.taxon_id
#     if n % 1000 == 0:
#         print(n)
#     current_p_taxon_id = row.parent_taxon_id
#     p = [current_taxon_id]
#     # print(current_taxon_id)
#     if current_p_taxon_id:
#         p.append(current_p_taxon_id)
#         has_next = True
#         while has_next:
#             # 如果next已經串過 就不要再串一次
#             next_t = name_final[name_final.taxon_id==current_p_taxon_id].parent_taxon_id.values[0]
#             # print(next_t)
#             if next_t:
#                 p.append(next_t)
#                 row = name_final[(name_final.taxon_id==next_t)&(name_final.parent_taxon_id.notnull())]
#                 if len(row):
#                     if row.path.values[0]:
#                         tmp_p = row.path.values[0].split('>')[1:]
#                         p += tmp_p
#                         has_next = False
#                     else:
#                     # current_taxon_id = next_t
#                         current_p_taxon_id = row.parent_taxon_id.values[0]
#                         p.append(current_p_taxon_id)
#                         has_next = True
#                 else:
#                     has_next = False
#             else:
#                 has_next = False
#     if len(p) > 1:
#         path = '>'.join(p)
#         name_final.loc[name_final.taxon_id==current_taxon_id,'path'] = path

    


# 先把parent_taxon_id加到api_taxon表裡
conn = pymysql.connect(**db_settings)
for n in name_final.index:
    if n % 1000 == 0:
        print(n)
    row = name_final.iloc[n]
    if row.parent_taxon_id:
        query = f"""
        UPDATE api_taxon SET parent_taxon_id = %s WHERE taxon_id = %s;
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (row.parent_taxon_id, row.taxon_id))
            conn.commit()

conn = pymysql.connect(**db_settings)

query = f"""
        INSERT INTO api_taxon_tree (taxon_id, path, parent_taxon_id)
        WITH RECURSIVE find_ancestor (taxon_id, path, parent_taxon_id) AS
        (
            SELECT taxon_id, cast(taxon_id as CHAR(500)) as path, parent_taxon_id
            FROM api_taxon
            WHERE parent_taxon_id IS NULL
            UNION ALL
            SELECT c.taxon_id, concat(cast(c.taxon_id as CHAR(500)) , '>',  path), c.parent_taxon_id
            FROM find_ancestor AS cp
            JOIN api_taxon AS c ON cp.taxon_id = c.parent_taxon_id
        )
        SELECT distinct *
        FROM find_ancestor
        """
with conn.cursor() as cursor:
    cursor.execute(query)
    conn.commit()


# 如果自己階層下面有is_in_taiwan的子階層，要把自己改成is_in_taiwan
conn = pymysql.connect(**db_settings)
query = """SELECT taxon_id FROM api_taxon_tree WHERE taxon_id IN (SELECT taxon_id FROM api_taxon WHERE is_in_taiwan != 1 OR is_in_taiwan IS NULL)"""
with conn.cursor() as cursor:
    cursor.execute(query)
    tw = cursor.fetchall()


tw_taxon_list = []

conn = pymysql.connect(**db_settings)

for tww in tw:
    tw_taxon_id = tww[0]
    query = "SELECT is_in_taiwan FROM api_taxon WHERE taxon_id IN (SELECT taxon_id FROM api_taxon_tree WHERE path like %s)"
    with conn.cursor() as cursor:
        cursor.execute(query, (f'%>{tw_taxon_id}%', ))
        tws = cursor.fetchall()
        tws = [t[0] for t in tws if t[0]==1]
        if tws:
            tw_taxon_list.append(tw_taxon_id)
            # print(tw_taxon_id)

for t in tw_taxon_list:
    print(t)
    query = """
            UPDATE api_taxon SET is_in_taiwan = 1 WHERE taxon_id = %s;
            """
    with conn.cursor() as cursor:
        cursor.execute(query, (t, ))
        conn.commit()


# 種&種下的alien_type
# 高階層 (<34) 的 alien_type
# 從下層往上抓
# 如果只有cultured階層的alien_type就給cultured

# naturalized
# native
# invasive
# cultured
# NULL

# get all taxon rank < 34
#  網站顯示要跟著修改

conn = pymysql.connect(**db_settings)
query = "SELECT taxon_id FROM api_taxon WHERE rank_id < 34;"
with conn.cursor() as cursor:
    cursor.execute(query)
    taxon = cursor.fetchall()

# TODO 需要考慮更新的問題，如果之後有原生的種&種下階層，就要把高階的cultured拿掉

query = """
    SELECT at.taxon_id, at.is_cultured, att.path, at.rank_id 
    FROM api_taxon at
    JOIN api_taxon_tree att ON att.taxon_id = at.taxon_id
    WHERE at.rank_id >= 34;"""
with conn.cursor() as cursor:
    cursor.execute(query)
    sp = cursor.fetchall()
    sp = pd.DataFrame(sp, columns=['taxon_id','is_cultured','path','rank_id'])

h_cultured = []
for t in taxon:
    # is_cultured = 0
    taxon_id = t[0]
    type_list = list(sp[sp['path'].str.contains(taxon_id)].is_cultured)
    if type_list:
        if all([tt == 1 for tt in type_list]):
            print(taxon_id)
            # is_cultured = 1
            h_cultured.append(taxon_id)


# 有沒有本身的alien_type有原生之類的 如果是的話就忽略

query = """
    SELECT at.taxon_id, at.alien_type 
    FROM api_taxon at
    WHERE at.taxon_id IN %s;"""
with conn.cursor() as cursor:
    cursor.execute(query, (h_cultured, ))
    check_alien = cursor.fetchall()
    check_alien = pd.DataFrame(check_alien, columns=['taxon_id', 'alien_type'])

# 全部都是空的
check_alien_list = check_alien.alien_type.to_list()
for cal in check_alien_list:
    for ccc in json.loads(cal):
        if ccc.get('alien_type'):
            if ccc.get('alien_type')!='cultured':
                print(cal)

# 直接改is_cultured
conn = pymysql.connect(**db_settings)
query = f"UPDATE api_taxon SET is_cultured = 1 WHERE taxon_id IN %s"
with conn.cursor() as cursor:
    cursor.execute(query, (h_cultured,))
    conn.commit()


# 先串下載檔案的階層
query = "SELECT t.taxon_id,tn.name, t.rank_id, t.common_name_c, att.path FROM api_taxon t \
        JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
        LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id"

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    cursor.execute(query)
    path_df = cursor.fetchall()
    path_df = pd.DataFrame(path_df, columns=['taxon_id','simple_name','rank','common_name_c','path'])

lin_map = {
    3: 'kingdom',
    12: 'phylum',
    18: 'classis',
    22: 'ordo',
    26: 'familia',
}


rank_map = {
    1: 'Domain', 2: 'Superkingdom', 3: 'Kingdom', 4: 'Subkingdom', 5: 'Infrakingdom', 6: 'Superdivision', 7: 'Division', 8: 'Subdivision', 9: 'Infradivision', 10: 'Parvdivision', 11: 'Superphylum', 12:
    'Phylum', 13: 'Subphylum', 14: 'Infraphylum', 15: 'Microphylum', 16: 'Parvphylum', 17: 'Superclass', 18: 'Class', 19: 'Subclass', 20: 'Infraclass', 21: 'Superorder', 22: 'Order', 23: 'Suborder',
    24: 'Infraorder', 25: 'Superfamily', 26: 'Family', 27: 'Subfamily', 28: 'Tribe', 29: 'Subtribe', 30: 'Genus', 31: 'Subgenus', 32: 'Section', 33: 'Subsection', 34: 'Species', 35: 'Subspecies', 36:
    'Nothosubspecies', 37: 'Variety', 38: 'Subvariety', 39: 'Nothovariety', 40: 'Form', 41: 'Subform', 42: 'Special Form', 43: 'Race', 44: 'Stirp', 45: 'Morph', 46: 'Aberration', 47: 'Hybrid Formula'}


for i in path_df.index:
    if i % 1000 == 0:
        print(i)
    row = path_df.iloc[i]
    if path := row.path:
        path = path.split('>')
        # 拿掉自己
        path = [p for p in path if p != row.taxon_id]
        # 3,12,18,22,26,30,34 
        if path:
            data = []
            higher = path_df[path_df.taxon_id.isin(path)&path_df['rank'].isin([3,12,18,22,26,30])][['simple_name','common_name_c','rank','taxon_id']]
            current_ranks = higher['rank'].to_list() + [row['rank']]
            for x in lin_map.keys():
                if x not in current_ranks and x < max(current_ranks) and x > min(current_ranks):
                    higher = pd.concat([higher, pd.Series({'rank': x, 'common_name_c': '地位未定', 'taxon_id': None, 'simple_name': None}).to_frame().T], ignore_index=True)
            # 從最大的rank開始補
            higher = higher.sort_values('rank', ignore_index=True, ascending=False)
            for hi in higher[higher.taxon_id.isnull()].index:
                found_hi = hi + 1
                while not higher.loc[found_hi].taxon_id:
                    found_hi += 1
                higher.loc[hi, 'simple_name'] = f'{higher.loc[found_hi].simple_name} {lin_map[higher.loc[hi]["rank"]]} incertae sedis'
                higher.loc[hi, 'common_name_c'] = '地位未定'
            for r in higher.index:
                rr = higher.iloc[r]
                r_rank_id = rr['rank']
                path_df.loc[i, f'{rank_map[r_rank_id].lower()}'] = rr['simple_name']
                path_df.loc[i, f'{rank_map[r_rank_id].lower()}_c'] = rr['common_name_c']


path_list = ['kingdom','kingdom_c', 'phylum', 'phylum_c', 'class', 'class_c', 'order','order_c', 'family', 'family_c', 'genus', 'genus_c']
path_df = path_df.replace({np.nan:None})

conn = pymysql.connect(**db_settings)

for i in path_df.index:
    if i % 1000 == 0:
        print(i)
    row = path_df.iloc[i]
    if not all(row[path_list].isnull()):
        query = f"UPDATE api_taxon SET path_dict = %s WHERE taxon_id = %s"
        with conn.cursor() as cursor:
            cursor.execute(query, (json.dumps(row[path_list].to_dict()),row.taxon_id))
            conn.commit()




# 保育資訊


# iucn v
# cites v
# protected v
# Redlist v
# Sensitive

protected = pd.read_csv('/code/data/conservation/protected.csv')
protected = protected.replace({np.nan: None})

iucn = pd.read_csv('/code/data/conservation/iucn.csv')
iucn = iucn.rename(columns={'scientificName': 'source_name', 'iucn_id': 'taxonid'})
iucn['source_name'] = iucn['source_name'].str.strip()
iucn = iucn.replace({np.nan: None})

red = pd.read_csv('/code/data/conservation/redlist.csv')
red = red.replace({np.nan: None})

sensitive = pd.read_csv('/code/data/conservation/sensitive.csv')
sensitive = sensitive.rename(columns={'學名': 'source_name'})
sensitive = sensitive.replace({np.nan: None})

cites = pd.read_csv('/code/data/conservation/cites.csv')
cites = cites.rename(columns={'FullName': 'source_name', 'TaxonId': 'cites_id', 'CurrentListing':'cites_listing'})
cites = cites.replace({np.nan: None})
cites = cites[cites.cites_listing.notnull()]

# TODO 可能需要考慮同名異物的情況


def get_conservation_info(taxon_id, protected=protected, red=red, sensitive=sensitive, cites=cites, iucn=iucn, last_updated=last_updated):
    conn = pymysql.connect(**db_settings)
    query = f'SELECT atu.id, tn.name, atu.taxon_id, atu.status, atu.is_latest, atu.taxon_name_id \
                FROM api_taxon_usages atu JOIN taxon_names tn ON atu.taxon_name_id = tn.id \
                WHERE atu.status != "mispplied" and taxon_id = %s'
    with conn.cursor() as cursor:
        cursor.execute(query, (taxon_id, ))
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
        determine_name_check.append({'taxon_id': taxon_id, 'type': 'protected'})
        protected_note, protected_row = determine_name(protected_df, taxon_id, 'protected', results)
        if len(protected_row):
            protected_category = protected_row.Category.values[0]
    elif len(protected_df) == 1:
        protected_category = protected_df.Category.values[0]
        protected_note = [{'name': protected_df.source_name.values[0],'protected_category': protected_df.Category.values[0], 'is_primary': True}]
    # IUCN - 目前抓到的都是只有一個對應的name
    iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note = None, None, None, None, []
    # 改成先比對有沒有iucn_id 有的話再用api抓 就不用每個都經過api
    iucn_df = results.merge(iucn, left_on='name', right_on='source_name')
    iucn_df['drop_row'] = False
    for i in iucn_df.index:
        c_iucn_id = iucn_df.iloc[i].taxonid
        url = f"https://apiv3.iucnredlist.org/api/v3/species/id/{c_iucn_id}?token={env('IUCN_TOKEN')}"
        data = requests.get(url)
        if data.status_code == 200:
            r = data.json().get('result')
            if r:
                iucn_assessment_date = r[0].get('assessment_date')
                iucn_category = r[0].get('category')
                iucn_criteria = r[0].get('criteria')
                iucn_df.loc[i, 'assessment_date'] = iucn_assessment_date
                iucn_df.loc[i, 'category'] = iucn_category
                iucn_df.loc[i, 'criteria'] = iucn_criteria
            else:
                iucn_df.loc[iucn_df.taxonid==c_iucn_id,'drop_row'] = True
    iucn_df = iucn_df.replace({np.nan: None})
    iucn_df = iucn_df[iucn_df.drop_row==False].reset_index(drop=True)
    if len(iucn_df) > 1:
        iucn_note, iucn_row = determine_name(iucn_df, taxon_id, 'iucn', results)
        determine_name_check.append({'taxon_id': taxon_id, 'type': 'icun'})
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
    # CITES - 改用檔案
    cites_df = results.merge(cites, left_on='name', right_on='source_name')
    cites_df = cites_df[['taxon_id','source_name','cites_id','cites_listing','name']]
    cites_df = cites_df.drop_duplicates()
    cites_df = cites_df.reset_index(drop=True)
    cites_id, cites_listing, cites_note = None, None, []
    if len(cites_df) > 1:
        cites_note, cites_row = determine_name(cites_df, taxon_id, 'cites', results)
        determine_name_check.append({'taxon_id': taxon_id, 'type': 'cites'})
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
    red_df = results.merge(red, left_on='name', right_on='source_name')
    # red_df = red_df.rename(columns={'scientific_name': 'source_name'})
    red_df = red_df.replace({nan: None})
    red_df['category'] = red_df['category'].replace({None: 'NA'}) # category 空值是NA (not applicable)
    red_category, red_criteria, red_adjustment, red_note = None, None, None, []
    if len(red_df) > 1:
        red_note, red_row = determine_name(red_df, taxon_id, 'red', results)
        determine_name_check.append({'taxon_id': taxon_id, 'type': 'red'})
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
        determine_name_check.append({'taxon_id': taxon_id, 'type': 'sensitive'})
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
        return taxon_id, protected_category, json.dumps(protected_note, cls=NpEncoder), iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, json.dumps(iucn_note, cls=NpEncoder), cites_id, cites_listing, json.dumps(cites_note, cls=NpEncoder), red_category, red_criteria, red_adjustment, json.dumps(red_note, cls=NpEncoder), sensitive_default, sensitive_suggest,  json.dumps(sensitive_note, cls=NpEncoder), last_updated, last_updated
    else:
        return None


## 決定誰是主要的學名

# merge accepted_name

def determine_name(df,taxon_id,source, results):
    no_data = []
    notes = []
    conn = pymysql.connect(**db_settings)
    # if source not in ['red','protected','sensitive']:
    #     df = df.merge(results[(results['is_latest']==1)&(results['status']=='accepted')], on='taxon_id')
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
                    break
    if len(chosen_row):
        if source == 'cites':
            notes = [{'name': chosen_row['source_name'].values[0], 'listing': chosen_row.cites_listing.values[0], 'id': int(chosen_row.cites_id.values[0]), 'is_primary': True}]
            for tt in df.index:
                if df.loc[tt]['source_name'] != chosen_row['source_name'].values[0]:
                    notes += [{'name': df.loc[tt]['source_name'], 'listing': df.loc[tt].cites_listing, 'id': int(df.loc[tt]['cites_id']), 'is_primary': False}]
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
        no_data.append(t)
    return notes, chosen_row




conn = pymysql.connect(**db_settings)
conserv_dict = {1: 'protected', 5: 'iucn', 9: 'cites', 11: 'red', 16: 'sensitive'}
conserv_dict_display = {1: '保育類', 5: 'IUCN', 9: 'CITES', 11: '紅皮書', 16: '敏感物種'}

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

conserv_results = []
determine_name_check = []

for t in total_df.sort_values('tmp_taxon_id').tmp_taxon_id.unique():
    if t % 100 == 0:
        print(t)
    taxon_id = 't' + format(int(t), '07d') 
    new_conservation_info = get_conservation_info(taxon_id)
    # 寫入conservation_info
    if new_conservation_info:
        print(taxon_id)
        # conserv_results.append([taxon_id, new_conservation_info])
        query = f"""INSERT INTO api_conservation (taxon_id, protected_category, protected_note, iucn_taxon_id, 
                iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
                cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
                sensitive_default, sensitive_suggest,  sensitive_note, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
        with conn.cursor() as cursor:
            cursor.execute(query, new_conservation_info)
            conn.commit()
    else:
        query = f"""INSERT INTO api_conservation (taxon_id,  created_at, updated_at)
                VALUES (%s, %s, %s)
                """
        with conn.cursor() as cursor:
            cursor.execute(query, (taxon_id, last_updated, last_updated))
            conn.commit()



# # ===================
# query = "SELECT taxon_id FROM api_taxon WHERE taxon_id NOT IN (SELECT taxon_id FROM api_conservation);"
# with conn.cursor() as cursor:
#     cursor.execute(query)
#     results = cursor.fetchall()
#     results = [r[0] for r in results]

# for r in results:
#     query = f"""INSERT INTO api_conservation (taxon_id,  created_at, updated_at)
#             VALUES (%s, %s, %s)
#             """
#     with conn.cursor() as cursor:
#         cursor.execute(query, (r, last_updated, last_updated))
#         conn.commit()
# # ===================


determine_name_check = pd.DataFrame(determine_name_check)
determine_name_check = determine_name_check.drop_duplicates()

determine_name_check.to_csv('determine_name_check.csv', index=None)



# 相關連結






    # # api_conservation -> 要先建立taxon
    # # 要用同一個taxon裡面所有的names去抓
    # print('conserv')
    # conservation_info = get_conservation_info(taxon_id)
    # if conservation_info: # 如果有任何保育資訊的資料，新增/更新
    #     query = f"""
    #             INSERT INTO api_conservation
    #             (taxon_id, protected_category, protected_note, iucn_taxon_id, iucn_assessment_date, iucn_category, iucn_criteria, iucn_note, 
    #                 cites_id, cites_listing, cites_note, red_category, red_criteria, red_adjustment, red_note, 
    #                 sensitive_default, sensitive_suggest,  sensitive_note)
    #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #             """
    #     with conn.cursor() as cursor:
    #         cursor.execute(query,conservation_info)
    #         conn.commit()
    # # api_taxon_tree
    # query = f"""
            # INSERT INTO api_taxon_tree (taxon_id, path, parent_taxon_id)
            # WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id, parent_taxon_id) AS
            #     (
            #         SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id, b.taxon_id
            #         FROM api_taxon_usages c
            #         JOIN taxon_names t on c.taxon_name_id = t.id 
            #         LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
            #         WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
            #         UNION ALL
            #         SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id, b.taxon_id
            #         FROM find_ancestor cp
            #         JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
            #         JOIN taxon_names t on c.taxon_name_id = t.id 
            #         LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
            #         WHERE c.is_latest = 1 and c.status = 'accepted' 
            #     )
            # SELECT taxon_id, path, parent_taxon_id
            # FROM find_ancestor WHERE taxon_id = '{taxon_id}';
    #         """
    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    #     conn.commit()
    # # 1-2 確認有沒有修改到階層 -> 也有可能修改到其他相關taxon的階層
    # # 所有有該taxon_id為上階層的階層都要跟著改變
    # query = f"""
    #     WITH RECURSIVE find_ancestor (rank_id, taxon_name_id, path, taxon_id, parent_taxon_id) AS
    #         (
    #             SELECT t.rank_id, c.taxon_name_id, cast(c.taxon_id as CHAR(1000)) as path, c.taxon_id, b.taxon_id
    #             FROM api_taxon_usages c
    #             JOIN taxon_names t on c.taxon_name_id = t.id 
    #                     LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
    #             WHERE c.parent_taxon_name_id IS NULL and c.is_latest = 1 and c.status = 'accepted' 
    #             UNION ALL
    #             SELECT t.rank_id, c.taxon_name_id, concat(cast(c.taxon_id as CHAR(1000)) , '>',  path), c.taxon_id, b.taxon_id
    #             FROM find_ancestor cp
    #             JOIN api_taxon_usages c ON cp.taxon_name_id = c.parent_taxon_name_id
    #             JOIN taxon_names t on c.taxon_name_id = t.id 
    #                 LEFT JOIN api_taxon b ON c.parent_taxon_name_id = b.accepted_taxon_name_id
    #             WHERE c.is_latest = 1 and c.status = 'accepted' 
    #         )
    #     SELECT taxon_id, path, parent_taxon_id
    #     FROM find_ancestor WHERE path LIKE '%{taxon_id}%' AND taxon_id != '{taxon_id}';
    #     """
    # highers = []
    # h_path = ''
    # with conn.cursor() as cursor:
    #     cursor.execute(query)
    #     highers = cursor.fetchall()
    # # TODO 這邊會有原本有path後來沒有的情況嗎
    # for h in highers:
    #     query = f"SELECT path FROM api_taxon_tree WHERE taxon_id = '{h[0]}'"
    #     with conn.cursor() as cursor:
    #         cursor.execute(query)
    #         h_path = cursor.fetchone()
    #     if h_path:
    #         if h_path[0] != h[1]:
    #             # 修改taxon_tree
    #             query = f"UPDATE api_taxon_tree SET path = '{h[1]}', updated_at = CURRENT_TIMESTAMP, parent_taxon_id = '{h[2]}' WHERE taxon_id = '{h[0]}'"
    #             with conn.cursor() as cursor:
    #                     cursor.execute(query)
    #                     conn.commit()
    #             # insert api_taxon_history
    #             old_path_str_name = ''
    #             if h_path[0]:
    #                 o_path_list = h_path[0].split('>')
    #                 if len(o_path_list) > 1:
    #                     query = f"""SELECT an.formatted_name
    #                                 FROM api_taxon at
    #                                 JOIN api_names an ON an.taxon_name_id = at.accepted_taxon_name_id
    #                                 WHERE at.taxon_id IN ({str(o_path_list).replace('[','').replace(']','')})
    #                                 ORDER BY at.rank_id ASC
    #                             """
    #                     with conn.cursor() as cursor:
    #                         cursor.execute(query)
    #                         ops = cursor.fetchall()
    #                         ops = [o[0] for o in ops]
    #                         old_path_str_name = ('>').join(ops)
    #                         if old_path_str_name:
    #                             old_path_str_name = '原階層：'+old_path_str_name
    #             query = f"INSERT INTO api_taxon_history (type, taxon_id, note, reference_usage_id, content ) \
    #                 VALUES (%s, %s, %s, %s, %s)"
    #             with conn.cursor() as cursor:
    #                 cursor.execute(query, (4, h[0], json.dumps({'old': h_path[0], 'new': h[1] }), ru, old_path_str_name))
    #                 conn.commit()
    # # links -> 會需要先知道階層
    # print('link')
    # if links := get_links(taxon_id):
    #     query =  f"""UPDATE api_taxon
    #                 SET links=%s
    #                 WHERE taxon_id = '{taxon_id}'
    #                 """
    #     with conn.cursor() as cursor:
    #         cursor.execute(query, (json.dumps(links)))
    #         conn.commit()

# TODO 相關連結 & 保育資訊都是後面才處理




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



# 未來更新的時候要考慮可能有階層從cultured改為非cultured

# (sp['path'].str.contains(taxon_id)).any()










# 相關連結 & 保育資訊


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
                'algaebase':25,}


# --------- 相關連結

# 自然攝影中心 v
# COL -> 直接用nomenmatch比對, 裡面有全部資料 v -> 考慮用檔案比較快
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

# 如果是更新的話，先把舊的抓回來
def get_links(taxon_id, updated=False):
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
        url = f"http://host.docker.internal:8080/api.php?names={n}&source=col&format=json"
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

