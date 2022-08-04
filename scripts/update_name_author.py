
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


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}


# 選擇有作者資訊 / 原始組合 / 年份的學名
query = "select id, nomenclature_id, rank_id, name, original_taxon_name_id, formatted_authors, publish_year from \
         taxon_names where (id in (select taxon_name_id from person_taxon_name) or original_taxon_name_id is not null or publish_year is not null )  and rank_id <> 47;"

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
    if i % 1000 == 0:
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
for i in df.index:
    if i % 1000 == 0:
        print(i)
    row = df.iloc[i]
    query = f'UPDATE api_names SET name_author = "{row.formatted_author}" WHERE taxon_name_id = {row.taxon_name_id}'
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


conn = pymysql.connect(**db_settings)
for i in df.index:
    if i % 1000 == 0:
        print(i)
    row = df.iloc[i]
    query = f'UPDATE api_names SET name_author = "{row.formatted_author}" WHERE taxon_name_id = {row.taxon_name_id}, updated_at = CURRENT_TIMESTAMP'
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


# 確認有沒有新的name


# x = df[df.formatted_author!=df.formatted_authors][['taxon_name_id','formatted_authors','formatted_author']]
# x = x.rename(columns={'formatted_authors': 'old_author', 'formatted_author': 'new_author'})
# x.to_csv('作者確認.csv', index=None)

