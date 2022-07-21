import re
from conf.settings import env
import pymysql
import pandas as pd
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

# generate rank_map
# conn = pymysql.connect(**db_settings)
# rank_map = {}
# with conn.cursor() as cursor:
#     query = "SELECT id, JSON_EXTRACT(display,'$.\"en-us\"') FROM ranks;"
#     cursor.execute(query)
#     results = cursor.fetchall()
#     for r in results:
#         rank_map.update({r[0]: r[1].replace('"', '')})

# rank_map_c = {}
# with conn.cursor() as cursor:
#     query = "SELECT id, JSON_EXTRACT(display,'$.\"zh-tw\"') FROM ranks;"
#     cursor.execute(query)
#     results = cursor.fetchall()
#     for r in results:
#         rank_map_c.update({r[0]: r[1].replace('"', '')})

rank_map = {
    1: 'Domain', 2: 'Superkingdom', 3: 'Kingdom', 4: 'Subkingdom', 5: 'Infrakingdom', 6: 'Superdivision', 7: 'Division', 8: 'Subdivision', 9: 'Infradivision', 10: 'Parvdivision', 11: 'Superphylum', 12:
    'Phylum', 13: 'Subphylum', 14: 'Infraphylum', 15: 'Microphylum', 16: 'Parvphylum', 17: 'Superclass', 18: 'Class', 19: 'Subclass', 20: 'Infraclass', 21: 'Superorder', 22: 'Order', 23: 'Suborder',
    24: 'Infraorder', 25: 'Superfamily', 26: 'Family', 27: 'Subfamily', 28: 'Tribe', 29: 'Subtribe', 30: 'Genus', 31: 'Subgenus', 32: 'Section', 33: 'Subsection', 34: 'Species', 35: 'Subspecies', 36:
    'Nothosubspecies', 37: 'Variety', 38: 'Subvariety', 39: 'Nothovariety', 40: 'Form', 41: 'Subform', 42: 'Special Form', 43: 'Race', 44: 'Stirp', 45: 'Morph', 46: 'Aberration', 47: 'Hybrid Formula'}

rank_map_c = {1: '域', 2: '總界', 3: '界', 4: '亞界', 5: '下界', 6: '超部|總部', 7: '部|類', 8: '亞部|亞類', 9: '下部|下類', 10: '小部|小類', 11: '超門|總門', 12: '門', 13: '亞門', 14: '下門', 15: '小門', 16: '小門', 17: '超綱|總綱', 18: '綱',
              19: '亞綱', 20: '下綱', 21: '超目|總目', 22: '目', 23: '亞目', 24: '下目', 25: '超科|總科', 26: '科', 27: '亞科', 28: '族', 29: '亞族', 30: '屬', 31: '亞屬', 32: '組|節', 33: '亞組|亞節', 34: '種', 35: '亞種', 36: '雜交亞種',
              37: '變種', 38: '亞變種', 39: '雜交變種', 40: '型', 41: '亞型', 42: '特別品型', 43: '種族', 44: '種族', 45: '形態型', 46: '異常個體', 47: '雜交組合'}

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

def update_citations():
    conn = pymysql.connect(**db_settings)
    query = "select id from `references` where created_at > (select max(updated_at) from api_citations) or updated_at > (select max(updated_at) from api_citations);"
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    id_list = [str(r[0]) for r in results]
    if id_list:
        query = f"SELECT p.last_name, p.first_name, p.middle_name, pr.reference_id, pr.order, r.publish_year \
                FROM person_reference pr \
                JOIN persons p ON pr.person_id = p.id \
                JOIN `references` r on pr.reference_id = r.id \
                WHERE r.id IN ({','.join(id_list)});"
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
        conn = pymysql.connect(**db_settings)
        for i in citation_df.index:
            row = citation_df.iloc[i]
            with conn.cursor() as cursor:
                query = "INSERT INTO api_citations (reference_id, author, content, updated_at) VALUES(%s, %s, %s, CURRENT_TIMESTAMP) \
                        ON DUPLICATE KEY UPDATE author=%s, content=%s, updated_at = CURRENT_TIMESTAMP; "        
                cursor.execute(query, (row.reference_id, row.author, row.content, row.author, row.content))
                conn.commit()


def update_names():
    query = "SELECT id FROM taxon_names WHERE rank_id <> 47 AND (created_at > (select max(updated_at) from api_names) or updated_at > (select max(updated_at) from api_names))"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    name_list = [str(r[0]) for r in results]
    # 如果parent name有修改再修改
    query = "SELECT tnhp.taxon_name_id FROM taxon_name_hybrid_parent tnhp \
        JOIN taxon_names tn ON tnhp.parent_taxon_name_id = tn.id \
        WHERE tn.created_at > (select max(updated_at) from api_names) or tn.updated_at > (select max(updated_at) from api_names)\
              or tnhp.taxon_name_id NOT IN (SELECT taxon_name_id FROM api_names)"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    hybrid_name_list = [str(r[0]) for r in results]
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
        query = f"WITH view as (SELECT tnhp.taxon_name_id, an.name_with_tag FROM taxon_name_hybrid_parent tnhp \
                JOIN api_names an ON tnhp.parent_taxon_name_id = an.taxon_name_id \
                WHERE tnhp.taxon_name_id IN ({','.join(hybrid_name_list)}) \
                ORDER BY tnhp.order) \
                SELECT taxon_name_id, group_concat(name_with_tag SEPARATOR ' × ') FROM view \
                GROUP BY taxon_name_id \
                "
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        for r in results:
            rows.append([r[0], r[1]])
    conn = pymysql.connect(**db_settings)
    for r in rows:
        with conn.cursor() as cursor:
            query = "INSERT INTO api_names (taxon_name_id, name_with_tag, updated_at) VALUES(%s, %s, CURRENT_TIMESTAMP) \
                    ON DUPLICATE KEY UPDATE name_with_tag=%s, updated_at = CURRENT_TIMESTAMP; "        
            cursor.execute(query, (r[0], r[1], r[1]))
            conn.commit()



