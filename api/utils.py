import re
from conf.settings import env, SOLR_PREFIX
import pymysql
import pandas as pd
import json
import numpy as np
import requests
from django.utils import timezone
from datetime import datetime, timedelta
from typing import List, Dict


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}


def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False


conn = pymysql.connect(**db_settings)


status_map = {'accepted': 'Accepted', 'misapplied': 'Misapplied', 'not-accepted': 'Not accepted', 'deleted': 'Deleted', 'undetermined': 'Undetermined'}


redlist_map = {
  'EX': 'EX', 'EW': 'EW', 'RE': 'RE', 'CR': 'NCR', 'EN': 'NEN', 'VU': 'NVU', 'NT': 'NNT',
  'LC': 'NLC', 'DD': 'DD', 'NA': 'NA', 'NE': 'NE'
}

redlist_map_rev = {
  'EX': 'EX', 'EW': 'EW', 'RE': 'RE', 'NCR': 'CR', 'NEN': 'EN', 'NVU': 'VU', 'NNT': 'NT',
  'NLC': 'LC', 'DD': 'DD', 'NA': 'NA', 'NE': 'NE'
}

cites_map = { 'I': '1','II':'2','III':'3','NC':'NC'}

protected_map = {'第一級': 'I', '第二級': 'II', '第三級': 'III', '珍貴稀有植物':'1'}

rank_map, rank_map_c,rank_map_c_reverse, rank_order_map = {}, {}, {}, {}
conn = pymysql.connect(**db_settings)
query = "SELECT id, display, `order` from ranks"
with conn.cursor() as cursor:
    cursor.execute(query)
    ranks = cursor.fetchall()
    rank_map = dict(zip([r[0] for r in ranks], [eval(r[1])['en-us'] for r in ranks]))
    rank_map_c = dict(zip([r[0] for r in ranks], [eval(r[1])['zh-tw'] for r in ranks]))
    rank_map_c_reverse = dict(zip([eval(r[1])['zh-tw'] for r in ranks],[r[0] for r in ranks]))
    rank_order_map = dict(zip([r[0] for r in ranks], [r[2] for r in ranks]))

conn.close()

# 林奈階層

# 林奈階層

lin_map = {
    3: 'kingdom',
    12: 'phylum',
    18: 'classis',
    22: 'ordo',
    26: 'familia',
}

lin_map_w_order = {
    50: {'name': '', 'rank_order': 0},
    49: {'name': '', 'rank_order': 1},
    3: {'name': 'kingdom', 'rank_order': 5},
    12: {'name': 'phylum', 'rank_order': 14},
    18: {'name': 'classis', 'rank_order': 23},
    22: {'name': 'ordo', 'rank_order': 27},
    26: {'name': 'familia', 'rank_order': 32},
    30: {'name': '', 'rank_order': 36},
}

lin_ranks = [50, 49, 3, 12, 18, 22, 26, 30, 34]
sub_lin_ranks = [35,36,37,38,39,40,41,42,43,44,45,46]


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



var_dict = requests.get("https://raw.githubusercontent.com/TaiBIF/tbia-portal/main/data/variants.json")
var_dict = var_dict.json()

comp_dict = requests.get("https://raw.githubusercontent.com/TaiBIF/tbia-portal/main/data/composites.json")
comp_dict = comp_dict.json()

# 1. 異體字群組

variant_groups: List[List[str]] = var_dict

# 2. 會意字 ↔ 合成組合 映射
composite_map: Dict[str, str] = comp_dict
reverse_composite_map: Dict[str, str] = {v: k for k, v in composite_map.items()}

# 3. 查詢某個字的異體群組
def get_word_variants(char: str) -> List[str]:
    for group in variant_groups:
        if char in group:
            return group
    return [char]

# 4. 對一串文字生成正則 pattern，例如「台灣」→ [台臺]灣
def generate_pattern_from_word(word: str) -> str:
    return ''.join(
        f"[{''.join(get_word_variants(c))}]" if len(get_word_variants(c)) > 1 else c
        for c in word
    )

# 5. 主處理函式：將輸入文字轉換為包含異體字與會意字 pattern 的版本
def process_text_variants(text: str) -> str:
    result = ''
    i = 0
    while i < len(text):
        matched = False
        # 處理會意字組合：優先處理最長的詞組
        for composite, composed in composite_map.items():
            if text.startswith(composite, i):
                pattern = f"({composite}|{generate_pattern_from_word(composed)})"
                result += pattern
                i += len(composite)
                matched = True
                break
            elif text.startswith(composed, i):
                pattern = f"({composite}|{generate_pattern_from_word(composed)})"
                result += pattern
                i += len(composed)
                matched = True
                break
        if not matched:
            char = text[i]
            variants = get_word_variants(char)
            if len(variants) > 1:
                result += f"[{''.join(variants)}]"
            else:
                result += char
            i += 1
    return result


# def get_variants(string):
#   new_string = ''
#   # 單個異體字
#   for s in string:    
#     if len(var_df[var_df['char']==s]):
#       new_string += var_df[var_df['char']==s].pattern.values[0]
#     else:
#       new_string += s
#   # 兩個異體字
#   for i in var_df_2.index:
#     char = var_df_2.loc[i, 'char']
#     if char in new_string:
#       new_string = new_string.replace(char,f"{var_df_2.loc[i, 'pattern']}")
#   return new_string

spe_chars = ['+','-', '&','&&', '||', '!','(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '/', '.']

def escape_solr_query(string):
    final_string = ''
    for s in string:
        if s in spe_chars:
            final_string += f'\{s}'
        else:
            final_string += s
    return final_string


def remove_rank_char(text):
    replace_words = [' subsp. ',' nothosubsp.',' var. ',' subvar. ',' nothovar. ',' fo. ',' subf. ',' f.sp. ',' race ',' strip ',' m. ',' ab. ',' × ','× ']
    pattern = '|'.join(map(re.escape, replace_words))
    text = re.sub(pattern, ' ', text)
    return text


def get_conditioned_solr_search(req): 

    query_list = []
    # query_list.append('is_in_taiwan:true')
    # query_list.append('is_deleted:false')

    # 如果有輸入keyword的話preselect 但是limit offset要加在preselect這邊
    # /.* .*/


    if taxon_id := req.get('taxon_id'):
        query_list.append('taxon_id:{}'.format(taxon_id))


    # NOTE 如果有 scientific_name 或 common_name 的話 要先查詢一次 取得taxon_id

    # has_name_search = False

    name_query_list = []

    if keyword := req.get('scientific_name','').strip():
        keyword_wo_rank = remove_rank_char(keyword)
        keyword_wo_rank = process_text_variants(keyword_wo_rank)

        keyword = escape_solr_query(keyword)
        keyword = process_text_variants(keyword)
        # name_query_list.append('search_name:/{}/'.format(keyword))
        name_query_list.append(f"search_name:/{keyword}/ OR search_name_wo_rank:/{keyword_wo_rank}/")

    if common_name_keyword := req.get('common_name','').strip():

        common_name_keyword = process_text_variants(common_name_keyword)
        name_query_list.append('search_name:/{}/'.format(common_name_keyword))
        name_query_list.append('-taxon_name_id:*')

    if name_query_list:

        # NOTE 這邊未來可能要考慮加上limit offset在這邊 因為有俗名會超過一百個taxon_id
        # 要確認搭配其他參數有沒有問題

        query = { "query": "*:*",
            "offset": 0,
            "limit": 0,
            "filter": name_query_list,
            "facet": {
                    "taxon_id": {
                        "type": "terms",
                        "field": "taxon_id",
                        "limit": -1,
                        }
                    }
                }
        
        query_req = json.dumps(query)

        resp = requests.post(f'{SOLR_PREFIX}taxa/select?', data=query_req, headers={'content-type': "application/json" })
        resp = resp.json()

        # 找不到的話也要限制回傳

        if resp['facets']['count'] > 0:

            taxon_ids = [t.get('val') for t in resp['facets']['taxon_id']['buckets']]
        
            if taxon_ids:
                query_list.append('taxon_id:({})'.format(' OR '.join(taxon_ids)))
            else:
                query_list.append('-*:*')

        else:
            query_list.append('-*:*')

    rank = req.get('rank')
    if rank:
        rank_id = list(rank_map.keys())[list(rank_map.values()).index(rank)]
        query_list.append('taxon_rank_id:{}'.format(rank_id))


    # 棲地 系列 
    habitat_list = ['is_terrestrial','is_freshwater','is_brackish','is_marine']
    habitat_cond = []
    for i in habitat_list:
        if req.get(i) == 'true':
            habitat_cond.append("{}:true".format(i))
        elif req.get(i) == 'false':
            habitat_cond.append("{}:false".format(i))

    if habitat_cond:
        query_list.append(f"({' OR '.join(habitat_cond)})")


    is_list = ['is_endemic','is_fossil','is_in_taiwan','is_hybrid','including_not_official']

    # TODO 待確認
    for i in is_list:
        if req.get(i) == 'true':
            query_list.append("{}:true".format(i))
        elif req.get(i) == 'false':
            query_list.append("{}:false".format(i))

    # if reg.get('is_in_taiwn')
    # 預設為true
    # query_list.append("is_in_taiwan:{}".format(req.get('is_in_taiwan','true')))


    # alien_type

    if alien_type := req.get('alien_type'):
        query_list.append('alien_type:"{}"'.format(alien_type))


    # 日期

    if updated_at := req.get('updated_at', '').strip().strip('"').strip("'"):
        if validate(updated_at):
            updated_at += 'T00:00:00Z'
            query_list.append('updated_at:[{} TO *]'.format(updated_at))

    if created_at := req.get('created_at', '').strip().strip('"').strip("'"):
        if validate(created_at):
            created_at += 'T00:00:00Z'
            query_list.append('created_at:[{} TO *]'.format(created_at))


    # 保育資訊

    for con in ['protected','redlist','iucn','sensitive']:
        if cs := req.getlist(con):
            cs_list = []
            for css in cs:
                if css == 'null':
                    cs_list.append(f'-{con}:*')
                else:
                    cs_list.append(f'{con}:"{css}"')
            if cs_list:
                c_str = f"({' OR '.join(cs_list)})"
                query_list.append(c_str)


    # CITES類別要用like

    if cs := req.getlist('cites'):
        cs_list = []
        for css in cs:
            if css == 'null':
                cs_list.append(f'-cites:*')
            else:
                cs_list.append(f'cites:/.*{css}.*/')
        if cs_list:
            c_str = f"({' OR '.join(cs_list)})"
            query_list.append(c_str)

    # 取得指定分類群以下的所有階層物種資料，可輸入學名或中文名

    if taxon_group := req.get('taxon_group'):
        conn = pymysql.connect(**db_settings)

        query_1 = f"""SELECT t.taxon_id FROM taxon_names tn 
                    JOIN api_taxon t ON tn.id = t.accepted_taxon_name_id 
                    LEFT JOIN api_common_name acn ON acn.taxon_id = t.taxon_id  
                    WHERE (tn.name = %s OR acn.name_c REGEXP %s) AND t.is_deleted = 0 """
        with conn.cursor() as cursor:
            cursor.execute(query_1, (taxon_group, process_text_variants(taxon_group)))
            t_id = cursor.fetchall()           
            if len(t_id):
                # 可能不只一筆
                t_str = [ 'path:/.*{}.*/'.format(t[0]) for t in t_id]
                query_list.append(f"({' OR '.join(t_str)})")
        
        conn.close()

    # print(query_list)

    # 要加上status = accepted 避免重複

    # if not name_query_list:
    query_list.append('taxon_name_id:*')
    query_list.append('status:accepted')


    return query_list


def get_whitelist(conn):
        
    conn = pymysql.connect(**db_settings)

    # 1. 同模出現在不同分類群

    query = "SELECT reference_usage_id FROM api_usage_whitelist WHERE whitelist_type = 1"
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        whitelist_list_1 = cursor.fetchall()
        whitelist_list_1 = [r[0] for r in whitelist_list_1]

    # 2. 同學名出現在不同分類群

    query = "SELECT taxon_name_id FROM api_usage_whitelist WHERE whitelist_type = 2"
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        whitelist_list_2 = cursor.fetchall()
        whitelist_list_2 = [r[0] for r in whitelist_list_2]

    # 3. 一組 reference_id, accepted_taxon_name_id, taxon_name_id, 對到多個ru_id

    query = "SELECT accepted_taxon_name_id, taxon_name_id, reference_id FROM api_usage_whitelist WHERE whitelist_type = 3"
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        whitelist_list_3 = pd.DataFrame(cursor.fetchall(), columns=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'])

    return whitelist_list_1, whitelist_list_2, whitelist_list_3


def check_taxon_usage():
    # 每日更新檢查usage

    # NOTE 以下都不考慮reference_id=95 因為只是單純的俗名backbone

    conn = pymysql.connect(**db_settings)

    # 取得當前的白名單

    whitelist_list_1, whitelist_list_2, whitelist_list_3 = get_whitelist(conn)

    query = """SELECT ru.id, ru.status, ru.accepted_taxon_name_id, ru.taxon_name_id, ru.reference_id, tn.object_group, tn.autonym_group,
                        r.properties ->> '$.check_list_type'
                FROM reference_usages ru 
                JOIN taxon_names tn ON tn.id = ru.taxon_name_id
                JOIN `references` r ON r.id = ru.reference_id
                WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined") AND ru.deleted_at IS NULL AND ru.accepted_taxon_name_id IS NOT NULL 
            """
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        ref_group_pair_total = cursor.fetchall()
        ref_group_pair_total = pd.DataFrame(ref_group_pair_total, columns=['ru_id', 'ru_status', 'accepted_taxon_name_id', 'taxon_name_id', 'reference_id',
                                                                        'object_group', 'autonym_group', 'check_list_type'])
        # ref_group_pair_total = ref_group_pair_total.replace({np.nan:None}) 
        ref_group_pair_total = ref_group_pair_total[ref_group_pair_total.check_list_type != 4] # !=4 寫在query裡會排除掉null
        # 排除reference_id = 95
        ref_group_pair_total = ref_group_pair_total[ref_group_pair_total.reference_id!=95]
        ref_group_pair_total = ref_group_pair_total.drop_duplicates()
        ref_group_pair_total = ref_group_pair_total.reset_index(drop=True)
        ref_group_pair_total = ref_group_pair_total.replace({np.nan:None})

    now = timezone.now() + timedelta(hours=8)

    # 1. 是不是有fixed usage_id 被刪除
    error_type = 1

    query = "select fixed_reference_usage_id from api_taxon where is_deleted = 0 and fixed_reference_usage_id in (select id from reference_usages where deleted_at is not null);"
    

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        deleted_fixed_usages = cursor.fetchall()
        for d in deleted_fixed_usages:
            with conn.cursor() as cursor:
                query = """INSERT INTO api_usage_check (reference_usage_id, error_type, updated_at) VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
                execute_line = cursor.execute(query, (d[0], error_type, now, now))
                conn.commit()

    # 2. autonym / 同模：同一篇文獻 在不同分類群 同時出現 accepted和not-acceped

    error_type = 2

    check_obj_data = ref_group_pair_total[(~ref_group_pair_total.ru_id.isin(whitelist_list_1))&(ref_group_pair_total.ru_status!='misapplied')][['object_group','reference_id','ru_status','accepted_taxon_name_id']].drop_duplicates().groupby(['reference_id','object_group'],as_index=False).nunique()
    check_obj_data = check_obj_data[(check_obj_data.ru_status>1)&(check_obj_data.accepted_taxon_name_id>1)][['object_group','reference_id']]

    oo_to_check = []
    # 應該要在同一個ref才行
    for oo in check_obj_data.to_dict('records'):
        # 無效名的接受名不是同模就不行
        rows = ref_group_pair_total[(~ref_group_pair_total.ru_id.isin(check_obj_data))&(ref_group_pair_total.ru_status=='not-accepted')&(ref_group_pair_total.object_group==oo.get('object_group'))&(ref_group_pair_total.reference_id==oo.get('reference_id'))]
        acp_ids = rows.accepted_taxon_name_id.to_list()
        for aaa in acp_ids:
            if not len(ref_group_pair_total[(ref_group_pair_total.taxon_name_id==aaa)&(ref_group_pair_total.object_group==oo.get('object_group'))]):
                oo_to_check.append(oo.get('object_group'))

    for col in check_obj_data.columns:
        check_obj_data[col] = check_obj_data[col].astype(ref_group_pair_total[col].dtype)

    rows_to_check = check_obj_data[check_obj_data.object_group.isin(oo_to_check)].merge(ref_group_pair_total)
    rows_to_check = rows_to_check[~rows_to_check.ru_id.isin(whitelist_list_1)]
    rows_to_check = rows_to_check.replace({None: 0})

    for row in rows_to_check.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (reference_usage_id, autonym_group, object_group, error_type, updated_at) VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('ru_id'), row.get('autonym_group'), row.get('object_group'), error_type, now, now))
            conn.commit()

    # 3. autonym / 同模：同一篇文獻中有多個not-accepted在不同分類群。

    error_type = 3


    check_obj_data = ref_group_pair_total[(~ref_group_pair_total.ru_id.isin(whitelist_list_1))&(ref_group_pair_total.ru_status=='not-accepted')][['object_group','reference_id','accepted_taxon_name_id']].drop_duplicates().groupby(['reference_id','object_group'],as_index=False).nunique()
    check_obj_data_list = check_obj_data[check_obj_data.accepted_taxon_name_id>1].to_dict('records')

    oo_to_check = []
    # 應該要在同一個ref才行
    for oo in check_obj_data_list:
        # 無效名的接受名不是同模就不行
        acp_ids = ref_group_pair_total[(~ref_group_pair_total.ru_id.isin(whitelist_list_1))&(ref_group_pair_total.ru_status=='not-accepted')&(ref_group_pair_total.object_group==oo.get('object_group'))&(ref_group_pair_total.reference_id==oo.get('reference_id'))].accepted_taxon_name_id.to_list()
        for aaa in acp_ids:
            if not len(ref_group_pair_total[(ref_group_pair_total.taxon_name_id==aaa)&(ref_group_pair_total.object_group==oo.get('object_group'))]):
                oo_to_check.append(oo.get('object_group'))

    for col in check_obj_data.columns:
        check_obj_data[col] = check_obj_data[col].astype(ref_group_pair_total[col].dtype)

    rows_to_check = check_obj_data[check_obj_data.object_group.isin(oo_to_check)].merge(ref_group_pair_total)
    rows_to_check = rows_to_check[~rows_to_check.ru_id.isin(whitelist_list_1)]
    rows_to_check = rows_to_check.replace({None: 0})

    for row in rows_to_check.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (reference_usage_id, autonym_group, object_group, error_type, updated_at) VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('ru_id'), row.get('autonym_group'), row.get('object_group'), error_type, now, now))
            conn.commit()


    # 4. 同模（不包含autonym）：同一篇文獻中多個accepted

    error_type = 4


    check_obj_data = ref_group_pair_total[(~ref_group_pair_total.ru_id.isin(whitelist_list_1))&(ref_group_pair_total.autonym_group.isnull())&(ref_group_pair_total.object_group.notnull())&(ref_group_pair_total.ru_status=='accepted')][['object_group','taxon_name_id','reference_id']].drop_duplicates().groupby(['reference_id','object_group'],as_index=False).nunique()
    check_obj_data_list = check_obj_data[check_obj_data.taxon_name_id>1].to_dict('records')

    rows_to_check = pd.DataFrame()
    for cc in check_obj_data_list:
        rows = ref_group_pair_total[(~ref_group_pair_total.ru_id.isin(whitelist_list_1))&(ref_group_pair_total.ru_status=='accepted')&(ref_group_pair_total.object_group==cc.get('object_group'))&(ref_group_pair_total.reference_id==cc.get('reference_id'))]
        rows_to_check = pd.concat([rows, rows_to_check],ignore_index=True)


    if len(rows_to_check):
        rows_to_check = rows_to_check[~rows_to_check.ru_id.isin(whitelist_list_1)]
        rows_to_check = rows_to_check.replace({None: 0})

    for row in rows_to_check.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (reference_usage_id, autonym_group, object_group, error_type, updated_at) VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('ru_id'), row.get('autonym_group'), row.get('object_group'), error_type, now, now))
            conn.commit()


    # 5. 確認accepted_taxon_name_id, taxon_name_id, reference_id是不是只對到一個status

    error_type = 5


    check_ru_unique = ref_group_pair_total[['accepted_taxon_name_id', 'taxon_name_id', 'reference_id', 'ru_status']]
    a = check_ru_unique.groupby(['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'],as_index=False).count()
    a = a[a.ru_status>1]

    for col in a.columns:
        if col in whitelist_list_3.keys():
            a[col] = a[col].astype(whitelist_list_3[col].dtype)

    df_diff = a.merge(whitelist_list_3, on=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'], how='left', indicator=True)
    df_result = df_diff[df_diff['_merge'] == 'left_only'].drop(columns=['_merge'])
    df_result = df_result.replace({None: 0})

    for row in df_result.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (accepted_taxon_name_id, taxon_name_id, reference_id, error_type, updated_at) VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('accepted_taxon_name_id'), row.get('taxon_name_id'), row.get('reference_id'), error_type, now, now))
            conn.commit()


    # 6. 學名在同一篇文獻中 被設定成兩個分類群的同物異名

    error_type = 6

    check_not_accepted_unique = ref_group_pair_total[ref_group_pair_total.ru_status=='not-accepted'][['accepted_taxon_name_id', 'taxon_name_id', 'reference_id']].drop_duplicates()
    b = check_not_accepted_unique.groupby(['taxon_name_id', 'reference_id'],as_index=False).count()
    b = b[(b.accepted_taxon_name_id>1) & (~b.taxon_name_id.isin(whitelist_list_2))]
    b = b.replace({None: 0})

    for row in b.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (taxon_name_id, reference_id, error_type, updated_at) VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('taxon_name_id'), row.get('reference_id'), error_type, now, now))
            conn.commit()


    # 7. 同一個分類群有一個以上的接受名

    error_type = 7


    all_pair = ref_group_pair_total[['accepted_taxon_name_id','reference_id']].drop_duplicates()
    check_pair = ref_group_pair_total[ref_group_pair_total.ru_status=='accepted'][['accepted_taxon_name_id','reference_id','ru_status']].drop_duplicates()
    a = check_pair.groupby(['accepted_taxon_name_id','reference_id'],as_index=False).count()
    a = all_pair.merge(a, how='left')

    a_more = a[a.ru_status > 1]
    a_more = a_more.replace({None: 0})

    for row in a_more.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (accepted_taxon_name_id, reference_id, error_type, updated_at) VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('accepted_taxon_name_id'), row.get('reference_id'), error_type, now, now))
            conn.commit()


    # 8. 同一個分類群裡面沒有任何接受名

    error_type = 8


    a_none = a[a.ru_status.isna()]
    a_none = a_none.replace({None: 0})

    for row in a_none.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (accepted_taxon_name_id, reference_id, error_type, updated_at) VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('accepted_taxon_name_id'), row.get('reference_id'), error_type, now, now))
            conn.commit()


    # 9. 同一個學名出現在同一篇文獻中的兩個分類群(不同accepted_taxon_name_id) 且不是誤用
    # 可能和前面有重複 但前面只有考慮同模 沒有考慮到單純學名相同

    error_type = 9


    all_pair = ref_group_pair_total[ref_group_pair_total.ru_status!='misapplied'][['accepted_taxon_name_id','reference_id','taxon_name_id']].drop_duplicates()
    a = all_pair.groupby(['reference_id','taxon_name_id'],as_index=False).count()
    a = a[(a.accepted_taxon_name_id>1)& (~a.taxon_name_id.isin(whitelist_list_2))]
    a = a.replace({None: 0})


    for row in a.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (taxon_name_id, reference_id, error_type, updated_at) VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('taxon_name_id'), row.get('reference_id'), error_type, now, now))
            conn.commit()


    # 10. 一組 reference_id, accepted_taxon_name_id, taxon_name_id, 只對到一個ru_id

    error_type = 10


    a = ref_group_pair_total[['ru_id','reference_id','accepted_taxon_name_id','taxon_name_id']].drop_duplicates()
    a = a.groupby(['reference_id','accepted_taxon_name_id','taxon_name_id'], as_index=False).count()
    a = a[a.ru_id>1]

    df_diff = a.merge(whitelist_list_3, on=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'], how='left', indicator=True)
    df_result = df_diff[df_diff['_merge'] == 'left_only'].drop(columns=['_merge'])
    df_result = df_result.replace({None: 0})

    for row in df_result.to_dict('records'):
        with conn.cursor() as cursor:
            query = """INSERT INTO api_usage_check (accepted_taxon_name_id, taxon_name_id, reference_id, error_type, updated_at) VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                            updated_at = %s;
                        """ 
            execute_line = cursor.execute(query, (row.get('accepted_taxon_name_id'), row.get('taxon_name_id'), row.get('reference_id'), error_type, now, now))
            conn.commit()

    # 新增error_type = 11 for 記錄檢查的時間
    with conn.cursor() as cursor:
        query = """INSERT INTO api_usage_check (error_type, updated_at) VALUES (11, %s)
                                    ON DUPLICATE KEY UPDATE 
                            updated_at = %s;

                    """ 
        execute_line = cursor.execute(query, (now, now))
        conn.commit()

    return 'done!'

# TODO 每日更新的話 要排除掉已經insert的usage

# taxon_id = request.GET.get('taxon_id', '').strip()
# taxon_group = request.GET.get('taxon_group', '').strip()
# updated_at = request.GET.get('updated_at', '').strip().strip('"').strip("'")
# created_at = request.GET.get('created_at', '').strip().strip('"').strip("'")
# including_not_official = request.GET.get('including_not_official', 'true')

# conn = pymysql.connect(**db_settings)


# if taxon_id:  # 不考慮其他條件
#     base_query = f"WITH base_query AS (SELECT * FROM api_taxon t WHERE t.taxon_id = '{taxon_id}')"
#     count_query = f"SELECT count(*) FROM api_taxon t WHERE t.taxon_id = '{taxon_id}'"
# else:
#     conditions = [] # 在query中 和 info_query是分開的

#     if including_not_official == 'false': # false: 排除未經正式紀錄
#         conditions += ["t.not_official != 1"]


#     # base_query = f"WITH base_query AS (SELET t.taxon_id FROM api_taxon t order by id limit {limit} offset {offset})"
#     # join_usage_and_name = False
#     # join_common_name = False
#     name_taxon_id = []
#     common_name_taxon_id = []
#     join_conserv = False
#     join_taxon_tree = False

#     # 學名 scientific_name 可能是接受/非接受/誤用
#     if sci_name := request.GET.get('scientific_name', ''):
#         # 先query一次
#         name_query = """
#                     SELECT distinct (taxon_id) FROM api_taxon_usages where is_deleted = 0 and taxon_name_id IN ( 
#                         SELECT id
#                         FROM taxon_names 
#                         WHERE deleted_at is null AND `name` = %s)
#                     """
#         with conn.cursor() as cursor:
#             cursor.execute(name_query, (sci_name, ))
#             name_taxon_id = cursor.fetchall()
#             name_taxon_id = [n[0] for n in name_taxon_id]

#     # 俗名 common_name
#     if common_name := request.GET.get('common_name', ''):
#         common_name = get_variants(common_name)
#         common_name_query = """
#                 SELECT distinct taxon_id
#                 FROM api_common_name  
#                 WHERE name_c REGEXP %s
#             """
        
#         with conn.cursor() as cursor:
#             cursor.execute(common_name_query, (common_name, ))
#             common_name_taxon_id = cursor.fetchall()
#             common_name_taxon_id = [n[0] for n in common_name_taxon_id]

#     if name_taxon_id and common_name_taxon_id:
#         # 要找兩個的交集
#         preselect_taxon_id = list(set(name_taxon_id).intersection(common_name_taxon_id))
#     else:
#         preselect_taxon_id = name_taxon_id + common_name_taxon_id

#     if preselect_taxon_id:
#         conditions += [f"t.taxon_id IN {str(preselect_taxon_id).replace('[','(').replace(']',')')}"]


#     # 直接查taxon的表 不需要join
#     for i in ['is_hybrid', 'is_endemic', 'is_in_taiwan', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine']:
#         var = request.GET.get(i, '').strip()
#         if var == 'true' or var == '1':
#             conditions += [f"t.{i} = 1"]
#         elif var == 'false' or var == '0':
#             conditions += [f"t.{i} = 0"]
        
#     if var := request.GET.get('alien_type', '').strip():
#         conditions += [f't.main_alien_type = "{var}"']

#     # if var := request.GET.get('alien_type', '').strip():
#     #     conditions += ['''JSON_CONTAINS(t.alien_type, '{"alien_type":"''' + var + '''"}')  > 0''']

#     if updated_at:
#         if not validate(updated_at):
#             response = {"status": {"code": 400, "message": "Bad Request: Incorrect DATE(updated_at) value"}}
#             return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
#         conditions += [f"date(t.updated_at) > '{updated_at}'"]
#     if created_at:
#         if not validate(created_at):
#             response = {"status": {"code": 400, "message": "Bad Request: Incorrect DATE(created_at) value"}}
#             return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
#         conditions += [f"date(t.created_at) > '{created_at}'"]

#     if rank := request.GET.get('rank'):
#         try:
#             rank_id = list(rank_map.keys())[list(rank_map.values()).index(rank)]
#             conditions += [f't.rank_id = {rank_id}']
#         except:
#             response = {"status": {"code": 400, "message": "Bad Request: Incorrect rank"}}
#             return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

#     # 保育資訊
#     if cs := request.GET.getlist('redlist'):
#         cs_list = []
#         for css in cs:
#             if css == 'null':
#                 cs_list.append(f'ac.red_category IS NULL')
#             else:
#                 # if redlist_map.get(css):
#                 cs_list.append(f'ac.red_category = "{redlist_map.get(css)}"')
#         if cs_list:
#             conditions.append(f"({' OR '.join(cs_list)})")
#             join_conserv = True

#     if cs := request.GET.getlist('protected'):
#         cs_list = []
#         for css in cs:
#             if css == 'null':
#                 cs_list.append(f'ac.protected_category IS NULL')
#             else:
#                 cs_list.append(f'ac.protected_category = "{css}"')
#         if cs_list:
#             conditions.append(f"({' OR '.join(cs_list)})")
#             join_conserv = True

#     if cs := request.GET.getlist('iucn'):
#         cs_list = []
#         for css in cs:
#             if css == 'null':
#                 cs_list.append(f'ac.iucn_category IS NULL')
#             else:
#                 cs_list.append(f'ac.iucn_category = "{css}"')
#         if cs_list:
#             conditions.append(f"({' OR '.join(cs_list)})")
#             join_conserv = True

#     if cs := request.GET.getlist('sensitive'):
#         cs_list = []
#         for css in cs:
#             if css == 'null':
#                 cs_list.append(f'ac.sensitive_suggest IS NULL')
#             else:
#                 cs_list.append(f'ac.sensitive_suggest = "{css}"')
#         if cs_list:
#             conditions.append(f"({' OR '.join(cs_list)})")
#             join_conserv = True

#     # CITES類別要用like
#     if cs := request.GET.getlist('cites'):
#         cs_list = []
#         for css in cs:
#             if css == 'null':
#                 cs_list.append(f'ac.cites_listing IS NULL')
#             else:
#                 # if cites_map.get(css):
#                 cs_list.append(f'ac.cites_listing like "%{cites_map.get(css)}%"')
#         if cs_list:
#             conditions.append(f"({' OR '.join(cs_list)})")
#             join_conserv = True

#     if taxon_group:
#         # 先抓taxon_id再判斷有沒有其他condition要考慮
#         query_1 = f"""SELECT t.taxon_id FROM taxon_names tn 
#                     JOIN api_taxon t ON tn.id = t.accepted_taxon_name_id 
#                     LEFT JOIN api_common_name acn ON acn.taxon_id = t.taxon_id  
#                     WHERE tn.name = %s OR acn.name_c REGEXP %s"""
#         with conn.cursor() as cursor:
#             cursor.execute(query_1, (taxon_group, get_variants(taxon_group)))
#             t_id = cursor.fetchall()           
#             if len(t_id):
#                 # 可能不只一筆
#                 t_str = [ f"att.path like '%>{t[0]}%'" for t in t_id]
#                 conditions.append(f"({' OR '.join(t_str)})")
#                 join_taxon_tree = True
#             else:  # 如果沒有結果的話用回傳空值
#                 response = {"status": {"code": 200, "message": "Success"},
#                             "info": {"total": 0, "limit": limit, "offset": offset}, "data": []}
#                 return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")

#     if len(conditions):
#         for l in range(len(conditions)):
#             if l == 0:
#                 cond_str = f"WHERE {conditions[l]}"
#                 # query = f"{query} WHERE {conditions[l]}"
#                 # count_query = f"{count_query} WHERE {conditions[l]}"
#             else:
#                 cond_str += f' AND {conditions[l]}'
#                 # query += f' AND {conditions[l]}'
#                 # count_query += f" AND {conditions[l]}"

#         base_query = f'''WITH base_query AS (
#                         SELECT t.* FROM api_taxon t
#                         {'LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id' if join_taxon_tree else ''}
#                         {'LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id' if join_conserv else ''}
#                         {cond_str}
#                         ORDER BY t.id  LIMIT {limit} OFFSET {offset} )'''
#         count_query = f'''
#                         SELECT count(*) FROM api_taxon t 
#                         {'LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id' if join_taxon_tree else ''}
#                         {'LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id' if join_conserv else ''}
#                         {cond_str}
#                         '''
        

#     else:
#         # 如果沒有任何condition 直接 limit offset
#         base_query = f"WITH base_query AS (SELECT t.* FROM api_taxon t ORDER BY t.id LIMIT {limit} OFFSET {offset}) "
#         count_query = f"SELECT count(*) FROM api_taxon"

# # 最後整理回傳資料使用

# info_query = """
#         SELECT t.taxon_id, t.rank_id, t.accepted_taxon_name_id, acn.name_c, 
#             t.is_hybrid, t.is_endemic, t.is_in_taiwan, t.main_alien_type, t.alien_note, t.is_fossil, t.is_terrestrial, 
#             t.is_freshwater, t.is_brackish, t.is_marine, ac.cites_listing, ac.iucn_category, ac.red_category, 
#             ac.protected_category, ac.sensitive_suggest, 
#             t.created_at, t.updated_at, tn.name, an.name_author, an.formatted_name, t.is_deleted, t.new_taxon_id, t.not_official, att.parent_taxon_id
#         FROM base_query t 
#             JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id 
#             LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id 
#             LEFT JOIN api_common_name acn ON t.taxon_id = acn.taxon_id and acn.is_primary = 1
#             LEFT JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id 
#             LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id 
#         """
# with conn.cursor() as cursor:
#     cursor.execute(count_query)
#     len_total = cursor.fetchall()[0][0]
#     query = base_query + info_query

#     cursor.execute(query)
#     df = pd.DataFrame(cursor.fetchall(), columns=['taxon_id', 'rank', 'name_id', 'common_name_c', 
#                                                   'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial',
#                                                   'is_freshwater', 'is_brackish', 'is_marine', 'cites', 'iucn', 'redlist', 'protected', 'sensitive',
#                                                   'created_at', 'updated_at', 'simple_name', 'name_author', 'formatted_name', 'is_deleted', 'new_taxon_id', 'not_official','parent_taxon_id'])
#     # 0, 1 要轉成true, false (但可能會有null)
#     if len(df):
#         # 在這步取得alternative_common_name
#         name_c_query = "select name_c, taxon_id from api_common_name where taxon_id IN %s and is_primary = 0"
#         cursor.execute(name_c_query, (df.taxon_id.to_list(),))
#         name_c = cursor.fetchall()
#         if len(name_c):
#             name_c = pd.DataFrame(name_c, columns=['alternative_name_c', 'taxon_id'])
#             name_c = name_c.groupby(['taxon_id'], as_index = False).agg({'alternative_name_c': ','.join})
#             df = df.merge(name_c, how='left')
#         else:
#             df['alternative_name_c'] = None
#         df = df.replace({np.nan: None})
#         is_list = ['is_in_taiwan','is_hybrid', 'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine', 'not_official']
#         df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True})
#         # 階層
#         df['rank'] = df['rank'].apply(lambda x: rank_map[x])
#         # 日期格式 yy-mm-dd
#         # df['created_at'] = df.created_at.dt.strftime('%Y-%m-%d')
#         # df['updated_at'] = df.updated_at.dt.strftime('%Y-%m-%d')
#         df['created_at'] = df.created_at.apply(lambda x: str(x).split(' ')[0])
#         df['updated_at'] = df.updated_at.apply(lambda x: str(x).split(' ')[0])

#         # 同物異名 & 誤用名
#         df['synonyms'] = ''
#         df['formatted_synonyms'] = ''
#         df['misapplied'] = ''
#         df['formatted_misapplied'] = ''

#         query = f"SELECT DISTINCT tu.taxon_id, tu.status, an.formatted_name, tn.name \
#                     FROM api_taxon_usages tu \
#                     JOIN api_names an ON tu.taxon_name_id = an.taxon_name_id \
#                     JOIN taxon_names tn ON tu.taxon_name_id = tn.id \
#                     WHERE tu.taxon_id IN %s and tu.status IN ('not-accepted', 'misapplied') AND tu.is_deleted != 1;"
#         cursor.execute(query, (df.taxon_id.to_list(),))
#         other_names = cursor.fetchall()
#         other_names = pd.DataFrame(other_names, columns=['taxon_id','status','formatted_name','name'])
#         other_names = other_names.groupby(['taxon_id', 'status'], as_index = False).agg({'formatted_name': ','.join, 'name': ','.join})
#         other_names = other_names.to_dict('records')
#         for o in other_names:
#             if o.get('status') == 'not-accepted':
#                 df.loc[df['taxon_id'] == o.get('taxon_id'), 'synonyms'] = o.get('name')
#                 df.loc[df['taxon_id'] == o.get('taxon_id'), 'formatted_synonyms'] = o.get('formatted_name')
#             elif o.get('status') == 'misapplied':
#                 df.loc[df['taxon_id'] == o.get('taxon_id'), 'misapplied'] = o.get('name')
#                 df.loc[df['taxon_id'] == o.get('taxon_id'), 'formatted_misapplied'] = o.get('formatted_name')

#         query = "SELECT r.id, c.short_author, r.type \
#                     FROM `references` r  \
#                     LEFT JOIN api_citations c ON r.id = c.reference_id \
#                     JOIN api_taxon_usages atu ON r.id = atu.reference_id  \
#                     WHERE atu.taxon_id IN %s"  
#         conn = pymysql.connect(**db_settings)
#         with conn.cursor() as cursor:
#             cursor.execute(query, (df.taxon_id.to_list(), ))
#             refs = pd.DataFrame(cursor.fetchall(), columns=['reference_id', 'ref', 'type'])


#         for i in df.index:
#             row = df.iloc[i]
#             final_aliens = []
#             if row.alien_status_note:
#                 # alien_rows = json.loads(row.alien_status_note)
#                 alien_rows = pd.DataFrame(json.loads(row.alien_status_note))
#                 if len(alien_rows):
#                     # ref_list = alien_rows.reference_id.to_list()
#                     # print(alien_rows.keys())
#                     alien_rows = alien_rows.merge(refs,how='left')
#                     alien_rows = alien_rows.replace({np.nan: None})
#                     # 排除backbone & note 為null
#                     # 是backbone 沒有note
#                     # 不顯示
#                     alien_rows = alien_rows[~((alien_rows['type'].isin([4,6]))&(alien_rows.status_note.isnull()))]
#                     alien_rows = alien_rows.sort_values('is_latest', ascending=False)
#                     alien_rows = alien_rows[['alien_type','status_note','ref','type']].drop_duplicates()
#                     for at in alien_rows.to_dict('records'):
#                         # 是backbone 有note
#                         # 歸化: note
#                         if at.get('type') in [4,6] and at.get('status_note'):
#                             final_aliens.append(f"{at.get('alien_type')}: {at.get('status_note')}")
#                         # 不是backbone 有note
#                         # 原生: Chang-Yang et al., 2022 (note)
#                         elif at.get('status_note'):
#                             final_aliens.append(f"{at.get('alien_type')}: {at.get('ref')} ({at.get('status_note')})")
#                         # 不是backbone 沒有notenote
#                         # 原生: Chang-Yang et al., 2022
#                         else:
#                             final_aliens.append(f"{at.get('alien_type')}: {at.get('ref')}")

#             df.loc[i, 'alien_status_note'] = '|'.join(final_aliens)

#         df['cites'] = df['cites'].apply(lambda x: x.replace('1','I').replace('2','II').replace('3','III') if x else x)
#         df['redlist'] = df['redlist'].apply(lambda x: redlist_map_rev[x] if x else x)

#         # TODO 這邊的status要確認
#         df['taxon_status'] = df['is_deleted'].replace({1: 'deleted', 0: 'accepted'})

#         # 排序
#         df = df[['taxon_id', 'taxon_status', 'name_id', 'simple_name', 'name_author', 'formatted_name', 'synonyms', 'formatted_synonyms', 'misapplied', 'formatted_misapplied',
#                 'rank', 'common_name_c', 'alternative_name_c', 'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
#                  'is_marine','not_official', 'cites', 'iucn', 'redlist', 'protected', 'sensitive', 'created_at', 'updated_at', 'new_taxon_id', 'parent_taxon_id']]

#         df = df.replace({np.nan: None, '': None})
#         df['name_id'] = df['name_id'].replace({np.nan: 0}).astype('int64').replace({0: None})
