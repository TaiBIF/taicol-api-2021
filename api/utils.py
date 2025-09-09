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


def get_whitelist():
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        # 使用單一查詢獲取所有需要的資料
        query = """
        SELECT 
            whitelist_type,
            reference_usage_id,
            taxon_name_id,
            accepted_taxon_name_id,
            reference_id
        FROM api_usage_check 
        WHERE whitelist_type IN (1, 2, 3)
        """
        cursor.execute(query)
        all_results = cursor.fetchall()
        # 初始化結果容器
        whitelist_list_1 = [] # 1. 同模出現在不同分類群
        whitelist_list_2 = [] # 2. 同學名出現在不同分類群
        whitelist_list_3_data = [] # 3. 一組 reference_id, accepted_taxon_name_id, taxon_name_id, 對到多個ru_id
        # 根據 whitelist_type 分類處理結果
        for row in all_results:
            whitelist_type, ref_usage_id, taxon_name_id, accepted_taxon_name_id, reference_id = row
            if whitelist_type == 1:
                whitelist_list_1.append(ref_usage_id)
            elif whitelist_type == 2:
                whitelist_list_2.append(taxon_name_id)
            elif whitelist_type == 3:
                whitelist_list_3_data.append({
                    'accepted_taxon_name_id': accepted_taxon_name_id,
                    'taxon_name_id': taxon_name_id,
                    'reference_id': reference_id
                })
        # 建立 DataFrame
        whitelist_list_3 = pd.DataFrame(whitelist_list_3_data) if whitelist_list_3_data else pd.DataFrame(columns=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'])
        return whitelist_list_1, whitelist_list_2, whitelist_list_3


def check_taxon_usage():
    """每日更新檢查usage - 優化版本"""
    
    # NOTE 以下都不考慮reference_id=95 因為只是單純的俗名backbone
    
    # 取得當前的白名單
    whitelist_list_1, whitelist_list_2, whitelist_list_3 = get_whitelist()
    
    conn = pymysql.connect(**db_settings)
    now = timezone.now() + timedelta(hours=8)
    
    try:
        # 獲取主要數據
        usage_df = _get_reference_usage_data(conn)
        
        # 執行各項檢查
        _check_deleted_fixed_usages(conn, now)  # Error type 1
        _check_autonym_accepted_not_accepted(conn, usage_df, whitelist_list_1, now)  # Error type 2
        _check_autonym_multiple_not_accepted(conn, usage_df, whitelist_list_1, now)  # Error type 3
        _check_multiple_accepted_same_object_group(conn, usage_df, whitelist_list_1, now)  # Error type 4
        _check_unique_status_constraint(conn, usage_df, whitelist_list_3, now)  # Error type 5
        _check_multiple_synonyms(conn, usage_df, whitelist_list_2, now)  # Error type 6
        _check_multiple_accepted_names(conn, usage_df, now)  # Error type 7
        _check_no_accepted_names(conn, usage_df, now)  # Error type 8
        _check_taxon_name_in_multiple_groups(conn, usage_df, whitelist_list_2, now)  # Error type 9
        _check_duplicate_reference_usage_ids(conn, usage_df, whitelist_list_3, now)  # Error type 10
        _record_check_timestamp(conn, now)  # Error type 11
        
    finally:
        conn.close()
    
    return 'done!'


def _get_reference_usage_data(conn):
    """獲取主要的reference usage數據"""
    query = """
        SELECT ru.id, ru.status, ru.accepted_taxon_name_id, ru.taxon_name_id, ru.reference_id, 
               tn.object_group, tn.autonym_group, r.properties ->> '$.check_list_type'
        FROM reference_usages ru 
        JOIN taxon_names tn ON tn.id = ru.taxon_name_id
        JOIN `references` r ON r.id = ru.reference_id
        WHERE ru.is_title != 1 
          AND ru.status NOT IN ("", "undetermined") 
          AND ru.deleted_at IS NULL 
          AND ru.accepted_taxon_name_id IS NOT NULL 
          AND ru.reference_id != 95
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        
    df = pd.DataFrame(data, columns=[
        'ru_id', 'ru_status', 'accepted_taxon_name_id', 'taxon_name_id', 
        'reference_id', 'object_group', 'autonym_group', 'check_list_type'
    ])
    
    # 過濾和清理數據
    df = df[df.check_list_type != 4]  # !=4 寫在query裡會排除掉null
    df = df.drop_duplicates().reset_index(drop=True)
    df = df.replace({np.nan: None})
    df = df.drop(columns=['check_list_type'])
    
    return df


def _insert_or_update_usage_check(conn, data, now):
    """統一的插入或更新函數"""
    query = """
        INSERT INTO api_usage_check (reference_usage_id, autonym_group, object_group, 
                                   accepted_taxon_name_id, taxon_name_id, reference_id, 
                                   error_type, whitelist_type, updated_at) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE updated_at = %s
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query, (*data, now, now))
        conn.commit()


def _check_deleted_fixed_usages(conn, now):
    """檢查被刪除的fixed usage_id"""
    query = """
        SELECT fixed_reference_usage_id 
        FROM api_taxon 
        WHERE is_deleted = 0 
          AND fixed_reference_usage_id IN (
              SELECT id FROM reference_usages WHERE deleted_at IS NOT NULL
          )
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        deleted_fixed_usages = cursor.fetchall()
        
        for usage_id, in deleted_fixed_usages:
            _insert_or_update_usage_check(
                conn, (usage_id, None, None, None, None, None, 1, None), now
            )


def _check_autonym_accepted_not_accepted(conn, df, whitelist_list_1, now):
    """檢查autonym/同模：同一篇文獻在不同分類群同時出現accepted和not-accepted"""
    filtered_df = df[
        (~df.ru_id.isin(whitelist_list_1)) & 
        (df.ru_status != 'misapplied')
    ]
    
    check_data = (filtered_df[['object_group', 'reference_id', 'ru_status', 'accepted_taxon_name_id']]
                  .drop_duplicates()
                  .groupby(['reference_id', 'object_group'], as_index=False)
                  .nunique())
    
    problematic_refs = check_data[
        (check_data.ru_status > 1) & 
        (check_data.accepted_taxon_name_id > 1)
    ][['object_group', 'reference_id']]
    
    # 找出需要檢查的object_group
    oo_to_check = []
    for _, row in problematic_refs.iterrows():
        object_group, reference_id = row['object_group'], row['reference_id']
        
        not_accepted_rows = df[
            (~df.ru_id.isin(whitelist_list_1)) &
            (df.ru_status == 'not-accepted') &
            (df.object_group == object_group) &
            (df.reference_id == reference_id)
        ]
        
        acp_ids = not_accepted_rows.accepted_taxon_name_id.tolist()
        for acp_id in acp_ids:
            if not len(df[
                (df.taxon_name_id == acp_id) & 
                (df.object_group == object_group)
            ]):
                oo_to_check.append(object_group)
                break
    
    # 處理問題記錄
    if oo_to_check:
        rows_to_check = (problematic_refs[problematic_refs.object_group.isin(oo_to_check)]
                        .merge(df))
        rows_to_check = rows_to_check[~rows_to_check.ru_id.isin(whitelist_list_1)]
        rows_to_check = rows_to_check.replace({None: 0})

        for _, row in rows_to_check.iterrows():
            _insert_or_update_usage_check(
                conn, (row['ru_id'], row.get('autonym_group'), row['object_group'], 
                      0, 0, 0, 2, 1), now
            )


def _check_autonym_multiple_not_accepted(conn, df, whitelist_list_1, now):
    """檢查autonym/同模：同一篇文獻中有多個not-accepted在不同分類群"""
    filtered_df = df[
        (~df.ru_id.isin(whitelist_list_1)) & 
        (df.ru_status == 'not-accepted')
    ]
    
    check_data = (filtered_df[['object_group', 'reference_id', 'accepted_taxon_name_id']]
                  .drop_duplicates()
                  .groupby(['reference_id', 'object_group'], as_index=False)
                  .nunique())
    
    problematic_refs = check_data[check_data.accepted_taxon_name_id > 1]
    
    oo_to_check = []
    for _, row in problematic_refs.iterrows():
        object_group, reference_id = row['object_group'], row['reference_id']
        
        acp_ids = filtered_df[
            (filtered_df.object_group == object_group) &
            (filtered_df.reference_id == reference_id)
        ].accepted_taxon_name_id.tolist()
        
        for acp_id in acp_ids:
            if not len(df[
                (df.taxon_name_id == acp_id) & 
                (df.object_group == object_group)
            ]):
                oo_to_check.append(object_group)
                break
    
    if oo_to_check:
        rows_to_check = (problematic_refs[problematic_refs.object_group.isin(oo_to_check)]
                        .merge(df))
        rows_to_check = rows_to_check[~rows_to_check.ru_id.isin(whitelist_list_1)]
        rows_to_check = rows_to_check.replace({None: 0})

        for _, row in rows_to_check.iterrows():
            _insert_or_update_usage_check(
                conn, (row['ru_id'], row.get('autonym_group'), row['object_group'], 
                      0, 0, 0, 3, 1), now
            )


def _check_multiple_accepted_same_object_group(conn, df, whitelist_list_1, now):
    """檢查同模（不包含autonym）：同一篇文獻中多個accepted"""
    filtered_df = df[
        (~df.ru_id.isin(whitelist_list_1)) &
        (df.autonym_group.isnull()) &
        (df.object_group.notnull()) &
        (df.ru_status == 'accepted')
    ]
    
    check_data = (filtered_df[['object_group', 'taxon_name_id', 'reference_id']]
                  .drop_duplicates()
                  .groupby(['reference_id', 'object_group'], as_index=False)
                  .nunique())
    
    problematic_refs = check_data[check_data.taxon_name_id > 1]
    
    rows_to_check = pd.DataFrame()
    for _, row in problematic_refs.iterrows():
        matching_rows = df[
            (~df.ru_id.isin(whitelist_list_1)) &
            (df.ru_status == 'accepted') &
            (df.object_group == row['object_group']) &
            (df.reference_id == row['reference_id'])
        ]
        rows_to_check = pd.concat([rows_to_check, matching_rows], ignore_index=True)
    
    rows_to_check = rows_to_check.replace({None: 0})

    for _, row in rows_to_check.iterrows():
        _insert_or_update_usage_check(
            conn, (row['ru_id'], row.get('autonym_group'), row['object_group'], 
                  0, 0, 0, 4, 1), now
        )


def _check_unique_status_constraint(conn, df, whitelist_list_3, now):
    """檢查accepted_taxon_name_id, taxon_name_id, reference_id是否只對到一個status"""
    check_data = df[['accepted_taxon_name_id', 'taxon_name_id', 'reference_id', 'ru_status']]
    grouped = check_data.groupby(['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'], 
                                 as_index=False).count()
    problematic = grouped[grouped.ru_status > 1]
    
    # 轉換數據類型以匹配白名單
    for col in problematic.columns:
        if col in whitelist_list_3.columns:
            problematic[col] = problematic[col].astype(whitelist_list_3[col].dtype)
    
    # 排除白名單
    df_diff = problematic.merge(
        whitelist_list_3, 
        on=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'], 
        how='left', 
        indicator=True
    )
    df_result = df_diff[df_diff['_merge'] == 'left_only'].drop(columns=['_merge'])
    df_result = df_result.replace({None: 0})

    for _, row in df_result.iterrows():
        _insert_or_update_usage_check(
            conn, (0, 0, 0, row['accepted_taxon_name_id'], 
                  row['taxon_name_id'], row['reference_id'], 5, 3), now
        )


def _check_multiple_synonyms(conn, df, whitelist_list_2, now):
    """檢查學名在同一篇文獻中被設定成兩個分類群的同物異名"""
    not_accepted_data = df[df.ru_status == 'not-accepted'][
        ['accepted_taxon_name_id', 'taxon_name_id', 'reference_id']
    ].drop_duplicates()
    
    grouped = not_accepted_data.groupby(['taxon_name_id', 'reference_id'], as_index=False).count()
    problematic = grouped[
        (grouped.accepted_taxon_name_id > 1) & 
        (~grouped.taxon_name_id.isin(whitelist_list_2))
    ]
    problematic = problematic.replace({None: 0})
    
    for _, row in problematic.iterrows():
        _insert_or_update_usage_check(
            conn, (0, 0, 0, 0, row['taxon_name_id'], 
                  row['reference_id'], 6, 2), now
        )


def _check_multiple_accepted_names(conn, df, now):
    """檢查同一個分類群有一個以上的接受名"""
    all_pairs = df[['accepted_taxon_name_id', 'reference_id']].drop_duplicates()
    accepted_pairs = df[df.ru_status == 'accepted'][
        ['accepted_taxon_name_id', 'reference_id', 'ru_status']
    ].drop_duplicates()
    
    grouped = accepted_pairs.groupby(['accepted_taxon_name_id', 'reference_id'], 
                                   as_index=False).count()
    merged = all_pairs.merge(grouped, how='left')
    
    multiple_accepted = merged[merged.ru_status > 1]
    multiple_accepted = multiple_accepted.replace({None: 0})

    for _, row in multiple_accepted.iterrows():
        _insert_or_update_usage_check(
            conn, (0, 0, 0, row['accepted_taxon_name_id'], 
                  0, row['reference_id'], 7, 0), now
        )


def _check_no_accepted_names(conn, df, now):
    """檢查同一個分類群裡面沒有任何接受名"""
    all_pairs = df[['accepted_taxon_name_id', 'reference_id']].drop_duplicates()
    accepted_pairs = df[df.ru_status == 'accepted'][
        ['accepted_taxon_name_id', 'reference_id', 'ru_status']
    ].drop_duplicates()
    
    grouped = accepted_pairs.groupby(['accepted_taxon_name_id', 'reference_id'], 
                                   as_index=False).count()
    merged = all_pairs.merge(grouped, how='left')
    
    no_accepted = merged[merged.ru_status.isna()]
    no_accepted = no_accepted.replace({None: 0})
    
    for _, row in no_accepted.iterrows():
        _insert_or_update_usage_check(
            conn, (0, 0, 0, row['accepted_taxon_name_id'], 
                  0, row['reference_id'], 8, 0), now
        )


def _check_taxon_name_in_multiple_groups(conn, df, whitelist_list_2, now):
    """檢查同一個學名出現在同一篇文獻中的兩個分類群且不是誤用"""
    filtered_df = df[df.ru_status != 'misapplied'][
        ['accepted_taxon_name_id', 'reference_id', 'taxon_name_id']
    ].drop_duplicates()
    
    grouped = filtered_df.groupby(['reference_id', 'taxon_name_id'], as_index=False).count()
    problematic = grouped[
        (grouped.accepted_taxon_name_id > 1) & 
        (~grouped.taxon_name_id.isin(whitelist_list_2))
    ]
    problematic = problematic.replace({None: 0})
    
    for _, row in problematic.iterrows():
        _insert_or_update_usage_check(
            conn, (0, 0, 0, 0, row['taxon_name_id'], 
                  row['reference_id'], 9, 2), now
        )


def _check_duplicate_reference_usage_ids(conn, df, whitelist_list_3, now):
    """檢查一組reference_id, accepted_taxon_name_id, taxon_name_id對到多個ru_id"""
    unique_data = df[['ru_id', 'reference_id', 'accepted_taxon_name_id', 'taxon_name_id']].drop_duplicates()
    grouped = unique_data.groupby(['reference_id', 'accepted_taxon_name_id', 'taxon_name_id'], 
                                 as_index=False).count()
    problematic = grouped[grouped.ru_id > 1]
    
    # 排除白名單
    df_diff = problematic.merge(
        whitelist_list_3, 
        on=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'], 
        how='left', 
        indicator=True
    )
    df_result = df_diff[df_diff['_merge'] == 'left_only'].drop(columns=['_merge'])
    df_result = df_result.replace({None: 0})

    for _, row in df_result.iterrows():
        _insert_or_update_usage_check(
            conn, (0, 0, 0, row['accepted_taxon_name_id'], 
                  row['taxon_name_id'], row['reference_id'], 10, 0), now
        )


def _record_check_timestamp(conn, now):
    """記錄檢查的時間戳"""
    query = """
        INSERT INTO api_usage_check (error_type, updated_at) VALUES (11, %s)
        ON DUPLICATE KEY UPDATE updated_at = %s
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query, (now, now))
        conn.commit()