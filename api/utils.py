import re
import ast
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
    "database": env('DB_DBNAME'),
}


def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False


# TaiCOL taxon_id 格式：t + 7 位數字
TAXON_ID_RE = re.compile(r'^t\d{7}$')


def is_valid_taxon_id(value):
    return bool(value) and bool(TAXON_ID_RE.match(str(value)))


def map_values(s, mapping):
    """取代 Series.replace(dict)，結果（值與 dtype）與 pandas 2.2 舊版 replace 自動 downcast 相同，
    但不會觸發 'Downcasting behavior in `replace` is deprecated' FutureWarning。"""
    # 舊版 replace 的 None key 只會比對到 object 欄位中的 NaN；數值欄位的 NaN 保持不變
    na_to_none_key = None in mapping and s.dtype == object
    matched = False
    replaced_with_none = False
    values = []
    for x in s.tolist():
        if x is None or (isinstance(x, float) and x != x):
            hit = None in mapping and (x is None or na_to_none_key)
            key = None
        else:
            try:
                hit = x in mapping
            except TypeError:  # unhashable（例如 list）
                hit = False
            key = x
        if hit:
            matched = True
            replaced_with_none = replaced_with_none or mapping[key] is None
            values.append(mapping[key])
        else:
            values.append(x)
    if not matched:
        # 沒有任何值被替換：數值 / bool 欄位維持原 dtype；object 欄位舊版仍會嘗試 downcast
        return s.infer_objects() if s.dtype == object else s.copy()
    result = pd.Series(values, index=s.index, dtype=object, name=s.name)
    if s.dtype != object and replaced_with_none:
        return result  # 數值欄位換成 None 時，舊版保留 object（None 不會變回 NaN）
    return result.infer_objects()


def to_int_or_none(s):
    """NaN / None -> None，其餘轉 int（取代 replace({np.nan: 0}).astype('int64').replace({0: None}) 等寫法）"""
    return s.astype(float).fillna(0).astype('int64').replace({0: None})


def safe_literal(value):
    """取代 eval：只解析 JSON / Python literal，不執行任意程式碼"""
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return ast.literal_eval(value)


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
    rank_map = dict(zip([r[0] for r in ranks], [safe_literal(r[1])['en-us'] for r in ranks]))
    rank_map_c = dict(zip([r[0] for r in ranks], [safe_literal(r[1])['zh-tw'] for r in ranks]))
    rank_map_c_reverse = dict(zip([safe_literal(r[1])['zh-tw'] for r in ranks],[r[0] for r in ranks]))
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

# 補上 \ | < > # @（Lucene 查詢 / 正規式的特殊字元），避免跳脫不完整
spe_chars = ['\\', '+','-', '&', '|', '!','(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '/', '.', '<', '>', '#', '@']

def escape_solr_query(string):
    final_string = ''
    for s in str(string):
        if s in spe_chars:
            final_string += f'\\{s}'
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
        if is_valid_taxon_id(taxon_id):
            query_list.append('taxon_id:"{}"'.format(taxon_id))
        else:
            # 格式不符視同查無資料
            query_list.append('-*:*')

    # NOTE 如果有 scientific_name 或 common_name 的話 要先查詢一次 取得taxon_id

    # has_name_search = False

    name_query_list = []

    if keyword := req.get('scientific_name','').strip():
        keyword_wo_rank = remove_rank_char(keyword)
        keyword_wo_rank = escape_solr_query(keyword_wo_rank)  # 原本未跳脫，可注入 Solr 語法
        keyword_wo_rank = process_text_variants(keyword_wo_rank)

        keyword = escape_solr_query(keyword)
        keyword = process_text_variants(keyword)
        # name_query_list.append('search_name:/{}/'.format(keyword))
        name_query_list.append(f"search_name:/{keyword}/ OR search_name_wo_rank:/{keyword_wo_rank}/")

    if common_name_keyword := req.get('common_name','').strip():

        common_name_keyword = escape_solr_query(common_name_keyword)  # 原本未跳脫
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


    is_list = ['is_endemic','is_fossil','is_in_taiwan','is_hybrid']

    # TODO 待確認
    for i in is_list:
        if req.get(i) == 'true':
            query_list.append("{}:true".format(i))
        elif req.get(i) == 'false':
            query_list.append("{}:false".format(i))

    if req.get('including_not_official') == 'true':
        query_list.append("not_official:true")

    elif  req.get('including_not_official') == 'false':
        query_list.append("not_official:false")

    # if reg.get('is_in_taiwn')
    # 預設為true
    # query_list.append("is_in_taiwan:{}".format(req.get('is_in_taiwan','true')))


    # alien_type

    if alien_type := req.get('alien_type'):
        query_list.append('alien_type:"{}"'.format(escape_solr_query(alien_type)))


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
                    cs_list.append(f'{con}:"{escape_solr_query(css)}"')
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
                cs_list.append(f'cites:/.*{escape_solr_query(css)}.*/')
        if cs_list:
            c_str = f"({' OR '.join(cs_list)})"
            query_list.append(c_str)

    # 取得指定分類群以下的所有階層物種資料，可輸入學名或中文名

    if taxon_group := req.get('taxon_group'):
        conn = pymysql.connect(**db_settings)

        query_1 = f"""SELECT DISTINCT t.taxon_id FROM taxon_names tn 
                    JOIN api_taxon t ON tn.id = t.accepted_taxon_name_id 
                    LEFT JOIN api_common_name acn ON acn.taxon_id = t.taxon_id  
                    WHERE (tn.name = %s OR acn.name_c REGEXP %s) AND t.is_deleted = 0 """
        with conn.cursor() as cursor:
            # REGEXP 的 pattern 先跳脫正規式特殊字元，避免使用者輸入任意正規式（ReDoS / 語法錯誤）
            # 前後加 ^ $：中文名需完全相同（僅容許異體字，如 台/臺），不做部分比對
            name_c_pattern = '^(' + process_text_variants(re.escape(taxon_group)) + ')$'
            cursor.execute(query_1, (taxon_group, name_c_pattern))
            t_id = cursor.fetchall()
            if len(t_id):
                # 可能不只一筆
                t_str = [ 'path:/.*{}.*/'.format(escape_solr_query(t[0])) for t in t_id]
                query_list.append(f"({' OR '.join(t_str)})")
            else:
                # 查無指定分類群：回傳 0 筆（原本會因為沒有加條件而回傳全部物種）
                query_list.append('-*:*')
        
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
        WHERE whitelist_type IN (1, 2, 3) and is_whitelist = 1
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