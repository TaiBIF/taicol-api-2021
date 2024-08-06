import re
from conf.settings import env, SOLR_PREFIX
import pymysql
import pandas as pd
from datetime import datetime
import json
import numpy as np
import requests

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
  'EX': 'NEX', 'EW': 'NEW', 'RE': 'NRE', 'CR': 'NCR', 'EN': 'NEN', 'VU': 'NVU', 'NT': 'NNT',
  'LC': 'NLC', 'DD': 'NDD', 'NA': 'NA', 'NE': 'NE'
}

redlist_map_rev = {
  'NEX': 'EX', 'NEW': 'EW', 'NRE': 'RE', 'NCR': 'CR', 'NEN': 'EN', 'NVU': 'VU', 'NNT': 'NT',
  'NLC': 'LC', 'NDD': 'DD', 'NA': 'NA', 'NE': 'NE'
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


var_df = pd.DataFrame([
('刺','[刺刺]'),
('刺','[刺刺]'),
('葉','[葉葉]'),
('葉','[葉葉]'),
('鈎','[鈎鉤]'),
('鉤','[鈎鉤]'),
('臺','[臺台]'),
('台','[臺台]'),
('螺','[螺螺]'),
('螺','[螺螺]'),
('羣','[群羣]'),
('群','[群羣]'),
('峯','[峯峰]'),
('峰','[峯峰]'),
('曬','[晒曬]'),
('晒','[晒曬]'),
('裏','[裏裡]'),
('裡','[裏裡]'),
('薦','[荐薦]'),
('荐','[荐薦]'),
('艷','[豔艷]'),
('豔','[豔艷]'),
('粧','[妝粧]'),
('妝','[妝粧]'),
('濕','[溼濕]'),
('溼','[溼濕]'),
('樑','[梁樑]'),
('梁','[梁樑]'),
('秘','[祕秘]'),
('祕','[祕秘]'),
('污','[汙污]'),
('汙','[汙污]'),
('册','[冊册]'),
('冊','[冊册]'),
('唇','[脣唇]'),
('脣','[脣唇]'),
('朶','[朵朶]'),
('朵','[朵朶]'),
('鷄','[雞鷄]'),
('雞','[雞鷄]'),
('猫','[貓猫]'),
('貓','[貓猫]'),
('踪','[蹤踪]'),
('蹤','[蹤踪]'),
('恒','[恆恒]'),
('恆','[恆恒]'),
('獾','[貛獾]'),
('貛','[貛獾]'),
('万','[萬万]'),
('萬','[萬万]'),
('两','[兩两]'),
('兩','[兩两]'),
('椮','[槮椮]'),
('槮','[槮椮]'),
('体','[體体]'),
('體','[體体]'),
('鳗','[鰻鳗]'),
('鰻','[鰻鳗]'),
('蝨','[虱蝨]'),
('虱','[虱蝨]'),
('鲹','[鰺鲹]'),
('鰺','[鰺鲹]'),
('鳞','[鱗鳞]'),
('鱗','[鱗鳞]'),
('鳊','[鯿鳊]'),
('鯿','[鯿鳊]'),
('鯵','[鰺鯵]'),
('鰺','[鰺鯵]'),
('鲨','[鯊鲨]'),
('鯊','[鯊鲨]'),
('鹮','[䴉鹮]'),
('䴉','[䴉鹮]'),
('鴴','(行鳥|鴴)'),
('鵐','(鵐|巫鳥)'),
('䱵','(䱵|魚翁)'),
('䲗','(䲗|魚銜)'),
('䱀','(䱀|魚央)'),
('䳭','(䳭|即鳥)'),
('鱼','[魚鱼]'),
('魚','[魚鱼]'),
('万','[萬万]'),
('萬','[萬万]'),
('鹨','[鷚鹨]'),
('鷚','[鷚鹨]'),
('蓟','[薊蓟]'),
('薊','[薊蓟]'),
('黒','[黑黒]'),
('黑','[黑黒]'),
('隠','[隱隠]'),
('隱','[隱隠]'),
('黄','[黃黄]'),
('黃','[黃黄]'),
('囓','[嚙囓]'),
('嚙','[嚙囓]'),
('莨','[茛莨]'),
('茛','[茛莨]'),
('霉','[黴霉]'),
('黴','[黴霉]'),
('莓','[苺莓]'),  
('苺','[苺莓]'),  
('藥','[葯藥]'),  
('葯','[葯藥]'),  
('菫','[堇菫]'),
('堇','[堇菫]')], columns=['char','pattern'])
var_df['idx'] = var_df.groupby(['pattern']).ngroup()

var_df_2 = pd.DataFrame([('行鳥','(行鳥|鴴)'),
('蝦虎','[鰕蝦]虎'),
('鰕虎','[鰕蝦]虎'),
('巫鳥','(鵐|巫鳥)'),
('魚翁','(䱵|魚翁)'),
('魚銜','(䲗|魚銜)'),
('魚央','(䱀|魚央)'),
('游蛇','[遊游]蛇'),
('遊蛇','[遊游]蛇'),
('即鳥','(䳭|即鳥)'),
('椿象','[蝽椿]象'),
('蝽象','[蝽椿]象')], columns=['char','pattern'])


def get_variants(string):
  new_string = ''
  # 單個異體字
  for s in string:    
    if len(var_df[var_df['char']==s]):
      new_string += var_df[var_df['char']==s].pattern.values[0]
    else:
      new_string += s
  # 兩個異體字
  for i in var_df_2.index:
    char = var_df_2.loc[i, 'char']
    if char in new_string:
      new_string = new_string.replace(char,f"{var_df_2.loc[i, 'pattern']}")
  return new_string


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

        keyword = get_variants(keyword)
        name_query_list.append('search_name:/{}/'.format(keyword))

    if common_name_keyword := req.get('common_name','').strip():

        common_name_keyword = get_variants(common_name_keyword)
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

        taxon_ids = [t.get('val') for t in resp['facets']['taxon_id']['buckets']]
        
        if taxon_ids:
            query_list.append('taxon_id:({})'.format(' OR '.join(taxon_ids)))


    rank = req.get('rank')
    if rank:
        rank_id = list(rank_map.keys())[list(rank_map.values()).index(rank)]
        query_list.append('taxon_rank_id:{}'.format(rank_id))


    # 棲地 系列 
    habitat_list = ['is_terrestrial','is_freshwater','is_brackish','is_marine']
    habitat_cond = []
    for i in habitat_list:
        if req.get(i):
            habitat_cond.append("{}:true".format(i))

    if habitat_cond:
        query_list.append(f"({' OR '.join(habitat_cond)})")


    is_list = ['is_endemic','is_fossil','is_in_taiwan','is_hybrid','including_not_official']

    for i in is_list:
        if req.get(i):
            query_list.append("{}:true".format(i))


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
            cursor.execute(query_1, (taxon_group, get_variants(taxon_group)))
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
