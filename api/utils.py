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



redlist_map = {
  'EX': 'NEX', 'EW': 'NEW', 'RE': 'NRE', 'CR': 'NCR', 'EN': 'NEN', 'VU': 'NVU', 'NT': 'NNT',
  'LC': 'NLC', 'DD': 'NDD', 'NA': 'NA', 'NE': 'NE'
}

cites_map = { 'I': '1','II':'2','III':'3','NC':'NC'}

protected_map = {'第一級': 'I', '第二級': 'II', '第三級': 'III'}


rank_map = {
    1: 'Domain', 2: 'Superkingdom', 3: 'Kingdom', 4: 'Subkingdom', 5: 'Infrakingdom', 6: 'Superdivision', 7: 'Division', 8: 'Subdivision', 9: 'Infradivision', 10: 'Parvdivision', 11: 'Superphylum', 12:
    'Phylum', 13: 'Subphylum', 14: 'Infraphylum', 15: 'Microphylum', 16: 'Parvphylum', 17: 'Superclass', 18: 'Class', 19: 'Subclass', 20: 'Infraclass', 21: 'Superorder', 22: 'Order', 23: 'Suborder',
    24: 'Infraorder', 25: 'Superfamily', 26: 'Family', 27: 'Subfamily', 28: 'Tribe', 29: 'Subtribe', 30: 'Genus', 31: 'Subgenus', 32: 'Section', 33: 'Subsection', 34: 'Species', 35: 'Subspecies', 36:
    'Nothosubspecies', 37: 'Variety', 38: 'Subvariety', 39: 'Nothovariety', 40: 'Form', 41: 'Subform', 42: 'Special Form', 43: 'Race', 44: 'Stirp', 45: 'Morph', 46: 'Aberration', 47: 'Hybrid Formula'}


rank_map_c = {1: '域', 2: '總界', 3: '界', 4: '亞界', 5: '下界', 6: '超部|總部', 7: '部|類', 8: '亞部|亞類', 9: '下部|下類', 10: '小部|小類', 11: '超門|總門', 12: '門', 13: '亞門', 14: '下門', 15: '小門', 16: '小門', 17: '超綱|總綱', 18: '綱',
              19: '亞綱', 20: '下綱', 21: '超目|總目', 22: '目', 23: '亞目', 24: '下目', 25: '超科|總科', 26: '科', 27: '亞科', 28: '族', 29: '亞族', 30: '屬', 31: '亞屬', 32: '組|節', 33: '亞組|亞節', 34: '種', 35: '亞種', 36: '雜交亞種',
              37: '變種', 38: '亞變種', 39: '雜交變種', 40: '型', 41: '亞型', 42: '特別品型', 43: '種族', 44: '種族', 45: '形態型', 46: '異常個體', 47: '雜交組合'}

# 林奈階層

lin_map = {
    3: 'kingdom',
    12: 'phylum',
    18: 'classis',
    22: 'ordo',
    26: 'familia',
}

lin_ranks = [3,12,18,22,26,30,34]


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



