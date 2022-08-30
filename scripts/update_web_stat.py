# update stat for web visualization
# TODO 目前是新增不是更新

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


# 首頁統計
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    query = f"""WITH ids AS(
                SELECT DISTINCT(ru.reference_id) AS id
                FROM api_taxon_usages atu 
                JOIN reference_usages ru ON atu.reference_usage_id = ru.id
                WHERE ru.is_title != 1
                UNION 
                SELECT distinct(tn.reference_id) AS id FROM taxon_names tn
                WHERE tn.reference_id IS NOT NULL) 
                SELECT count(distinct(id)) FROM ids"""  
    cursor.execute(query)
    ref_count = cursor.fetchone()
    ref_count = ref_count[0]
    # 收錄物種數 (排除有種下的種階層)
    query = """SELECT COUNT(at.taxon_id) FROM api_taxon at
                JOIN ranks r ON at.rank_id = r.id 
                WHERE r.id >= 34 AND at.taxon_id NOT IN (
                SELECT att.parent_taxon_id FROM api_taxon_tree att 
                JOIN api_taxon at ON att.taxon_id = at.taxon_id
                JOIN ranks r ON at.rank_id = r.id
                WHERE r.id > 34 AND att.parent_taxon_id IS NOT NULL);"""
    cursor.execute(query)
    taxon_count = cursor.fetchone()
    taxon_count = taxon_count[0]
    # 收錄學名數
    query = "SELECT COUNT(DISTINCT(taxon_name_id)) FROM api_taxon_usages"
    cursor.execute(query)
    name_count = cursor.fetchone()
    name_count = name_count[0]
# 寫入資料庫
with conn.cursor() as cursor:
    query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('index', 'reference', {ref_count})"
    cursor.execute(query)
    conn.commit()
    query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('index', 'taxon', {taxon_count})"
    cursor.execute(query)
    conn.commit()
    query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('index', 'name', {name_count})"
    cursor.execute(query)
    conn.commit()


# 各階層物種數統計 rank_count
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    query = f"""SELECT r.key, COUNT(DISTINCT(at.taxon_id)) FROM api_taxon at
                JOIN ranks r ON at.rank_id = r.id
                WHERE at.rank_id IN (3,12,18,22,26,30,34)
                GROUP BY at.rank_id
                ORDER BY at.rank_id ASC
                """  
    cursor.execute(query)
    rank_count = cursor.fetchall() 
    for r in rank_count:
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('rank_count', '{r[0]}', {r[1]})"
        cursor.execute(query)
        conn.commit()



# 各界物種數統計 kingdom_count
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    # 先用rank_id=3找出taxonID, 再計算path有包含該taxonID的數量（但要種）
    query = f"""SELECT at.taxon_id, tn.name FROM api_taxon at
                JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id 
                WHERE at.rank_id = 3
                """
    cursor.execute(query)
    kingdom = cursor.fetchall()
    # k_results = []
    for k in kingdom:
        query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                    JOIN api_taxon at ON att.taxon_id = at.taxon_id
                    JOIN ranks r ON at.rank_id = r.id
                    WHERE r.id = 34 AND att.path LIKE '%>{k[0]}%' 
                    """
        cursor.execute(query)
        tmp = cursor.fetchone()
        # k_results += [(k[1], tmp[0])]
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('kingdom_count', '{k[1]}', {tmp[0]})"
        cursor.execute(query)
        conn.commit()
# 沒有回傳就是0
# [('Plantae', 9497), ('Animalia', 37813), ('Chromista', 1561), ('Fungi', 6460), ('Protozoa', 1137)]


# 物種來源比例 source_count
conn = pymysql.connect(**db_settings)
source_dict = {'native': '原生', 'naturalized': '歸化', 'cultured': '栽培豢養', 'invasive': '入侵', 'None': '無資料'}
with conn.cursor() as cursor:
    query = f"""SELECT at.alien_type, COUNT(at.taxon_id) FROM api_taxon at 
                JOIN ranks r ON at.rank_id = r.id
                WHERE r.id = 34
                GROUP BY at.alien_type"""  
    cursor.execute(query)
    source_count = cursor.fetchall()
    for s in source_count:
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('source_count', '{source_dict[str(s[0])]}', {s[1]})"
        cursor.execute(query)
        conn.commit()
# (('native', 53603), ('naturalized', 1247), ('cultured', 1715), ('invasive', 218), (None, 22))

# 各類生物種數&特有比例
conn = pymysql.connect(**db_settings)
e_group = [['昆蟲',['Insecta']],['魚類',['Actinopterygii', 'Chondrichthyes', 'Myxini']], ['爬蟲類',['Reptilia']],
            ['真菌(含地衣)',['Fungi']], ['植物',['Plantae']], ['鳥類', ['Aves']], ['哺乳類', ['Mammalia']]]
all_taxon_id = []
endemic_count = []
with conn.cursor() as cursor:
    for e in e_group:
        name_str = str(e[1]).replace('[','(').replace(']',')')
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name IN {name_str}
                """
        cursor.execute(query)
        e_taxon = cursor.fetchall()
        all_taxon_id += [et[0] for et in e_taxon]
        condition_list = []
        for et in e_taxon:
            condition_list += [f"att.path LIKE '%>{et[0]}%'" ]
        condition = ' OR '.join(condition_list)
        query = f"""SELECT at.is_endemic, COUNT(att.taxon_id) FROM api_taxon_tree att
                    JOIN api_taxon at ON att.taxon_id = at.taxon_id
                    JOIN ranks r ON at.rank_id = r.id
                    WHERE r.id = 34 AND ({condition})
                    GROUP BY at.is_endemic
                    """
        cursor.execute(query)
        tmp = cursor.fetchall() 
        total, endemic = 0, 0
        for t in tmp:
            total += t[1]
            if t[0] == 1:
                endemic += t[1]
        endemic_count += [(e[0], endemic, total)]
    # 其他
    condition_list = []
    for et in all_taxon_id:
        condition_list += [f"att.path NOT LIKE '%>{et}%'" ]
    condition = ' AND '.join(condition_list)
    query = f"""SELECT at.is_endemic, COUNT(att.taxon_id) FROM api_taxon_tree att
                JOIN api_taxon at ON att.taxon_id = at.taxon_id
                JOIN ranks r ON at.rank_id = r.id
                WHERE r.id = 34 AND ({condition})
                GROUP BY at.is_endemic
            """
    cursor.execute(query)
    tmp = cursor.fetchall() 
    total, endemic = 0, 0
    for t in tmp:
        total += t[1]
        if t[0] == 1:
            endemic += t[1]
    endemic_count += [('其他', endemic, total)]
    for e in endemic_count:
        query = f"INSERT INTO api_web_stat (title, category, count, total_count) VALUES ('endemic_count', '{e[0]}', {e[1]}, {e[2]})"
        cursor.execute(query)
        conn.commit()

# 台灣與全球物種數比較
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    plantae_compare = [['藻類植物',['Charophyta','Chlorophyta','Rhodophyta']],['苔蘚植物',['Anthocerotophyta','Bryophyta','Marchantiophyta']], ['蕨類植物',['Lycopodiopsida','Polypodiopsida']],
                       ['裸子植物',['Cycadopsida','Ginkgoopsida','Pinopsida']], ['顯花植物',['Magnoliopsida']]]
    plantae_compare_result = []
    for c in plantae_compare:
        name_str = str(c[1]).replace('[','(').replace(']',')')
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name IN {name_str}
                """
        cursor.execute(query)
        c_taxon = cursor.fetchall()
        condition_list = []
        for ct in c_taxon:
            condition_list += [f"att.path LIKE '%>{ct[0]}%'" ]
        condition = ' OR '.join(condition_list)
        query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                    JOIN api_taxon at ON att.taxon_id = at.taxon_id
                    JOIN ranks r ON at.rank_id = r.id
                    WHERE r.id = 34 AND ({condition})
                    """
        cursor.execute(query)
        tmp = cursor.fetchone() 
        plantae_compare_result += [[c[0],tmp[0]]]
    for s in plantae_compare_result:
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('plantae_compare', '{s[0]}', {s[1]})"
        cursor.execute(query)
        conn.commit()
    animalia_compare = [['環節動物門','Annelida'],['節肢動物門','Arthropoda'],['脊索動物門','Chordata'],['刺胞動物門','Cnidaria'],['棘皮動物門','Echinodermata'],
                        ['軟體動物門','Mollusca'],['扁形動物門','Platyhelminthes'],['輪蟲動物門','Rotifera']]
    animalia_compare_result = []
    for c in animalia_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                    JOIN api_taxon at ON att.taxon_id = at.taxon_id
                    JOIN ranks r ON at.rank_id = r.id
                    WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                    """
        cursor.execute(query)
        tmp = cursor.fetchone() 
        animalia_compare_result += [[c[0],tmp[0]]]   
    for s in animalia_compare_result:
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('animalia_compare', '{s[0]}', {s[1]})"
        cursor.execute(query)
        conn.commit()
    arthropoda_compare = [['蛛形綱','Arachnida'],['彈尾綱','Collembola'],['橈足綱','Copepoda'],['倍足綱','Diplopoda'],['內口綱','Entognatha'],['昆蟲綱','Insecta'],
                          ['軟甲綱','Malacostraca'],['介形蟲綱','Ostracoda']]
    arthropoda_compare_result = []
    for c in arthropoda_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            arthropoda_compare_result += [[c[0],tmp[0]]]   
        else:
            arthropoda_compare_result += [[c[0],0]]   
    for s in arthropoda_compare_result:
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('arthropoda_compare', '{s[0]}', {s[1]})"
        cursor.execute(query)
        conn.commit()
    chordata_compare = [['條鰭魚綱','Actinopterygii'],['兩生綱','Amphibia'],['海鞘綱','Ascidiacea'],['鳥綱','Aves'],['軟骨魚綱','Chondrichthyes'],['哺乳綱','Mammalia'],
                        ['盲鰻綱','Myxini'],['海樽綱','Thaliacea']]
    chordata_compare_result = []
    for c in chordata_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            chordata_compare_result += [[c[0],tmp[0]]]   
        else:
            chordata_compare_result += [[c[0],0]]   
    for s in chordata_compare_result:
        query = f"INSERT INTO api_web_stat (title, category, count) VALUES ('chordata_compare', '{s[0]}', {s[1]})"
        cursor.execute(query)
        conn.commit()



# 臺灣與全球物種物種數比較表 
kingdom_compare = [['病毒界','Viruses','10,434','徐亞莉、葉錫東、吳和生、黃元品、趙磐華、涂堅'],
                   ['細菌界','Bacteria','9,980','袁國芳、楊秋忠'],
                   ['古菌界','Archaea','377','賴美津'],
                   ['原生生物界','Protozoa','2,614','劉錦惠、王建平'],
                   ['原藻界','Chromista','62,311','黃淑芳、吳俊宗、謝煥儒'],
                   ['真菌界','Fungi','146,154','賴明洲、謝文瑞、陳金亮、謝煥儒、曾顯雄、吳聲華、黃俞菱等']]

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    for c in kingdom_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '{format(tmp[0],',')}', '{c[2]}', '{c[3]}')"
        else:
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '0', '{c[2]}', '{c[3]}')"
        cursor.execute(query)
        conn.commit()


plantae_compare = [['植物界>藻類植物',['Charophyta','Chlorophyta','Rhodophyta'],'20,066','王建平、林綉美、黃淑芳'],
                   ['植物界>苔蘚植物',['Anthocerotophyta','Bryophyta','Marchantiophyta'],'21,018','蔣鎮宇'], 
                   ['植物界>蕨類植物',['Lycopodiopsida','Polypodiopsida'],'13,661','郭城孟、TPG'],
                   ['植物界>裸子植物',['Cycadopsida','Ginkgoopsida','Pinopsida'],'1,420','彭鏡毅'], 
                   ['植物界>顯花植物',['Magnoliopsida'],'339,411','彭鏡毅、謝長富、鍾國芳、林政道']]
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    for c in plantae_compare:
        name_str = str(c[1]).replace('[','(').replace(']',')')
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name IN {name_str}
                """
        cursor.execute(query)
        c_taxon = cursor.fetchall()
        condition_list = []
        for ct in c_taxon:
            condition_list += [f"att.path LIKE '%>{ct[0]}%'" ]
        if condition_list:
            condition = ' OR '.join(condition_list)
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND ({condition})
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '{format(tmp[0],',')}', '{c[2]}', '{c[3]}')"
        else:
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '0', '{c[2]}', '{c[3]}')"
        cursor.execute(query)
        conn.commit()



animalia_compare = [['動物界>海綿動物門','Porifera','9,540','宋克義'],
                    ['動物界>刺胞動物門','Cnidaria','14,791','戴昌鳳、羅文增、鄭有容、廖運志'],
                    ['動物界>扁形動物門','Platyhelminthes','21,447','施秀惠、陳宣汶'],
                    ['動物界>圓形動物門','Nematoda','13,129','施秀惠'],
                    ['動物界>線形動物門','Nematomorpha','356','邱名鍾'],
                    ['動物界>鉤頭動物門','Acanthocephala','1,330','陳宣汶'],
                    ['動物界>輪蟲動物門','Rotifera','2,014','張文炳']]

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    for c in animalia_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '{format(tmp[0],',')}', '{c[2]}', '{c[3]}')"
        else:
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '0', '{c[2]}', '{c[3]}')"
        cursor.execute(query)
        conn.commit()



arthropoda_compare = [['動物界>節肢動物門>介形蟲綱','Ostracoda','11,079','胡忠恆、陶錫珍'],
                      ['動物界>節肢動物門>海蜘蛛綱','Pycnogonida','1,366','孫頌堯'],
                      ['動物界>節肢動物門>軟甲綱','Malacostraca','36,162','何平合、陳天任、施習德、石長泰、羅文增'],
                      ['動物界>節肢動物門>橈足綱','Copepoda','14,674','石長泰、林清龍、王建平、廖運志、鄭有容'],
                      ['動物界>節肢動物門>鞘甲綱','Thecostraca','N/A','陳國勤、吳文哲'],
                      ['動物界>節肢動物門>蛛形綱','Arachnida','89,189','黃坤煒、廖治榮、朱耀沂、羅英元'],
                      ['動物界>節肢動物門>倍足綱','Diplopoda','13,232','張學文、Zoltán Korsós'],
                      ['動物界>節肢動物門>唇足綱','Chilopoda','3,145','趙瑞隆'],
                      ['動物界>節肢動物門>內口綱','Entognatha','759','吳文哲'],
                      ['動物界>節肢動物門>彈尾綱','Collembola','8,767','齊心、張智涵、鄭欣如'],
                      ['動物界>節肢動物門>昆蟲綱','Insecta','952,776','吳文哲、楊正澤、周樑鎰、蕭旭峰、顏聖紘、李奇峰、周文一、李春霖、楊曼妙、蔡明諭、徐堉峰、吳士緯、林宗岐、鄭明倫、徐歷鵬、蔡經甫等'],
                      ['動物界>節肢動物門>鰓足綱','Branchiopoda','1,421','黃祥麟、周蓮香'],
                      ['動物界>節肢動物門>肢口綱','Merostomata','5','謝蕙蓮']]


conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    for c in arthropoda_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '{format(tmp[0],',')}', '{c[2]}', '{c[3]}')"
        else:
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '0', '{c[2]}', '{c[3]}')"
        cursor.execute(query)
        conn.commit()


animalia_compare_2 = [['動物界>紐形動物門','Nemertea','1,353','賴亦德'],
                      ['動物界>環節動物門','Annelida','17,071','張智涵、陳俊宏、謝蕙蓮'],
                      ['動物界>星蟲動物門','Sipuncula','206','薛攀文'],
                      ['動物界>軟體動物門','Mollusca','119,766','盧重成、巫文隆、賴景陽、李彥錚'],
                      ['動物界>腕足動物門','Brachiopoda','435',''],
                      ['動物界>緩步動物門','Tardigrada','1,018','李曉晨'],
                      ['動物界>苔蘚動物門','Bryozoa','20,573','Dennis P. Gordon'],
                      ['動物界>毛顎動物門','Chaetognatha','132','羅文增'],
                      ['動物界>棘皮動物門','Echinodermata','11,554','趙世民、李坤瑄']]


conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    for c in animalia_compare_2:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '{format(tmp[0],',')}', '{c[2]}', '{c[3]}')"
        else:
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '0', '{c[2]}', '{c[3]}')"
        cursor.execute(query)
        conn.commit()

chordata_compare = [['動物界>脊索動物門>狹心綱','Leptocardii','30','林秀瑾'],
                    ['動物界>脊索動物門>海樽綱','Thaliacea','78','羅文增'],
                    ['動物界>脊索動物門>海鞘綱','Ascidiacea','2,966',''],
                    ['動物界>脊索動物門>盲鰻綱','Myxini','82','莫顯蕎、邵廣昭'],
                    ['動物界>脊索動物門>軟骨魚綱','Chondrichthyes','1,282','莊守正、李柏峰、邵廣昭'],
                    ['動物界>脊索動物門>條鰭魚綱','Actinopterygii','32,513','邵廣昭、陳正平、陳義雄、陳麗淑、何宣慶、吳高逸、黃世彬等'],
                    ['動物界>脊索動物門>爬蟲綱','Reptilia','N/A','呂光洋、陳添喜、李培芬'],
                    ['動物界>脊索動物門>兩生綱','Amphibia','8,054','吳聲海、楊懿如'],
                    ['動物界>脊索動物門>鳥綱','Aves','10,599','劉小如、丁宗蘇'],
                    ['動物界>脊索動物門>哺乳綱','Mammalia','6,025','周蓮香、李玲玲、王明智、鄭錫奇']]

conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    for c in chordata_compare:
        query = f"""SELECT at.taxon_id FROM api_taxon at
                    JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
                    WHERE tn.name = '{c[1]}'
                """
        cursor.execute(query)
        c_taxon = cursor.fetchone()
        if c_taxon:
            query = f"""SELECT COUNT(att.taxon_id) FROM api_taxon_tree att
                        JOIN api_taxon at ON att.taxon_id = at.taxon_id
                        JOIN ranks r ON at.rank_id = r.id
                        WHERE r.id = 34 AND att.path LIKE '%>{c_taxon[0]}%'
                        """
            cursor.execute(query)
            tmp = cursor.fetchone() 
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '{format(tmp[0],',')}', '{c[2]}', '{c[3]}')"
        else:
            query = f"INSERT INTO api_web_table (path, count, total_count, provider) VALUES ('{c[0]}', '0', '{c[2]}', '{c[3]}')"
        cursor.execute(query)
        conn.commit()


# 全部
conn = pymysql.connect(**db_settings)
with conn.cursor() as cursor:
    query = f"INSERT INTO api_web_table (path, count, total_count) VALUES ('合計', {taxon_count}, '>2,050,000')"
    cursor.execute(query)
    conn.commit()


