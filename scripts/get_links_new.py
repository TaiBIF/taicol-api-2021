# 2022-08-30
# update taxon links
import re
import itertools
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

ncbi = pd.read_csv('/code/data/link/ncbi.csv')
irmng = pd.read_csv('/code/data/link/irmng.csv')
fishbase = pd.read_csv('/code/data/link/fishbase.csv')
nc = pd.read_csv('/code/data/link/NC-fixurls.csv')
orthoptera = pd.read_table('/code/data/link/orthoptera_taxon.txt',usecols=['scientificName','acceptedNameUsageID'])
orthoptera = orthoptera.drop_duplicates()
flow = pd.read_csv('/code/data/link/flow.csv')
wsc = pd.read_csv('/code/data/link/wsc.csv')


def get_links(taxon_id, updated=False):
    # 需要更新的：nc, irmng, orthoptera, gisd, Amphibian Species of the World
    links = []
    nm_error = False
    conn = pymysql.connect(**db_settings)
    query = f'SELECT atu.id, tn.name, atu.taxon_id, atu.status, atu.is_latest, atu.taxon_name_id, tn.rank_id \
                FROM api_taxon_usages atu JOIN taxon_names tn ON atu.taxon_name_id = tn.id \
                WHERE atu.status != "mispplied" and taxon_id = "{taxon_id}"'
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        results = pd.DataFrame(results)
        results = results.rename(columns={0: 'id', 1: 'name', 2: 'taxon_id', 3: 'status', 4: 'is_latest', 5:'taxon_name_id', 6:'rank_id' })
        conn.close()
    accepted_name = results[(results['is_latest']==1)&(results['status']=='accepted')]['name'].values[0]
    rank_id = results[(results['is_latest']==1)&(results['status']=='accepted')]['rank_id'].values[0]
    query = f"SELECT path FROM api_taxon_tree WHERE taxon_id = '{taxon_id}'"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query)
        path = cursor.fetchall()
        conn.close()
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
        else:
            nm_error = True
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
    conn = pymysql.connect(**db_settings)
    query = f"select id from api_taxon where alien_type='invasive' and taxon_id = '{taxon_id}' ;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        exists = cursor.fetchall()
        conn.close()
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
            conn.close()
            hie_str = ''
            for d in data:
                hie_str += d[0] + '/'
            if hie_str:
                links += [{'source':'amphibiansoftheworld', 'suffix':hie_str}]
    if updated:
        query = f"SELECT links FROM api_taxon WHERE taxon_id = '{taxon_id}'"
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            old_links = cursor.fetchone()
            old_links = json.loads(old_links[0])
            conn.close()
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
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query)
            exists = cursor.fetchall()
            conn.close()
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
    # 排除重複
    final_links = []
    for l in links:
        if l not in final_links:
            final_links.append(l)
    return final_links, nm_error



conn = pymysql.connect(**db_settings)
query = f"select taxon_id from api_taxon;"
with conn.cursor() as cursor:
    cursor.execute(query)
    taxon_list = cursor.fetchall()
    taxon_list = [t[0] for t in  taxon_list]
    conn.close()


for t in taxon_list:
    print(t)
    l, nm_error = get_links(t)
    if nm_error:
        with open("/code/data/nm_error.txt", "a") as file_object:
            # Append 'hello' at the end of file
            file_object.write(f",{t}")
    if l:
        conn = pymysql.connect(**db_settings)
        query =  f"""UPDATE api_taxon
                    SET links=%s
                    WHERE taxon_id = '{t}'
                    """
        with conn.cursor() as cursor:
            cursor.execute(query, json.dumps(l))
            conn.commit()
            conn.close()

