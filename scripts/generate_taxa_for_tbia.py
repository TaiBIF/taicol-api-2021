from conf.settings import env
import pymysql
import pandas as pd
from api.utils import rank_map_c, rank_map
import datetime

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

conn = pymysql.connect(**db_settings)

query = """SELECT at.taxon_id, at.accepted_taxon_name_id, tn.name, tn.formatted_authors, an.name_with_tag,
            at.common_name_c,  at.alternative_name_c, r.display ->> '$."en-us"', att.path
            FROM api_taxon at
            JOIN taxon_names tn ON at.accepted_taxon_name_id = tn.id
            LEFT JOIN ranks r ON tn.rank_id = r.id
            LEFT JOIN api_taxon_tree att ON at.taxon_id = att.taxon_id
            LEFT JOIN api_names an ON at.accepted_taxon_name_id = an.taxon_name_id
        """
with conn.cursor() as cursor:
    cursor.execute(query)
    df = cursor.fetchall()
    df = pd.DataFrame(df, columns=['taxon_id', 'accepted_namecode', 'scientificName', 'name_author','formatted_name', 
                                    'common_name_c', 'alternative_name_c', 'taxon_rank', 'path'])

# 取 misapplied & synonyms
df['synonyms'] = ''
df['formatted_synonyms'] = ''
df['misapplied'] = ''
df['formatted_misapplied'] = ''
query = f"SELECT tu.taxon_id, tu.status, GROUP_CONCAT(DISTINCT(an.name_with_tag) SEPARATOR ','), GROUP_CONCAT(DISTINCT(tn.name) SEPARATOR ',') \
            FROM api_taxon_usages tu \
            JOIN api_names an ON tu.taxon_name_id = an.taxon_name_id \
            JOIN taxon_names tn ON tu.taxon_name_id = tn.id \
            WHERE tu.taxon_id IN ({str(df.taxon_id.to_list()).replace('[','').replace(']','')}) and tu.status IN ('synonyms', 'misapplied') \
            GROUP BY tu.status, tu.taxon_id;"
with conn.cursor() as cursor:
    cursor.execute(query)
    other_names = cursor.fetchall()
for o in other_names:
    if o[1] == 'synonyms':
        df.loc[df['taxon_id'] == o[0], 'synonyms'] = o[3]
        df.loc[df['taxon_id'] == o[0], 'formatted_synonyms'] = o[2]
    elif o[1] == 'misapplied':
        df.loc[df['taxon_id'] == o[0], 'misapplied'] = o[3]
        df.loc[df['taxon_id'] == o[0], 'formatted_misapplied'] = o[2]

# 取所有階層
# 階層不存作者


for i in df.index:
    if i % 1000==0:
        print(i)
    if i >= 78019:
        conn = pymysql.connect(**db_settings)
        if df.path[i]:
            path = df.path[i].split('>')
            taxon_id = df.taxon_id[i]
            # 先拿掉自己
            path = [p for p in path if p != taxon_id]
            if path:
                query = f"SELECT t.taxon_id, t.accepted_taxon_name_id, tn.name, \
                        tn.formatted_authors, an.name_with_tag, t.rank_id, t.common_name_c \
                        FROM api_taxon t \
                        JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
                        JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id \
                        WHERE t.taxon_id IN ({str(path).replace('[','').replace(']','')}) \
                        ORDER BY t.rank_id DESC"
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    for r in results:
                        rank_name = rank_map[r[5]].lower().replace(" ", "")
                        df.loc[i, rank_name] = r[2]
                        # 屬以下才有斜體格式
                        if r[5] >=30:
                            col_name = 'formatted_' + rank_name
                            df.loc[i, col_name] = r[4]
                        # 如果有中文加上中文
                        if r[6]:
                            rank_name_c = rank_name + '_c'
                            df.loc[i, rank_name_c] = r[6]

# id 要轉成 int
# 要把0改成None
# df['accepted_namecode'] = df['accepted_namecode'].fillna(0)
df = df.astype({'accepted_namecode': "int"})
# df['accepted_namecode'] = df['accepted_namecode'].replace({0: None})

# scientific name 會有換行符號
# results.scientific_name = results.scientific_name.str.replace(r'[\n\s]+', ' ')

# 階層全部小寫
df['taxon_rank'] = df['taxon_rank'].str.lower()

# 整理columns -> 但無法確定會有什麼欄位...
# rename

df = df.drop(columns=['path'])
df = df.rename(columns={'taxon_id': 'id', 'accepted_namecode': 'scientificNameID', 
                        'taxon_rank': 'taxonRank'})

today = datetime.datetime.now().strftime('%Y%m%d')

df.to_csv(f'source_taicol_for_tbia_{today}.csv', index=False)
