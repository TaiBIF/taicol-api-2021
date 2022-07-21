# 種下階層的species_id改為種

import re
import itertools
from conf.settings import env
import pymysql
import pandas as pd
import requests
from datetime import datetime
import json

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

conn = pymysql.connect(**db_settings)


query = """
            select ru.taxon_name_id, tn.rank_id ,ru.parent_taxon_name_id , tnn.rank_id from reference_usages ru
            join taxon_names tn ON ru.taxon_name_id = tn.id
            join taxon_names tnn ON ru.parent_taxon_name_id = tnn.id
            where tn.rank_id > 34;
        """

with conn.cursor() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ['taxon_name_id', 'rank', 'parent_taxon_name_id', 'parent_rank']

query = """
        SELECT id, properties->>"$.species_id" FROM taxon_names;
        """

with conn.cursor() as cursor:
    cursor.execute(query)
    pars = cursor.fetchall()
    pars = pd.DataFrame(pars)
    pars.columns = ['taxon_name_id', 'correct_parent']


pars = pars[pars.correct_parent!='null']
pars['correct_parent'] = pars.correct_parent.apply(int)

results = results.merge(pars, how='left')

results = results[results.correct_parent!=results.parent_taxon_name_id]

results = results.reset_index()

# 3324
conn = pymysql.connect(**db_settings)

for i in results.index:
    print(i)
    row = results.iloc[i]
    query = f"""UPDATE reference_usages SET parent_taxon_name_id = {row.correct_parent}, 
                    updated_at = CURRENT_TIMESTAMP WHERE taxon_name_id = {row.taxon_name_id} """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
