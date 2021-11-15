from django.test import TestCase

# Create your tests here.
from django.test import TestCase

# Create your tests here.
common_query = SELECT tn.id, tn.nomenclature_id, tn.rank_id, tn.name, tn.formatted_authors, \
                tn.properties, tn.original_taxon_name_id, tn.created_at, tn.updated_at, \
                n.name, JSON_EXTRACT(r.display,'$."en-us"') \
                FROM taxon_names AS tn \
                LEFT JOIN nomenclatures AS n ON tn.nomenclature_id = n.id \
                LEFT JOIN ranks AS r ON tn.rank_id = r.id 
                

# with conn.cursor() as cursor:
#     query = ""
#     cursor.execute(query)
#     result = cursor.fetchall()

# 1. name_id: 不考慮其他變數
name_id = ''
scientific_name = ''
updated_at = '2'
created_at = ''
taxon_group = ''

conditions = []
if updated_at:
    conditions += [f"date(updated_at) > '{updated_at}'"]
if created_at:
    conditions += [f"date(created_at) > '{created_at}'"]

if name_id:  # 不考慮其他條件
    query = f"{common_query} WHERE id = {name_id}"
    print('name_id: ', query)
elif scientific_name:  # 不考慮分類群, scientific_name, updated_at, created_at
    query = f"{common_query} WHERE name = '{scientific_name}'"
    for c in conditions:
        query += " AND " + c
    print('name: ', query)
elif taxon_group:
    
    # 先由 學名 / 中文名 找出符合的name_id
    query_1 = f"SELECT id FROM taxon_names WHERE name = '{taxon_group}'"
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        cursor.execute(query_1)
        results = cursor.fetchall()
    # find all child id
    for r in results: # could be more than 1
        current_id = r[0]
        query_taxon_group = f"""
                        select  taxon_name_id,
                        from    (select * from reference_usages
                                order by parent_taxon_name_id, taxon_name_id) reference_usages,
                                (select @pv := '{current_id}') initialisation
                        where   find_in_set(parent_taxon_name_id, @pv) > 0
                        and     @pv := concat(@pv, ',', taxon_name_id)
                        """
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            cursor.execute(query_taxon_group)
            results = cursor.fetchall()


    # 再找出所有該name以下階層的id
    query = ''
    print('taxon_group: ', query)
else:
    # updated_at, created_at or no condition
    if len(conditions) == 1:
        query = f"{common_query} WHERE {conditions[0]}"
    elif len(conditions) == 2:
        query = f"{common_query} WHERE {conditions[0]} AND {conditions[1]}"
    else:  # len == 0
        query = common_query
    print('else: ', query)


with conn.cursor() as cursor:
    cursor.execute(query)
    name_results = cursor.fetchall()
    name_results = [list(item) for item in name_results]

