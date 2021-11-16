from os import error
from django.shortcuts import render
from django.http import (
    JsonResponse,
    HttpResponseRedirect,
    Http404,
    HttpResponse,
)
from django.core.paginator import Paginator
import json
import pymysql
from conf.settings import env
import pandas as pd
import datetime
from json import JSONEncoder


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}


class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


def name(request):
    # print(type(request.GET.get('limit', 20)), request.GET.get('page', 1))

    try:
        limit = int(request.GET.get('limit', 20))
        page = int(request.GET.get('page', 1))
    except:
        response = {"status": {"code": 400,
                               "message": "Bad Request: Type error of limit or page"}}
        return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

    try:
        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id', 'scientific_name', 'common_name', 'updated_at', 'created_at', 'taxon_group', 'limit', 'page']):
            response = {"status": {"code": 400,
                                   "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
        # elif not isinstance(request.GET.get('limit', 20), int) or not isinstance(request.GET.get('page', 1), int):

        name_id = request.GET.get('name_id', '')
        scientific_name = request.GET.get('scientific_name', '')
        updated_at = request.GET.get('updated_at', '')
        created_at = request.GET.get('created_at', '')
        taxon_group = request.GET.get('taxon_group', '')
        # limit = request.GET.get('limit', 20)
        # page = request.GET.get('page', 1)
        limit = 300 if limit > 300 else limit  # 最大值 300

        print(name_id, scientific_name, updated_at, created_at, taxon_group)
        conn = pymysql.connect(**db_settings)
        common_query = "SELECT tn.id, tn.nomenclature_id, tn.rank_id, tn.name, tn.formatted_authors, \
                        tn.properties, tn.original_taxon_name_id, tn.note, tn.created_at, tn.updated_at, \
                        n.name , JSON_EXTRACT(r.display,'$.\"en-us\"'), \
                        JSON_EXTRACT(tn.properties,'$.\"is_hybrid_formula\"'), \
                        JSON_EXTRACT(tn.properties,'$.\"reference_name\"'), \
                        JSON_EXTRACT(tn.properties,'$.\"type_name\"'), \
                        JSON_OBJECT( \
                                'latin_genus', JSON_EXTRACT(tn.properties,'$.\"latin_genus\"'), \
                                'latin_s1', JSON_EXTRACT(tn.properties,'$.\"latin_s1\"') ,\
                                's2_rank', JSON_EXTRACT(tn.properties,'$.\"s2_rank\"'), \
                                'latin_s2', JSON_EXTRACT(tn.properties,'$.\"latin_s2\"'), \
                                's3_rank',JSON_EXTRACT(tn.properties,'$.\"s3_rank\"'), \
                                'latin_s3',JSON_EXTRACT(tn.properties,'$.\"latin_s3\"'), \
                                's4_rank',JSON_EXTRACT(tn.properties,'$.\"s4_rank\"'), \
                                'latin_s4',JSON_EXTRACT(tn.properties,'$.\"latin_s4\"'))\
                        FROM taxon_names AS tn \
                        LEFT JOIN nomenclatures AS n ON tn.nomenclature_id = n.id \
                        LEFT JOIN ranks AS r ON tn.rank_id = r.id \
                        LEFT JOIN reference_usages AS ru ON tn.id = ru.taxon_name_id"

        # name_id, nomenclature_id, rank_id, simple_name, name_author, tn_properties, original_name_id, note
        # created_at, updated_at, nomenclature_name, rank, is_hybrid, protologue, type_name_id, latin_genus,
        # latin_s1, s2_rank, latin_s2, s3_rank, latin_s3, s4_rank, latin_s4

        conditions = []
        if updated_at:
            conditions += [f"date(tn.updated_at) > '{updated_at}'"]
        if created_at:
            conditions += [f"date(tn.created_at) > '{created_at}'"]

        if name_id:  # 不考慮其他條件
            query = f"{common_query} WHERE tn.id = {name_id}"
            print('name_id: ', query)
        elif scientific_name:  # 不考慮分類群, scientific_name, updated_at, created_at
            query = f"{common_query} WHERE tn.name = '{scientific_name}'"
            for c in conditions:
                query += " AND " + c
            print('name: ', query)
        elif taxon_group:
            # 先由 學名 / 中文名 找出符合的name_id
            query_1 = f"SELECT id FROM taxon_names WHERE name = '{taxon_group}'"
            # conn = pymysql.connect(**db_settings)
            results = ()
            with conn.cursor() as cursor:
                cursor.execute(query_1)
                results = cursor.fetchall()
            # find all child id
            print('s')
            all_child_results = ()
            for r in results:  # could be more than 1
                current_id = r[0]
                query_taxon_group = f"select  taxon_name_id \
                                from    (select * from reference_usages order by parent_taxon_name_id, taxon_name_id) reference_usages, \
                                        (select @pv := '{current_id}') initialisation \
                                where   find_in_set(parent_taxon_name_id, @pv) > 0 \
                                and     @pv := concat(@pv, ',', taxon_name_id) \
                                "
                # conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:
                    cursor.execute(query_taxon_group)
                    child_results = cursor.fetchall()
                    all_child_results += child_results

            all_results = results + all_child_results

            if all_results:
                query = f"{common_query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                for c in conditions:
                    query += " AND " + c
            else:
                # 沒有結果的狀態
                query = f"{common_query} LIMIT 0"
            # print('taxon_group: ', query)
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
            name_results = pd.DataFrame(name_results, columns=['name_id', 'nomenclature_id', 'rank_id', 'simple_name',
                                                               'name_author', 'tn_properties', 'original_name_id', 'note',
                                                               'created_at', 'updated_at', 'nomenclature_name', 'rank', 'is_hybrid', 'protologue', 'type_name_id', 'name'])

            len_total = len(name_results)
            # pagination
            paginator = Paginator(name_results, limit)
            total_page = paginator.num_pages
            if page > total_page:
                response = {"status": {"code": 400,
                                       "message": "Bad Request: Page does not exist"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

            # 只處理限制筆數
            current_df = paginator.page(page).object_list
            # find type_name
            current_df['type_name'] = None
            for t in current_df.type_name_id:
                if t:
                    query_type_name = f"SELECT name FROM taxon_names WHERE id = {t}"
                    with conn.cursor() as cursor:
                        cursor.execute(query_type_name)
                        type_name_result = cursor.fetchone()
                    if type_name_result:
                        current_df.loc[current_df.type_name_id ==
                                       t, 'type_name'] = type_name_result[0]

            # find hybrid_parent
            current_df['hybrid_parent'] = None
            for h in current_df[['is_hybrid', 'name_id']].index:
                if current_df.loc[h]['is_hybrid'] == 'true':
                    query_hybrid_parent = f"SELECT tn.name FROM taxon_name_hybrid_parent AS tnhp \
                                            LEFT JOIN taxon_names AS tn ON tn.id = tnhp.parent_taxon_name_id \
                                            WHERE tnhp.taxon_name_id = {current_df.loc[h]['name_id']} "
                    with conn.cursor() as cursor:
                        cursor.execute(query_hybrid_parent)
                        hybrid_name_result = cursor.fetchall()
                    hybrid_names = ', '.join(item[0]
                                             for item in hybrid_name_result)
                    current_df.loc[current_df.name_id == current_df.loc[h]
                                   ['name_id'], 'hybrid_parent'] = hybrid_names

            # organize results
            # only rank >= 34 has 物種學名分欄 & original_name_id
            current_df.loc[current_df.rank_id < 34, 'name'] = '{}'
            current_df.loc[current_df.rank_id < 34, 'original_name_id'] = None

            # remove double quote in rank field
            current_df['rank'] = current_df['rank'].replace(
                '\"', '', regex=True)

            # date to string
            # current_df['created_at'] = current_df['created_at'].dt.strftime(
            #     '%Y-%m-%d %H:%M:%S')
            # current_df['updated_at'] = current_df['updated_at'].dt.strftime(
            #     '%Y-%m-%d %H:%M:%S')

            # remove null/empty/None element in 'name' json
            for n in current_df.index:
                tmp = json.loads(str(current_df.name[n]))
                tmp = {k: v for k, v in tmp.items() if v}
                current_df.loc[n, 'name'] = [tmp]

            # subset & rename columns
            current_df = current_df[['name_id', 'nomenclature_name', 'rank', 'simple_name', 'name_author', 'name', 'original_name_id',
                                    'is_hybrid', 'hybrid_parent', 'protologue', 'type_name', 'created_at', 'updated_at']]

            current_df['is_hybrid'] = current_df['is_hybrid'].replace(
                'false', False).replace('true', True)

            current_df.loc[current_df['protologue']
                           == "null", 'protologue'] = None
            current_df.loc[current_df['name_author']
                           == "", 'name_author'] = None

            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len_total, "limit": limit, "current_page": page, "total_page": total_page}, "data": current_df.to_dict('records')}
    except:
        response = {"status": {"code": 500,
                               "message": "Unexpected Error"}}

    return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
    # https://www.django-rest-framework.org/api-guide/exceptions/
