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
import numpy as np

from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.schemas import AutoSchema


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}


def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False


class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


class TaxonView(APIView):
    @swagger_auto_schema(
        operation_summary='取得物種',
        # operation_description='我是 GET 的說明',
        manual_parameters=[
            # 暫時先不做
            # openapi.Parameter(
            #     name='concept_id',
            #     in_=openapi.IN_QUERY,
            #     description='分類觀',
            #     type=openapi.TYPE_INTEGER
            # ),
            openapi.Parameter(
                name='taxon_id',
                in_=openapi.IN_QUERY,
                description='物種ID',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='taxon_group',
                in_=openapi.IN_QUERY,
                description='分類群',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='created_at',
                in_=openapi.IN_QUERY,
                description='建立日期',
                type=openapi.FORMAT_DATE
            ),
            openapi.Parameter(
                name='updated_at',
                in_=openapi.IN_QUERY,
                description='更新日期',
                type=openapi.FORMAT_DATE
            ),
            openapi.Parameter(
                name='limit',
                in_=openapi.IN_QUERY,
                description='每頁限制筆數',
                type=openapi.TYPE_INTEGER
            ),
            openapi.Parameter(
                name='offset',
                in_=openapi.IN_QUERY,
                description='指定每頁起始編號',
                type=openapi.TYPE_INTEGER
            ),
            openapi.Parameter(
                name='is_hybrid',
                in_=openapi.IN_QUERY,
                description='是否為雜交',
                type=openapi.TYPE_BOOLEAN
            ),
            openapi.Parameter(
                name='is_endemic',
                in_=openapi.IN_QUERY,
                description='是否為臺灣特有種',
                type=openapi.TYPE_BOOLEAN
            ),
            openapi.Parameter(
                name='alien_type',
                in_=openapi.IN_QUERY,
                description='外來屬性',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='is_fossil',
                in_=openapi.IN_QUERY,
                description='是否為化石種',
                type=openapi.TYPE_BOOLEAN
            ),
            openapi.Parameter(
                name='is_terrestrial',
                in_=openapi.IN_QUERY,
                description='棲地是否為陸域',
                type=openapi.TYPE_BOOLEAN
            ),
            openapi.Parameter(
                name='is_freshwater',
                in_=openapi.IN_QUERY,
                description='棲地是否為淡水',
                type=openapi.TYPE_BOOLEAN
            ),
            openapi.Parameter(
                name='is_brackish',
                in_=openapi.IN_QUERY,
                description='棲地是否為半鹹水',
                type=openapi.TYPE_BOOLEAN
            ),
            openapi.Parameter(
                name='is_marine',
                in_=openapi.IN_QUERY,
                description='棲地是否為海洋',
                type=openapi.TYPE_BOOLEAN
            ),
        ]
    )
    def get(self, request, *args, **krgs):

        try:
            limit = int(request.GET.get('limit', 20))
            offset = int(request.GET.get('offset', 1))
        except:
            response = {"status": {"code": 400, "message": "Bad Request: Type error of limit or page"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

        response = {}
        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")


class NameView(APIView):

    # openapi.TYPE_STRING、openapi.TYPE_NUMBER、openapi.TYPE_INTEGER、openapi.TYPE_BOOLEAN、openapi.TYPE_ARRAY、openapi.TYPE_FILE

    @swagger_auto_schema(
        operation_summary='取得學名',
        # operation_description='我是 GET 的說明',
        manual_parameters=[
            openapi.Parameter(
                name='name_id',
                in_=openapi.IN_QUERY,
                description='學名ID',
                type=openapi.TYPE_INTEGER
            ),
            openapi.Parameter(
                name='scientific_name',
                in_=openapi.IN_QUERY,
                description='學名',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='taxon_group',
                in_=openapi.IN_QUERY,
                description='分類群',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='created_at',
                in_=openapi.IN_QUERY,
                description='建立日期',
                type=openapi.FORMAT_DATE
            ),
            openapi.Parameter(
                name='updated_at',
                in_=openapi.IN_QUERY,
                description='更新日期',
                type=openapi.FORMAT_DATE
            ),
            openapi.Parameter(
                name='limit',
                in_=openapi.IN_QUERY,
                description='每頁限制筆數',
                type=openapi.TYPE_INTEGER
            ),
            openapi.Parameter(
                name='offset',
                in_=openapi.IN_QUERY,
                description='指定每頁起始編號',
                type=openapi.TYPE_INTEGER
            ),
        ]
    )
    def get(self, request, *args, **krgs):
        # print(type(request.GET.get('limit', 20)), request.GET.get('page', 1))
        try:
            limit = int(request.GET.get('limit', 20))
            offset = int(request.GET.get('offset', 0))
        except:
            # 如果有錯的話直接改成預設值
            limit = 20
            offset = 0
            # response = {"status": {"code": 400, "message": "Bad Request: Type error of limit or page"}}
            # return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

        try:
            if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id', 'scientific_name', 'common_name', 'updated_at', 'created_at', 'taxon_group', 'limit', 'offset']):
                response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
            # elif not isinstance(request.GET.get('limit', 20), int) or not isinstance(request.GET.get('page', 1), int):

            # only consider first parameter
            name_id = request.GET.getlist('name_id', [''])[0].lstrip().rstrip()
            scientific_name = request.GET.getlist('scientific_name', [''])[0].lstrip().rstrip()
            updated_at = request.GET.getlist('updated_at', [''])[0].lstrip().rstrip()
            created_at = request.GET.getlist('created_at', [''])[0].lstrip().rstrip()
            taxon_group = request.GET.getlist('taxon_group', [''])[0].lstrip().rstrip()
            # limit = request.GET.get('limit', 20)
            # page = request.GET.get('page', 1)
            limit = 300 if limit > 300 else limit  # 最大值 300

            # print(name_id, scientific_name, updated_at, created_at, taxon_group)
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
            c_query = "SELECT COUNT(*) FROM taxon_names tn"

            # name_id, nomenclature_id, rank_id, simple_name, name_author, tn_properties, original_name_id, note
            # created_at, updated_at, nomenclature_name, rank, is_hybrid, protologue, type_name_id, latin_genus,
            # latin_s1, s2_rank, latin_s2, s3_rank, latin_s3, s4_rank, latin_s4
            conditions = []
            if updated_at:
                if not validate(updated_at):
                    response = {"status": {"code": 400, "message": "Bad Request: Incorrect DATE(updated_at) value"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                conditions += [f"date(tn.updated_at) > '{updated_at}'"]
            if created_at:
                if not validate(created_at):
                    response = {"status": {"code": 400, "message": "Bad Request: Incorrect DATE(created_at) value"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                conditions += [f"date(tn.created_at) > '{created_at}'"]

            if name_id:  # 不考慮其他條件
                query = f"{common_query} WHERE tn.id = '{name_id}'"
                count_query = f"{c_query} WHERE tn.id = '{name_id}'"
                # print('name_id: ', query)
            elif scientific_name:  # 不考慮分類群, scientific_name, updated_at, created_at
                query = f"{common_query} WHERE tn.name = '{scientific_name}'"
                count_query = f"{c_query} WHERE tn.name = '{scientific_name}'"
                for c in conditions:
                    query += " AND " + c
                    count_query += " AND " + c
                # print('name: ', query)
            elif taxon_group:
                # 先由 學名 / 中文名 找出符合的name_id
                query_1 = f"SELECT id FROM taxon_names WHERE name = '{taxon_group}'"
                # conn = pymysql.connect(**db_settings)
                results = ()
                with conn.cursor() as cursor:
                    cursor.execute(query_1)
                    results = cursor.fetchall()
                # find all child id
                all_child_results = ()
                for r in results:  # could be more than 1
                    current_id = r[0]
                    # get recursive names
                    query_taxon_group = f"with recursive cte (id, taxon_name_id, parent_taxon_name_id) as ( \
                                        select     id, taxon_name_id,parent_taxon_name_id \
                                        from       reference_usages \
                                        where      parent_taxon_name_id = {current_id} \
                                        union all \
                                        select     ru.id, ru.taxon_name_id, ru.parent_taxon_name_id \
                                        from       reference_usages ru \
                                        inner join cte \
                                                on ru.parent_taxon_name_id = cte.taxon_name_id \
                                        ) \
                                        select taxon_name_id from cte \
                                    "
                    # conn = pymysql.connect(**db_settings)
                    with conn.cursor() as cursor:
                        cursor.execute(query_taxon_group)
                        child_results = cursor.fetchall()
                        all_child_results += child_results

                all_results = results + all_child_results

                if all_results:
                    query = f"{common_query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                    count_query = f"{c_query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                    for c in conditions:
                        query += " AND " + c
                        count_query += " AND " + c
                else:
                    # 沒有結果的狀態
                    query = f"{common_query} LIMIT 0"
                    count_query = f"{c_query} LIMIT 0"
                # print('taxon_group: ', query)
            else:
                # updated_at, created_at or no condition
                if len(conditions) == 1:
                    query = f"{common_query} WHERE {conditions[0]}"
                    count_query = f"{c_query} WHERE {conditions[0]}"
                elif len(conditions) == 2:
                    query = f"{common_query} WHERE {conditions[0]} AND {conditions[1]}"
                    count_query = f"{c_query} WHERE {conditions[0]} AND {conditions[1]}"
                else:  # len == 0
                    query = common_query
                    count_query = c_query
                # print('else: ', query)
            with conn.cursor() as cursor:
                query += f' LIMIT {limit} OFFSET {offset}'  # 只處理限制筆數
                cursor.execute(query)
                current_df = cursor.fetchall()
                current_df = [list(item) for item in current_df]
                current_df = pd.DataFrame(current_df, columns=['name_id', 'nomenclature_id', 'rank_id', 'simple_name',
                                                               'name_author', 'tn_properties', 'original_name_id', 'note',
                                                               'created_at', 'updated_at', 'nomenclature_name', 'rank', 'is_hybrid', 'protologue', 'type_name_id', 'name'])

                cursor.execute(count_query)
                len_total = cursor.fetchall()[0][0]
                current_df['type_name'] = None
                for t in current_df.type_name_id:
                    if t:
                        query_type_name = f"SELECT name FROM taxon_names WHERE id = {t}"
                        with conn.cursor() as cursor:
                            cursor.execute(query_type_name)
                            type_name_result = cursor.fetchone()
                        if type_name_result:
                            current_df.loc[current_df.type_name_id == t, 'type_name'] = type_name_result[0]

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
                        current_df.loc[current_df.name_id == current_df.loc[h]['name_id'], 'hybrid_parent'] = hybrid_names

                # organize results
                # only rank >= 34 has 物種學名分欄 & original_name_id
                current_df.loc[current_df.rank_id < 34, 'name'] = '{}'
                current_df.loc[current_df.rank_id < 34, 'original_name_id'] = None

                current_df = current_df.replace({np.nan: None})

                # remove double quote in rank & protologue field
                current_df['rank'] = current_df['rank'].replace('\"', '', regex=True)
                current_df['protologue'] = current_df['protologue'].replace('\"', '', regex=True)
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

                current_df['is_hybrid'] = current_df['is_hybrid'].replace('false', False).replace('true', True)

                current_df.loc[current_df['protologue'] == "null", 'protologue'] = None
                current_df.loc[current_df['name_author'] == "", 'name_author'] = None

                response = {"status": {"code": 200, "message": "Success"},
                            "info": {"total": len_total, "limit": limit, "offset": offset}, "data": current_df.to_dict('records')}
        except:
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
        # https://www.django-rest-framework.org/api-guide/exceptions/
