from os import error
from re import L
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

from .utils import rank_map_c, rank_map


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


class ReferencesView(APIView):
    @swagger_auto_schema(
        operation_summary='取得文獻',
        # operation_description='我是 GET 的說明',
        manual_parameters=[
            openapi.Parameter(
                name='name_id',
                in_=openapi.IN_QUERY,
                description='學名ID',
                type=openapi.TYPE_STRING
            ),
        ]
    )
    def get(self, request, *args, **krgs):

        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id']):
            response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
        try:
            data = []  # 如果沒有輸入name_id, 不回傳資料
            len_total = 0
            if name_id := request.GET.get('name_id'):
                query = f"SELECT ru.reference_id, r.title, r.subtitle, ru.status, ru.properties->>'$.indications', \
                         JSON_EXTRACT(ru.properties, '$.is_taiwan'), JSON_EXTRACT(ru.properties, '$.is_endemic'), ru.properties->>'$.alien_type' \
                         FROM reference_usages ru \
                         JOIN `references` r ON ru.reference_id = r.id \
                         WHERE ru.taxon_name_id = {name_id} and r.id != 153"  # 不給TaiCOL backbone
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    df = pd.DataFrame(cursor.fetchall(), columns=['reference_id', 'title', 'subtitle', 'status', 'indications', 'is_taiwan', 'is_endemic', 'alien_type'])
                    if len(df):
                        is_list = ['is_endemic', 'is_taiwan']
                        df[is_list] = df[is_list].replace({0: False, 1: True})
                        df['citation'] = ''
                        for i in df.index:
                            row = df.iloc[i]
                            if row.indications and row.indications != '[]':
                                df.loc[i, 'indications'] = ','.join(eval(row.indications))
                            else:
                                df.loc[i, 'indications'] = None
                            subtitle_split = row.subtitle.split(', ')
                            author = ', '.join(subtitle_split[0:2]) + '.'
                            source = ', '.join(subtitle_split[2:])
                            df.loc[i, 'citation'] = f"{author} {row.title}. {source}."
                        df = df[['reference_id', 'citation', 'status', 'indications', 'is_taiwan', 'is_endemic', 'alien_type']]
                        len_total = len(df)
                        data = df.to_dict('records')

            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len_total}, "data": data}
        except:
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")


class HigherTaxaView(APIView):
    @swagger_auto_schema(
        operation_summary='取得較高階層',
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
        ]
    )
    def get(self, request, *args, **krgs):

        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['taxon_id']):
            response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

        try:
            data = []  # 如果沒有輸入taxon_id, 不回傳資料
            if taxon_id := request.GET.get('taxon_id'):
                # 取得child_taxon_id = taxon_id的所有資料，但不包含自己
                query = f"SELECT distinct(th.parent_taxon_id), t.accepted_taxon_name_id, tn.name, tn.formatted_authors, an.name_with_tag, t.rank_id, t.common_name_c \
                        FROM api_taxon_hierarchy th \
                        JOIN api_taxon t ON th.parent_taxon_id = t.taxon_id \
                        JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
                        JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id \
                        WHERE th.parent_taxon_id != '{taxon_id}' and th.child_taxon_id = '{taxon_id}' \
                        ORDER BY t.rank_id DESC"
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    for r in results:
                        data += [{'taxon_id': r[0], 'name_id': r[1], 'simple_name': r[2], 'name_author': r[3], 'formatted_name': r[4],
                                  'rank': rank_map[r[5]], 'common_name_c': r[6]}]
            response = {"status": {"code": 200, "message": "Success"},
                        "data": data}
        except:
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")


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
            try:
                limit = int(request.GET.get('limit', 20))
                offset = int(request.GET.get('offset', 0))
            except:
                # 如果有錯的話直接改成預設值
                limit = 20
                offset = 0

            if request.GET.keys() and not set(
                list(request.GET.keys())) <= set(
                ['taxon_id', 'taxon_group', 'updated_at', 'created_at', 'limit', 'offset', 'is_hybrid', 'is_endemic', 'alien_type', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
                 'is_marine']):
                response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

            # only consider first parameter
            taxon_id = request.GET.get('taxon_id', '').strip()
            taxon_group = request.GET.get('taxon_group', '').strip()
            updated_at = request.GET.get('updated_at', '').strip().replace('"', '').replace("'", "")
            created_at = request.GET.get('created_at', '').strip().replace('"', '').replace("'", "")
            limit = 300 if limit > 300 else limit  # 最大值 300

            conn = pymysql.connect(**db_settings)
            query = "SELECT t.taxon_id, t.rank_id, t.accepted_taxon_name_id, t.common_name_c, t.alternative_name_c, \
                            t.is_hybrid, t.is_endemic, t.alien_type, t.is_fossil, t.is_terrestrial, \
                            t.is_freshwater, t.is_brackish, t.is_marine, t.cites, t.iucn, t.redlist, t.protected, t.sensitive, \
                            t.created_at, t.updated_at, tn.name, tn.formatted_authors, an.name_with_tag FROM api_taxon t \
                            JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
                            JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id"
            count_query = "SELECT COUNT(*) FROM api_taxon t"

            if taxon_id:  # 不考慮其他條件
                query = f"{query} WHERE t.taxon_id = '{taxon_id}'"
                count_query = f"{count_query} WHERE t.taxon_id = '{taxon_id}'"
            else:
                conditions = []
                for i in ['is_hybrid', 'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine']:
                    var = request.GET.get(i, '').strip()
                    if var == 'true':
                        conditions += [f"t.{i} = 1"]
                    elif var == 'false':
                        conditions += [f"{i} = 0"]
                if var := request.GET.get('t.alien_type', '').strip():
                    conditions += [f"t.alien_type = '{var}'"]
                if updated_at:
                    if not validate(updated_at):
                        response = {"status": {"code": 400, "message": "Bad Request: Incorrect DATE(updated_at) value"}}
                        return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                    conditions += [f"date(t.updated_at) > '{updated_at}'"]
                if created_at:
                    if not validate(created_at):
                        response = {"status": {"code": 400, "message": "Bad Request: Incorrect DATE(created_at) value"}}
                        return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                    conditions += [f"date(t.created_at) > '{created_at}'"]
                if taxon_group:
                    # 先抓taxon_id再判斷有沒有其他condition要考慮
                    query_1 = f"SELECT th.child_taxon_id FROM taxon_names tn \
                        JOIN api_taxon_usages tu ON tn.id = tu.taxon_name_id \
                        JOIN api_taxon t ON tu.taxon_id = t.taxon_id \
                        JOIN api_taxon_hierarchy th ON tu.taxon_id = th.parent_taxon_id \
                        WHERE tn.name = '{taxon_group}' OR t.common_name_c = '{taxon_group}' OR find_in_set('{taxon_group}',t.alternative_name_c) "
                    with conn.cursor() as cursor:
                        cursor.execute(query_1)
                        results = cursor.fetchall()
                        if results:
                            results = str([i[0] for i in results]).replace('[', '(').replace(']', ')')
                            conditions += [f"t.taxon_id IN {results}"]
                        else:  # 如果沒有結果的話用回傳空值
                            response = {"status": {"code": 200, "message": "Success"},
                                        "info": {"total": 0, "limit": limit, "offset": offset}, "data": []}
                            return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")

                for l in range(len(conditions)):
                    if l == 0:
                        query = f"{query} WHERE {conditions[l]}"
                        count_query = f"{count_query} WHERE {conditions[l]}"
                    else:
                        query += f' AND {conditions[l]}'
                        count_query += f" AND {conditions[l]}"

            with conn.cursor() as cursor:
                cursor.execute(count_query)
                len_total = cursor.fetchall()[0][0]
                query += f' LIMIT {limit} OFFSET {offset}'  # 只處理限制筆數
                cursor.execute(query)
                df = pd.DataFrame(cursor.fetchall(), columns=['taxon_id', 'rank', 'name_id', 'common_name_c', 'alternative_name_c',
                                                              'is_hybrid', 'is_endemic', 'alien_type', 'is_fossil', 'is_terrestrial',
                                                              'is_freshwater', 'is_brackish', 'is_marine', 'cites', 'iucn', 'redlist', 'protected', 'sensitive',
                                                              'created_at', 'updated_at', 'simple_name', 'name_author', 'formatted_name'])
                # 0, 1 要轉成true, false (但可能會有null)
                if len(df):
                    is_list = ['is_hybrid', 'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine']
                    df[is_list] = df[is_list].replace({0: False, 1: True})
                    # 階層
                    df['rank'] = df['rank'].apply(lambda x: rank_map[x])
                    # 日期格式 yy-mm-dd
                    df['created_at'] = df.created_at.dt.strftime('%Y-%m-%d')
                    df['updated_at'] = df.updated_at.dt.strftime('%Y-%m-%d')
                    # 同物異名 & 誤用名
                    df['synonyms'] = ''
                    df['formatted_synonyms'] = ''
                    df['misapplied'] = ''
                    df['formatted_misapplied'] = ''
                    query = f"SELECT tu.taxon_id, tu.status, GROUP_CONCAT(an.name_with_tag SEPARATOR ','), GROUP_CONCAT(tn.name SEPARATOR ',') \
                    FROM api_taxon_usages tu \
                    JOIN api_names an ON tu.taxon_name_id = an.taxon_name_id \
                    JOIN taxon_names tn ON tu.taxon_name_id = tn.id \
                    WHERE tu.taxon_id IN (%s) and tu.status IN ('synonyms', 'misapplied') \
                    GROUP BY tu.status, tu.taxon_id;"
                    cursor.execute(query, ','.join(df.taxon_id.to_list()))
                    other_names = cursor.fetchall()
                    for o in other_names:
                        if o[1] == 'synonyms':
                            df.loc[df['taxon_id'] == o[0], 'synonyms'] = o[3]
                            df.loc[df['taxon_id'] == o[0], 'formatted_synonyms'] = o[2]
                        elif o[1] == 'misapplied':
                            df.loc[df['taxon_id'] == o[0], 'misapplied'] = o[3]
                            df.loc[df['taxon_id'] == o[0], 'formatted_misapplied'] = o[2]
                    # 排序
                    df = df[['taxon_id', 'name_id', 'simple_name', 'name_author', 'formatted_name', 'synonyms', 'formatted_synonyms', 'misapplied', 'formatted_misapplied',
                            'rank', 'common_name_c', 'alternative_name_c', 'is_hybrid', 'is_endemic', 'alien_type', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
                             'is_marine', 'cites', 'iucn', 'redlist', 'protected', 'sensitive', 'created_at', 'updated_at']]

                # 加上其他欄位
                response = {"status": {"code": 200, "message": "Success"},
                            "info": {"total": len_total, "limit": limit, "offset": offset}, "data": df.to_dict('records')}
        except:
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

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

            # 如果有重複的參數，只考慮最後面的那個 (default)
            name_id = request.GET.get('name_id', '').strip()
            scientific_name = request.GET.get('scientific_name', '').strip()
            updated_at = request.GET.get('updated_at', '').strip().replace('"', '').replace("'", "")
            created_at = request.GET.get('created_at', '').strip().replace('"', '').replace("'", "")
            taxon_group = request.GET.get('taxon_group', '').strip()
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
                                    'latin_s4',JSON_EXTRACT(tn.properties,'$.\"latin_s4\"')), an.name_with_tag \
                            FROM taxon_names AS tn \
                            LEFT JOIN nomenclatures AS n ON tn.nomenclature_id = n.id \
                            LEFT JOIN ranks AS r ON tn.rank_id = r.id \
                            LEFT JOIN reference_usages AS ru ON tn.id = ru.taxon_name_id \
                            LEFT JOIN api_names as an ON tn.id = an.taxon_name_id"
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
                                        select taxon_name_id from cte"
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
                                                               'created_at', 'updated_at', 'nomenclature_name', 'rank', 'is_hybrid', 'protologue', 'type_name_id', 'name', 'formatted_name'])

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
                current_df = current_df[['name_id', 'nomenclature_name', 'rank', 'simple_name', 'name_author', 'formatted_name', 'name', 'original_name_id',
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
