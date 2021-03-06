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

from api.utils import rank_map_c, rank_map, update_citations, update_names

import requests

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

match_url = env('NOMENMATCH_URL')


def web_stat_stat(request):
        conn = pymysql.connect(**db_settings)
        response = {}
        with conn.cursor() as cursor:
            # 各界物種數
            query = """SELECT category, count FROM api_web_stat WHERE title = 'kingdom_count'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['kingdom_count'] = results
            # 各階層數量
            query = """SELECT category, count FROM api_web_stat WHERE title = 'rank_count'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['rank_count'] = results
            # 各類生物種數&特有比例
            query = """SELECT category, count, total_count FROM api_web_stat WHERE title = 'endemic_count'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['endemic_count'] = results
            # 物種來源比例
            query = """SELECT category, count FROM api_web_stat WHERE title = 'source_count'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['source_count'] = results
            # 全球物種數比較
            query = """SELECT category, count, total_count FROM api_web_stat WHERE title = 'kingdom_count'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['kingdom_compare'] = results
            query = """SELECT category, count, total_count FROM api_web_stat WHERE title = 'animalia_compare'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['animalia_compare'] = results
            query = """SELECT category, count, total_count FROM api_web_stat WHERE title = 'arthropoda_compare'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['arthropoda_compare'] = results
            query = """SELECT category, count, total_count FROM api_web_stat WHERE title = 'chordata_compare'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['chordata_compare'] = results
            query = """SELECT category, count, total_count FROM api_web_stat WHERE title = 'plantae_compare'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['plantae_compare'] = results
            # 全球物種數比較總表
            query = """SELECT path, total_count, count, provider FROM api_web_table"""  
            cursor.execute(query)
            results = cursor.fetchall()
            response['compare_table'] = results

        return HttpResponse(json.dumps(response))


def web_index_stat(request):
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            query = """SELECT category, count FROM api_web_stat WHERE title = 'index'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            # [["reference", 121], ["taxon", 60204], ["name", 86725]]        
            return HttpResponse(json.dumps(results))


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


class NameMatchView(APIView):
    @swagger_auto_schema(
        operation_summary='取得學名比對',
        # operation_description='我是 GET 的說明',
        manual_parameters=[
            openapi.Parameter(
                name='name_id',
                in_=openapi.IN_QUERY,
                description='學名ID',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='name',
                in_=openapi.IN_QUERY,
                description='名字',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='best',
                in_=openapi.IN_QUERY,
                description='是否只取最佳結果',
                type=openapi.TYPE_STRING
            ),
        ]
    )
    def get(self, request, *args, **krgs):

        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id', 'name', 'best']):
            response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
        try:
            namecode_list = []
            conn = pymysql.connect(**db_settings)            
            df = pd.DataFrame(columns=['taxon_id', 'usage_status'])
            if name_id := request.GET.get('name_id'):
                namecode_list = [name_id]
            elif name := request.GET.get('name'): # 如果是查name, 接NomenMatchAPI
                best = request.GET.get('best')
                if best and not best in ['yes', 'no']:
                    response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameter value"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                elif not best:
                    best = 'yes'
                else:
                    best = request.GET.get('best')
                namecode_list = []
                url = f"{match_url}?names={name}&best={best}&format=json&source=taicol"
                result = requests.get(url)
                if result.status_code == 200:
                    result = result.json()
                    for d in result['data']:
                        for ds in d:
                            for r in ds['results']:
                                namecode_list.append(r.get('namecode'))
            if namecode_list:
                with conn.cursor() as cursor:
                    query = f"SELECT t.name, t1.name, atu.taxon_id, atu.status \
                        FROM api_taxon_usages atu \
                        JOIN api_taxon at ON atu.taxon_id = at.taxon_id  \
                        JOIN taxon_names t ON atu.taxon_name_id = t.id  \
                        JOIN taxon_names t1 ON at.accepted_taxon_name_id = t1.id  \
                        WHERE atu.taxon_name_id IN ({','.join(namecode_list)})"  
                    # print(','.join(namecode_list))
                    cursor.execute(query)
                    # cursor.execute(query)
                    df = pd.DataFrame(cursor.fetchall(), columns=['matched_name', 'accepted_name', 'taxon_id', 'usage_status'])
                    df = df.replace({np.nan: None})
                    if len(df):
                        df = df.drop_duplicates()
                        # 如果reference_id是153, 則以空值取代
                        # df.loc[df['reference_id']==153, 'reference_year'] = None
                        # df['reference_id'] = df['reference_id'].replace({153:None})
                        df['usage_status'] = df['usage_status'].replace({'accepted': 'Accepted', 'misapplied': 'Misapplied', 'synonyms': 'Not accepted'})
            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(df)}, "data": df.to_dict('records')}
            # return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")


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
            df = pd.DataFrame(columns=['reference_id', 'citation', 'status', 'indications', 'is_taiwan', 'is_endemic', 'alien_type'])
            if name_id := request.GET.get('name_id'):
                query = f"SELECT ru.reference_id, CONCAT(c.author, ' ' ,c.content), ru.status, ru.properties->>'$.indications', \
                         JSON_EXTRACT(ru.properties, '$.is_in_taiwan'), JSON_EXTRACT(ru.properties, '$.is_endemic'), ru.properties->>'$.alien_type' \
                         FROM reference_usages ru \
                         JOIN `references` r ON ru.reference_id = r.id \
                         JOIN api_citations c ON ru.reference_id = c.reference_id \
                         WHERE ru.taxon_name_id = {name_id} and r.id != 153 and ru.status != ''"  # 不給TaiCOL backbone
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    df = pd.DataFrame(cursor.fetchall(), columns=['reference_id', 'citation', 'status', 'indications', 'is_taiwan', 'is_endemic', 'alien_type'])
                    df = df.replace({np.nan: None})
                    if len(df):
                        is_list = ['is_endemic', 'is_taiwan']
                        df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True})
                        for i in df.index:
                            row = df.iloc[i]
                            if row.indications and row.indications != '[]':
                                df.loc[i, 'indications'] = ','.join(eval(row.indications))
                            else:
                                df.loc[i, 'indications'] = None
                # 加上原始文獻
                query = f"SELECT c.reference_id, CONCAT(c.author, ' ' ,c.content) \
                        FROM taxon_names tn \
                        JOIN api_citations c ON tn.reference_id = c.reference_id    \
                        WHERE tn.id = {name_id} AND tn.reference_id IS NOT NULL "
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    for r in results:
                        if r[0] not in df.reference_id.to_list():
                            df = df.append({'reference_id': r[0], 'citation': r[1], 'status': None,
                                            'indications': None, 'is_taiwan': None, 'is_endemic': None, 'alien_type': None}, ignore_index=True)
            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(df)}, "data": df.to_dict('records')}
        except Exception as er:
            print(er)
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
                # 分成兩階段 先抓回path，再去抓name
                conn = pymysql.connect(**db_settings)
                query = f"SELECT path FROM api_taxon_tree WHERE taxon_id = '{taxon_id}'"
                path = ''
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    path = cursor.fetchone()
                if path:
                    path = path[0].split('>')              
                    query = f"SELECT t.taxon_id, t.accepted_taxon_name_id, tn.name, \
                            an.name_author, an.formatted_name, t.rank_id, t.common_name_c \
                            FROM api_taxon t \
                            JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
                            JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id \
                            WHERE t.taxon_id IN ({str(path).replace('[','').replace(']','')}) \
                            ORDER BY t.rank_id DESC"
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        results = cursor.fetchall()
                        for r in results:
                            data += [{'taxon_id': r[0], 'name_id': r[1], 'simple_name': r[2], 'name_author': r[3], 'formatted_name': r[4],
                                    'rank': rank_map[r[5]], 'common_name_c': r[6]}]
            response = {"status": {"code": 200, "message": "Success"},
                        "data": data}
        except Exception as er:
            print(er)
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
            except Exception as er:
                print(er)
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
            updated_at = request.GET.get('updated_at', '').strip().strip('"').strip("'")
            created_at = request.GET.get('created_at', '').strip().strip('"').strip("'")
            limit = 300 if limit > 300 else limit  # 最大值 300

            conn = pymysql.connect(**db_settings)
            query = "SELECT t.taxon_id, t.rank_id, t.accepted_taxon_name_id, t.common_name_c, t.alternative_name_c, \
                            t.is_hybrid, t.is_endemic, t.alien_type, t.is_fossil, t.is_terrestrial, \
                            t.is_freshwater, t.is_brackish, t.is_marine, ac.cites_listing, ac.iucn_category, ac.red_category, ac.protected_category, ac.sensitive_suggest, \
                            t.created_at, t.updated_at, tn.name, an.name_author, an.formatted_name FROM api_taxon t \
                            JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id \
                            JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id \
                            LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id"
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
                if var := request.GET.get('alien_type', '').strip():
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
                    query_1 = f"SELECT t.taxon_id FROM taxon_names tn \
                                JOIN api_taxon t ON tn.id = t.accepted_taxon_name_id \
                                WHERE tn.name = '{taxon_group}' OR t.common_name_c = '{taxon_group}' OR find_in_set('{taxon_group}',t.alternative_name_c) "
                    with conn.cursor() as cursor:
                        cursor.execute(query_1)
                        t_id = cursor.fetchall()               
                        # 可能不只一筆
                        query_2 = "SELECT taxon_id FROM api_taxon_tree WHERE"
                        t_count = 0
                        for t in t_id:
                            t_count += 1
                            if t_count == 1:
                                query_2 += f" path like '%>{t[0]}%' or taxon_id = '{t[0]}'"
                            else:
                                query_2 += f" or path like '%>{t[0]}%' or taxon_id = '{t[0]}'"
                        if t_count > 0:
                            with conn.cursor() as cursor:
                                cursor.execute(query_2)
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
                    df = df.replace({np.nan: None})
                    is_list = ['is_hybrid', 'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine']
                    df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True})
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
                    query = f"SELECT tu.taxon_id, tu.status, GROUP_CONCAT(DISTINCT(an.formatted_name) SEPARATOR ','), GROUP_CONCAT(DISTINCT(tn.name) SEPARATOR ',') \
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
        except Exception as er:
            print(er)
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
        except Exception as er:
            print(er)
            limit, offset = 20, 0  # 如果有錯的話直接改成預設值
        try:
            if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id', 'scientific_name', 'common_name', 'updated_at', 'created_at', 'taxon_group', 'limit', 'offset']):
                response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
            # 如果有重複的參數，只考慮最後面的那個 (default)
            name_id = request.GET.get('name_id', '').strip()
            scientific_name = request.GET.get('scientific_name', '').strip()
            updated_at = request.GET.get('updated_at', '').strip().strip('"').strip("'")
            created_at = request.GET.get('created_at', '').strip().strip('"').strip("'")
            taxon_group = request.GET.get('taxon_group', '').strip()
            limit = 300 if limit > 300 else limit  # 最大值 300

            # update names
            update_names()
            update_citations()

            # print(name_id, scientific_name, updated_at, created_at, taxon_group)
            conn = pymysql.connect(**db_settings)
            query = "SELECT tn.id, tn.rank_id, tn.name, an.name_author, \
                            tn.original_taxon_name_id, tn.note, tn.created_at, tn.updated_at, \
                            n.name, \
                            JSON_EXTRACT(tn.properties,'$.is_hybrid_formula'), \
                            CONCAT_WS(c.author, ' ', c.content), \
                            tn.properties ->> '$.type_name', \
                            tn.properties ->> '$.latin_genus', \
                            tn.properties ->> '$.latin_s1',\
                            tn.properties ->> '$.species_layers',\
                            an.formatted_name \
                            FROM taxon_names AS tn \
                            JOIN nomenclatures n ON tn.nomenclature_id = n.id \
                            LEFT JOIN api_names an ON tn.id = an.taxon_name_id \
                            LEFT JOIN api_citations c ON tn.reference_id = c.reference_id"
            count_query = "SELECT COUNT(*) FROM taxon_names tn"

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
                query = f"{query} WHERE tn.id = '{name_id}'"
                count_query = f"{count_query} WHERE tn.id = '{name_id}'"
            elif scientific_name:  # 不考慮分類群, scientific_name, updated_at, created_at
                query = f"{query} WHERE tn.name = '{scientific_name}'"
                count_query = f"{count_query} WHERE tn.name = '{scientific_name}'"
                for c in conditions:
                    query += " AND " + c
                    count_query += " AND " + c
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
                    query = f"{query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                    count_query = f"{count_query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                    for c in conditions:
                        query += " AND " + c
                        count_query += " AND " + c
                else:
                    # 沒有結果的狀態
                    response = {"status": {"code": 200, "message": "Success"},
                                "info": {"total": 0, "limit": limit, "offset": offset}, "data": []}
                    return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
                # print('taxon_group: ', query)
            else:
                for l in range(len(conditions)):
                    if l == 0:
                        query = f"{query} WHERE {conditions[l]}"
                        count_query = f"{count_query} WHERE {conditions[l]}"
                    else:
                        query += f' AND {conditions[l]}'
                        count_query += f" AND {conditions[l]}"
                # print('else: ', query)
            with conn.cursor() as cursor:
                query += f' LIMIT {limit} OFFSET {offset}'  # 只處理限制筆數
                cursor.execute(query)
                df = cursor.fetchall()
                df = [list(item) for item in df]
                df = pd.DataFrame(df, columns=['name_id', 'rank', 'simple_name', 'name_author', 'original_name_id', 'note',
                                               'created_at', 'updated_at', 'nomenclature_name', 'is_hybrid', 'protologue',
                                               'type_name_id', 'latin_genus', 'latin_s1', 'species_layers', 'formatted_name'])
                cursor.execute(count_query)
                len_total = cursor.fetchall()[0][0]
                # only rank >= 34 has 物種學名分欄 & original_name_id
                df.loc[df['rank'] < 34, 'name'] = '{}'
                df.loc[df['rank'] < 34, 'original_name_id'] = None
                df['rank'] = df['rank'].apply(lambda x: rank_map[x])
                # df['type_name'] = None
                # for t in df.type_name_id:
                #     if t:
                #         query_type_name = f"SELECT name FROM taxon_names WHERE id = {t}"
                #         with conn.cursor() as cursor:
                #             cursor.execute(query_type_name)
                #             type_name_result = cursor.fetchone()
                #         if type_name_result:
                #             df.loc[df.type_name_id == t, 'type_name'] = type_name_result[0]
                # find hybrid_parent
                df['hybrid_parent'] = None

                for h in df[['is_hybrid', 'name_id']].index:
                    # TODO 目前is_hybrid都被設成False，這樣會抓不到，先暫時寫成下面的處理
                    # if df.loc[h]['is_hybrid'] == 'true':
                    #     query_hybrid_parent = f"SELECT GROUP_CONCAT( CONCAT(tn.name, ' ',tn.formatted_authors) SEPARATOR ' × ' ) FROM taxon_name_hybrid_parent AS tnhp \
                    #                             JOIN taxon_names AS tn ON tn.id = tnhp.parent_taxon_name_id \
                    #                             WHERE tnhp.taxon_name_id = {df.loc[h]['name_id']} \
                    #                             GROUP BY tnhp.taxon_name_id"
                    #     with conn.cursor() as cursor:
                    #         cursor.execute(query_hybrid_parent)
                    #         hybrid_name_result = cursor.fetchall()
                    #     df.loc[df.name_id == df.loc[h]['name_id'], 'hybrid_parent'] = hybrid_name_result[0]
                    query_hybrid_parent = f"SELECT GROUP_CONCAT( CONCAT_WS(tn.name, ' ',an.name_author) SEPARATOR ' × ' ) FROM taxon_name_hybrid_parent AS tnhp \
                                            JOIN taxon_names AS tn ON tn.id = tnhp.parent_taxon_name_id \
                                            LEFT JOIN api_names an ON an.taxon_name_id = tn.id \
                                            WHERE tnhp.taxon_name_id = {df.loc[h]['name_id']} \
                                            GROUP BY tnhp.taxon_name_id"
                    with conn.cursor() as cursor:
                        cursor.execute(query_hybrid_parent)
                        hybrid_name_result = cursor.fetchall()
                        if hybrid_name_result:
                            df.loc[df.name_id == df.loc[h]['name_id'], 'hybrid_parent'] = hybrid_name_result[0][0]
                            df.loc[df.name_id == df.loc[h]['name_id'], 'is_hybrid'] = True

                # organize results
                df = df.replace({np.nan: None})

                # 日期格式 yy-mm-dd
                if len(df):
                    df['created_at'] = df.created_at.dt.strftime('%Y-%m-%d')
                    df['updated_at'] = df.updated_at.dt.strftime('%Y-%m-%d')

                # remove null/empty/None element in 'name' json
                for n in df.index:
                    name = {'latin_genus': df.latin_genus[n], 'latin_s1': df.latin_s1[n]}
                    count = 2
                    for s in eval(df.species_layers[n]):
                        if s.get('rank_abbreviation') and s.get('latin_name'):
                            name.update({f's{count}_rank': s.get('rank_abbreviation'), f'latin_s{count}': s.get('latin_name')})
                            count += 1
                    name = {k: v for k, v in name.items() if v and v != 'null'}
                    df.loc[n, 'name'] = [name]
                    if df.original_name_id[n]:
                        df.loc[n, 'original_name_id'] = int(df.original_name_id[n])
                    if df.type_name_id[n]:
                        df.loc[n, 'type_name_id'] = int(df.type_name_id[n])
                    else:
                        df.loc[n, 'type_name_id'] = None

                # subset & rename columns
                df = df[['name_id', 'nomenclature_name', 'rank', 'simple_name', 'name_author', 'formatted_name', 'name', 'original_name_id',
                         'is_hybrid', 'hybrid_parent', 'protologue', 'type_name_id', 'created_at', 'updated_at']]

                df['is_hybrid'] = df['is_hybrid'].replace('false', False).replace('true', True)

                df.loc[df['name_author'] == "", 'name_author'] = None
                response = {"status": {"code": 200, "message": "Success"},
                            "info": {"total": len_total, "limit": limit, "offset": offset}, "data": df.to_dict('records')}
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
        # https://www.django-rest-framework.org/api-guide/exceptions/
