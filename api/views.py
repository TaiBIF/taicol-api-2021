
# from django.shortcuts import render
from django.http import (
    # JsonResponse,
    # HttpResponseRedirect,
    # Http404,
    HttpResponse,
)
# from django.core.paginator import Paginator
import json
import pymysql
from conf.settings import env, SOLR_PREFIX
import pandas as pd
import datetime
from json import JSONEncoder
import numpy as np

from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from api.utils import *
import requests

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

match_url = env('NOMENMATCH_URL')



# TODO 這邊可以改成 同一張表 query 一次就好嗎
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
            # 全球物種數修改時間
            query = """SELECT DATE_FORMAT(updated_at, '%Y-%m-%d') FROM api_web_table WHERE path = '全球物種數更新時間'"""  
            cursor.execute(query)
            results = cursor.fetchone()
            if len(results):
                results = results[0]
            response['global_updated'] = results

        return HttpResponse(json.dumps(response))


def web_index_stat(request):
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            query = """SELECT category, count FROM api_web_stat WHERE title = 'index'"""  
            cursor.execute(query)
            results = cursor.fetchall()
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


class TaxonVersionView(APIView):
    @swagger_auto_schema(
        operation_summary='物種有效名的版本紀錄',
        # operation_description='我是 GET 的說明',
        manual_parameters=[
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
            data = []
            if taxon_id := request.GET.get('taxon_id'):
                conn = pymysql.connect(**db_settings)
                # 抓過去的
                # 如果是backbone不給type
                with conn.cursor() as cursor:     
                    query = f'''SELECT ath.note, DATE_FORMAT(ath.updated_at, "%%Y-%%m-%%d"), ru.reference_id, r.type FROM api_taxon_history ath
                                LEFT JOIN reference_usages ru ON ath.reference_id = ru.reference_id and ath.accepted_taxon_name_id = ru.accepted_taxon_name_id and ath.taxon_name_id = ru.taxon_name_id
                                LEFT JOIN `references` r ON ru.reference_id = r.id
                                WHERE ath.taxon_id = %s AND ath.`type` = 5 ORDER BY ath.updated_at ASC;'''
                    cursor.execute(query, (taxon_id,))
                    first = cursor.fetchone()
                    if first:
                        if first[3] != 4: # 如果不是backbone
                            data.append({'taxon_id': taxon_id, 'name_id': int(json.loads(first[0]).get('taxon_name_id')), 'reference_id': first[2], 'updated_at': first[1]})
                        else:
                            data.append({'taxon_id': taxon_id, 'name_id': int(json.loads(first[0]).get('taxon_name_id')), 'reference_id': None, 'updated_at': first[1]})
                with conn.cursor() as cursor:     
                    query = f'''SELECT ath.note, DATE_FORMAT(ath.updated_at, "%%Y-%%m-%%d"), ru.reference_id, r.type FROM api_taxon_history ath
                                LEFT JOIN reference_usages ru ON ath.reference_id = ru.reference_id and ath.accepted_taxon_name_id = ru.accepted_taxon_name_id and ath.taxon_name_id = ru.taxon_name_id
                                LEFT JOIN `references` r ON ru.reference_id = r.id
                                WHERE ath.taxon_id = %s AND ath.`type` = 0 ORDER BY ath.updated_at ASC;'''
                    cursor.execute(query, (taxon_id,))
                    nids = cursor.fetchall()
                if nids:
                    for n in nids:
                        if n[3] != 4:
                            data.append({'taxon_id': taxon_id, 'name_id': int(json.loads(n[0]).get('new_taxon_name_id')), 'reference_id': n[2], 'updated_at': n[1]})
                        else:
                            data.append({'taxon_id': taxon_id, 'name_id': int(json.loads(n[0]).get('new_taxon_name_id')), 'reference_id': None, 'updated_at': n[1]})

            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(data)}, "data": data}
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")


class NamecodeView(APIView):
    @swagger_auto_schema(
        operation_summary='取得新舊TaiCOL namecode轉換',
        # operation_description='我是 GET 的說明',
        manual_parameters=[
            openapi.Parameter(
                name='name_id',
                in_=openapi.IN_QUERY,
                description='新版TaiCOL學名ID',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='namecode',
                in_=openapi.IN_QUERY,
                description='舊版TaiCOL namecode',
                type=openapi.TYPE_STRING
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

        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id', 'namecode', 'limit', 'offset']):
            response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
        try:
            limit = int(request.GET.get('limit', 20))
            offset = int(request.GET.get('offset', 0))
        except Exception as er:
            print(er)
            # 如果有錯的話直接改成預設值
            limit = 20
            offset = 0
        try:
            if name_id := request.GET.getlist('name_id'):
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:     
                    query = """
                    WITH cte
                        AS
                        (
                            SELECT distinct anc.namecode, anc.taxon_name_id, atu.taxon_id, atu.status, at.is_deleted, at.is_in_taiwan
                            FROM api_namecode anc
                            LEFT JOIN api_taxon_usages atu ON atu.taxon_name_id = anc.taxon_name_id
                            LEFT JOIN api_taxon at ON at.taxon_id = atu.taxon_id
                            WHERE anc.taxon_name_id IN %s 
                            LIMIT %s OFFSET %s
                        )
                    SELECT namecode, taxon_name_id, 
                    JSON_ARRAYAGG(JSON_OBJECT('taxon_id', taxon_id, 'status', status, 'is_deleted', is_deleted, 'is_in_taiwan', is_in_taiwan))
                    FROM cte GROUP BY namecode, taxon_name_id;
                    """
                    cursor.execute(query, (name_id,limit,offset))
                    df = pd.DataFrame(cursor.fetchall(), columns=['namecode', 'name_id','taxon'])
                    for i in df.index:
                        row = df.iloc[i]
                        taxon_tmp = json.loads(row.taxon)
                        taxon_tmp = pd.DataFrame(taxon_tmp)
                        # 排序規則： 
                        # Taiwan+有效 accepted
                        # Taiwan+無效 not-accepted
                        # Taiwan+誤用 misapplied
                        custom_dict = {'accepted': 0, 'not-accepted': 1, 'misapplied': 2}
                        taxon_tmp = taxon_tmp.sort_values(by=['status'], key=lambda x: x.map(custom_dict)).sort_values(by='is_in_taiwan',ascending=False)
                        taxon_tmp['is_in_taiwan'] = taxon_tmp['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True})
                        taxon_tmp = taxon_tmp.rename(columns={'status': 'usage_status'})
                        taxon_tmp = taxon_tmp[['taxon_id','usage_status','is_in_taiwan']]
                        df.loc[i,'taxon'] = taxon_tmp.to_json(orient='records')
                    if len(df):
                        df['taxon'] = df['taxon'].replace({np.nan:'[]'})
                        df['taxon'] = df['taxon'].apply(json.loads)


            elif namecode := request.GET.getlist('namecode'):
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:     
                    query = """
                    WITH cte
                        AS
                        (
                            SELECT distinct anc.namecode, anc.taxon_name_id, atu.taxon_id, atu.status, at.is_deleted, at.is_in_taiwan
                            FROM api_namecode anc
                            LEFT JOIN api_taxon_usages atu ON atu.taxon_name_id = anc.taxon_name_id
                            LEFT JOIN api_taxon at ON at.taxon_id = atu.taxon_id
                            WHERE anc.namecode IN %s 
                            LIMIT %s OFFSET %s
                        )
                    SELECT namecode, taxon_name_id, 
                    JSON_ARRAYAGG(JSON_OBJECT('taxon_id', taxon_id, 'status', status, 'is_deleted', is_deleted, 'is_in_taiwan', is_in_taiwan))
                    FROM cte GROUP BY namecode, taxon_name_id;
                    """
                    cursor.execute(query, (namecode,limit,offset))
                    df = pd.DataFrame(cursor.fetchall(), columns=['namecode', 'name_id', 'taxon'])
                    for i in df.index:
                        row = df.iloc[i]
                        taxon_tmp = json.loads(row.taxon)
                        taxon_tmp = pd.DataFrame(taxon_tmp)
                        # 排序規則： 
                        # Taiwan+有效 accepted
                        # Taiwan+無效 not-accepted
                        # Taiwan+誤用 misapplied
                        custom_dict = {'accepted': 0, 'not-accepted': 1, 'misapplied': 2}
                        taxon_tmp = taxon_tmp.sort_values(by=['status'], key=lambda x: x.map(custom_dict)).sort_values(by='is_in_taiwan',ascending=False)
                        taxon_tmp['is_in_taiwan'] = taxon_tmp['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True})
                        taxon_tmp = taxon_tmp.rename(columns={'status': 'usage_status'})
                        taxon_tmp = taxon_tmp[['taxon_id','usage_status','is_in_taiwan']]
                        df.loc[i,'taxon'] = taxon_tmp.to_json(orient='records')
                    if len(df):
                        df['taxon'] = df['taxon'].replace({np.nan:'[]'})
                        df['taxon'] = df['taxon'].apply(json.loads)
                    # for i in df.index:
                    #     row = df.iloc[i]
                    #     taxon_tmp = json.loads(row.taxon)
                    #     taxon_final = []
                    #     for t in taxon_tmp:
                    #         if t.get('is_deleted'):
                    #             taxon_final.append({'taxon_id': t.get('taxon_id'), 'usage_status': 'deleted'})
                    #         elif t.get('taxon_id'):
                    #             taxon_final.append({'taxon_id': t.get('taxon_id'), 'usage_status': t.get('status')})
                    #     df.loc[i,'taxon'] = json.dumps(taxon_final)
                    # if len(df):
                    #     df['taxon'] = df['taxon'].replace({np.nan:'[]'})
                    #     df['taxon'] = df['taxon'].apply(json.loads)
                
            else:
                df = pd.DataFrame()

            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(df)}, "data": df.to_dict('records')}
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")


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
            name_id_list = []
            conn = pymysql.connect(**db_settings)            
            df = pd.DataFrame(columns=['taxon_id', 'usage_status'])
            if name_id := request.GET.get('name_id'):
                name_id_list.append(int(name_id))
            elif name := request.GET.get('name'): # 如果是查name, 接NomenMatchAPI
                best = request.GET.get('best')
                if best and not best in ['yes', 'no']:
                    response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameter value"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                elif not best:
                    best = 'yes'
                else:
                    best = request.GET.get('best')
                url = f"{match_url}?names={name}&best={best}&format=json&source=taicol"
                result = requests.get(url)
                if result.status_code == 200:
                    result = result.json()
                    # 取得name_id
                    name_list = []
                    for d in result['data']:
                        for ds in d:
                            for r in ds['results']:
                                name_list.append(r.get('simple_name'))
                    if name_list:
                        with conn.cursor() as cursor:
                            query = "SELECT id FROM taxon_names where name IN %s;"
                            cursor.execute(query, (name_list,))
                            name_ = cursor.fetchall()
                            name_id_list = [n[0] for n in name_]
            if name_id_list:
                with conn.cursor() as cursor:
                    query = f"SELECT distinct t.name, t.id, t1.name, t1.id, atu.taxon_id, atu.status, at.is_deleted \
                        FROM api_taxon_usages atu \
                        JOIN api_taxon at ON atu.taxon_id = at.taxon_id  \
                        JOIN taxon_names t ON atu.taxon_name_id = t.id  \
                        JOIN taxon_names t1 ON at.accepted_taxon_name_id = t1.id  \
                        WHERE atu.taxon_name_id IN %s AND t.deleted_at IS NULL AND t1.deleted_at IS NULL"  
                    cursor.execute(query, (name_id_list,))
                    df = pd.DataFrame(cursor.fetchall(), columns=['matched_name', 'matched_name_id', 'accepted_name', 'accepted_name_id', 'taxon_id', 'usage_status', 'is_deleted'])
                    df = df.replace({np.nan: None, '': None})
                    if len(df):
                        df = df.drop_duplicates()
                        df.loc[df.is_deleted==1, 'usage_status'] = 'deleted'
                        df['usage_status'] = df['usage_status']
                        df = df.drop(columns=['is_deleted'])
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
            # NOTE 這邊的alien_type是reference_usage的 不是taxon的
            df = pd.DataFrame(columns=['reference_id', 'citation', 'status', 'indications', 'is_in_taiwan', 'is_endemic', 'alien_type','is_deleted'])
            if name_id := request.GET.get('name_id'):
                query = f"SELECT ru.reference_id, CONCAT_WS(' ' ,c.author, c.content), ru.status, ru.properties->>'$.indications', \
                         JSON_EXTRACT(ru.properties, '$.is_in_taiwan'), JSON_EXTRACT(ru.properties, '$.is_endemic'), ru.properties->>'$.alien_type', r.deleted_at \
                         FROM reference_usages ru \
                         JOIN `references` r ON ru.reference_id = r.id \
                         JOIN api_citations c ON ru.reference_id = c.reference_id \
                         WHERE ru.taxon_name_id = %s AND r.type != 4 AND ru.status != '' \
                         AND ru.is_title != 1 AND ru.deleted_at IS NULL"  # 不給backbone
                conn = pymysql.connect(**db_settings)
                with conn.cursor() as cursor:
                    cursor.execute(query, (name_id,))
                    df = pd.DataFrame(cursor.fetchall(), columns=['reference_id', 'citation', 'usage_status', 'indications', 'is_in_taiwan', 'is_endemic', 'alien_type', 'is_deleted'])
                    df = df.replace({np.nan: None})
                    if len(df):
                        df['is_deleted'] = df['is_deleted'].apply(lambda x: True if x else False)
                        is_list = ['is_endemic', 'is_in_taiwan']
                        df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True})
                        for i in df.index:
                            row = df.iloc[i]
                            if row.indications and row.indications != '[]':
                                df.loc[i, 'indications'] = ','.join(eval(row.indications))
                            else:
                                df.loc[i, 'indications'] = None
                # 加上原始文獻
                query = f"SELECT c.reference_id, CONCAT_WS(' ' , c.author, c.content) \
                        FROM taxon_names tn \
                        JOIN api_citations c ON tn.reference_id = c.reference_id    \
                        WHERE tn.id = %s AND tn.reference_id IS NOT NULL AND tn.deleted_at IS NULL"
                with conn.cursor() as cursor:
                    cursor.execute(query, (name_id, ))
                    results = cursor.fetchall()
                    for r in results:
                        if r[0] not in df.reference_id.to_list():
                            df = df.append({'reference_id': r[0], 'citation': r[1], 'status': None,
                                            'indications': None, 'is_in_taiwan': None, 'is_endemic': None, 'alien_type': None}, ignore_index=True)
                df = df.replace({np.nan: None, '': None})
                df['reference_id'] = df['reference_id'].replace({np.nan: 0}).astype('int64').replace({0: None})
                df['usage_status'] = df['usage_status']

            else:
                df = pd.DataFrame()
        
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

                taxon_resp = requests.get(f'{SOLR_PREFIX}taxa/select?fq=taxon_name_id:*&fq=status:accepted&q=taxon_id:{taxon_id}&fl=path,taxon_rank_id')
                if taxon_resp.status_code == 200:
                    if taxon_resp.json()['response']['numFound']:
                        info = taxon_resp.json()['response']['docs'][0]
                        if path := info.get('path'):
                            path = path.split('>')
                            path_str = ' OR '.join(path)

                            # NOTE 這邊可能會需要query已經刪除的taxon
                            path_resp = requests.get(f'{SOLR_PREFIX}taxa/select?fq=taxon_name_id:*&fq=status:accepted&q=taxon_id:({path_str})&fl=taxon_id,accepted_taxon_name_id,simple_name,name_author,formatted_accepted_name,taxon_rank_id,common_name_c&rows=1000')
                            if path_resp.status_code == 200:
                                higher = pd.DataFrame(path_resp.json()['response']['docs'])
                                musthave_cols = ['taxon_id','accepted_taxon_name_id','simple_name','name_author','formatted_accepted_name','taxon_rank_id','common_name_c']
                                for m in musthave_cols:
                                    if m not in higher.keys():
                                        higher[m] = None
                                higher = higher.rename(columns={'accepted_taxon_name_id': 'name_id', 'formatted_accepted_name': 'formatted_name',
                                                                'taxon_rank_id': 'rank_id'})
                                higher['rank_id'] = higher['rank_id'].apply(int)
                                higher['rank_order'] = higher['rank_id'].apply(lambda x: rank_order_map[x])
                                
                                # rank_order
                                # 補上階層未定 
                                # 先找出應該要有哪些林奈階層
                                current_rank_orders = higher.rank_order.to_list()
                                for x in lin_map.keys():
                                    now_order = lin_map_w_order[x]['rank_order']
                                    if now_order not in current_rank_orders and now_order < max(current_rank_orders) and now_order > min(current_rank_orders):
                                        higher = pd.concat([higher, pd.Series({'rank_id': x, 'common_name_c': '地位未定', 'taxon_id': None, 'rank_order': lin_map_w_order[x]['rank_order']}).to_frame().T], ignore_index=True)

                                # 從最大的rank開始補
                                higher = higher.sort_values('rank_order', ignore_index=True, ascending=False)
                                higher = higher.replace({np.nan: None})
                                for hi in higher[higher.taxon_id.isnull()].index:
                                    # 病毒域可能會找不到東西補 
                                    found_hi = hi + 1
                                    if found_hi < len(higher):
                                        while not higher.loc[found_hi].taxon_id:
                                            found_hi += 1
                                    higher.loc[hi, 'simple_name'] = f'{higher.loc[found_hi].simple_name} {lin_map[higher.loc[hi]["rank_id"]]} incertae sedis'
                                    higher.loc[hi, 'common_name_c'] = '地位未定'
                                higher = higher.replace({np.nan: None, '': None})
                                higher['rank'] = higher['rank_id'].apply(lambda x: rank_map[x])
                                higher = higher.replace({np.nan: None, '': None})
                                higher['name_id'] = higher['name_id'].replace({np.nan: 0}).astype('int64').replace({0: None})
                                data = higher[['taxon_id','name_id','simple_name','name_author','formatted_name','rank','common_name_c']].to_dict(orient='records')

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
                name='rank',
                in_=openapi.IN_QUERY,
                description='階層',
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
                name='is_in_taiwan',
                in_=openapi.IN_QUERY,
                description='是否存在於臺灣',
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
            openapi.Parameter(
                name='protected',
                in_=openapi.IN_QUERY,
                description='保育類',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='sensitive',
                in_=openapi.IN_QUERY,
                description='敏感物種',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='redlist',
                in_=openapi.IN_QUERY,
                description='臺灣紅皮書評估',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='iucn',
                in_=openapi.IN_QUERY,
                description='IUCN評估',
                type=openapi.TYPE_STRING
            ),
            openapi.Parameter(
                name='cites',
                in_=openapi.IN_QUERY,
                description='CITES附錄',
                type=openapi.TYPE_STRING
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
                ['taxon_id', 'scientific_name', 'common_name', 'taxon_group', 'updated_at', 'created_at', 'limit', 'offset', 'is_hybrid', 'is_endemic', 
                'is_in_taiwan', 'alien_type', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine',
                'protected','redlist','iucn', 'cites', 'rank', 'sensitive','including_not_official']):
                response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

            # only consider first parameter
            # 輸入taxon_id
            taxon_id = request.GET.get('taxon_id', '').strip()
            taxon_group = request.GET.get('taxon_group', '').strip()
            updated_at = request.GET.get('updated_at', '').strip().strip('"').strip("'")
            created_at = request.GET.get('created_at', '').strip().strip('"').strip("'")
            including_not_official = request.GET.get('including_not_official', 'true')
            limit = 300 if limit > 300 else limit  # 最大值 300

            conn = pymysql.connect(**db_settings)

            # query = """
            #         WITH base_query AS (SELECT distinct t.taxon_id FROM api_taxon t 
            #         JOIN api_taxon_usages atu ON t.taxon_id = atu.taxon_id
            #         JOIN taxon_names tnn ON atu.taxon_name_id = tnn.id
            #         LEFT JOIN api_common_name acn ON t.taxon_id = acn.taxon_id
            #         LEFT JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id 
            #         LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id 
            #         LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id """
            
            # api_common_name -> 如果有查中文名
            # api_taxon_tree -> 如果有查taxon_group
            # api_names -> 查詢時不需要
            # taxon_names -> 如果有查name 但先不用join兩次
            # api_taxon_usages -> 如果有查name
            # api_conservation -> 如果有查 api_conservation

            # count_query = """SELECT COUNT(distinct(t.taxon_id)) FROM api_taxon t
            #                 JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id 
            #                 JOIN api_taxon_usages atu ON t.taxon_id = atu.taxon_id
            #                 JOIN taxon_names tnn ON atu.taxon_name_id = tnn.id
            #                 LEFT JOIN api_common_name acn ON t.taxon_id = acn.taxon_id
            #                 LEFT JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id 
            #                 LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id
            #                 LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id 
            #             """


            if taxon_id:  # 不考慮其他條件
                base_query = f"WITH base_query AS (SELECT * FROM api_taxon t WHERE t.taxon_id = '{taxon_id}')"
                count_query = f"SELECT count(*) FROM api_taxon t WHERE t.taxon_id = '{taxon_id}'"
            else:
                conditions = [] # 在query中 和 info_query是分開的

                if including_not_official == 'false': # false: 排除未經正式紀錄
                    conditions += ["t.not_official != 1"]


                # base_query = f"WITH base_query AS (SELET t.taxon_id FROM api_taxon t order by id limit {limit} offset {offset})"
                # join_usage_and_name = False
                # join_common_name = False
                name_taxon_id = []
                common_name_taxon_id = []
                join_conserv = False
                join_taxon_tree = False

                # 學名 scientific_name 可能是接受/非接受/誤用
                if sci_name := request.GET.get('scientific_name', ''):
                    # 先query一次
                    name_query = """
                                SELECT distinct (taxon_id) FROM api_taxon_usages where is_deleted = 0 and taxon_name_id IN ( 
                                    SELECT id
                                    FROM taxon_names 
                                    WHERE deleted_at is null AND `name` = %s)
                                """
                    with conn.cursor() as cursor:
                        cursor.execute(name_query, (sci_name, ))
                        name_taxon_id = cursor.fetchall()
                        name_taxon_id = [n[0] for n in name_taxon_id]

                # 俗名 common_name
                if common_name := request.GET.get('common_name', ''):
                    common_name = get_variants(common_name)
                    common_name_query = """
                            SELECT distinct taxon_id
                            FROM api_common_name  
                            WHERE name_c REGEXP %s
                        """
                    
                    with conn.cursor() as cursor:
                        cursor.execute(common_name_query, (common_name, ))
                        common_name_taxon_id = cursor.fetchall()
                        common_name_taxon_id = [n[0] for n in common_name_taxon_id]

                if name_taxon_id and common_name_taxon_id:
                    # 要找兩個的交集
                    preselect_taxon_id = list(set(name_taxon_id).intersection(common_name_taxon_id))
                else:
                    preselect_taxon_id = name_taxon_id + common_name_taxon_id

                if preselect_taxon_id:
                    conditions += [f"t.taxon_id IN {str(preselect_taxon_id).replace('[','(').replace(']',')')}"]


                # 直接查taxon的表 不需要join
                for i in ['is_hybrid', 'is_endemic', 'is_in_taiwan', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine']:
                    var = request.GET.get(i, '').strip()
                    if var == 'true' or var == '1':
                        conditions += [f"t.{i} = 1"]
                    elif var == 'false' or var == '0':
                        conditions += [f"t.{i} = 0"]
                    
                if var := request.GET.get('alien_type', '').strip():
                    conditions += [f't.main_alien_type = "{var}"']

                # if var := request.GET.get('alien_type', '').strip():
                #     conditions += ['''JSON_CONTAINS(t.alien_type, '{"alien_type":"''' + var + '''"}')  > 0''']

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

                if rank := request.GET.get('rank'):
                    try:
                        rank_id = list(rank_map.keys())[list(rank_map.values()).index(rank)]
                        conditions += [f't.rank_id = {rank_id}']
                    except:
                        response = {"status": {"code": 400, "message": "Bad Request: Incorrect rank"}}
                        return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

                # 保育資訊
                if cs := request.GET.getlist('redlist'):
                    cs_list = []
                    for css in cs:
                        if css == 'null':
                            cs_list.append(f'ac.red_category IS NULL')
                        else:
                            # if redlist_map.get(css):
                            cs_list.append(f'ac.red_category = "{redlist_map.get(css)}"')
                    if cs_list:
                        conditions.append(f"({' OR '.join(cs_list)})")
                        join_conserv = True

                if cs := request.GET.getlist('protected'):
                    cs_list = []
                    for css in cs:
                        if css == 'null':
                            cs_list.append(f'ac.protected_category IS NULL')
                        else:
                            cs_list.append(f'ac.protected_category = "{css}"')
                    if cs_list:
                        conditions.append(f"({' OR '.join(cs_list)})")
                        join_conserv = True

                if cs := request.GET.getlist('iucn'):
                    cs_list = []
                    for css in cs:
                        if css == 'null':
                            cs_list.append(f'ac.iucn_category IS NULL')
                        else:
                            cs_list.append(f'ac.iucn_category = "{css}"')
                    if cs_list:
                        conditions.append(f"({' OR '.join(cs_list)})")
                        join_conserv = True

                if cs := request.GET.getlist('sensitive'):
                    cs_list = []
                    for css in cs:
                        if css == 'null':
                            cs_list.append(f'ac.sensitive_suggest IS NULL')
                        else:
                            cs_list.append(f'ac.sensitive_suggest = "{css}"')
                    if cs_list:
                        conditions.append(f"({' OR '.join(cs_list)})")
                        join_conserv = True

                # CITES類別要用like
                if cs := request.GET.getlist('cites'):
                    cs_list = []
                    for css in cs:
                        if css == 'null':
                            cs_list.append(f'ac.cites_listing IS NULL')
                        else:
                            # if cites_map.get(css):
                            cs_list.append(f'ac.cites_listing like "%{cites_map.get(css)}%"')
                    if cs_list:
                        conditions.append(f"({' OR '.join(cs_list)})")
                        join_conserv = True

                if taxon_group:
                    # 先抓taxon_id再判斷有沒有其他condition要考慮
                    query_1 = f"""SELECT t.taxon_id FROM taxon_names tn 
                                JOIN api_taxon t ON tn.id = t.accepted_taxon_name_id 
                                LEFT JOIN api_common_name acn ON acn.taxon_id = t.taxon_id  
                                WHERE tn.name = %s OR acn.name_c REGEXP %s"""
                    with conn.cursor() as cursor:
                        cursor.execute(query_1, (taxon_group, get_variants(taxon_group)))
                        t_id = cursor.fetchall()           
                        if len(t_id):
                            # 可能不只一筆
                            t_str = [ f"att.path like '%>{t[0]}%'" for t in t_id]
                            conditions.append(f"({' OR '.join(t_str)})")
                            join_taxon_tree = True
                        else:  # 如果沒有結果的話用回傳空值
                            response = {"status": {"code": 200, "message": "Success"},
                                        "info": {"total": 0, "limit": limit, "offset": offset}, "data": []}
                            return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")

                if len(conditions):
                    for l in range(len(conditions)):
                        if l == 0:
                            cond_str = f"WHERE {conditions[l]}"
                            # query = f"{query} WHERE {conditions[l]}"
                            # count_query = f"{count_query} WHERE {conditions[l]}"
                        else:
                            cond_str += f' AND {conditions[l]}'
                            # query += f' AND {conditions[l]}'
                            # count_query += f" AND {conditions[l]}"

                    base_query = f'''WITH base_query AS (
                                    SELECT t.* FROM api_taxon t
                                    {'LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id' if join_taxon_tree else ''}
                                    {'LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id' if join_conserv else ''}
                                    {cond_str}
                                    ORDER BY t.id  LIMIT {limit} OFFSET {offset} )'''
                    count_query = f'''
                                    SELECT count(*) FROM api_taxon t 
                                    {'LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id' if join_taxon_tree else ''}
                                    {'LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id' if join_conserv else ''}
                                    {cond_str}
                                    '''
                    

                else:
                    # 如果沒有任何condition 直接 limit offset
                    base_query = f"WITH base_query AS (SELECT t.* FROM api_taxon t ORDER BY t.id LIMIT {limit} OFFSET {offset}) "
                    count_query = f"SELECT count(*) FROM api_taxon"

            # 最後整理回傳資料使用

            info_query = """
                    SELECT t.taxon_id, t.rank_id, t.accepted_taxon_name_id, acn.name_c, 
                        t.is_hybrid, t.is_endemic, t.is_in_taiwan, t.main_alien_type, t.alien_note, t.is_fossil, t.is_terrestrial, 
                        t.is_freshwater, t.is_brackish, t.is_marine, ac.cites_listing, ac.iucn_category, ac.red_category, 
                        ac.protected_category, ac.sensitive_suggest, 
                        t.created_at, t.updated_at, tn.name, an.name_author, an.formatted_name, t.is_deleted, t.new_taxon_id, t.not_official, att.parent_taxon_id
                    FROM base_query t 
                        JOIN taxon_names tn ON t.accepted_taxon_name_id = tn.id 
                        LEFT JOIN api_taxon_tree att ON t.taxon_id = att.taxon_id 
                        LEFT JOIN api_common_name acn ON t.taxon_id = acn.taxon_id and acn.is_primary = 1
                        LEFT JOIN api_names an ON t.accepted_taxon_name_id = an.taxon_name_id 
                        LEFT JOIN api_conservation ac ON t.taxon_id = ac.taxon_id 
                    """
            with conn.cursor() as cursor:
                cursor.execute(count_query)
                len_total = cursor.fetchall()[0][0]
                query = base_query + info_query

                cursor.execute(query)
                df = pd.DataFrame(cursor.fetchall(), columns=['taxon_id', 'rank', 'name_id', 'common_name_c', 
                                                              'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial',
                                                              'is_freshwater', 'is_brackish', 'is_marine', 'cites', 'iucn', 'redlist', 'protected', 'sensitive',
                                                              'created_at', 'updated_at', 'simple_name', 'name_author', 'formatted_name', 'is_deleted', 'new_taxon_id', 'not_official','parent_taxon_id'])
                # 0, 1 要轉成true, false (但可能會有null)
                if len(df):
                    # 在這步取得alternative_common_name
                    name_c_query = "select name_c, taxon_id from api_common_name where taxon_id IN %s and is_primary = 0"
                    cursor.execute(name_c_query, (df.taxon_id.to_list(),))
                    name_c = cursor.fetchall()
                    if len(name_c):
                        name_c = pd.DataFrame(name_c, columns=['alternative_name_c', 'taxon_id'])
                        name_c = name_c.groupby(['taxon_id'], as_index = False).agg({'alternative_name_c': ','.join})
                        df = df.merge(name_c, how='left')
                    else:
                        df['alternative_name_c'] = None
                    df = df.replace({np.nan: None})
                    is_list = ['is_in_taiwan','is_hybrid', 'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine', 'not_official']
                    df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True})
                    # 階層
                    df['rank'] = df['rank'].apply(lambda x: rank_map[x])
                    # 日期格式 yy-mm-dd
                    # df['created_at'] = df.created_at.dt.strftime('%Y-%m-%d')
                    # df['updated_at'] = df.updated_at.dt.strftime('%Y-%m-%d')
                    df['created_at'] = df.created_at.apply(lambda x: str(x).split(' ')[0])
                    df['updated_at'] = df.updated_at.apply(lambda x: str(x).split(' ')[0])

                    # 同物異名 & 誤用名
                    df['synonyms'] = ''
                    df['formatted_synonyms'] = ''
                    df['misapplied'] = ''
                    df['formatted_misapplied'] = ''

                    query = f"SELECT DISTINCT tu.taxon_id, tu.status, an.formatted_name, tn.name \
                                FROM api_taxon_usages tu \
                                JOIN api_names an ON tu.taxon_name_id = an.taxon_name_id \
                                JOIN taxon_names tn ON tu.taxon_name_id = tn.id \
                                WHERE tu.taxon_id IN %s and tu.status IN ('not-accepted', 'misapplied') AND tu.is_deleted != 1;"
                    cursor.execute(query, (df.taxon_id.to_list(),))
                    other_names = cursor.fetchall()
                    other_names = pd.DataFrame(other_names, columns=['taxon_id','status','formatted_name','name'])
                    other_names = other_names.groupby(['taxon_id', 'status'], as_index = False).agg({'formatted_name': ','.join, 'name': ','.join})
                    other_names = other_names.to_dict('records')
                    for o in other_names:
                        if o.get('status') == 'not-accepted':
                            df.loc[df['taxon_id'] == o.get('taxon_id'), 'synonyms'] = o.get('name')
                            df.loc[df['taxon_id'] == o.get('taxon_id'), 'formatted_synonyms'] = o.get('formatted_name')
                        elif o.get('status') == 'misapplied':
                            df.loc[df['taxon_id'] == o.get('taxon_id'), 'misapplied'] = o.get('name')
                            df.loc[df['taxon_id'] == o.get('taxon_id'), 'formatted_misapplied'] = o.get('formatted_name')

                    query = "SELECT r.id, c.short_author, r.type \
                                FROM `references` r  \
                                LEFT JOIN api_citations c ON r.id = c.reference_id \
                                JOIN api_taxon_usages atu ON r.id = atu.reference_id  \
                                WHERE atu.taxon_id IN %s"  
                    conn = pymysql.connect(**db_settings)
                    with conn.cursor() as cursor:
                        cursor.execute(query, (df.taxon_id.to_list(), ))
                        refs = pd.DataFrame(cursor.fetchall(), columns=['reference_id', 'ref', 'type'])


                    for i in df.index:
                        row = df.iloc[i]
                        final_aliens = []
                        if row.alien_status_note:
                            # alien_rows = json.loads(row.alien_status_note)
                            alien_rows = pd.DataFrame(json.loads(row.alien_status_note))
                            if len(alien_rows):
                                # ref_list = alien_rows.reference_id.to_list()
                                print(alien_rows.keys())
                                alien_rows = alien_rows.merge(refs,how='left')
                                alien_rows = alien_rows.replace({np.nan: None})
                                # 排除backbone & note 為null
                                # 是backbone 沒有note
                                # 不顯示
                                alien_rows = alien_rows[~((alien_rows['type'].isin([4,6]))&(alien_rows.status_note.isnull()))]
                                alien_rows = alien_rows.sort_values('is_latest', ascending=False)
                                alien_rows = alien_rows[['alien_type','status_note','ref','type']].drop_duplicates()
                                for at in alien_rows.to_dict('records'):
                                    # 是backbone 有note
                                    # 歸化: note
                                    if at.get('type') in [4,6] and at.get('status_note'):
                                        final_aliens.append(f"{at.get('alien_type')}: {at.get('status_note')}")
                                    # 不是backbone 有note
                                    # 原生: Chang-Yang et al., 2022 (note)
                                    elif at.get('status_note'):
                                        final_aliens.append(f"{at.get('alien_type')}: {at.get('ref')} ({at.get('status_note')})")
                                    # 不是backbone 沒有notenote
                                    # 原生: Chang-Yang et al., 2022
                                    else:
                                        final_aliens.append(f"{at.get('alien_type')}: {at.get('ref')}")

                        df.loc[i, 'alien_status_note'] = '|'.join(final_aliens)

                    df['cites'] = df['cites'].apply(lambda x: x.replace('1','I').replace('2','II').replace('3','III') if x else x)
                    df['redlist'] = df['redlist'].apply(lambda x: redlist_map_rev[x] if x else x)

                    # TODO 這邊的status要確認
                    df['taxon_status'] = df['is_deleted'].replace({1: 'deleted', 0: 'accepted'})

                    # 排序
                    df = df[['taxon_id', 'taxon_status', 'name_id', 'simple_name', 'name_author', 'formatted_name', 'synonyms', 'formatted_synonyms', 'misapplied', 'formatted_misapplied',
                            'rank', 'common_name_c', 'alternative_name_c', 'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
                             'is_marine','not_official', 'cites', 'iucn', 'redlist', 'protected', 'sensitive', 'created_at', 'updated_at', 'new_taxon_id', 'parent_taxon_id']]

                    df = df.replace({np.nan: None, '': None})
                    df['name_id'] = df['name_id'].replace({np.nan: 0}).astype('int64').replace({0: None})

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
                name='rank',
                in_=openapi.IN_QUERY,
                description='階層',
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
            if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id', 'scientific_name', 'common_name', 'rank', 'updated_at', 'created_at', 'taxon_group', 'limit', 'offset']):
                response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
            # 如果有重複的參數，只考慮最後面的那個 (default)
            name_id = request.GET.get('name_id', '').strip()
            scientific_name = request.GET.get('scientific_name', '').strip()
            updated_at = request.GET.get('updated_at', '').strip().strip('"').strip("'")
            created_at = request.GET.get('created_at', '').strip().strip('"').strip("'")
            taxon_group = request.GET.get('taxon_group', '').strip()
            limit = 300 if limit > 300 else limit  # 最大值 300

            # # update names
            # update_names()
            # update_citations()

            conn = pymysql.connect(**db_settings)
            # 這邊的namecode concat應該不會超過上限 維持原本寫法
            base_query = "SELECT * FROM taxon_names tn "
            query = "SELECT tn.id, tn.rank_id, tn.name, an.name_author, \
                            tn.original_taxon_name_id, tn.note, tn.created_at, tn.updated_at, \
                            n.name, JSON_EXTRACT(tn.properties,'$.is_hybrid'), \
                            CONCAT_WS(' ', c.author, c.content), \
                            tn.properties ->> '$.type_name', \
                            tn.properties ->> '$.latin_genus', \
                            tn.properties ->> '$.latin_s1',\
                            tn.properties ->> '$.species_layers',\
                            an.formatted_name, GROUP_CONCAT(anc.namecode), tn.deleted_at\
                            FROM base_query AS tn \
                            JOIN nomenclatures n ON tn.nomenclature_id = n.id \
                            LEFT JOIN api_namecode anc ON tn.id = anc.taxon_name_id \
                            LEFT JOIN api_names an ON tn.id = an.taxon_name_id \
                            LEFT JOIN api_citations c ON tn.reference_id = c.reference_id \
                            GROUP BY tn.id ORDER BY tn.id "
            count_query = "SELECT COUNT(*) FROM taxon_names tn"

            conditions = []
            # conditions = ['tn.deleted_at IS NULL']
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

            if rank := request.GET.get('rank'):
                try:
                    rank_id = list(rank_map.keys())[list(rank_map.values()).index(rank)]
                    conditions += [f'tn.rank_id = {rank_id}']
                except:
                    response = {"status": {"code": 400, "message": "Bad Request: Incorrect rank"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

            if name_id:  # 不考慮其他條件
                base_query = f"{base_query} WHERE tn.id = '{name_id}'"
                count_query = f"{count_query} WHERE tn.id = '{name_id}'"
            elif scientific_name:  # 不考慮分類群, scientific_name, updated_at, created_at
                base_query = f"{base_query} WHERE tn.name = '{scientific_name}'"
                count_query = f"{count_query} WHERE tn.name = '{scientific_name}'"
                for c in conditions:
                    base_query += " AND " + c
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
                                        where      parent_taxon_name_id = {current_id} and is_title != 1 and deleted_at is null \
                                        union all \
                                        select     ru.id, ru.taxon_name_id, ru.parent_taxon_name_id \
                                        from       reference_usages ru \
                                        inner join cte on ru.parent_taxon_name_id = cte.taxon_name_id \
                                        where      ru.deleted_at is null \
                                        ) \
                                        select taxon_name_id from cte"
                    # conn = pymysql.connect(**db_settings)
                    with conn.cursor() as cursor:
                        cursor.execute(query_taxon_group)
                        child_results = cursor.fetchall()
                        all_child_results += child_results

                all_results = results + all_child_results

                if all_results:
                    base_query = f"{base_query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                    count_query = f"{count_query} WHERE tn.id IN {str(tuple((item[0] for item in all_results)))}"
                    for c in conditions:
                        base_query += " AND " + c
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
                        base_query = f"{base_query} WHERE {conditions[l]}"
                        count_query = f"{count_query} WHERE {conditions[l]}"
                    else:
                        query += f' AND {conditions[l]}'
                        count_query += f" AND {conditions[l]}"
                # print('else: ', query)
            with conn.cursor() as cursor:
                query = f'WITH base_query AS ({base_query} ORDER BY tn.id LIMIT {limit} OFFSET {offset} ) {query}'  # 只處理限制筆數
                cursor.execute(query)
                df = cursor.fetchall()
                df = [list(item) for item in df]
                df = pd.DataFrame(df, columns=['name_id', 'rank', 'simple_name', 'name_author', 'original_name_id', 'note',
                                               'created_at', 'updated_at', 'nomenclature_name', 'is_hybrid', 'protologue',
                                               'type_name_id', 'latin_genus', 'latin_s1', 'species_layers', 'formatted_name', 'namecode', 'is_deleted'])
                cursor.execute(count_query)
                len_total = cursor.fetchall()[0][0]


                # print(query)
                # print(count_query)
                # only rank >= 34 has 物種學名分欄 & original_name_id
                if len(df):
                    df.loc[df['rank'] < 34, 'name'] = '{}'
                    df.loc[df['rank'] < 34, 'original_name_id'] = None
                    df['rank'] = df['rank'].apply(lambda x: rank_map[x])
                

                # find hybrid_parent
                df['hybrid_parent'] = None

                for h in df[['is_hybrid', 'name_id']].index:
                    if df.loc[h]['is_hybrid'] == 'true':
                        # 這邊的namecode concat應該不會超過上限 維持原本寫法
                        query_hybrid_parent = f"SELECT GROUP_CONCAT( CONCAT(tn.name, ' ',tn.formatted_authors) SEPARATOR ' × ' ) FROM taxon_name_hybrid_parent AS tnhp \
                                                JOIN taxon_names AS tn ON tn.id = tnhp.parent_taxon_name_id \
                                                WHERE tnhp.taxon_name_id = {df.loc[h]['name_id']} \
                                                GROUP BY tnhp.taxon_name_id"
                        with conn.cursor() as cursor:
                            cursor.execute(query_hybrid_parent)
                            hybrid_name_result = cursor.fetchall()
                        if hybrid_name_result:
                            df.loc[df.name_id == df.loc[h]['name_id'], 'hybrid_parent'] = hybrid_name_result[0]

                # organize results
                df = df.replace({np.nan: None})

                if len(df):


                    # 加上taxon
                    with conn.cursor() as cursor:     
                        query = """
                        WITH cte
                            AS
                            (
                                SELECT distinct atu.taxon_name_id, atu.taxon_id, atu.status, at.is_in_taiwan
                                FROM api_taxon_usages atu
                                LEFT JOIN api_taxon at ON at.taxon_id = atu.taxon_id
                                WHERE atu.taxon_name_id IN %s  and at.is_deleted != 1
                            )
                        SELECT taxon_name_id, 
                        JSON_ARRAYAGG(JSON_OBJECT('taxon_id', taxon_id, 'status', status, 'is_in_taiwan', is_in_taiwan))
                        FROM cte GROUP BY taxon_name_id;
                        """
                        cursor.execute(query, (df.name_id.to_list(),))
                        taxon_df = pd.DataFrame(cursor.fetchall(), columns=['name_id', 'taxon'])
                        for i in taxon_df.index:
                            row = taxon_df.iloc[i]
                            taxon_tmp = json.loads(row.taxon)
                            taxon_tmp = pd.DataFrame(taxon_tmp)
                            # 排序規則： 
                            # Taiwan+有效 accepted
                            # Taiwan+無效 not-accepted
                            # Taiwan+誤用 misapplied
                            custom_dict = {'accepted': 0, 'not-accepted': 1, 'misapplied': 2}
                            taxon_tmp = taxon_tmp.sort_values(by=['status'], key=lambda x: x.map(custom_dict)).sort_values(by='is_in_taiwan',ascending=False)
                            taxon_tmp['is_in_taiwan'] = taxon_tmp['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True})
                            taxon_tmp = taxon_tmp.rename(columns={'status': 'usage_status'})
                            taxon_tmp = taxon_tmp[['taxon_id','usage_status','is_in_taiwan']]
                            taxon_df.loc[i,'taxon'] = taxon_tmp.to_json(orient='records')

                    if len(taxon_df):
                        df = df.merge(taxon_df, how='left')    
                    else:
                        df['taxon'] = '[]'
                    

                    df['taxon'] = df['taxon'].replace({np.nan:'[]'})
                    df['taxon'] = df['taxon'].apply(json.loads)

                    # 日期格式 yy-mm-dd
                    # df['created_at'] = df.created_at.dt.strftime('%Y-%m-%d')
                    # df['updated_at'] = df.updated_at.dt.strftime('%Y-%m-%d')
                    df['created_at'] = df.created_at.apply(lambda x: str(x).split(' ')[0])
                    df['updated_at'] = df.updated_at.apply(lambda x: str(x).split(' ')[0])

                    # 是否已刪除
                    df['is_deleted'] = df.is_deleted.apply(lambda x: True if x else False)

                    # remove null/empty/None element in 'name' json
                    for n in df.index:
                        name = {'latin_genus': df.latin_genus[n], 'latin_s1': df.latin_s1[n]}
                        count = 2
                        for s in json.loads(df.species_layers[n]):
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
                            'is_hybrid', 'hybrid_parent', 'protologue', 'type_name_id', 'namecode', 'taxon', 'is_deleted', 'created_at', 'updated_at']]

                    df['is_hybrid'] = df['is_hybrid'].replace('false', False).replace('true', True)
                    df.loc[df['name_author'] == "", 'name_author'] = None
                    df = df.replace({np.nan: None, '': None})
                    df[['name_id','original_name_id','type_name_id']] = df[['name_id','original_name_id','type_name_id']].replace({None: 0, np.nan: 0})
                    df[['name_id','original_name_id','type_name_id']] = df[['name_id','original_name_id','type_name_id']].replace({np.nan: 0}).astype('int64').replace({0: None})
                response = {"status": {"code": 200, "message": "Success"},
                            "info": {"total": len_total, "limit": limit, "offset": offset}, "data": df.to_dict('records')}
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}


        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
        # https://www.django-rest-framework.org/api-guide/exceptions/
