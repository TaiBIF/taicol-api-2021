
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

reference_type_map = {
    1: 'Journal Article',
    2: 'Book Chapter',
    3: 'Book',
    4: 'Backbone',
    5: 'Checklist',
    6: 'Backbone'
}

# type= 1 or 2 or 3 地位是相同的 
custom_reference_type_order = {
    1: 2,
    2: 2,
    3: 2,
    4: 4,
    5: 3,
    6: 1
}


bio_group_map = {
    "Insects": "昆蟲",
    "Spiders": "蜘蛛",
    "Fishes": "魚類",
    "Reptiles": "爬蟲類",
    "Amphibians": "兩棲類",
    "Birds": "鳥類",
    "Mammals": "哺乳類",
    "Vascular Plants": "維管束植物",
    "Ferns": "蕨類植物",
    "Mosses": "苔蘚植物",
    "Algae": "藻類",
    "Viruses": "病毒",
    "Bacteria": "細菌",
    "Fungi": "真菌",
}


# is_in_taiwan 調整

# reference_usages原始狀態
# 1=true
# 0=false
# 2,null=null

# taxon底下的狀態
# 1=true
# 0,2,null=false


# 改成 同一張表 query 一次
def web_stat_stat(request):
    conn = pymysql.connect(**db_settings)
    response = {}
    with conn.cursor() as cursor:
        query = "SELECT  category, count, title, total_count FROM api_web_stat "
        cursor.execute(query)
        results = cursor.fetchall()
        result_df = pd.DataFrame(results, columns=['category','count','title','total_count'])
        result_df = result_df.replace({np.nan: None})
        for tt in result_df.title.unique():
            response[tt] = result_df[result_df.title==tt][['category','count','total_count']].to_dict('records')
        response['kingdom_compare'] = result_df[result_df.title=='kingdom_count'][['category','count','total_count']].to_dict('records')
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
    conn.close()

    return HttpResponse(json.dumps(response))


def web_index_stat(request):
        conn = pymysql.connect(**db_settings)
        with conn.cursor() as cursor:
            query = """SELECT category, count FROM api_web_stat WHERE title = 'index'"""  
            cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            return HttpResponse(json.dumps(results))



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
                conn.close()
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
                        taxon_tmp['is_in_taiwan'] = taxon_tmp['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, 2: False, '2': False, None: False, '': False})
                        taxon_tmp = taxon_tmp.rename(columns={'status': 'taicol_name_status'})
                        taxon_tmp = taxon_tmp[['taxon_id','taicol_name_status','is_in_taiwan']]
                        df.loc[i,'taxon'] = taxon_tmp.to_json(orient='records')
                    if len(df):
                        df['taxon'] = df['taxon'].replace({np.nan:'[]'})
                        df['taxon'] = df['taxon'].apply(json.loads)
                conn.close()

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
                        taxon_tmp['is_in_taiwan'] = taxon_tmp['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, '2': False, None: False, '': False})
                        taxon_tmp = taxon_tmp.rename(columns={'status': 'taicol_name_status'})
                        taxon_tmp = taxon_tmp[['taxon_id','taicol_name_status','is_in_taiwan']]
                        df.loc[i,'taxon'] = taxon_tmp.to_json(orient='records')
                    if len(df):
                        df['taxon'] = df['taxon'].replace({np.nan:'[]'})
                        df['taxon'] = df['taxon'].apply(json.loads)
                conn.close()
                
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

        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name', 'best', 'only_taiwan', 'bio_group', 'rank', 'kingdom']):
            response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
        try:
            # name_id_list = []
            namecode_list = []
            data = []

            conn = pymysql.connect(**db_settings)
            df = pd.DataFrame(columns=['taxon_id', 'taicol_name_status'])

            # 拿掉此查詢條件
            # if name_id := request.GET.get('name_id'):
            #     name_id_list.append(int(name_id))

            if name := request.GET.get('name'): # 如果是查name, 接NomenMatchAPI

                best = request.GET.get('best')
                if best and not best in ['yes', 'no']:
                    response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameter value"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                elif not best:
                    best = 'yes'
                else:
                    best = request.GET.get('best')

                query_dict =  {
                    'names': name,
                    'best': best,
                    'format': 'json',
                    'source': 'taicol'
                }

                # only_taiwan={yes/no} 是否僅比對臺灣物種。

                only_taiwan = request.GET.get('only_taiwan', 'yes')

                if only_taiwan == 'yes':
                    query_dict['is_in_taiwan'] = True

                # rank={string} 比對階層
                
                if ranks := request.GET.getlist('rank'):
                    query_dict['taxon_rank'] = ",".join(ranks)

                # kingdom 比對生物界

                if kingdoms := request.GET.getlist('kingdom'):
                    query_dict['kingdom'] = ",".join([f'"{k}"' for k in kingdoms])

                # bio_group 比對常見類群

                if bio_group := request.GET.get('bio_group'):
                    if bio_group != 'all':
                        # 中英比對
                        if bio_group in bio_group_map.keys():
                            bio_group = bio_group_map[bio_group]
                    query_dict['bio_group'] = bio_group


                resp = requests.post(match_url, data=query_dict)

                if resp.status_code == 200:
                    resp = resp.json()
                    data = resp['data']
                    tmp_df = pd.DataFrame()
                    
                    for ddd in data:
                        for dd in ddd:
                            for d in dd['results']:
                                tmp_dict = {
                                    "matched_name": d['simple_name'],
                                    "taxon_id": d['namecode'],
                                    "taicol_name_status": d['name_status']
                                }
                                namecode_list.append(d['namecode'])
                                tmp_df = pd.concat([tmp_df, pd.DataFrame([tmp_dict])], ignore_index=True)
            if namecode_list:
                # 先抓原本的資料 後面再抓accepted_usage
                with conn.cursor() as cursor:
                    query = f"SELECT distinct t.name, t.id, t1.name, t1.id, atu.taxon_id, atu.status, at.is_deleted \
                        FROM api_taxon_usages atu \
                        JOIN api_taxon at ON atu.taxon_id = at.taxon_id  \
                        JOIN taxon_names t ON atu.taxon_name_id = t.id  \
                        JOIN taxon_names t1 ON at.accepted_taxon_name_id = t1.id \
                        WHERE atu.taxon_id IN %s AND t.deleted_at IS NULL AND t1.deleted_at IS NULL"
                    cursor.execute(query, (namecode_list,))
                    df = pd.DataFrame(cursor.fetchall(), columns=['matched_name', 'matched_name_id', 'accepted_name', 'accepted_name_id', 'taxon_id', 'taicol_name_status', 'is_deleted'])
                    df = df.replace({np.nan: None, '': None, 'null': None})
                    df = tmp_df.merge(df)
                    df = df.reset_index(drop=True)
                    if len(df):
                        df = df.drop_duplicates()
                        df.loc[df.is_deleted==1, 'taicol_name_status'] = 'deleted'
                        df['taicol_name_status'] = df['taicol_name_status']
                        df = df.drop(columns=['is_deleted'])
                        # 取得 matched_name_accepted_usage
                        query = """SELECT ru.taxon_name_id, ru.id, ru.reference_id, r.type, r.publish_year,
                                 CONCAT_WS(' ' ,c.author, c.content), JSON_EXTRACT(ru.properties, '$.is_in_taiwan') 
                                 FROM reference_usages ru 
                                 JOIN `references` r ON ru.reference_id = r.id
                                 LEFT JOIN api_citations c ON ru.reference_id = c.reference_id
                                 WHERE ru.taxon_name_id IN %s AND ru.status = 'accepted' AND ru.deleted_at IS NULL AND ru.is_title != 1
                                """
                        with conn.cursor() as cursor:
                            cursor.execute(query, (list(df.accepted_name_id.unique()),))
                            matched_name_accepted_usage = pd.DataFrame(cursor.fetchall(), columns=['accepted_name_id', 'usage_id', 'reference_id', 'reference_type', 'publish_year', 'citation', 'is_in_taiwan'])
                            if len(matched_name_accepted_usage):
                                matched_name_accepted_usage = matched_name_accepted_usage.replace({np.nan: None, '': None, 'null': None})
                                matched_name_accepted_usage['is_in_taiwan'] = matched_name_accepted_usage['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, 2: None, '2': None, '': None})                        
                                matched_name_accepted_usage['reference_order'] = matched_name_accepted_usage['reference_type'].apply(lambda x: custom_reference_type_order[x])
                                # 先處理排序 先排year再排type
                                matched_name_accepted_usage = matched_name_accepted_usage.sort_values('publish_year', ascending=False).sort_values('reference_order')
                                # 先處理 reference_type = 4 的
                                matched_name_accepted_usage['publish_year'] = matched_name_accepted_usage['publish_year'].apply(lambda x: int(x) if x else None)
                                matched_name_accepted_usage.loc[matched_name_accepted_usage.reference_type.isin([4,6]), 'reference_id'] = None
                                matched_name_accepted_usage.loc[matched_name_accepted_usage.reference_type.isin([4,6]), 'publish_year'] = None
                                matched_name_accepted_usage.loc[matched_name_accepted_usage.reference_type.isin([4,6]), 'citation'] = 'TaiCOL Backbone'
                                matched_name_accepted_usage['reference_type'] = matched_name_accepted_usage['reference_type'].apply(lambda x: reference_type_map[x] if x else None)
                                matched_name_accepted_usage['publish_year'] = matched_name_accepted_usage['publish_year'].fillna(0).astype(int).replace({0: None})
                                matched_name_accepted_usage['reference_id'] = matched_name_accepted_usage['reference_id'].fillna(0).astype(int).replace({0: None})
                                for name_id in df.accepted_name_id.unique():
                                    df.loc[df.accepted_name_id==name_id,'matched_name_accepted_usage'] = json.dumps(matched_name_accepted_usage[matched_name_accepted_usage.accepted_name_id==name_id][['usage_id', 'reference_id', 'reference_type','publish_year', 'citation', 'is_in_taiwan']].to_dict('records'))
                        # 取得 matched_name_usage
                        query = """SELECT ru.taxon_name_id, ru.id, ru.reference_id, r.type, r.publish_year,
                                 CONCAT_WS(' ' ,c.author, c.content), ru.status, ru.accepted_taxon_name_id, JSON_EXTRACT(ru.properties, '$.is_in_taiwan') 
                                 FROM reference_usages ru 
                                 JOIN `references` r ON ru.reference_id = r.id
                                 LEFT JOIN api_citations c ON ru.reference_id = c.reference_id
                                 WHERE ru.taxon_name_id IN %s AND ru.deleted_at IS NULL AND ru.is_title != 1
                                """
                        with conn.cursor() as cursor:
                            cursor.execute(query, (list(df.matched_name_id.unique()),))
                            matched_name_usage = pd.DataFrame(cursor.fetchall(), columns=['matched_name_id', 'usage_id', 'reference_id', 
                                                                                           'reference_type', 'publish_year', 'citation', 
                                                                                           'usage_status', 'accepted_name_id','is_in_taiwan'])
                            if len(matched_name_usage):
                                matched_name_usage = matched_name_usage.replace({np.nan: None, '': None, 'null': None})
                                matched_name_usage['is_in_taiwan'] = matched_name_usage['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, 2: None, '2': None, '': None})
                                matched_name_usage['reference_order'] = matched_name_usage['reference_type'].apply(lambda x: custom_reference_type_order[x])
                                # 先處理排序 先排year再排type
                                matched_name_usage = matched_name_usage.sort_values('publish_year', ascending=False).sort_values('reference_order')
                                # 先處理 reference_type = 4 的
                                matched_name_usage['publish_year'] = matched_name_usage['publish_year'].apply(lambda x: int(x) if x else None)
                                matched_name_usage.loc[matched_name_usage.reference_type.isin([4,6]), 'reference_id'] = None
                                matched_name_usage.loc[matched_name_usage.reference_type.isin([4,6]), 'publish_year'] = None
                                matched_name_usage.loc[matched_name_usage.reference_type.isin([4,6]), 'citation'] = 'TaiCOL Backbone'
                                matched_name_usage['reference_type'] = matched_name_usage['reference_type'].apply(lambda x: reference_type_map[x] if x else None)
                                matched_name_usage['publish_year'] = matched_name_usage['publish_year'].fillna(0).astype(int).replace({0: None})
                                matched_name_usage['reference_id'] = matched_name_usage['reference_id'].fillna(0).astype(int).replace({0: None})
                                for name_id in df.matched_name_id.unique():
                                    df.loc[df.matched_name_id==name_id,'matched_name_usage'] = json.dumps(matched_name_usage[matched_name_usage.matched_name_id==name_id][['usage_id', 'reference_id', 'reference_type',
                                                                                                                                                                           'publish_year', 'citation', 'usage_status', 'accepted_name_id', 'is_in_taiwan']].to_dict('records'))
                        df['matched_name_usage'] = df['matched_name_usage'].apply(json.loads)
                        df['matched_name_accepted_usage'] = df['matched_name_accepted_usage'].apply(json.loads)
                        df = df[['matched_name', 'matched_name_id', 'taxon_id', 'taicol_name_status', 'accepted_name', 'accepted_name_id', 'matched_name_accepted_usage', 'matched_name_usage']]
                        data = df.to_dict('records')

            conn.close()
            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(df)}, "data": data}

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
        if request.GET.keys() and not set(list(request.GET.keys())) <= set(['name_id','usage_id']):
            response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
            return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
        try:
            data = []
            # NOTE 這邊的alien_type是reference_usage的 不是taxon的
            df = pd.DataFrame(columns=['usage_id', 'name_id', 'reference_id', 'reference_type', 'publish_year', 'accepted_name_id', 'citation', 'status', 'indications', 'is_in_taiwan', 'is_endemic', 'alien_type','is_deleted'])
            conn = pymysql.connect(**db_settings)
            if name_id := request.GET.get('name_id'):
                query = f"SELECT ru.id, ru.taxon_name_id, ru.reference_id, r.type, r.publish_year, CONCAT_WS(' ' ,c.author, c.content), ru.status, ru.accepted_taxon_name_id, ru.properties->>'$.indications', \
                         JSON_EXTRACT(ru.properties, '$.is_in_taiwan'), JSON_EXTRACT(ru.properties, '$.is_endemic'), ru.properties->>'$.alien_type', r.deleted_at \
                         FROM reference_usages ru \
                         JOIN `references` r ON ru.reference_id = r.id \
                         JOIN api_citations c ON ru.reference_id = c.reference_id \
                         WHERE ru.taxon_name_id = %s AND r.type != 4 AND ru.status != '' \
                         AND ru.is_title != 1 AND ru.deleted_at IS NULL"  # 不給backbone
                
                with conn.cursor() as cursor:
                    cursor.execute(query, (name_id,))
                    df = pd.DataFrame(cursor.fetchall(), columns=['usage_id', 'name_id', 'reference_id', 'reference_type', 'publish_year', 'citation', 'usage_status', 'accepted_name_id', 'indications', 'is_in_taiwan', 'is_endemic', 'alien_type', 'is_deleted'])
            # usage反查時不排除backbone
            elif usage_id := request.GET.get('usage_id'):
                query = f"SELECT ru.id, ru.taxon_name_id, ru.reference_id, r.type, r.publish_year, CONCAT_WS(' ' ,c.author, c.content), ru.status, ru.accepted_taxon_name_id, ru.properties->>'$.indications', \
                         JSON_EXTRACT(ru.properties, '$.is_in_taiwan'), JSON_EXTRACT(ru.properties, '$.is_endemic'), ru.properties->>'$.alien_type', r.deleted_at \
                         FROM reference_usages ru \
                         JOIN `references` r ON ru.reference_id = r.id \
                         LEFT JOIN api_citations c ON ru.reference_id = c.reference_id \
                         WHERE ru.id = %s  AND ru.status != '' \
                         AND ru.is_title != 1 AND ru.deleted_at IS NULL"  # 不給backbone
                # AND r.type != 4
                with conn.cursor() as cursor:
                    cursor.execute(query, (usage_id,))
                    df = pd.DataFrame(cursor.fetchall(), columns=['usage_id', 'name_id', 'reference_id', 'reference_type', 'publish_year', 'citation', 'usage_status', 'accepted_name_id', 'indications', 'is_in_taiwan', 'is_endemic', 'alien_type', 'is_deleted'])
            
            if name_id:
                query = f"SELECT c.reference_id, CONCAT_WS(' ' , c.author, c.content), r.type, r.publish_year \
                        FROM taxon_names tn \
                        JOIN api_citations c ON tn.reference_id = c.reference_id    \
                        JOIN `references` r ON tn.reference_id = r.id    \
                        WHERE tn.id = %s AND tn.reference_id IS NOT NULL AND tn.deleted_at IS NULL AND tn.is_publish = 1"
                with conn.cursor() as cursor:
                    cursor.execute(query, (name_id, ))
                    results = cursor.fetchall()
                    for r in results:
                        if r[0] not in df.reference_id.to_list():
                            # 這邊不會有backbone
                            df = pd.concat([df, pd.DataFrame([{'name_id': int(name_id), 'reference_id': r[0], 'reference_type': r[2], 'publish_year': int(r[3]), 'citation': r[1], 'usage_status': 'accepted'}])], ignore_index=True)

            if len(df):
                df = df.replace({np.nan: None, 'null': None})
                df['publish_year'] = df['publish_year'].apply(lambda x: int(x) if x else None)
                df['reference_order'] = df['reference_type'].apply(lambda x: custom_reference_type_order[x])
                df = df.sort_values('publish_year', ascending=False).sort_values('reference_order')
                df.loc[df.reference_type.isin([4,6]), 'reference_id'] = None
                df.loc[df.reference_type.isin([4,6]), 'publish_year'] = None
                df.loc[df.reference_type.isin([4,6]), 'citation'] = 'TaiCOL Backbone'
                df['reference_type'] = df['reference_type'].apply(lambda x: reference_type_map[x] if x else None)
                df['publish_year'] = df['publish_year'].fillna(0).astype(int).replace({0: None})
                df['reference_id'] = df['reference_id'].fillna(0).astype(int).replace({0: None})
                df['is_deleted'] = df['is_deleted'].apply(lambda x: True if x else False)
                # is_list = ['is_endemic', 'is_in_taiwan']
                df['is_endemic'] = df['is_endemic'].replace({0: False, 1: True, '0': False, '1': True})
                df['is_in_taiwan'] = df['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, 2: None, '2': None, '': None})
                for i in df.index:
                    row = df.iloc[i]
                    if row.indications and row.indications != '[]':
                        df.loc[i, 'indications'] = ','.join(eval(row.indications))
                    else:
                        df.loc[i, 'indications'] = None
                                
                df = df.replace({np.nan: None, '': None})
                df['reference_id'] = df['reference_id'].replace({np.nan: 0}).astype('int64').replace({0: None})
                df['usage_id'] = df['usage_id'].replace({np.nan: 0}).astype('int64').replace({0: None})
                df['accepted_name_id'] = df['accepted_name_id'].replace({np.nan: 0}).astype('int64').replace({0: None})
                df = df.drop(columns=['reference_order'])
                data = df.to_dict('records')

                new_data = []
                for d in data:
                    if not d.get('usage_id'):
                        poping_keys = [k for k in df.keys() if k not in ['name_id', 'reference_id', 'reference_type', 'publish_year', 'citation',  'usage_status']]
                        for p in poping_keys:
                            d.pop(p)
                    new_data.append(d)

            # 加上原始文獻 status補上accepted

            conn.close()
        
            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(df)}, "data": new_data}
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

            solr_query_list = get_conditioned_solr_search(req=request.GET)

            limit = 300 if limit > 300 else limit  # 最大值 300

            query = { "query": "*:*",
                    "offset": offset,
                    "limit": limit,
                    "filter": solr_query_list,
                    "sort": 'taxon_id asc',
                    }

            query_req = json.dumps(query)

            resp = requests.post(f'{SOLR_PREFIX}taxa/select?', data=query_req, headers={'content-type': "application/json" })
            resp = resp.json()

            count = resp['response']['numFound']

            # 這邊應該要改成docs才對 因為有可能給了錯誤的offset 造成沒有回傳docs
            if resp['response']['docs']:

                df = pd.DataFrame(resp['response']['docs'])

                # df = df
                # [['taxon_id', 'taxon_status', 'name_id', 'simple_name', 'name_author', 'formatted_name', 'synonyms', 'formatted_synonyms', 'misapplied', 'formatted_misapplied',
                #     'rank', 'common_name_c', 'alternative_name_c', 'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
                #          'is_marine','not_official', 'cites', 'iucn', 'redlist', 'protected', 'sensitive', 'created_at', 'updated_at', 'new_taxon_id', 'parent_taxon_id']]

                # 從這邊開始merge從solr過來的資料
                df = df.rename(columns={
                        'formatted_accepted_name': 'formatted_name',
                        'status': 'taxon_status',
                        'accepted_taxon_name_id': 'name_id',
                        'taxon_rank_id': 'rank',
                    })
                

                # # 一定要有的欄位
                # musthave_cols = ['search_name','usage_status','taxon_id','formatted_name','rank','common_name_c',
                #     'is_hybrid','is_in_taiwan','is_endemic','alien_type','is_fossil','is_terrestrial',
                #     'is_freshwater','is_brackish','is_marine','not_official','cites','iucn','redlist','protected']

                # for m in musthave_cols:
                #     if m not in df.keys():
                #         df[m] = None

                df['created_at'] = df.created_at.apply(lambda x: x[0].split('T')[0])
                df['updated_at'] = df.updated_at.apply(lambda x: x[0].split('T')[0])

                # rank_id to rank
                df['rank'] = df['rank'].apply(lambda x: rank_map[int(x)])

                
                df['taxon_status'] = df['is_deleted'].replace({True: 'deleted', False: 'accepted'})

                df = df.replace({np.nan: '', None: ''})

                # 欄位順序
                cols = ['taxon_id', 'taxon_status', 'name_id', 'simple_name', 'name_author', 'formatted_name', 'synonyms', 'formatted_synonyms', 'misapplied', 'formatted_misapplied',
                        'rank', 'common_name_c', 'alternative_name_c', 'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
                        'is_marine','not_official', 'cites', 'iucn', 'redlist', 'protected', 'sensitive', 'created_at', 'updated_at', 'new_taxon_id', 'parent_taxon_id']

                for c in cols:
                    if c not in df.keys():
                        df[c] = None


                # 0 / 1 要改成 true / false
                # not_official
                is_list = ['is_hybrid','is_in_taiwan','is_endemic','is_fossil','is_terrestrial','is_freshwater','is_brackish','is_marine','not_official']
                df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True, 'true': True, 'false': False})
                df['is_in_taiwan'] = df['is_in_taiwan'].replace({2: False, '2': False, None: False})
                # df[is_list] = df[is_list].replace({0: 'false', 1: 'true', '0': 'false', '1': 'true', True: 'true', False: 'false'})

                # cites要改成 I,II,III
                df['cites'] = df['cites'].apply(lambda x: x.replace('1','I').replace('2','II').replace('3','III') if x else x)
                df['redlist'] = df['redlist'].apply(lambda x: redlist_map_rev[x] if x else x)

                df = df.replace({np.nan: None, '': None})
                df['name_id'] = df['name_id'].replace({np.nan: 0}).astype('int64').replace({0: None})

                df = df[cols]
            else:
                df = pd.DataFrame()

            # 加上其他欄位
            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": count, "limit": limit, "offset": offset}, "data": df.to_dict('records')}
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
            scientific_name = remove_rank_char(scientific_name)

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

            conditions = ['tn.is_publish = 1']

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
                base_query = f"{base_query} WHERE tn.search_name = '{remove_rank_char(scientific_name)}' OR tn.name = '{scientific_name}' "
                count_query = f"{count_query}  WHERE tn.search_name = '{remove_rank_char(scientific_name)}' OR tn.name = '{scientific_name}' "
                for c in conditions:
                    base_query += " AND " + c
                    count_query += " AND " + c
            elif taxon_group:
                # 先由 學名 / 中文名 找出符合的name_id
                query_1 = f"SELECT id FROM taxon_names WHERE name = '{taxon_group}' AND is_publish = 1"
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

                    query = { "query": "*:*",
                        "offset": 0,
                        "limit": 1000000,
                        "filter": ['taxon_name_id:({})'.format(' OR '.join([str(n) for n in df.name_id.to_list()])), 'is_deleted:false', ],
                        "fields": ['taxon_id', 'status', 'is_in_taiwan','taxon_name_id']
                        }
                    
                    query_req = json.dumps(query)

                    resp = requests.post(f'{SOLR_PREFIX}taxa/select?', data=query_req, headers={'content-type': "application/json" })
                    resp = resp.json()

                    # print(resp)
                    taxon_df = pd.DataFrame(resp['response']['docs'])
                    if len(taxon_df):
                        taxon_df['taxon_name_id'] = taxon_df['taxon_name_id'].astype(int)
                        taxon_df = taxon_df.rename(columns={'taxon_name_id': 'name_id'})
                        taxon_df = taxon_df.groupby('name_id').apply(lambda x: x[['taxon_id', 'status', 'is_in_taiwan']].to_dict('records')).reset_index(name='taxon')

                        for i in taxon_df.index:
                            row = taxon_df.iloc[i]
                            # taxon_tmp = json.loads(row.taxon)
                            taxon_tmp = pd.DataFrame(row.taxon)
                            # 排序規則： 
                            # Taiwan+有效 accepted
                            # Taiwan+無效 not-accepted
                            # Taiwan+誤用 misapplied
                            custom_dict = {'accepted': 0, 'not-accepted': 1, 'misapplied': 2}
                            taxon_tmp = taxon_tmp.sort_values(by=['status'], key=lambda x: x.map(custom_dict)).sort_values(by='is_in_taiwan',ascending=False)
                            taxon_tmp['is_in_taiwan'] = taxon_tmp['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, '2': False, None: False})
                            taxon_tmp = taxon_tmp.rename(columns={'status': 'taicol_name_status'})
                            taxon_tmp = taxon_tmp[['taxon_id','taicol_name_status','is_in_taiwan']]
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


def update_check_usage(request):
    try:
        # 只接受從工具傳來的request
        # 用ip來判斷
        ALLOWED_HOST_FOR_USAGE_CHECK = env.list('ALLOWED_HOST_FOR_USAGE_CHECK')
        for host in ALLOWED_HOST_FOR_USAGE_CHECK:
            if host in request.META.get('HTTP_X_FORWARDED_FOR'):
                a = check_taxon_usage()
                response = {"status": {"code": 200, "message": "Usage checked!"}}
                break
            else:
                response = {"status": {"code": 403, "message": "Forbidden"}}

    except Exception as er:
        print(er)
        response = {"status": {"code": 500, "message": "Unexpected Error"}}

    return HttpResponse(json.dumps(response))

