
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
from api.utils_for_usage import *
import requests
from django.views.decorators.csrf import csrf_exempt
from sqlalchemy import create_engine


def safe_json_dumps(x):
    if x is None or (not isinstance(x, (dict, list)) and pd.isna(x)):
        return None
    try:
        return json.dumps(
            x,
            default=lambda o: o.item() if hasattr(o, 'item') else str(o)
        )
    except Exception as e:
        print(f"JSON 轉換失敗：{x}，錯誤：{e}")
        return None


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


# 給工具使用的API
def get_taxon_by_higher(request):

    only_in_taiwan = request.GET.get('only_in_taiwan')
    exclude_cultured = request.GET.get('exclude_cultured')
    
    # 因為已經有限定是最高上階層是科 所以底下的不用再限定
    higher_taxa = request.GET.get('higher_taxa','')
    higher_taxa = higher_taxa.split(',')


    query_list = []
    query_list.append('is_deleted:false')

    if only_in_taiwan == 'yes':
        query_list.append('is_in_taiwan:true')

    if exclude_cultured == 'yes':
        query_list.append('-alien_type:cultured')

    # higher_taxa = request.GET.getlist('higher_taxa')
    
    query_list.append('path:({})'.format((' OR ').join([f'/.*{f}.*/' for f in higher_taxa])))

    # print(query_list)

    query = { "query": "*:*",
        # "offset": offset,
        "limit": 0,
        "filter": query_list,
        # "sort": 'search_name asc',
        "facet": {"taxon_id": {
                        'type': 'terms',
                        'field': 'taxon_id',
                        'mincount': 1,
                        'limit': -1,
                        'offset': 0,
                        'allBuckets': False,
                        'numBuckets': False
                  }}
    }

    query_req = json.dumps(query)

    resp = requests.post(f'{SOLR_PREFIX}taxa/select?', data=query_req, headers={'content-type': "application/json" })
    resp = resp.json()
    taxon_ids = [r['val'] for r in resp['facets']['taxon_id']['buckets']] if resp['facets'].get('count') else []


    return HttpResponse(json.dumps(taxon_ids), content_type='application/json')

    # 回傳path包含higher_taxa的所有taxon_id 




@csrf_exempt
def generate_checklist(request):

    data = json.loads(request.body)
    pairs = list({(item['reference_id'], item['group']) for item in data})
    use_common_name_backbone = True if len([p for p in pairs if p[0] == 95]) else False


    conn = pymysql.connect(**db_settings)

    # # 最後一筆學名使用更新的時間當成last_updated
    # # 這邊應該也要考慮刪除的時間 ?
    query = '''SELECT max(updated_at) FROM reference_usages ru 
                WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined") 
                AND ru.deleted_at IS NULL 
                AND ru.accepted_taxon_name_id IS NOT NULL
                UNION ALL
                SELECT max(deleted_at) FROM reference_usages  WHERE deleted_at IS NOT NULL
                '''

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        last_updateds = cursor.fetchall()
        last_updateds = [l[0] for l in last_updateds]
        last_updated = max(last_updateds)


    # 只先處理需要刪除的usage

    query = '''select id, reference_id, taxon_name_id from reference_usages where (taxon_name_id in (SELECT id FROM taxon_names WHERE deleted_at IS NOT NULL) or 
                                                    accepted_taxon_name_id in (SELECT id FROM taxon_names WHERE deleted_at IS NOT NULL) or 
                                                    reference_id in (select id from `references` WHERE deleted_at IS NOT NULL)) 
                                                    and deleted_at is null;'''
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        deleted_ids = cursor.fetchall()
        deleted_ru_ids = [i[0] for i in deleted_ids]

    if len(deleted_ru_ids):
        query = """UPDATE reference_usages SET deleted_at = %s WHERE id IN %s"""
        with conn.cursor() as cursor:
            execute_line = cursor.execute(query, (last_updated, deleted_ru_ids))
            conn.commit()
        # 新增到import_usage_log
        for ddd in deleted_ids:
            query = """
            INSERT INTO `import_usage_logs` (`reference_id`, `action`, `user_id`, `reference_usage_id`, `taxon_name_id`)
            VALUES (%s, 5, 5, %s, %s);"""
            with conn.cursor() as cursor:
                execute_line = cursor.execute(query, (ddd[1], ddd[0], ddd[2]))
                conn.commit()


    conn = pymysql.connect(**db_settings)

    #NOTE 用reference_id + group去抓資料
    
    placeholders = ",".join(["(%s, %s)"] * len(pairs))
    params = [item for pair in pairs for item in pair]

    query = f'''
            SELECT ru.id, ru.reference_id, ru.taxon_name_id, ru.accepted_taxon_name_id, ru.status, 
                r.properties ->> '$.check_list_type', ru.per_usages, r.publish_year, 
                ru.properties ->> '$.is_in_taiwan', atu.taxon_id, ru.parent_taxon_name_id, ru.`group`,
                tn.rank_id, tn.original_taxon_name_id, tn.name,
                tn.properties ->> '$.latin_genus', tn.properties ->> '$.latin_s1', 
                tn.properties ->> '$.species_layers', JSON_LENGTH(tn.properties ->> '$.species_layers'),
                tn.nomenclature_id, tn.properties ->> '$.is_hybrid', 
                tn.object_group, tn.autonym_group
                FROM reference_usages ru 
                JOIN `references` r ON r.id = ru.reference_id
                JOIN taxon_names tn ON tn.id = ru.taxon_name_id
                LEFT JOIN api_taxon_usages atu ON atu.is_deleted = 0 and atu.reference_id = ru.reference_id and atu.accepted_taxon_name_id = ru.accepted_taxon_name_id and atu.taxon_name_id = ru.taxon_name_id
                WHERE ru.is_title != 1 AND ru.status NOT IN ("", "undetermined") AND ru.deleted_at IS NULL AND ru.accepted_taxon_name_id IS NOT NULL AND (ru.reference_id, ru.`group`) IN ({placeholders})
            '''

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, params)
        tmp = cursor.fetchall()
        ref_group_pair_total = pd.DataFrame(tmp, columns=['ru_id','reference_id','taxon_name_id','accepted_taxon_name_id', 
                                                        'ru_status', 'check_list_type', 'per_usages','publish_year', 
                                                        'is_in_taiwan','taxon_id','parent_taxon_name_id','group',
                                                        'rank_id', 'original_taxon_name_id', 'name', 
                                                        'latin_genus', 'latin_s1', 'species_layers', 'layer_count', 
                                                        'nomenclature_id','is_hybrid',
                                                        'object_group', 'autonym_group'
                                                        ])
        # 排除俗名backbone
        ref_group_pair_total = ref_group_pair_total[ref_group_pair_total.reference_id!=95]
        ref_group_pair_total = ref_group_pair_total[ref_group_pair_total.check_list_type != 4] # !=4 寫在query裡會排除掉null
        ref_group_pair_total = ref_group_pair_total.drop_duplicates()
        ref_group_pair_total = ref_group_pair_total.reset_index(drop=True)
        ref_group_pair_total = ref_group_pair_total.replace({np.nan:None})

    # TODO 這邊要補抓per_usage中的reference or 不用?
    
    name_df = ref_group_pair_total[['taxon_name_id', 'rank_id', 'original_taxon_name_id', 'name', 
                                            'latin_genus', 'latin_s1', 'species_layers', 'layer_count', 
                                            'nomenclature_id','is_hybrid',
                                            'object_group', 'autonym_group']].drop_duplicates()
    name_df = name_df.reset_index(drop=True)
    name_df = name_df.replace({np.nan:None})

    query = '''SELECT r.id, r.publish_year, JSON_EXTRACT(r.properties, "$.doi"), r.`type`, ac.publish_date
                FROM `references` r 
                LEFT JOIN api_citations ac ON ac.reference_id = r.id
                WHERE r.is_publish = 1 AND r.id IN %s
                '''

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, (list(ref_group_pair_total.reference_id.unique()),))
        refs = cursor.fetchall()
        refs = pd.DataFrame(refs)
        refs = refs.rename(columns={0: 'reference_id', 1: 'publish_year', 2: 'doi', 3: 'type', 4: 'publish_date'})

    refs = refs.replace({np.nan:None})
    refs['publish_date'] = refs['publish_date'].replace({None:''})
    refs['publish_year'] = refs['publish_year'].apply(int)


    # 欄位順序
    ref_group_pair_total = ref_group_pair_total[['ru_id','reference_id','taxon_name_id','accepted_taxon_name_id',
                                                    'rank_id','ru_status','original_taxon_name_id','latin_genus','latin_s1',
                                                    'species_layers','layer_count','check_list_type',
                                                    'per_usages','publish_year','is_in_taiwan','nomenclature_id','taxon_id',
                                                    'object_group', 'autonym_group','parent_taxon_name_id','group']]
    ref_group_pair_total = ref_group_pair_total.replace({np.nan:None})

    ref_group_pair_total['publish_year'] = ref_group_pair_total['publish_year'].apply(int)

    # NOTE 不檢查資料

    # 加入誤用的per_usages
    # 這邊應該要處理全部才對 後面加的時候才不會漏掉
    misapplied_accepted_taxon_name_id = ref_group_pair_total[ref_group_pair_total.ru_status=='misapplied'].accepted_taxon_name_id.unique()
    len(misapplied_accepted_taxon_name_id)

    # 在分類學中，"pro parte"意思是"部分地" 是一個用來描述分類單元或文獻引用的術語，表示某一名稱或描述只適用於特定的一部分，而不是全部。

    # c = 0
    for mm in misapplied_accepted_taxon_name_id: # 1184
        # c += 1
        # if c % 100 == 0:
        #     print('now', c)
        rows = ref_group_pair_total[(ref_group_pair_total.accepted_taxon_name_id==mm)&(ref_group_pair_total.ru_status=='misapplied')].to_dict('records')
        # 先處理一般的情況 只要有在per_usages中 就全部併入
        for row in rows: # row代表 誤用名的誤用學名使用
            usage = json.loads(row.get('per_usages'))
            current_name_id = row.get('taxon_name_id')
            accepted_taxon_name_id = row.get('accepted_taxon_name_id')
            # reference_id = row.get('reference_id')
            # 原則: 只併入misapplied的本身 其他底下的usage不收錄
            # 先處理從usage去抓
            is_pro_parte = False
            for uu in usage:
                if uu.get('pro_parte') == True:
                    is_pro_parte = True
                # # 相同引用
                # if refs[refs.reference_id==uu.get('reference_id')].publish_year.values[0] > row['publish_year']:
                #     # NOTE 這邊應該是錯誤的 理論上per_usages中的要是比較舊的文獻才對
                #     print('mm', mm, 'uu', uu)
                #     pass
                # else:
                # 誤用名本身的接受學名使用
                # 先修改 ref_group_pair_total
                now_ru_id = []
                if len(ref_group_pair_total[(ref_group_pair_total.reference_id==uu.get('reference_id'))&(ref_group_pair_total.accepted_taxon_name_id==current_name_id)]):
                    # 只併入有效 / 誤用的學名使用  其他無效名移除
                    ref_group_pair_total = ref_group_pair_total[~((ref_group_pair_total.reference_id==uu.get('reference_id'))&(ref_group_pair_total.accepted_taxon_name_id==current_name_id)&(ref_group_pair_total.ru_status=='not-accepted'))]
                    # NOTE 在這步先修改accepted_taxon_name_id 以便後面的分組 但最後存資料庫的時候要改回來 以後對資料才對得起來
                    # 用ru_id比較不會出錯 因為會去修改usage的status & accepted name (now_ru_id = 誤用名的接受學名使用)
                    # 這邊的now_ru_id應該要變成list才對 因為有可能是有效 or 誤用
                    now_ru_id = ref_group_pair_total[(ref_group_pair_total.reference_id==uu.get('reference_id'))&(ref_group_pair_total.accepted_taxon_name_id==current_name_id)].ru_id.to_list()
                    # NOTE 2024-12-21 reference_id 也修改 反正是在年代之後 不然後面會有影響
                    ref_group_pair_total.loc[ref_group_pair_total.ru_id.isin(now_ru_id), 'ru_status'] = 'misapplied'
                    ref_group_pair_total.loc[ref_group_pair_total.ru_id.isin(now_ru_id), 'accepted_taxon_name_id'] = accepted_taxon_name_id
                    ref_group_pair_total.loc[ref_group_pair_total.ru_id.isin(now_ru_id), 'reference_id'] = row.get('reference_id')
            # 找出 *誤用學名的對應有效學名使用* 的 is_in_taiwan 以下跟per_usage無關
            if not is_pro_parte:
                if len(ref_group_pair_total[(ref_group_pair_total.accepted_taxon_name_id==row.get('accepted_taxon_name_id'))&(ref_group_pair_total.reference_id==row.get('reference_id'))&(ref_group_pair_total.ru_status=='accepted')]):
                    current_is_in_taiwan = ref_group_pair_total[(ref_group_pair_total.accepted_taxon_name_id==row.get('accepted_taxon_name_id'))&(ref_group_pair_total.reference_id==row.get('reference_id'))&(ref_group_pair_total.ru_status=='accepted')].is_in_taiwan.values[0]
                    # 如果is_in_taiwan = 1 即使per_usages中沒有提到該文獻 只要也將誤用名設為有效 且文獻較舊 則一樣納入
                    # TODO 如果有 pro parte 這邊就不併入較早的文獻
                    if current_is_in_taiwan == '1':
                        publish_year = row.get('publish_year')
                        # 需要判斷年份的順序 & is_in_taiwan = 1
                        # 如果有對應的 tmp_taxon_id 改成相同的 tmp_taxon_id 並將地位改為誤用
                        if len(ref_group_pair_total[(ref_group_pair_total.accepted_taxon_name_id==current_name_id)&(ref_group_pair_total.is_in_taiwan=='1')&(ref_group_pair_total.publish_year<publish_year)]):
                            merging_refs = ref_group_pair_total[(ref_group_pair_total.ru_status=='accepted')&(ref_group_pair_total.accepted_taxon_name_id==current_name_id)&(ref_group_pair_total.is_in_taiwan=='1')&(ref_group_pair_total.publish_year<publish_year)].reference_id.unique()
                            for mr in merging_refs:
                                # 只併入有效&誤用的學名使用 其他無效名移除
                                ref_group_pair_total = ref_group_pair_total[~((ref_group_pair_total.reference_id==mr)&(ref_group_pair_total.accepted_taxon_name_id==current_name_id)&(ref_group_pair_total.ru_status=='not-accepted'))]
                                # results = results[~((results.reference_id==mr)&(results.accepted_taxon_name_id==current_name_id)&(results.ru_status=='not-accepted'))]
                                # NOTE 在這步先修改accepted_taxon_name_id 以便後面的分組 但最後存資料庫的時候要改回來 以後對資料才對得起來
                                # 用ru_id比較不會出錯 因為會去修改usage的status & accepted name (now_ru_id = 誤用名的接受學名使用)
                                # 這邊的now_ru_id應該要變成list才對 因為有可能是有效 or 誤用
                                now_ru_id = ref_group_pair_total[(ref_group_pair_total.reference_id==mr)&(ref_group_pair_total.accepted_taxon_name_id==current_name_id)].ru_id.to_list()
                                ref_group_pair_total.loc[ref_group_pair_total.ru_id.isin(now_ru_id), 'ru_status'] = 'misapplied'
                                ref_group_pair_total.loc[ref_group_pair_total.ru_id.isin(now_ru_id), 'accepted_taxon_name_id'] = accepted_taxon_name_id
                                ref_group_pair_total.loc[ref_group_pair_total.ru_id.isin(now_ru_id), 'reference_id'] = row.get('reference_id')



    results = ref_group_pair_total
    results = results.drop(columns=['check_list_type','taxon_id'])

    # 排除俗名backbone (前面的步驟應該已經將對應的taxon_name_id usage抓進來了)
    results = results[results.reference_id!=95]
    # 從 stauts=accepted 的 進行名錄更新流程
    results = results[results.ru_status=='accepted']
    results = results.drop_duplicates()
    results = results.replace({np.nan:None})
    results = results.reset_index(drop=True)
    results = results.sort_values('rank_id').reset_index(drop=True)


    count = 0
    total_df = pd.DataFrame(columns=['ru_id','reference_id','accepted_taxon_name_id'])

    ref_group_pair_total_obj = ref_group_pair_total[(ref_group_pair_total.object_group.notnull())&(ref_group_pair_total.ru_status=='accepted')][['reference_id','accepted_taxon_name_id','object_group']].drop_duplicates()
    ref_group_pair_total_misapplied = ref_group_pair_total[ref_group_pair_total.ru_status=='misapplied'][['reference_id','accepted_taxon_name_id','taxon_name_id']].drop_duplicates()

    for i in results.index: # 33155
        row = results.iloc[i]
        if row.ru_id not in total_df.ru_id.to_list():
            new_names = [row.taxon_name_id]
            name_list = [row.taxon_name_id]
            df = pd.DataFrame(columns=['reference_id','accepted_taxon_name_id'])
            # 取得相關學名
            while len(new_names) > 0:
                for nn in new_names:
                    if nn in new_names:
                        object_group = name_df[name_df.taxon_name_id==nn].object_group.values[0]
                        new_names, df, name_list = get_related_names(taxon_name_id=nn, df=df, new_names=new_names, name_list=name_list, 
                                                                    ref_group_pair_now=ref_group_pair_total, 
                                                                    object_group=object_group, 
                                                                    ref_group_pair_now_obj=ref_group_pair_total_obj, 
                                                                    ref_group_pair_now_misapplied=ref_group_pair_total_misapplied)
                    if not new_names:
                        break
            # 排除掉related_name中 status是misapplied的name
            df = df.drop_duplicates().reset_index(drop=True)
            df = df.merge(ref_group_pair_total[['ru_id','reference_id','accepted_taxon_name_id','taxon_name_id']])
            # return回來再merge ref_group_pair
            # 如果ref & group已存在在其他tmp_taxon_id，則納入該tmp_taxon_id分類群 
            if len(df):
                check_if_taxon_id = pd.DataFrame()
                if len(total_df):
                    check_if_taxon_id = total_df.merge(df)
                if len(check_if_taxon_id):
                    if len(check_if_taxon_id.tmp_taxon_id.unique()) == 1:
                        df['tmp_taxon_id'] = check_if_taxon_id.tmp_taxon_id.unique()[0]
                    else:
                        # 把全部都改成同一個tmp_taxon_id
                        tmp_taxon_id = check_if_taxon_id.tmp_taxon_id.unique()[0]
                        total_df.loc[total_df.ru_id.isin(check_if_taxon_id.ru_id.to_list()),'tmp_taxon_id'] = tmp_taxon_id
                        df['tmp_taxon_id'] = tmp_taxon_id
                else:
                    count += 1
                    tmp_taxon_id = count
                    df['tmp_taxon_id'] = tmp_taxon_id
                total_df = pd.concat([total_df,df], ignore_index=True)
                total_df = total_df.drop_duplicates()


    total_df = total_df[['reference_id', 'accepted_taxon_name_id','ru_id', 'taxon_name_id', 'tmp_taxon_id']].merge(ref_group_pair_total)

    # 應該不會有新的學名使用需要加入 因為已經用整個usage去跑

    total_df = total_df.drop_duplicates()

    # 判斷分群中誰為最新
    # 取最新接受名，其他為同物異名或誤用名
    # reference_id, group, taxon_name_id
    # 抓status, publish_year

    total_df = total_df.merge(refs[['reference_id','type','doi','publish_date']])


    # 決定誰是接受學名
    taxon_list = total_df.tmp_taxon_id.unique()
    len(taxon_list) 

    total_df['is_latest'] = False
    conn = pymysql.connect(**db_settings)

    cannot_decide = []
    for t in taxon_list:  # 11925
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        latest_ru_id_list = check_latest(temp=temp, conn=conn)
        if not len(latest_ru_id_list):
            cannot_decide.append(t)
        else:
            total_df.loc[total_df.tmp_taxon_id==t, 'is_latest'] = False
            total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True

    # 分類觀檢查



    # 2 同模異名檢查
    # TODO 這邊的白名單改用資料庫抓

    whitelist_list_1, whitelist_list_2, whitelist_list_3 = get_whitelist(conn)

    check_obj = total_df[(total_df.ru_status!='misapplied')&(~total_df.ru_id.isin(whitelist_list_1))][['object_group','tmp_taxon_id']].drop_duplicates().groupby(['object_group'], as_index=False).count()
    check_obj_list = check_obj[check_obj.tmp_taxon_id>1].object_group.unique()
    # len(check_obj_list) # 597

    reset_is_latest_list = []
    cannot_decide = []

    # c = 0
    for ooo in check_obj_list: # 7100,5341 / 7645, 5036
        # c += 1
        # if c % 100 == 0:
        #     print(c)
        # 改用同模本身的usage判斷
        # 所有的同模式學名之學名使用，應併入文獻優先的學名使用對應的有效學名的分類群。
        # 整群併入 (accepted_name_id + reference_id相同的) 但要是同模accepted_name_id
        temp = total_df[(total_df.object_group==ooo)&(total_df.ru_status!='misapplied')]
        # ≈ = temp[temp.ru_status=='accepted'] # 同模的accepted usages
        rows = total_df[total_df.tmp_taxon_id.isin(temp.tmp_taxon_id.unique())]
        newest_ru_id_list = check_status_latest(temp=temp, conn=conn)
        reset_is_latest_list += list(temp.tmp_taxon_id.unique())
        if len(total_df[total_df.ru_id.isin(newest_ru_id_list)].tmp_taxon_id.unique()) == 1:
            # 併入的tmp_taxon_id
            merging_tmp_taxon_id = temp[temp.ru_id.isin(newest_ru_id_list)].tmp_taxon_id.values[0]
            # 所有同模accepted usages都併入同一個tmp_taxon_id 不管地位
            merging_pairs = temp[(temp.ru_status=='accepted')&(temp.tmp_taxon_id!=merging_tmp_taxon_id)][['accepted_taxon_name_id','reference_id']].drop_duplicates().to_dict('records')
            merging_ru_ids = temp.ru_id.to_list() # 這邊就會包含單純的無效名 (接受名非同模)
            for mm in merging_pairs: # 這邊會包含如果同模異名是accepted 併入整個無效名
                merging_ru_ids += rows[(rows.accepted_taxon_name_id==mm.get('accepted_taxon_name_id'))&(rows.reference_id==mm.get('reference_id'))].ru_id.to_list()
            total_df.loc[total_df.ru_id.isin(merging_ru_ids),'tmp_taxon_id'] = merging_tmp_taxon_id
        # else:
        #     cannot_decide.append({'object_group': ooo, 'ru_id': temp[['ru_id', 'rank_id','reference_id', 'taxon_name_id', 'accepted_taxon_name_id', 'original_taxon_name_id', 'tmp_taxon_id']]})


    # # print(cannot_decide) 
    # for cc in cannot_decide:
    #     print('object_group', cc['object_group'])
    #     print(cc['ru_id'].sort_values('tmp_taxon_id'))
    # # 有可能會一直有cannot_decide的資料存在 同模異名若最新文獻是同一篇 代表兩個是獨立taxon


    reset_is_latest_list = list(dict.fromkeys(reset_is_latest_list))

    cannot_decide = []

    # c = 0
    for t in reset_is_latest_list: # 
        # c+=1
        # if c % 1000 == 0:
        #     print(c)
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True


    # 同學名 

    check_status = total_df[(total_df.object_group.isnull())&(total_df.ru_status.isin(['accepted','not-accepted']))][['taxon_name_id','tmp_taxon_id']].drop_duplicates().groupby(['taxon_name_id'], as_index=False).count()
    check_status_list = check_status[check_status.tmp_taxon_id>1].taxon_name_id.unique()
    check_status_list = [cs for cs in check_status_list if cs not in whitelist_list_2]     

    reset_is_latest_list = []

    # 學名之間可能會互相影響 


    has_more = True
    while has_more:
        for ccc in check_status_list:
            # 用學名本身的usage判斷
            temp = total_df[(total_df.taxon_name_id==ccc)&(total_df.ru_status!='misapplied')]
            reset_is_latest_list += list(temp.tmp_taxon_id.unique())
            newest_ru_id_list = check_status_latest(temp=temp, conn=conn)
            if len(newest_ru_id_list) == 1:
                newest_ru_id = newest_ru_id_list[0]
                # 併入的tmp_taxon_id
                merging_tmp_taxon_id = temp[temp.ru_id==newest_ru_id].tmp_taxon_id.values[0]
                # 如果其他異名在另一個分類群為有效 整群併入
                accepted_tmp_taxon_ids = temp[(temp.taxon_name_id==ccc)&(temp.is_latest==True)&(temp.ru_status=='accepted')].tmp_taxon_id.to_list()
                total_df.loc[total_df.tmp_taxon_id.isin(accepted_tmp_taxon_ids),'tmp_taxon_id'] = merging_tmp_taxon_id
                # 如果其他異名在另一個分類群為無效 只併入無效的該筆學名使用併入
                not_accepted_ru_ids = temp[(temp.taxon_name_id==ccc)&~(temp.tmp_taxon_id.isin(accepted_tmp_taxon_ids)&(temp.ru_status=='not-accepted'))].ru_id.to_list()
                total_df.loc[total_df.ru_id.isin(not_accepted_ru_ids),'tmp_taxon_id'] = merging_tmp_taxon_id
            else:
                cannot_decide.append(ccc)
            # cannot_decide.append(ccc)
        check_status = total_df[total_df.ru_status.isin(['accepted','not-accepted'])][['taxon_name_id','tmp_taxon_id']].drop_duplicates().groupby(['taxon_name_id'], as_index=False).count()
        check_status_list = check_status[check_status.tmp_taxon_id>1].taxon_name_id.unique()
        # 無法決定的就跳過
        check_status_list = [cs for cs in check_status_list if cs not in whitelist_list_2 and cs not in cannot_decide]
        # loop_count += 1
        if not len(check_status_list):
            has_more = False


    # 若沒有把所有需要重新決定最新的分類群處理完 會造成後面有問題

    reset_is_latest_list = list(dict.fromkeys(reset_is_latest_list))

    cannot_decide = []


    conn = pymysql.connect(**db_settings)

    for t in reset_is_latest_list: # 3403
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True


    # 如果在一個tmp_taxon_id裡，最新文獻中有兩個有效分類群，則代表可能有物種拆分的情況產生
    total_df = total_df.drop_duplicates()
    total_df = total_df.reset_index(drop=True)
    total_df = total_df.replace({np.nan: None})

    # step 3. 若分類群中有兩筆最新接受名，且為上下階層的關係，將其獨立

    check_tmp = total_df[(total_df.is_latest==1)&(total_df.ru_status=='accepted')][['tmp_taxon_id','reference_id','accepted_taxon_name_id']].drop_duplicates().groupby(['tmp_taxon_id','reference_id'], as_index=False).count()
    check_tmp_taxon_id = check_tmp[check_tmp.accepted_taxon_name_id>1].tmp_taxon_id.to_list()

    reset_is_latest_list = []

    # NOTE 目前會被組在一起的情況
    # 1. 相同accepted_taxon_name_id的學名使用
    # 2. 承名種下的學名使用
    # 3. 接受名為同模式學名的學名使用（原始組合名本身、有相同的原始組合名、是對方的原始組合名）

    # 需要拆分的一定是 1. 承名種下的關係 2. 同模式學名關係
    # 先處理上下階層的關係

    no_parent = [] 
    # 要確定 check_tmp_taxon_id 中 有哪些需要加入no_parent
    for ctt in check_tmp_taxon_id:
        rows = total_df[total_df.tmp_taxon_id==ctt]
        # 如果有任兩個最新接受名彼此是上下階層的關係 則加入no_parent判斷
        rows_latest = rows[(rows.is_latest==True)&(rows.ru_status=='accepted')]
        rows_latest_acp_name = rows_latest.taxon_name_id.to_list()
        for rlan in rows_latest_acp_name:
            if len(rows_latest[rows_latest.parent_taxon_name_id==rlan]):
                # print(ctt)
                no_parent.append(ctt)

    # 上階層同為最新接受名的情況
    for s in no_parent: # 214
        rows = total_df[total_df.tmp_taxon_id==s]
        # 限定最新接受名是種下階層
        # 有可能兩個都是種下 用max_layer_count來判斷誰是下階層
        rows_latest = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)&(rows.rank_id.isin(sub_lin_ranks))]
        max_layer_count = rows_latest.layer_count.max()
        # 上階層給予新的tmp_taxon_id
        parent_taxon_name_id = rows_latest[rows_latest.layer_count==max_layer_count].parent_taxon_name_id.values[0]
        current_accepted_taxon_name_id = rows_latest[rows_latest.layer_count==max_layer_count].accepted_taxon_name_id.values[0]
        # 2024-12 這邊直接改成按照階層分
        df_parent = rows[rows.layer_count==max_layer_count]
        df = rows[rows.layer_count!=max_layer_count]
        new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
        reset_is_latest_list.append(s)
        reset_is_latest_list.append(new_tmp_taxon_id)
        # 給予下階層新的tmp_taxon_id
        total_df.loc[total_df.ru_id.isin(df.ru_id.to_list()),'tmp_taxon_id'] = new_tmp_taxon_id
        total_df, cannot_decide = reset_latest(total_df,[s,new_tmp_taxon_id], conn)


    # 20241209
    reset_is_latest_list = list(dict.fromkeys(reset_is_latest_list))

    cannot_decide = []

    for t in reset_is_latest_list: # 428
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True

    # print(cannot_decide)


    # step 7. 若最新接受名是種下，需檢查種階層有沒有包含在裡面，有的話將其獨立
    # NOTE 確認剩下的check_tmp_taxon_id是不是都是承名種下
    # 是的話應該會一起出現在下方的no_parent中


    # 這邊好像不一定一定是種下 也有可能種被設定為最新接受名 因為group order的關係 -> 最新文獻為同一篇 但接受名不同 -> 移到 step 8
    # 處理種階層可能被包在種下的無效情況
    sub_tmp_list = list(total_df[(total_df.ru_status=='accepted')&(total_df.is_latest==True)&(total_df.rank_id.isin(sub_lin_ranks))].tmp_taxon_id.unique())

    no_parent = []

    c = 0
    for s in sub_tmp_list: # 4274
        c += 1
        if c % 100 == 0:
            print(c)
        rows = total_df[total_df.tmp_taxon_id==s]
        ref_group_pair = ref_group_pair_total[ref_group_pair_total.ru_id.isin(rows.ru_id.to_list())]
        # # 確認是不是所有layer_count都相同
        if len(ref_group_pair.layer_count.unique()) > 1: 
            # 確認自己的上階層是不是在分開的taxon 
            max_layer_count = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].layer_count.max()
            parent_taxon_name_id = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)&(rows.layer_count==max_layer_count)].parent_taxon_name_id.values[0]
            current_accepted_taxon_name_id = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)&(rows.layer_count==max_layer_count)].accepted_taxon_name_id.values[0]
            # 也要考慮 跨越兩層被組在一起的情況
            # 有上階層的接受學名使用 // 如果是上階層是分開taxon的無效名也可以
            # 1. 自己 和 自己的上階層被組在一起
            if len(rows[(rows.taxon_name_id==parent_taxon_name_id)&(rows.accepted_taxon_name_id!=current_accepted_taxon_name_id)&(rows.ru_status!='misapplied')]):
                # 如果上階層已經在其他分類群 在這邊可以忽略 因為有同物異名的關係 在後面會判斷誰要併入誰
                if not len(total_df[(~total_df.ru_id.isin(rows.ru_id.to_list()))&(total_df.taxon_name_id==parent_taxon_name_id)&(total_df.accepted_taxon_name_id!=current_accepted_taxon_name_id)&(total_df.ru_status!='misapplied')]):
                    no_parent.append(s)
            elif len(total_df[(total_df.taxon_name_id==parent_taxon_name_id)&(total_df.ru_status!='misapplied')]):
                # 2. 自己 和 自己上階層的上階層被組在一起
                # 先找到自己的上階層
                # 自己的上階層的上階層
                p_parent = total_df[(total_df.taxon_name_id==parent_taxon_name_id)&(total_df.ru_status!='misapplied')].parent_taxon_name_id.unique()
                if len(rows[(rows.taxon_name_id.isin(p_parent))&(rows.accepted_taxon_name_id!=current_accepted_taxon_name_id)&(rows.ru_status!='misapplied')]):
                    # 如果上上階層已經在其他分類群 在這邊可以忽略 因為有同物異名的關係 在後面會判斷誰要併入誰
                    if not len(total_df[(~total_df.ru_id.isin(rows.ru_id.to_list()))&(total_df.taxon_name_id.isin(p_parent))&(total_df.accepted_taxon_name_id!=current_accepted_taxon_name_id)&(total_df.ru_status!='misapplied')]):
                        no_parent.append(s)

    no_parent = list(dict.fromkeys(no_parent))


    cannot_decide = []
    # 處理上階層被合併在一起 但不是最新接受名的情況
    for s in no_parent: # 179
        rows = total_df[total_df.tmp_taxon_id==s]
        # 限定最新接受名是種下階層
        rows_latest = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)&(rows.rank_id.isin(sub_lin_ranks))]
        max_layer_count = rows_latest.layer_count.max()
        # 有可能兩個都是種下 用max_layer_count來判斷誰是下階層
        # 上階層的同物異名, 同模異名, 基礎名本身分為一群 給予新的tmp_taxon_id
        parent_taxon_name_id = rows_latest[rows_latest.layer_count==max_layer_count].parent_taxon_name_id.values[0]
        current_accepted_taxon_name_id = rows_latest[rows_latest.layer_count==max_layer_count].accepted_taxon_name_id.values[0]
        # 這邊直接改成按照階層分
        df_parent = rows[rows.layer_count==max_layer_count]
        df = rows[rows.layer_count!=max_layer_count]
        new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
        reset_is_latest_list.append(s)
        reset_is_latest_list.append(new_tmp_taxon_id)
        # 給予下階層新的tmp_taxon_id
        total_df.loc[total_df.ru_id.isin(df.ru_id.to_list()),'tmp_taxon_id'] = new_tmp_taxon_id
        total_df, cannot_decide = reset_latest(total_df,[s,new_tmp_taxon_id],conn)

    reset_is_latest_list = list(dict.fromkeys(reset_is_latest_list))

    cannot_decide = []

    for t in reset_is_latest_list: # 1380
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True


    # step 8. 若承名關係最新接受名為種，種與種下各自有有效的學名使用，且除backbone外沒有其他文獻指出他們為同物異名，將承名種下獨立出來

    spe_tmp_list = list(total_df[(total_df.ru_status=='accepted')&(total_df.is_latest==True)&(total_df.rank_id==34)&(total_df.autonym_group.notnull())].tmp_taxon_id.unique())

    reset_is_latest_list = []
    cannot_decide = []

    # 承名種下有被非backbone的文獻設定成同物異名

    for s in spe_tmp_list: # 1273
        rows = total_df[total_df.tmp_taxon_id==s]
        parent_auto_group = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].autonym_group.values[0]
        max_layer_count = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].layer_count.values[0]
        # parent_object_group = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].object_group.values[0]
        parent_taxon_name_id = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].taxon_name_id.values[0]
        # 先確定承名種下是不是在同一個taxon中 且有有效的學名使用
        sub_accepted_rows = rows[(rows.autonym_group==parent_auto_group)&(rows.parent_taxon_name_id==parent_taxon_name_id)&(rows.ru_status=='accepted')]
        if len(sub_accepted_rows):
            current_accepted_taxon_name_id = sub_accepted_rows.taxon_name_id.values[0]
            # 確認沒有除了backbone以外的同物異名關係
            # 種是種下的同物異名 / 種下是種的同物異名
            if not len(rows[(rows.accepted_taxon_name_id==parent_taxon_name_id)&(rows.taxon_name_id==current_accepted_taxon_name_id)&(rows.type!=4)&(rows.ru_status=='not-accepted')]) and not len(rows[(rows.accepted_taxon_name_id==current_accepted_taxon_name_id)&(rows.taxon_name_id==parent_taxon_name_id)&(rows.type!=4)&(rows.ru_status=='not-accepted')]):
                # 這邊直接改成按照階層分
                # df_parent = rows[rows.layer_count==max_layer_count]
                df = rows[rows.layer_count!=max_layer_count]
                new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
                reset_is_latest_list.append(s)
                reset_is_latest_list.append(new_tmp_taxon_id)
                # 給予下階層新的tmp_taxon_id
                total_df.loc[total_df.ru_id.isin(df.ru_id.to_list()),'tmp_taxon_id'] = new_tmp_taxon_id
                total_df, cannot_decide = reset_latest(total_df,[new_tmp_taxon_id,s],conn)


    # print(cannot_decide)

    reset_is_latest_list = list(dict.fromkeys(reset_is_latest_list))

    cannot_decide = []

    # c = 0
    for t in reset_is_latest_list: # 444
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True

    # NOTE 不檢查

    # step 10. 確認誤用在分類群的地位
    # 同時出現誤用與無效：若與同一分類群的有效名為同模，都改成無效。若與有效名非同模，判斷文獻優先性決定是誤用或無效，都改為判斷結果。
    # 誤用名若與同一分類群的有效名為同模式異名，需改為無效名。

    check_misapplied_list = total_df[total_df.ru_status=='misapplied'].tmp_taxon_id.unique()

    # has_more_than_one_status = []
    need_new_taxon_misapplied = []

    for t in check_misapplied_list:
        rows = total_df[total_df.tmp_taxon_id==t]
        misapplied_name_ids = rows[rows.ru_status=='misapplied'].taxon_name_id.unique()
        for mm in misapplied_name_ids:            
            mm_rows = rows[rows.taxon_name_id==mm]
            # 先確定和同一分類群的有效名為同模是不是同模式異名
            acp_name_object_group = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].object_group.values[0]
            misapplied_object_group = mm_rows.object_group.values[0]
            is_obj_syns = False
            if misapplied_object_group and acp_name_object_group and misapplied_object_group == acp_name_object_group:
                is_obj_syns = True
            if len(mm_rows[mm_rows.ru_status!='accepted'].ru_status.unique()) > 1:
                if is_obj_syns:
                    # 若與同一分類群的有效名為同模，都改成無效。
                    # 只修改原本地位為無效或誤用 有效的維持有效
                    total_df.loc[total_df.ru_id.isin(mm_rows[mm_rows.ru_status!='accepted'].ru_id.to_list()),'ru_status'] = 'not-accepted'
                else:
                    # 若與有效名非同模，判斷文獻優先性決定是誤用或無效，都改為判斷結果。
                    latest_misapplied_ru = check_status_latest(temp=mm_rows, conn=conn)
                    if len(latest_misapplied_ru) == 1:
                        latest_misapplied_ru = latest_misapplied_ru[0]
                        current_status = mm_rows[mm_rows.ru_id==latest_misapplied_ru].ru_status.values[0]
                        if current_status == 'misapplied':
                            # 應該要先確認誤用名是不是在其他獨立的taxon 且地位非誤用
                            # TODO 如果是同模異名也不需要拿走
                            if len(total_df[(total_df.taxon_name_id==mm)&(total_df.ru_status=='accepted')&(total_df.tmp_taxon_id==t)]) and not len(total_df[(total_df.taxon_name_id==mm)&(total_df.ru_status!='misapplied')&(total_df.tmp_taxon_id!=t)]):
                                need_new_taxon_misapplied.append(mm)
                        total_df.loc[total_df.ru_id.isin(mm_rows[mm_rows.ru_status!='accepted'].ru_id.to_list()),'ru_status'] = current_status
            else: # 只有誤用一種地位
                # 誤用名若與同一分類群的有效名為同模式異名，需改為無效名。
                # 確認是不是同模
                if is_obj_syns:
                    total_df.loc[total_df.ru_id.isin(mm_rows[mm_rows.ru_status=='misapplied'].ru_id.to_list()),'ru_status'] = 'not-accepted'



    cannot_decide = []

    for t in check_misapplied_list: # 600
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        latest_ru_id_list = check_latest(temp=temp, conn=conn)
        if not len(latest_ru_id_list):
            cannot_decide.append(t)
        else:
            total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
            total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True

    reset_is_latest_list = []

    for mm in need_new_taxon_misapplied:
        if len(total_df[(total_df.taxon_name_id==mm)&(total_df.ru_status!='misapplied')]):
            rows = total_df[total_df.accepted_taxon_name_id==mm]
            now_tmp_taxon_id = rows.tmp_taxon_id.values[0]
            # 底下的同物異名要分配
            ref_group_pair = ref_group_pair_total[ref_group_pair_total.taxon_name_id==mm]
            new_names = [mm]
            name_list = [mm]
            df = pd.DataFrame(columns=['reference_id','accepted_taxon_name_id'])
            ref_group_pair_now_obj = ref_group_pair[(ref_group_pair.object_group.notnull())&(ref_group_pair.ru_status=='accepted')][['reference_id','accepted_taxon_name_id','object_group']].drop_duplicates()
            while len(new_names) > 0:
                for nn in new_names:
                    if nn in new_names:
                        object_group = name_df[name_df.taxon_name_id==nn].object_group.values[0]
                        new_names, df, name_list = get_related_names_sub(taxon_name_id=nn, 
                                                                            df=df, 
                                                                            new_names=new_names, 
                                                                            name_list=name_list, 
                                                                            ref_group_pair_now=ref_group_pair, 
                                                                            object_group=object_group, 
                                                                            ref_group_pair_now_obj=ref_group_pair_now_obj, 
                                                                            )
            df = df.drop_duplicates()
            df = df.merge(ref_group_pair)
            # 底下同物異名也要拿過去 前面已經判斷過了 之前取就好
            now_syns = df[df.ru_status!='misapplied'].taxon_name_id.unique()
            syns_acp = rows[(rows.taxon_name_id.isin(now_syns))&(rows.ru_status!='misapplied')].accepted_taxon_name_id.unique()
            new_ru_list = df.ru_id.to_list()
            new_ru_list += rows[(rows.taxon_name_id.isin(now_syns))&(rows.ru_status!='misapplied')].ru_id.to_list()
            new_ru_list += rows[(rows.accepted_taxon_name_id.isin(syns_acp))].ru_id.to_list()
            new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
            reset_is_latest_list.append(now_tmp_taxon_id)
            reset_is_latest_list.append(new_tmp_taxon_id)
            total_df.loc[total_df.ru_id.isin(new_ru_list),'tmp_taxon_id'] = new_tmp_taxon_id


    reset_is_latest_list = list(dict.fromkeys(reset_is_latest_list))

    cannot_decide = []

    for t in reset_is_latest_list: # 1802
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True


    # NOTE 不檢查

    total_df['taxon_status'] = ''

    # c = 0  
    for i in total_df.tmp_taxon_id.unique(): # 11649
        # c += 1
        # if c % 100 == 0:
        #     print(c)
        accepted_name_id = total_df[(total_df['tmp_taxon_id'] == i) & (total_df['ru_status'] == 'accepted') & (total_df['is_latest'] == 1)]['taxon_name_id'].to_list()[0]
        not_accepted_name_ids = total_df[(total_df['tmp_taxon_id'] == i) & (total_df['ru_status'] == 'not-accepted') & (total_df['is_latest'] == 1)]['taxon_name_id'].to_list()
        # 誤用地位已經在前面的步驟確認了 所以這邊一定是誤用沒錯
        misapplied_name_ids = total_df[(total_df['tmp_taxon_id'] == i) & (total_df['ru_status'] == 'misapplied')]['taxon_name_id'].to_list()
        total_df.loc[(total_df.tmp_taxon_id==i)&(total_df.taxon_name_id.isin(misapplied_name_ids))&(total_df.taxon_name_id!=accepted_name_id)&(~total_df.taxon_name_id.isin(not_accepted_name_ids)),'taxon_status'] = 'misapplied'
        total_df.loc[(total_df.tmp_taxon_id==i)&(total_df.taxon_name_id==accepted_name_id),'taxon_status'] = 'accepted'
        total_df.loc[(total_df.tmp_taxon_id==i)&(total_df.taxon_status==''),'taxon_status'] = 'not-accepted'


    total_df = total_df.drop_duplicates()
    total_df = total_df.reset_index(drop=True)

    # 這邊在串回來的時候 要把accepted_taxon_name_id改回原本的 
    # 直接用ru_id串?
    conn = pymysql.connect(**db_settings)


    # 俗名backbone
    # 要確認是不是需要加上TaiCOL backbone
    if use_common_name_backbone:
        query = '''SELECT properties, id, accepted_taxon_name_id, taxon_name_id, reference_id
                    FROM reference_usages WHERE deleted_at IS NULL AND reference_id = 95 AND taxon_name_id IN %s'''
        with conn.cursor() as cursor:
            execute_line = cursor.execute(query, (list(total_df.taxon_name_id.unique()),))
            common_names_rus = cursor.fetchall()
            common_names_rus = pd.DataFrame(common_names_rus, columns=['properties','ru_id','accepted_taxon_name_id', 'taxon_name_id','reference_id'])
            common_names_rus['publish_year'] = 1000 


    query = '''SELECT ru.properties, ru.id, ru.accepted_taxon_name_id, ru.taxon_name_id, ru.reference_id, r.subtitle, ru.type_specimens, ru.status
                FROM reference_usages ru
                JOIN `references` r ON r.id = ru.reference_id
                WHERE ru.deleted_at IS NULL AND ru.id IN %s'''

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, (list(total_df.ru_id.unique()),))
        rus = cursor.fetchall()
        rus = pd.DataFrame(rus, columns=['properties','ru_id','accepted_taxon_name_id', 'taxon_name_id','reference_id', 'subtitle', 'type_specimens', 'ru_status'])

    # 因為前面誤用有調整accepted_taxon_name_id 所以在這邊調整回來
    total_df = total_df.drop(columns=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'])
    total_df = total_df.merge(rus[['ru_id', 'accepted_taxon_name_id', 'taxon_name_id', 'reference_id']])
    total_df = total_df.replace({np.nan: None})

    # 匯入包含per_usage、模式、屬性、俗名等，只是這邊是簡易異名表顯示。臺

    # 新增欄位 / 自訂欄位 要用
    # 不用匯人的: 標註、台灣分布地、新紀錄、原生/外來備註、備註

    # 需彙整的欄位:
    # 1 common_names v -> properties
    # 2 新增 / 自訂欄位 v -> properties
    # 3 模式標本 v -> type_specimens
    # 4 per_usages -> 須依優先序決定pro parte的相關設定 v -> per_usages

    # 依照優先序決定
    # 1 is_系列 v -> properties
    # 屬以上存在於臺灣設定為未知(2)，種、種下依照usage。 v
    # 2 alien_type v -> properties





    # 產出後需要重新排序
    # 屬 名先字母排序排 ， 分類群 再依 「有效名 」 的字母排序 ，無效名 /誤用名自己在分類 群中依照字母排序 。


    # id
    # parent_taxon_name_id
    # reference_id
    # accepted_taxon_name_id
    # taxon_name_id
    # status
    # group
    # order
    # per_usages
    # type_specimens
    # properties
    # updated_at


    query = '''SELECT max(tmp_checklist_id) from tmp_namespace_usages;'''

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        tmp_checklist_id = cursor.fetchone()[0]
        tmp_checklist_id = tmp_checklist_id + 1 if tmp_checklist_id else 1

    final_usages = []

    for nt in total_df.tmp_taxon_id.unique():
        # now_count += 1
        # if now_count % 10 == 0:
        #     print(now_count)
        rows = total_df[total_df['tmp_taxon_id']==nt]
        # 這邊也許不用檢查是不是只有最新的一筆有效
        # 如果決定不出來的 在前面一步就先選擇一個作為有效名
        # if len(rows[(rows['is_latest']==True) & (rows['taxon_status'] == 'accepted')][['reference_id', 'accepted_taxon_name_id', 'taxon_name_id']].drop_duplicates()) == 1:
        try:
            i = rows[(rows['is_latest']==True) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row
            row = total_df.iloc[i] # 接受的row
            accepted_taxon_name_id = row.accepted_taxon_name_id
            parent_taxon_name_id = row.parent_taxon_name_id
            # 彙整
            ru_list = rows[['publish_year','ru_id']]
            tmp_ru_df = rus.merge(ru_list,left_on=['ru_id'],right_on=['ru_id']) # 這裡是加上publish_year
            now_prop = determine_prop(conn, rows, accepted_taxon_name_id, tmp_ru_df, refs)
            if rank_order_map[row.rank_id] <= rank_order_map[30]: # 屬以上 顯示未知
                now_prop['is_in_taiwan'] = 2
            # is_hybrid
            name_list = rows.taxon_name_id.unique()
            now_prop['is_hybrid'] = True if len(name_df[(name_df.taxon_name_id.isin(name_list))&(name_df.is_hybrid=='true')]) else False
            # 1 俗名
            # 從這邊要加上俗名backbone的資料
            # 俗名只需排除重複就好
            if use_common_name_backbone:
                common_rus = common_names_rus[common_names_rus.taxon_name_id.isin(rows.taxon_name_id.to_list())]
                if len(common_rus):
                    common_rus = common_rus.reset_index(drop=True)
                    tmp_ru_df = pd.concat([tmp_ru_df, common_rus], ignore_index=True)            
            common_names = []
            for p in tmp_ru_df.properties.values:
                try:
                    if prop := json.loads(p):
                        # print(prop)
                        if prop.get('common_names'):
                            common_names += prop.get('common_names')
                except Exception as e: 
                    print('common_name', e)
                    pass
            if len(common_names):
                common_names = pd.DataFrame(common_names)
                # area 有台灣 / Taiwan不同寫法 可能會有重複的common name
                common_names = common_names.replace({np.nan: None})
                common_names = common_names.drop(columns=['area'])
                common_names = common_names.drop_duplicates().to_dict('records')
                now_prop['common_names'] = common_names
            # 2 新增 / 自訂欄位 -> 全部組合在一起 並加上citation 依年份排序
            additional_fields = []
            custom_fields = []
            for p in tmp_ru_df.to_dict('records'):
                if prop := json.loads(p.get('properties')):
                    try:
                        if prop.get('additional_fields'):
                            for pp in prop.get('additional_fields'):
                                pp['reference_id'] = p.get('reference_id')
                                pp['publish_year'] = p.get('publish_year')
                                pp['subtitle'] = p.get('subtitle')
                                additional_fields.append(pp)
                        if prop.get('custom_fields'):
                            for pp in prop.get('custom_fields'):
                                pp['reference_id'] = p.get('reference_id')
                                pp['publish_year'] = p.get('publish_year')
                                pp['subtitle'] = p.get('subtitle')
                                custom_fields.append(pp)
                    except Exception as e: 
                        print('additional / custom fields', e)
                        pass
            # additional_fields -> 根據 field_name group 在一起
            if len(additional_fields):
                additional_fields = pd.DataFrame(additional_fields)
                # 要維持是additional_fields的欄位 但彙整在一起
                # merged_additional_fields
                additional_fields['formatted'] = additional_fields.apply(lambda row: f"{row['field_value']} ({row['subtitle']})", axis=1)
                # 依據 field_name 和 publish_year 排序
                additional_fields = additional_fields.sort_values(by=['field_name', 'publish_year'])
                # 根據 field_name 分組，合併 formatted 欄位
                additional_fields = additional_fields.groupby('field_name')['formatted'].apply('<br>'.join).reset_index()
                additional_fields = additional_fields.rename(columns={'formatted': 'field_value'})
                additional_fields = additional_fields.replace({np.nan: None})
                additional_fields = additional_fields.to_dict('records')
                now_prop['additional_fields'] = additional_fields
            # custom_fields -> 根據 field_name_en group 在一起
            if len(custom_fields):
                custom_fields = pd.DataFrame(custom_fields)
                # 要維持是custom_fields的欄位 但彙整在一起
                # merged_custom_fields
                custom_fields['formatted'] = custom_fields.apply(lambda row: f"{row['field_value']} ({row['subtitle']})", axis=1)
                # 依據 field_name_en 和 publish_year 排序
                custom_fields = custom_fields.sort_values(by=['field_name_en', 'publish_year'])
                # 根據 field_name_en 分組，合併 formatted 欄位
                custom_fields = custom_fields.groupby('field_name_en')['formatted'].apply('<br>'.join).reset_index()
                custom_fields = custom_fields.rename(columns={'formatted': 'field_value'})
                custom_fields = custom_fields.replace({np.nan: None})
                custom_fields = custom_fields.to_dict('records')
                now_prop['custom_fields'] = custom_fields
            # 3 模式標本
            type_specimens = []
            for p in tmp_ru_df.type_specimens.values:
                try:
                    if now_spe := json.loads(p):
                        type_specimens += now_spe
                except Exception as e: 
                    print('type_specimens', e)
                    pass
            if len(type_specimens):
                type_specimens = pd.DataFrame(type_specimens)
                # type_specimens = type_specimens.drop_duplicates()
                type_specimens = type_specimens.replace({np.nan: None})
                type_specimens = type_specimens.to_dict('records')
            # 4 per_usages
            per_usages = []
            for p in rows.to_dict('records'):
                if now_usages := json.loads(p.get('per_usages')):
                    per_usages += [{**item, 'including_usage_id': p.get('ru_id')}  for item in now_usages]
            if len(per_usages):
                per_usages = pd.DataFrame(per_usages)
                # 如果有reference_id重複時 要依including_usage_reference_id優先序選擇優先的那個
                duplicated_refs = per_usages[per_usages.reference_id.duplicated()].reference_id.unique()
                for ref in duplicated_refs:
                    temp = rows[rows.ru_id.isin(per_usages[per_usages.reference_id==ref].including_usage_id.to_list())]
                    chosen_ru_list = check_prop_status_latest(temp, conn)
                    removing_ru_id = [rr for rr in temp.ru_id.to_list() if rr not in chosen_ru_list]
                    per_usages = per_usages[~per_usages.including_usage_id.isin(removing_ru_id)]
                per_usages = per_usages.replace({np.nan: None})
                per_usages = per_usages.drop(columns=['including_usage_id']).to_dict('records')
            # 所有屬性 資訊跟著有效名
            # 其他學名就存成無效 or 誤用
            # 要先在介面回傳預覽表，OK後才存進去my_namespace_usage
            # 還是先存入一個暫存的表 確定後再存入my_namespace_usage 匯入 or 選擇不匯入之後 再將這個暫時表的內容刪除
            # 在這邊整理要存入properties的欄位
            # 不管是什麼地位 學名都只保留一個
            taxon_names = rows[['taxon_name_id','taxon_status','rank_id', 'nomenclature_id']].drop_duplicates().to_dict('records')
            for rrr in taxon_names:
                # print(rrr.get('taxon_name_id'))
                now_dict = {
                    'tmp_taxon_id': nt,
                    'taxon_name_id': rrr.get('taxon_name_id'),
                    'status': rrr.get('taxon_status'),
                    'rank_id': rrr.get('rank_id') # for後面排序用的
                }
                if rrr.get('taxon_status') == 'accepted':
                    now_prop['indications'] = []
                    now_dict['properties'] = safe_json_dumps(now_prop)
                    now_dict['parent_taxon_name_id'] = parent_taxon_name_id
                    now_dict['per_usages'] = safe_json_dumps(per_usages)
                    now_dict['type_specimens'] = safe_json_dumps(type_specimens)
                else:
                    now_new_prop = {}
                    now_indications = []
                    # indications
                    if rrr.get('taxon_status') == 'misapplied':
                        if rrr.get('nomenclature_id') == 1: #動物
                            now_indications = ['not of']
                        elif rrr.get('nomenclature_id') == 2: #植物
                            now_indications = ['auct. non']
                    elif rrr.get('taxon_status') == 'not-accepted':
                        merged_indications = []
                        for pp in tmp_ru_df[(tmp_ru_df.ru_status == 'not-accepted') & (tmp_ru_df.taxon_name_id== rrr.get('taxon_name_id'))].properties.values:
                            try:
                                pp = json.loads(pp)
                                if len(pp.get('indications')):
                                    merged_indications += pp.get('indications')
                            except:
                                pass
                        merged_indications = list(set(merged_indications))
                        now_indications = [m for m in merged_indications if m != 'syn. nov.']
                    now_new_prop['indications'] = now_indications
                    now_dict['properties'] = safe_json_dumps(now_new_prop)
                    now_dict['parent_taxon_name_id'] = None
                    now_dict['per_usages'] = '[]'
                    now_dict['type_specimens'] = '[]'
                final_usages.append(now_dict)
        except Exception as e: 
            print('merging', e)
            pass

            # taxon_name_id
            # status
            # group -> 分類群
            # order -> 排序（不管分類群的總排序）
            # 如果是accepted才會有
            # parent_taxon_name_id
            # per_usages
            # properties
            # type_specimens
            # 統一新增
            # tmp_checklist_id
            # updated_at

    # print(final_usages)

    # 排序

    # 科->科底下的屬->屬底下的種&種下
    # 除了科以外 應該可以直接用字母排
    # 先根據字母排 再根據自己的上階層排 
    # 先排有效名
    final_usages = pd.DataFrame(final_usages)

    # 先取出屬 & 屬以下的
    final_usages['rank_order'] = final_usages.rank_id.apply(lambda x: rank_order_map[x])
    accepted_usages = final_usages[(final_usages.status=='accepted')&(final_usages.rank_order>=rank_order_map[30])][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']].drop_duplicates()
    accepted_usages = accepted_usages.merge(name_df[['taxon_name_id','name']].drop_duplicates())
    # 直接按照字母排序
    accepted_usages = accepted_usages.sort_values('name')
    accepted_usages = accepted_usages.reset_index(drop=True)

    # 再把屬以上的加進去
    # 從下往上排
    family_usages = final_usages[(final_usages.status=='accepted')&(final_usages.rank_order<rank_order_map[30])][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']].drop_duplicates()
    family_usages = family_usages.merge(name_df[['taxon_name_id','name']].drop_duplicates()).sort_values(['rank_id','name'],ascending=False)

    for ff in family_usages.to_dict('records'):
        new_row = final_usages[(final_usages.tmp_taxon_id==ff.get('tmp_taxon_id'))&(final_usages.status=='accepted')][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']]
        if len(accepted_usages[accepted_usages.parent_taxon_name_id==ff.get('taxon_name_id')]):
            min_id = accepted_usages[accepted_usages.parent_taxon_name_id==ff.get('taxon_name_id')].index.min()
            accepted_usages = pd.concat([accepted_usages.iloc[:min_id], new_row, accepted_usages.iloc[min_id:]]).reset_index(drop=True)
        else:
            # 如果沒有的話就放在最前面 這邊也需要按照字母排
            accepted_usages = pd.concat([new_row, accepted_usages]).reset_index(drop=True)

    # 最後再把每個tmp_taxon_id的usage加進去
    other_usages = final_usages[final_usages.status!='accepted'][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']]
    
    if len(other_usages):
        other_usages = other_usages.merge(name_df[['taxon_name_id','name']].drop_duplicates())
        other_usages = other_usages.sort_values('name')
        for oo in other_usages.tmp_taxon_id.unique():
            max_id = accepted_usages[accepted_usages.tmp_taxon_id==oo].index.max()+1
            new_rows = other_usages[other_usages.tmp_taxon_id==oo]
            accepted_usages = pd.concat([accepted_usages.iloc[:max_id], new_rows, accepted_usages.iloc[max_id:]]).reset_index(drop=True)

    final_usage_df = accepted_usages.merge(final_usages)
    final_usage_df = final_usage_df.reset_index(drop=True)

    final_usage_df['order'] = final_usage_df.index


    group_keys = final_usage_df['tmp_taxon_id'].drop_duplicates().reset_index(drop=True)
    group_id_map = {k: i+1 for i, k in enumerate(group_keys)}

    final_usage_df['group'] = final_usage_df['tmp_taxon_id'].map(group_id_map)

    # 存入資料庫
    final_usage_df['tmp_checklist_id'] = tmp_checklist_id
    final_usage_df = final_usage_df[['parent_taxon_name_id','tmp_checklist_id','taxon_name_id','status','group','order','per_usages','type_specimens','properties']]
    
    db_string = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(db_settings.get('user'), db_settings.get('password'), db_settings.get('host'), db_settings.get('port'), db_settings.get('db'))
    db = create_engine(db_string)

    # print(final_usage_df)

    final_usage_df.to_sql('tmp_namespace_usages',
        con=db,
        if_exists='append',   # 'fail' | 'replace' | 'append'
        index=False,
        chunksize=1000         # 每次 insert 幾筆資料（依需求調整）
    )

    # 回傳tmp_checklist_id給工具 工具再用這個id回傳usage給工具前端

    return HttpResponse(json.dumps({'tmp_checklist_id': tmp_checklist_id}))
