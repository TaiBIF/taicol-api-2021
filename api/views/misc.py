import json
import pymysql
import pandas as pd
import numpy as np

from django.http import HttpResponse
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from api.views._base import db_settings, DateTimeEncoder


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


class TaxonVersionView(APIView):
    @swagger_auto_schema(
        operation_summary='物種有效名的版本紀錄',
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
