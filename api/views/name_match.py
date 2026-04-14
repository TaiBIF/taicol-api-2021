import json
import pymysql
import pandas as pd
import numpy as np
import requests

from django.http import HttpResponse
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from api.views._base import (
    db_settings, DateTimeEncoder, match_url,
    bio_group_map, reference_type_map, custom_reference_type_order,
)


class NameMatchView(APIView):
    @swagger_auto_schema(
        operation_summary='取得學名比對',
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
            namecode_list = []
            data = []

            conn = pymysql.connect(**db_settings)
            df = pd.DataFrame(columns=['taxon_id', 'taicol_name_status'])

            if name := request.GET.get('name'):

                best = request.GET.get('best')
                if best and not best in ['yes', 'no']:
                    response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameter value"}}
                    return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")
                elif not best:
                    best = 'yes'
                else:
                    best = request.GET.get('best')

                query_dict = {
                    'names': name,
                    'best': best,
                    'format': 'json',
                    'source': 'taicol'
                }

                only_taiwan = request.GET.get('only_taiwan', 'yes')

                if only_taiwan == 'yes':
                    query_dict['is_in_taiwan'] = True

                if ranks := request.GET.getlist('rank'):
                    query_dict['taxon_rank'] = ",".join(ranks)

                if kingdoms := request.GET.getlist('kingdom'):
                    query_dict['kingdom'] = ",".join([f'"{k}"' for k in kingdoms])

                if bio_group := request.GET.get('bio_group'):
                    if bio_group != 'all':
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
                                matched_name_accepted_usage = matched_name_accepted_usage.sort_values('publish_year', ascending=False).sort_values('reference_order')
                                matched_name_accepted_usage['publish_year'] = matched_name_accepted_usage['publish_year'].apply(lambda x: int(x) if x else None)
                                matched_name_accepted_usage.loc[matched_name_accepted_usage.reference_type.isin([4,6]), 'reference_id'] = None
                                matched_name_accepted_usage.loc[matched_name_accepted_usage.reference_type.isin([4,6]), 'publish_year'] = None
                                matched_name_accepted_usage.loc[matched_name_accepted_usage.reference_type.isin([4,6]), 'citation'] = 'TaiCOL Backbone'
                                matched_name_accepted_usage['reference_type'] = matched_name_accepted_usage['reference_type'].apply(lambda x: reference_type_map[x] if x else None)
                                matched_name_accepted_usage['publish_year'] = matched_name_accepted_usage['publish_year'].fillna(0).astype(int).replace({0: None})
                                matched_name_accepted_usage['reference_id'] = matched_name_accepted_usage['reference_id'].fillna(0).astype(int).replace({0: None})
                                for name_id in df.accepted_name_id.unique():
                                    df.loc[df.accepted_name_id==name_id,'matched_name_accepted_usage'] = json.dumps(matched_name_accepted_usage[matched_name_accepted_usage.accepted_name_id==name_id][['usage_id', 'reference_id', 'reference_type','publish_year', 'citation', 'is_in_taiwan']].to_dict('records'))
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
                                matched_name_usage = matched_name_usage.sort_values('publish_year', ascending=False).sort_values('reference_order')
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
