import json
import pymysql
import pandas as pd
import numpy as np

from django.http import HttpResponse
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from api.views._base import db_settings, DateTimeEncoder, reference_type_map, custom_reference_type_order


class ReferencesView(APIView):
    @swagger_auto_schema(
        operation_summary='取得文獻',
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
                    cursor.execute(query, (int(name_id),))
                    df = pd.DataFrame(cursor.fetchall(), columns=['usage_id', 'name_id', 'reference_id', 'reference_type', 'publish_year', 'citation', 'usage_status', 'accepted_name_id', 'indications', 'is_in_taiwan', 'is_endemic', 'alien_type', 'is_deleted'])
            # usage反查時不排除backbone
            elif usage_id := request.GET.get('usage_id'):
                query = f"SELECT ru.id, ru.taxon_name_id, ru.reference_id, r.type, r.publish_year, CONCAT_WS(' ' ,c.author, c.content), ru.status, ru.accepted_taxon_name_id, ru.properties->>'$.indications', \
                         JSON_EXTRACT(ru.properties, '$.is_in_taiwan'), JSON_EXTRACT(ru.properties, '$.is_endemic'), ru.properties->>'$.alien_type', r.deleted_at \
                         FROM reference_usages ru \
                         JOIN `references` r ON ru.reference_id = r.id \
                         LEFT JOIN api_citations c ON ru.reference_id = c.reference_id \
                         WHERE ru.id = %s  AND ru.status != '' \
                         AND ru.is_title != 1 AND ru.deleted_at IS NULL"
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
                df['is_endemic'] = df['is_endemic'].replace({0: False, 1: True, '0': False, '1': True})
                df['is_in_taiwan'] = df['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, 2: None, '2': None, '': None})
                df['indications'] = df['indications'].apply(lambda x:  ','.join(eval(x)) if x and x !='[]' else None)
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

            conn.close()

            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": len(df)}, "data": new_data}
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
