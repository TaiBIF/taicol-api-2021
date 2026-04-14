import json
import pandas as pd
import numpy as np
import requests

from django.http import HttpResponse
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from conf.settings import SOLR_PREFIX
from api.views._base import DateTimeEncoder
from api.utils import rank_map, redlist_map_rev, get_conditioned_solr_search


class TaxonView(APIView):
    @swagger_auto_schema(
        operation_summary='取得物種',
        manual_parameters=[
            openapi.Parameter(name='taxon_id', in_=openapi.IN_QUERY, description='物種ID', type=openapi.TYPE_STRING),
            openapi.Parameter(name='scientific_name', in_=openapi.IN_QUERY, description='學名', type=openapi.TYPE_STRING),
            openapi.Parameter(name='taxon_group', in_=openapi.IN_QUERY, description='分類群', type=openapi.TYPE_STRING),
            openapi.Parameter(name='rank', in_=openapi.IN_QUERY, description='階層', type=openapi.TYPE_STRING),
            openapi.Parameter(name='created_at', in_=openapi.IN_QUERY, description='建立日期', type=openapi.FORMAT_DATE),
            openapi.Parameter(name='updated_at', in_=openapi.IN_QUERY, description='更新日期', type=openapi.FORMAT_DATE),
            openapi.Parameter(name='limit', in_=openapi.IN_QUERY, description='每頁限制筆數', type=openapi.TYPE_INTEGER),
            openapi.Parameter(name='offset', in_=openapi.IN_QUERY, description='指定每頁起始編號', type=openapi.TYPE_INTEGER),
            openapi.Parameter(name='is_hybrid', in_=openapi.IN_QUERY, description='是否為雜交', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='is_endemic', in_=openapi.IN_QUERY, description='是否為臺灣特有種', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='is_in_taiwan', in_=openapi.IN_QUERY, description='是否存在於臺灣', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='alien_type', in_=openapi.IN_QUERY, description='外來屬性', type=openapi.TYPE_STRING),
            openapi.Parameter(name='is_fossil', in_=openapi.IN_QUERY, description='是否為化石種', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='is_terrestrial', in_=openapi.IN_QUERY, description='棲地是否為陸域', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='is_freshwater', in_=openapi.IN_QUERY, description='棲地是否為淡水', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='is_brackish', in_=openapi.IN_QUERY, description='棲地是否為半鹹水', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='is_marine', in_=openapi.IN_QUERY, description='棲地是否為海洋', type=openapi.TYPE_BOOLEAN),
            openapi.Parameter(name='protected', in_=openapi.IN_QUERY, description='保育類', type=openapi.TYPE_STRING),
            openapi.Parameter(name='sensitive', in_=openapi.IN_QUERY, description='敏感物種', type=openapi.TYPE_STRING),
            openapi.Parameter(name='redlist', in_=openapi.IN_QUERY, description='臺灣紅皮書評估', type=openapi.TYPE_STRING),
            openapi.Parameter(name='iucn', in_=openapi.IN_QUERY, description='IUCN評估', type=openapi.TYPE_STRING),
            openapi.Parameter(name='cites', in_=openapi.IN_QUERY, description='CITES附錄', type=openapi.TYPE_STRING),
        ]
    )
    def get(self, request, *args, **krgs):
        try:
            try:
                limit = int(request.GET.get('limit', 20))
                offset = int(request.GET.get('offset', 0))
            except Exception as er:
                print(er)
                limit = 20
                offset = 0

            if request.GET.keys() and not set(
                list(request.GET.keys())) <= set(
                ['taxon_id', 'scientific_name', 'common_name', 'taxon_group', 'updated_at', 'created_at', 'limit', 'offset', 'is_hybrid', 'is_endemic',
                'is_in_taiwan', 'alien_type', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish', 'is_marine',
                'protected','redlist','iucn', 'cites', 'rank', 'sensitive','including_not_official']):
                response = {"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}
                return HttpResponse(json.dumps(response, ensure_ascii=False), content_type="application/json,charset=utf-8")

            solr_query_list = get_conditioned_solr_search(req=request.GET)

            limit = 300 if limit > 300 else limit

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

            if resp['response']['docs']:

                df = pd.DataFrame(resp['response']['docs'])

                df = df.rename(columns={
                        'formatted_accepted_name': 'formatted_name',
                        'status': 'taxon_status',
                        'accepted_taxon_name_id': 'name_id',
                        'taxon_rank_id': 'rank',
                    })

                df['created_at'] = df.created_at.apply(lambda x: x[0].split('T')[0])
                df['updated_at'] = df.updated_at.apply(lambda x: x[0].split('T')[0])

                df['rank'] = df['rank'].apply(lambda x: rank_map[int(x)])

                df['taxon_status'] = df['is_deleted'].replace({True: 'deleted', False: 'accepted'})

                df = df.replace({np.nan: '', None: ''})

                cols = ['taxon_id', 'taxon_status', 'name_id','simple_name', 'name_author', 'formatted_name', 'synonyms', 'formatted_synonyms', 'misapplied', 'formatted_misapplied',
                        'rank', 'common_name_c', 'alternative_name_c', 'kingdom', 'is_hybrid', 'is_endemic', 'is_in_taiwan', 'alien_type', 'alien_status_note', 'is_fossil', 'is_terrestrial', 'is_freshwater', 'is_brackish',
                        'is_marine','not_official', 'cites', 'iucn', 'redlist', 'protected', 'sensitive', 'created_at', 'updated_at', 'new_taxon_id', 'parent_taxon_id']

                for c in cols:
                    if c not in df.keys():
                        df[c] = None

                is_list = ['is_hybrid','is_in_taiwan','is_endemic','is_fossil','is_terrestrial','is_freshwater','is_brackish','is_marine','not_official']
                df[is_list] = df[is_list].replace({0: False, 1: True, '0': False, '1': True, 'true': True, 'false': False})
                df['is_in_taiwan'] = df['is_in_taiwan'].replace({2: False, '2': False, None: False})

                df['cites'] = df['cites'].apply(lambda x: x.replace('1','I').replace('2','II').replace('3','III') if x else x)
                df['redlist'] = df['redlist'].apply(lambda x: redlist_map_rev[x] if x else x)

                df = df.replace({np.nan: None, '': None})
                df['name_id'] = df['name_id'].replace({np.nan: 0}).astype('int64').replace({0: None})

                df = df[cols]
            else:
                df = pd.DataFrame()

            response = {"status": {"code": 200, "message": "Success"},
                        "info": {"total": count, "limit": limit, "offset": offset}, "data": df.to_dict('records')}
        except Exception as er:
            print(er)
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
