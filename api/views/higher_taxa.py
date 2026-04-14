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
from api.utils import rank_map, rank_order_map, lin_map, lin_map_w_order


class HigherTaxaView(APIView):
    @swagger_auto_schema(
        operation_summary='取得較高階層',
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
                taxon_resp = requests.get(f'{SOLR_PREFIX}taxa/select?fq=taxon_name_id:*&fq=status:accepted&q=taxon_id:{taxon_id}&fl=path,taxon_rank_id')
                if taxon_resp.status_code == 200:
                    if taxon_resp.json()['response']['numFound']:
                        info = taxon_resp.json()['response']['docs'][0]
                        if path := info.get('path'):
                            path = path.split('>')
                            path_str = ' OR '.join(path)
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

                                current_rank_orders = higher.rank_order.to_list()
                                for x in lin_map.keys():
                                    now_order = lin_map_w_order[x]['rank_order']
                                    if now_order not in current_rank_orders and now_order < max(current_rank_orders) and now_order > min(current_rank_orders):
                                        higher = pd.concat([higher, pd.Series({'rank_id': x, 'common_name_c': '地位未定', 'taxon_id': None, 'rank_order': lin_map_w_order[x]['rank_order']}).to_frame().T], ignore_index=True)

                                higher = higher.sort_values('rank_order', ignore_index=True, ascending=False)
                                higher = higher.replace({np.nan: None})
                                for hi in higher[higher.taxon_id.isnull()].index:
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
