import json
import pymysql
import os
import time
from typing import List

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from sqlalchemy import create_engine

from conf.settings import env, SOLR_PREFIX
from api.views._base import db_settings
from api.utils import check_taxon_usage
from api.services._03_update_solr import BatchOptimizedSolrTaxonUpdater


def update_check_usage(request):
    try:
        ALLOWED_HOST_FOR_USAGE_CHECK = env.list('ALLOWED_HOST_FOR_USAGE_CHECK')
        for host in ALLOWED_HOST_FOR_USAGE_CHECK:
            if host in request.META.get('HTTP_X_FORWARDED_FOR'):
                check_taxon_usage()
                response = {"status": {"code": 200, "message": "Usage checked!"}}
                break
            else:
                response = {"status": {"code": 403, "message": "Forbidden"}}

    except Exception as er:
        print(er)
        response = {"status": {"code": 500, "message": "Unexpected Error"}}

    return HttpResponse(json.dumps(response))


def get_taxon_by_higher(request):
    only_in_taiwan = request.GET.get('only_in_taiwan')
    exclude_cultured = request.GET.get('exclude_cultured')

    higher_taxa = request.GET.get('higher_taxa','')
    higher_taxa = higher_taxa.split(',')

    query_list = []
    query_list.append('is_deleted:false')

    if only_in_taiwan == 'yes':
        query_list.append('is_in_taiwan:true')

    if exclude_cultured == 'yes':
        query_list.append('-alien_type:cultured')

    query_list.append('path:({})'.format((' OR ').join([f'/.*{f}.*/' for f in higher_taxa])))

    query = { "query": "*:*",
        "limit": 0,
        "filter": query_list,
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

    import requests
    query_req = json.dumps(query)
    resp = requests.post(f'{SOLR_PREFIX}taxa/select?', data=query_req, headers={'content-type': "application/json" })
    resp = resp.json()
    taxon_ids = [r['val'] for r in resp['facets']['taxon_id']['buckets']] if resp['facets'].get('count') else []

    return HttpResponse(json.dumps(taxon_ids), content_type='application/json')


@csrf_exempt
def generate_checklist(request):
    data = json.loads(request.body)
    pairs = list({(item['reference_id'], item['group']) for item in data['usages']})
    exclude_cultured = data['exclude_cultured']
    only_in_taiwan = data['only_in_taiwan']
    references = data['references']

    from api.services._04_generate_checklist import process_taxon_checklist

    final_usage_df, tmp_checklist_id = process_taxon_checklist(pairs, exclude_cultured, only_in_taiwan, references)
    db_string = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(
        db_settings.get('user'), db_settings.get('password'),
        db_settings.get('host'), db_settings.get('port'), db_settings.get('db')
    )
    db = create_engine(db_string)

    final_usage_df.to_sql('tmp_namespace_usages',
        con=db,
        if_exists='append',
        index=False,
        chunksize=1000
    )

    return HttpResponse(json.dumps({'tmp_checklist_id': tmp_checklist_id}))


def get_bearer_token(request):
    auth_header = request.META.get('HTTP_AUTHORIZATION', '')
    if not auth_header.startswith('Bearer '):
        return None
    return auth_header.split(' ')[1]


def is_valid_token(token):
    return token == env('SOLR_UPDATE_TOKEN')


def update_solr(request):
    token = get_bearer_token(request)
    if not token or not is_valid_token(token):
        return JsonResponse({'status': 'token error'}, status=401)

    update_type = request.GET.get('update_type', 'full')
    taxon_ids_param = request.GET.get('taxon_ids', '')

    if not taxon_ids_param:
        return JsonResponse({'status': 'missing taxon_ids'}, status=400)

    try:
        taxon_ids = [tid.strip() for tid in taxon_ids_param.split(',') if tid.strip()]
    except Exception:
        return JsonResponse({'status': 'invalid taxon_ids format'}, status=400)

    if not taxon_ids:
        return JsonResponse({'status': 'empty taxon_ids'}, status=400)

    start_time = time.time()

    try:
        if update_type == 'full':
            result = handle_batch_full_update(taxon_ids)
        elif update_type == 'partial':
            result = handle_batch_partial_update(taxon_ids)
        else:
            return JsonResponse({'status': 'invalid update_type'}, status=400)

        elapsed_time = time.time() - start_time

        if result.status_code == 200:
            response_data = json.loads(result.content)
            response_data['processing_time'] = f"{elapsed_time:.2f}s"
            response_data['batch_size'] = len(taxon_ids)
            return JsonResponse(response_data)

        return result

    except Exception as e:
        print(f"批次更新失敗: {e}")
        return JsonResponse({
            'status': 'failed',
            'error': str(e),
            'processing_time': f"{time.time() - start_time:.2f}s",
            'batch_size': len(taxon_ids)
        }, status=500)


def handle_batch_full_update(taxon_ids: List[str]):
    conn = None
    try:
        conn = pymysql.connect(**db_settings)

        placeholders = ','.join(['%s'] * len(taxon_ids))
        query = f"SELECT taxon_id, content FROM api_for_solr WHERE taxon_id IN ({placeholders})"

        taxon_data_map = {}

        with conn.cursor() as cursor:
            cursor.execute(query, taxon_ids)
            results = cursor.fetchall()

            for taxon_id, content in results:
                try:
                    solr_documents = json.loads(content)
                    taxon_data_map[taxon_id] = solr_documents
                except json.JSONDecodeError as e:
                    print(f"JSON 解析失敗: {taxon_id}, {e}")
                    continue

        if not taxon_data_map:
            return JsonResponse({'status': 'no data found'})

        updater = BatchOptimizedSolrTaxonUpdater(
            solr_base_url=os.environ.get('SOLR_PREFIX', 'http://localhost:8983/solr'),
            core_name='taxa'
        )

        success = updater.batch_full_replace_by_taxon_ids(taxon_data_map)

        if success:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM api_for_solr WHERE taxon_id IN ({placeholders})", taxon_ids)
                conn.commit()

            stats = updater.get_stats()

            return JsonResponse({
                'status': 'success',
                'update_type': 'batch_full',
                'processed_taxon_ids': list(taxon_data_map.keys()),
                'total_documents': sum(len(docs) if isinstance(docs, list) else 1 for docs in taxon_data_map.values()),
                'stats': stats
            })
        else:
            return JsonResponse({
                'status': 'failed',
                'error': 'batch_full_update_failed',
                'processed_taxon_ids': list(taxon_data_map.keys())
            })

    except Exception as e:
        if conn:
            conn.rollback()
        return JsonResponse({'status': 'failed', 'error': str(e)})
    finally:
        if conn:
            conn.close()


def handle_batch_partial_update(taxon_ids: List[str]):
    conn = None
    try:
        conn = pymysql.connect(**db_settings)

        placeholders = ','.join(['%s'] * len(taxon_ids))
        query = f"SELECT taxon_id, content, updated_at FROM api_for_solr WHERE taxon_id IN ({placeholders})"

        taxon_updates_map = {}

        with conn.cursor() as cursor:
            cursor.execute(query, taxon_ids)
            results = cursor.fetchall()

            for taxon_id, content, updated_at in results:
                try:
                    update_fields = json.loads(content)
                    update_fields['updated_at'] = updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    taxon_updates_map[taxon_id] = update_fields
                except json.JSONDecodeError as e:
                    print(f"JSON 解析失敗: {taxon_id}, {e}")
                    continue

        if not taxon_updates_map:
            return JsonResponse({'status': 'no data found'})

        updater = BatchOptimizedSolrTaxonUpdater(
            solr_base_url=os.environ.get('SOLR_PREFIX', 'http://localhost:8983/solr'),
            core_name='taxa'
        )

        success = updater.batch_partial_update_by_taxon_ids(taxon_updates_map)

        if success:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM api_for_solr WHERE taxon_id IN ({placeholders})", taxon_ids)
                conn.commit()

            return JsonResponse({
                'status': 'success',
                'update_type': 'batch_partial',
                'processed_taxon_ids': list(taxon_updates_map.keys()),
                'updated_fields': {tid: list(fields.keys()) for tid, fields in taxon_updates_map.items()}
            })
        else:
            return JsonResponse({
                'status': 'failed',
                'error': 'batch_partial_update_failed',
                'processed_taxon_ids': list(taxon_updates_map.keys())
            })

    except Exception as e:
        if conn:
            conn.rollback()
        return JsonResponse({'status': 'failed', 'error': str(e)})
    finally:
        if conn:
            conn.close()


def update_name(request):
    from api.services._02_update_name import TaxonomicNameUpdater

    param_mappings = {
        'name_id': lambda x: {'taxon_name_ids': [int(x)]},
        'person_id': lambda x: {'person_ids': [int(x)]},
        'min_taxon_name_id': lambda x: {'min_taxon_name_id': int(x)},
        'hybrid_name_id': lambda x: {'hybrid_name_ids': [int(x)]},
    }

    for param_name, param_converter in param_mappings.items():
        if param_value := request.GET.get(param_name):
            try:
                with TaxonomicNameUpdater(batch_size=10, max_retries=3) as updater:
                    kwargs = param_converter(param_value)
                    updater.run_update(**kwargs)
                return JsonResponse({'status': 'success'})
            except Exception:
                return JsonResponse({'status': 'failed'})

    return JsonResponse({'status': 'no_params', 'error': 'No valid parameters provided'})


def update_reference(request):
    from api.services._01_update_ref import CitationUpdater

    param_mappings = {
        'reference_id': lambda x: {'reference_ids': [int(x)]},
        'person_id': lambda x: {'person_ids': [int(x)]},
        'min_reference_id': lambda x: {'min_reference_id': int(x)},
    }

    for param_name, param_converter in param_mappings.items():
        if param_value := request.GET.get(param_name):
            try:
                with CitationUpdater(batch_size=10, max_retries=3) as updater:
                    kwargs = param_converter(param_value)
                    updater.run_update(**kwargs)
                return JsonResponse({'status': 'success'})
            except Exception:
                return JsonResponse({'status': 'failed'})

    return JsonResponse({'status': 'error', 'message': 'No valid parameters provided'})
