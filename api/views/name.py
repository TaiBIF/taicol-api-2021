import json
import pymysql
import pandas as pd
import numpy as np
import requests

from django.http import HttpResponse
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi

from conf.settings import SOLR_PREFIX
from api.views._base import db_settings, DateTimeEncoder
from api.utils import rank_map, remove_rank_char, validate


class NameView(APIView):

    @swagger_auto_schema(
        operation_summary='取得學名',
        manual_parameters=[
            openapi.Parameter(name='name_id', in_=openapi.IN_QUERY, description='學名ID', type=openapi.TYPE_INTEGER),
            openapi.Parameter(name='scientific_name', in_=openapi.IN_QUERY, description='學名', type=openapi.TYPE_STRING),
            openapi.Parameter(name='taxon_group', in_=openapi.IN_QUERY, description='分類群', type=openapi.TYPE_STRING),
            openapi.Parameter(name='rank', in_=openapi.IN_QUERY, description='階層', type=openapi.TYPE_STRING),
            openapi.Parameter(name='created_at', in_=openapi.IN_QUERY, description='建立日期', type=openapi.FORMAT_DATE),
            openapi.Parameter(name='updated_at', in_=openapi.IN_QUERY, description='更新日期', type=openapi.FORMAT_DATE),
            openapi.Parameter(name='limit', in_=openapi.IN_QUERY, description='每頁限制筆數', type=openapi.TYPE_INTEGER),
            openapi.Parameter(name='offset', in_=openapi.IN_QUERY, description='指定每頁起始編號', type=openapi.TYPE_INTEGER),
        ]
    )
    def get(self, request, *args, **krgs):
        try:
            limit = int(request.GET.get('limit', 20))
            offset = int(request.GET.get('offset', 0))
        except Exception:
            limit, offset = 20, 0

        limit = 300 if limit > 300 else limit

        allowed_params = {'name_id', 'scientific_name', 'common_name', 'rank', 'updated_at', 'created_at', 'taxon_group', 'limit', 'offset'}
        if request.GET.keys() and not set(request.GET.keys()) <= allowed_params:
            return HttpResponse(json.dumps({"status": {"code": 400, "message": "Bad Request: Unsupported parameters"}}), content_type="application/json")

        name_id = request.GET.get('name_id', '').strip()
        scientific_name = request.GET.get('scientific_name', '').strip()
        scientific_name_cleaned = remove_rank_char(scientific_name)
        updated_at = request.GET.get('updated_at', '').strip().strip('"').strip("'")
        created_at = request.GET.get('created_at', '').strip().strip('"').strip("'")
        taxon_group = request.GET.get('taxon_group', '').strip()
        rank = request.GET.get('rank')

        cte_sql = ""
        join_sql = ""
        where_sql = " WHERE tn.is_publish = 1 "
        sql_params = []

        conn = pymysql.connect(**db_settings)

        try:
            if taxon_group and not name_id:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM taxon_names WHERE name = %s AND is_publish = 1 LIMIT 1", (taxon_group,))
                    row = cursor.fetchone()

                if not row:
                    response = {"status": {"code": 200, "message": "Success"}, "info": {"total": 0, "limit": limit, "offset": offset}, "data": []}
                    return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")

                root_id = row[0]

                cte_sql = f"""
                WITH RECURSIVE hierarchy_scope (taxon_name_id) AS (
                    SELECT {root_id}
                    UNION DISTINCT
                    SELECT ru.taxon_name_id
                    FROM reference_usages ru
                    JOIN hierarchy_scope hs ON ru.parent_taxon_name_id = hs.taxon_name_id
                    WHERE ru.deleted_at IS NULL
                      AND ru.is_title != 1
                )
                """
                join_sql = " JOIN hierarchy_scope hs ON tn.id = hs.taxon_name_id "

            if name_id:
                where_sql += " AND tn.id = %s "
                sql_params.append(name_id)
            elif scientific_name:
                where_sql += " AND (tn.search_name = %s OR tn.name = %s) "
                sql_params.extend([scientific_name_cleaned, scientific_name])

            if updated_at:
                if not validate(updated_at): raise ValueError("Bad Date")
                where_sql += " AND date(tn.updated_at) > %s "
                sql_params.append(updated_at)

            if created_at:
                if not validate(created_at): raise ValueError("Bad Date")
                where_sql += " AND date(tn.created_at) > %s "
                sql_params.append(created_at)

            if rank:
                try:
                    rank_id = list(rank_map.keys())[list(rank_map.values()).index(rank)]
                    where_sql += " AND tn.rank_id = %s "
                    sql_params.append(rank_id)
                except Exception:
                    raise ValueError("Bad Rank")

            with conn.cursor() as cursor:
                count_query = f"{cte_sql} SELECT COUNT(DISTINCT tn.id) FROM taxon_names tn {join_sql} {where_sql}"
                cursor.execute(count_query, sql_params)
                len_total = cursor.fetchone()[0]

                if len_total == 0:
                    response = {"status": {"code": 200, "message": "Success"}, "info": {"total": 0, "limit": limit, "offset": offset}, "data": []}
                    return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")

                page_ids_query = f"""
                    {cte_sql}
                    SELECT DISTINCT tn.id
                    FROM taxon_names tn
                    {join_sql}
                    {where_sql}
                    ORDER BY tn.id
                    LIMIT {limit} OFFSET {offset}
                """
                cursor.execute(page_ids_query, sql_params)
                page_ids_rows = cursor.fetchall()

                target_ids = [r[0] for r in page_ids_rows]

                if target_ids:
                    ids_str = ','.join(map(str, target_ids))

                    select_fields = """
                        tn.id, tn.rank_id, tn.name AS simple_name, an.name_author,
                        tn.original_taxon_name_id, tn.note, tn.created_at, tn.updated_at,
                        n.name as nomenclature_name,
                        tn.properties ->> '$.is_hybrid' as is_hybrid_str,
                        tn.properties ->> '$.protologue' as protologue,
                        tn.properties ->> '$.type_name' as type_name_json,
                        tn.properties ->> '$.latin_genus' as latin_genus,
                        tn.properties ->> '$.latin_s1' as latin_s1,
                        tn.properties ->> '$.species_layers' as species_layers,
                        an.formatted_name,
                        GROUP_CONCAT(anc.namecode) as namecodes,
                        tn.deleted_at
                    """

                    main_query = f"""
                        SELECT {select_fields}
                        FROM taxon_names tn
                        JOIN nomenclatures n ON tn.nomenclature_id = n.id
                        LEFT JOIN api_namecode anc ON tn.id = anc.taxon_name_id
                        LEFT JOIN api_names an ON tn.id = an.taxon_name_id
                        WHERE tn.id IN ({ids_str})
                        GROUP BY tn.id
                        ORDER BY tn.id
                    """

                    cursor.execute(main_query)
                    data_rows = cursor.fetchall()
                else:
                    data_rows = []

                columns = ['name_id', 'rank', 'simple_name', 'name_author', 'original_name_id', 'note',
                           'created_at', 'updated_at', 'nomenclature_name', 'is_hybrid', 'protologue',
                           'type_name_id', 'latin_genus', 'latin_s1', 'species_layers', 'formatted_name',
                           'namecode', 'is_deleted']

                df = pd.DataFrame(list(data_rows), columns=columns)

                if not df.empty:
                    df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d')
                    df['updated_at'] = pd.to_datetime(df['updated_at']).dt.strftime('%Y-%m-%d')

                    df['is_deleted'] = df['is_deleted'].notna()
                    df['is_hybrid'] = df['is_hybrid'].replace({'false': False, 'true': True})

                    df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
                    mask_rank_low = df['rank'] < 34
                    df['rank'] = df['rank'].map(rank_map).fillna(df['rank'])

                    def process_name_column(row):
                        name_dict = {'latin_genus': row.get('latin_genus'), 'latin_s1': row.get('latin_s1')}
                        s_layers = row.get('species_layers')

                        if s_layers:
                            try:
                                if isinstance(s_layers, str):
                                    layers = json.loads(s_layers)
                                else:
                                    layers = s_layers
                                count = 2
                                for s in layers:
                                    if s.get('rank_abbreviation') and s.get('latin_name'):
                                        name_dict[f's{count}_rank'] = s.get('rank_abbreviation')
                                        name_dict[f'latin_s{count}'] = s.get('latin_name')
                                        count += 1
                            except Exception:
                                pass

                        final_name = {k: v for k, v in name_dict.items() if v and v != 'null'}

                        t_id = row.get('type_name_id')
                        try:
                            t_id = int(t_id) if t_id else None
                        except Exception:
                            t_id = None
                        return pd.Series([final_name, t_id])

                    df[['name', 'type_name_id_processed']] = df.apply(process_name_column, axis=1)
                    df['type_name_id'] = df['type_name_id_processed']

                    df.loc[mask_rank_low, 'name'] = "{}"
                    df.loc[mask_rank_low, 'original_name_id'] = None

                    for c in ['name_id', 'original_name_id']:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype('int64').replace(0, None)

                    hybrid_ids = df[df['is_hybrid'] == True]['name_id'].tolist()
                    hybrid_map = {}
                    if hybrid_ids:
                        ids_str = ','.join(map(str, hybrid_ids))
                        q_hybrid = f"""
                            SELECT tnhp.taxon_name_id, GROUP_CONCAT(CONCAT(tn.name, ' ', tn.formatted_authors) SEPARATOR ' × ')
                            FROM taxon_name_hybrid_parent tnhp
                            JOIN taxon_names tn ON tn.id = tnhp.parent_taxon_name_id
                            WHERE tnhp.taxon_name_id IN ({ids_str}) AND tn.is_publish = 1
                            GROUP BY tnhp.taxon_name_id
                        """
                        with conn.cursor() as cursor:
                            cursor.execute(q_hybrid)
                            for rid, name_str in cursor.fetchall():
                                hybrid_map[rid] = name_str
                    df['hybrid_parent'] = df['name_id'].map(hybrid_map)

                    df['taxon'] = '[]'
                    if not df.empty:
                        try:
                            query_solr = {
                                "query": "*:*", "offset": 0, "limit": 10000,
                                "filter": [f'taxon_name_id:({" OR ".join(df.name_id.astype(str))})', 'is_deleted:false'],
                                "fields": ['taxon_id', 'status', 'is_in_taiwan', 'taxon_name_id']
                            }
                            resp = requests.post(f'{SOLR_PREFIX}taxa/select?', json=query_solr).json()
                            solr_docs = resp.get('response', {}).get('docs', [])
                            if solr_docs:
                                t_df = pd.DataFrame(solr_docs)
                                for col in ['is_in_taiwan']:
                                    if col not in t_df.columns:
                                        t_df[col] = None
                                t_df['taxon_name_id'] = t_df['taxon_name_id'].astype(int)
                                def process_solr_group(g):
                                    custom_dict = {'accepted': 0, 'not-accepted': 1, 'misapplied': 2}
                                    g = g.copy()
                                    g['sort_val'] = g['status'].map(custom_dict).fillna(9)
                                    g = g.sort_values(['sort_val', 'is_in_taiwan'], ascending=[True, False])
                                    g['is_in_taiwan'] = g['is_in_taiwan'].replace({0: False, 1: True, '0': False, '1': True, '2': False, None: False})
                                    result = g[['taxon_id', 'status', 'is_in_taiwan']].rename(columns={'status': 'taicol_name_status'}).to_dict('records')
                                    return result
                                taxon_map = t_df.groupby('taxon_name_id').apply(process_solr_group).to_dict()
                                df['taxon'] = df['name_id'].map(taxon_map).fillna(pd.Series([[]] * len(df)))
                        except Exception as e:
                            print(f"Solr Error: {e}")

                    df = df.replace({np.nan: None})
                    final_columns = ['name_id', 'nomenclature_name', 'rank', 'simple_name', 'name_author',
                                     'formatted_name', 'name', 'original_name_id', 'is_hybrid', 'hybrid_parent',
                                     'protologue', 'type_name_id', 'namecode', 'taxon', 'is_deleted', 'created_at', 'updated_at']

                    data_result = df[[c for c in final_columns if c in df.columns]].to_dict('records')
                else:
                    data_result = []

            response = {
                "status": {"code": 200, "message": "Success"},
                "info": {"total": len_total, "limit": limit, "offset": offset},
                "data": data_result
            }

        except Exception as er:
            import traceback
            traceback.print_exc()
            response = {"status": {"code": 500, "message": "Unexpected Error"}}

        finally:
            conn.close()

        return HttpResponse(json.dumps(response, ensure_ascii=False, cls=DateTimeEncoder), content_type="application/json,charset=utf-8")
