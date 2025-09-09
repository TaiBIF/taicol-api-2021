import json
import numpy as np
import pandas as pd
from numpy import nan
import gc
import time
from api.services.utils.common import *


def get_dfs(pairs): # 更新資料前處理
    conn = get_conn()
    with conn.cursor() as cursor:
        placeholders = ",".join(["(%s, %s)"] * len(pairs))
        params = [item for pair in pairs for item in pair]
        ref_pairs = [p[0] for p in pairs]
        query = '''SELECT r.id, r.publish_year, JSON_EXTRACT(r.properties, "$.doi"), 
                        r.`type`, ac.publish_date, JSON_EXTRACT(r.properties, "$.book_title"), 
                        JSON_EXTRACT(r.properties, "$.volume"), JSON_EXTRACT(r.properties, "$.issue") 
                    FROM `references` r 
                    LEFT JOIN api_citations ac ON ac.reference_id = r.id
                    WHERE r.is_publish = 1 AND r.id IN %s
                    '''
        execute_line = cursor.execute(query, (ref_pairs,))
        ref_df = pd.DataFrame(cursor.fetchall(), columns=['reference_id', 'publish_year', 'doi', 'type', 'publish_date', 'book_title', 'volume', 'issue'])
        # 排除俗名backbone 及 個人建立名錄
        query = f'''
            SELECT 
                ru.id,
                ru.reference_id,
                ru.taxon_name_id,
                ru.accepted_taxon_name_id,
                ru.status,
                ru.per_usages,
                ru.properties ->> '$.is_in_taiwan',
                ru.parent_taxon_name_id,
                ru.`group`,
                tn.rank_id, 
                tn.nomenclature_id, 
                tn.object_group, 
                tn.autonym_group,
                JSON_LENGTH(tn.properties ->> '$.species_layers')
            FROM reference_usages ru
            JOIN `references` r 
                ON r.id = ru.reference_id 
                AND r.id != 95
            JOIN taxon_names tn
                ON tn.id = ru.taxon_name_id
            LEFT JOIN api_taxon_usages atu 
                ON atu.is_deleted = 0 
                AND atu.reference_id = ru.reference_id
                AND atu.accepted_taxon_name_id = ru.accepted_taxon_name_id
                AND atu.taxon_name_id = ru.taxon_name_id
            WHERE ru.is_title != 1 
                AND ru.status NOT IN ("", "undetermined") 
                AND ru.deleted_at IS NULL 
                AND ru.accepted_taxon_name_id IS NOT NULL 
                AND (
                    JSON_EXTRACT(r.properties, '$.check_list_type') IS NULL 
                    OR r.properties ->> '$.check_list_type' != '4'
                )
                AND (ru.reference_id, ru.`group`) IN ({placeholders})
            '''
        execute_line = cursor.execute(query, params)
        usage_data = cursor.fetchall()
        usage_df = pd.DataFrame(usage_data, columns=['ru_id', 'reference_id', 'taxon_name_id', 'accepted_taxon_name_id', 
                                                     'ru_status', 'per_usages', 'is_in_taiwan', 'parent_taxon_name_id',
                                                     'group', 'rank_id', 'nomenclature_id', 'object_group', 'autonym_group', 'layer_count'])
        del usage_data
        gc.collect()
        # 新增：查詢 reference_id = 95 的 reference_usage_id
        ref_95_query = f'''
            SELECT ru.id
            FROM reference_usages ru
            WHERE ru.reference_id = 95 AND taxon_name_id IN %s
                AND ru.is_title != 1 
                AND ru.status = 'accepted'
                AND ru.deleted_at IS NULL 
                AND ru.accepted_taxon_name_id IS NOT NULL 
            '''
        cursor.execute(ref_95_query, (list(usage_df.taxon_name_id.unique()),))
        common_name_rus = [row[0] for row in cursor.fetchall()]
    ref_df = ref_df.where(pd.notnull(ref_df), None)
    ref_df['publish_date'] = ref_df['publish_date'].fillna('')
    ref_df['publish_year'] = ref_df['publish_year'].apply(int)
    # 同個taxon_name_id, accepted_taxon_name_id, reference_id 對到兩個status的話 這邊會產生重複 所以排除
    usage_df = usage_df.where(pd.notnull(usage_df), None)
    usage_df = usage_df.merge(ref_df[['reference_id','publish_year']])
    usage_df = usage_df.drop_duplicates().reset_index(drop=True)
    ref_df = ref_df.set_index('reference_id')
    # 加入誤用的per_usages
    misapplied_accepted_taxon_name_id = usage_df[usage_df.ru_status=='misapplied'].accepted_taxon_name_id.unique()
    # 在分類學中，"pro parte"意思是"部分地" 是一個用來描述分類單元或文獻引用的術語，表示某一名稱或描述只適用於特定的一部分，而不是全部。
    usage_df['per_usages'] = usage_df['per_usages'].apply(lambda x: json.loads(x) if x else [])
    for mm in misapplied_accepted_taxon_name_id:
        rows = usage_df[(usage_df.accepted_taxon_name_id==mm)&(usage_df.ru_status=='misapplied')].to_dict('records')
        # 以一筆學名使用為單位
        for row in rows: # row代表 誤用名的誤用學名使用
            usages = row.get('per_usages')
            usages = [uu for uu in usages if uu.get('reference_id') in ref_df.index]
            current_taxon_name_id = row.get('taxon_name_id')
            current_accepted_taxon_name_id = row.get('accepted_taxon_name_id')
            current_reference_id = row.get('reference_id')
            is_pro_parte = any(item.get('pro_parte')==True for item in usages) # 確認是否有任何相同引用設定為pro parte
            # 只考慮usage_df裡面有的reference
            for uu in usages:
                # 相同引用
                if ref_df.at[uu.get('reference_id'),'publish_year'] <= row['publish_year']:
                    # 誤用名本身的接受學名使用
                    mask_1 = usage_df[(usage_df.reference_id==uu.get('reference_id'))&(usage_df.accepted_taxon_name_id==current_taxon_name_id)]
                    if len(mask_1):
                        # 移除無效的學名使用
                        removing_idx = mask_1[mask_1.ru_status=='not-accepted'].index
                        usage_df.drop(removing_idx, inplace=True)
                        # NOTE 在這步先修改accepted_taxon_name_id 以便後面的分組 但最後存資料庫的時候要改回來 以後對資料才對得起來 reference_id 也修改 不然後面會有影響
                        # 併入有效及誤用的學名使用
                        now_ru_id_idx = usage_df[(usage_df.reference_id==uu.get('reference_id'))
                                                            &(usage_df.accepted_taxon_name_id==current_taxon_name_id)].index
                        usage_df.loc[
                            now_ru_id_idx, 
                            ['ru_status','accepted_taxon_name_id','reference_id']
                        ] = ['misapplied',current_accepted_taxon_name_id,current_reference_id]
            # 以下跟per_usage無關
            # 如果有 pro parte 這邊就不併入較早的文獻
            if not is_pro_parte:
                # mask_2 是確認誤用的這篇文獻是否有設定存在於台灣
                mask_2 = usage_df[(usage_df.is_in_taiwan=='1')
                                    &(usage_df.accepted_taxon_name_id==current_accepted_taxon_name_id)
                                    &(usage_df.reference_id==current_reference_id)
                                    &(usage_df.ru_status=='accepted')]
                if not mask_2.empty: # 代表有設定存在於台灣
                    publish_year = row.get('publish_year')
                    # mask_3 是找其他設定該誤用名為有效的學名使用 -> 如果is_in_taiwan = 1 即使per_usages中沒有提到該文獻 只要也將誤用名設為有效 且文獻較舊 則一樣納入
                    mask_3 = usage_df[usage_df.accepted_taxon_name_id==current_taxon_name_id]
                    if not mask_3[(mask_3.is_in_taiwan=='1')&(mask_3.publish_year<publish_year)].empty: # 代表有更早的文獻需要納入
                        merging_ref_df = mask_3.loc[(mask_3.ru_status == 'accepted')&(mask_3.is_in_taiwan=='1')&(mask_3.publish_year<publish_year), 'reference_id'].unique()
                        # 移除無效的學名使用
                        remove_mask_idx = mask_3[mask_3.reference_id.isin(merging_ref_df)&(mask_3.ru_status == 'not-accepted')].index
                        usage_df.drop(remove_mask_idx, inplace=True)
                        # 併入有效及誤用的學名使用
                        update_mask_idx = mask_3[(mask_3.reference_id.isin(merging_ref_df))&(mask_3.ru_status != 'not-accepted')].index
                        usage_df.loc[
                            update_mask_idx,
                            ['ru_status', 'accepted_taxon_name_id', 'reference_id']
                        ] = ['misapplied', current_accepted_taxon_name_id, current_reference_id]
    usage_df = usage_df.drop(columns=['per_usages'])
    usage_df = usage_df.reset_index(drop=True)
    return ref_df, usage_df, common_name_rus


var_df = pd.read_csv('/code/api/services/variants_for_checklist.csv')


def create_replacement_mapping(var_df):
    """建立替換映射和字符集合"""
    replacement_dict = dict(zip(var_df['old_value'], var_df['new_value']))
    old_chars_set = set(var_df['old_value'])
    return replacement_dict, old_chars_set


REPLACEMENT_DICT, OLD_CHARS_SET = create_replacement_mapping(var_df)

def replace_char(string):
    string_chars = set(string)
    chars_to_replace = string_chars & OLD_CHARS_SET
    if not chars_to_replace:
        return string
    for old_char in chars_to_replace:
        string = string.replace(old_char, REPLACEMENT_DICT[old_char])
    return string



class UnionFind:
    def __init__(self, n):
        self.parent = np.arange(n)
        self.rank = np.zeros(n, dtype=int)
    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # 路徑壓縮 (迭代版)
            x = self.parent[x]
        return x
    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        # 按rank合併
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        elif self.rank[ry] < self.rank[rx]:
            self.parent[ry] = rx
        else:
            self.parent[ry] = rx
            self.rank[rx] += 1


def assign_group_and_tmp_taxon_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)
    n = len(df)
    uf_group = UnionFind(n)
    uf_taxon = UnionFind(n)
    # 建立索引避免重複查找
    def build_index(col_name, filter_mask=None):
        idx_map = {}
        if filter_mask is None:
            iterable = enumerate(df[col_name])
        else:
            iterable = ((i, val) for i, val in enumerate(df[col_name]) if filter_mask[i])
        for i, val in iterable:
            if pd.isna(val):
                continue
            idx_map.setdefault(val, []).append(i)
        return idx_map
    # -----------------------
    # 處理 group_id （不考慮 status）
    # -----------------------
    # 1. 相同 object_group
    object_group_idx = build_index('object_group', filter_mask=~df['object_group'].isna())
    for indices in object_group_idx.values():
        base = indices[0]
        for i in indices[1:]:
            uf_group.union(base, i)
    # 2. 相同 taxon_name_id
    taxon_name_idx = build_index('taxon_name_id')
    for indices in taxon_name_idx.values():
        base = indices[0]
        for i in indices[1:]:
            uf_group.union(base, i)
    # 3. 相同 accepted_taxon_name_id
    accepted_taxon_name_idx = build_index('accepted_taxon_name_id')
    for indices in accepted_taxon_name_idx.values():
        base = indices[0]
        for i in indices[1:]:
            uf_group.union(base, i)
    # 分配 group_id
    root_to_gid = {}
    group_ids = np.empty(n, dtype=int)
    next_gid = 0
    for i in range(n):
        root = uf_group.find(i)
        if root not in root_to_gid:
            root_to_gid[root] = next_gid
            next_gid += 1
        group_ids[i] = root_to_gid[root]
    df['group_id'] = group_ids
    # -----------------------
    # 處理 tmp_taxon_id（有條件，忽略 misapplied）
    # -----------------------
    non_misapplied_mask = df['ru_status'] != 'misapplied'
    df_non_misapplied_idx = np.where(non_misapplied_mask)[0]
    # 先建立只含非misapplied的子集索引map
    def build_index_cond(col_name, status_mask):
        idx_map = {}
        for i in df_non_misapplied_idx:
            val = df.at[i, col_name]
            if pd.isna(val):
                continue
            if status_mask is not None and not status_mask[i]:
                continue
            idx_map.setdefault(val, []).append(i)
        return idx_map
    # accepted_name_id + status in [accepted, not-accepted]
    mask_accepted_not = (df['ru_status'] == 'accepted') | (df['ru_status'] == 'not-accepted')
    accepted_taxon_idx_cond = {}
    for i in df_non_misapplied_idx:
        if mask_accepted_not[i]:
            val = df.at[i, 'accepted_taxon_name_id']
            if pd.isna(val):
                continue
            accepted_taxon_idx_cond.setdefault(val, []).append(i)
    for indices in accepted_taxon_idx_cond.values():
        base = indices[0]
        for i in indices[1:]:
            uf_taxon.union(base, i)
    # object_group + status == accepted
    mask_accepted = df['ru_status'] == 'accepted'
    object_group_idx_cond = {}
    for i in df_non_misapplied_idx:
        if mask_accepted[i]:
            val = df.at[i, 'object_group']
            if pd.isna(val):
                continue
            object_group_idx_cond.setdefault(val, []).append(i)
    for indices in object_group_idx_cond.values():
        base = indices[0]
        for i in indices[1:]:
            uf_taxon.union(base, i)
    # 分配 tmp_taxon_id
    root_to_tid = {}
    tmp_taxon_ids = np.full(n, -1, dtype=int)
    next_tid = 0
    for i in df_non_misapplied_idx:
        root = uf_taxon.find(i)
        if root not in root_to_tid:
            root_to_tid[root] = next_tid
            next_tid += 1
        tmp_taxon_ids[i] = root_to_tid[root]
    # 補上misapplied的tmp_taxon_id，根據 accepted_name_id + reference_id 對應
    misapplied_mask = df['ru_status'] == 'misapplied'
    taxon_filled = tmp_taxon_ids != -1
    df_with_taxon = df.loc[taxon_filled, ['accepted_taxon_name_id', 'reference_id']]
    taxon_id_series = pd.Series(tmp_taxon_ids[taxon_filled], index=df_with_taxon.index)
    for i in np.where(misapplied_mask)[0]:
        a_id = df.at[i, 'accepted_taxon_name_id']
        ref_id = df.at[i, 'reference_id']
        match = df_with_taxon[
            (df_with_taxon['accepted_taxon_name_id'] == a_id) &
            (df_with_taxon['reference_id'] == ref_id)
        ]
        if not match.empty:
            idx_match = match.index[0]
            tmp_taxon_ids[i] = taxon_id_series[idx_match]
        else:
            # 沒找到對應的tmp_taxon_id就保留-1或NaN
            tmp_taxon_ids[i] = -1
    # 將tmp_taxon_id放回df，使用NaN代替-1
    df['tmp_taxon_id'] = pd.Series(tmp_taxon_ids).replace(-1, np.nan)
    return df



def determine_taxon_status(df):
    """
    使用 groupby 和 transform 優化 taxon_status 分配
    """
    # 為每個 tmp_taxon_id 建立 accepted_name_id
    accepted_mask = (df['ru_status'] == 'accepted') & (df['is_latest'] == 1)
    df['accepted_name_id'] = df[accepted_mask].groupby('tmp_taxon_id')['taxon_name_id'].transform('first')
    # df['accepted_name_id'] = df.groupby('tmp_taxon_id')['accepted_name_id'].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))
    df['accepted_name_id'] = df.groupby('tmp_taxon_id')['accepted_name_id'].ffill().bfill()
   # 建立 not_accepted 標記
    not_accepted_mask = (df['ru_status'] == 'not-accepted') & (df['is_latest'] == 1)
    df['is_not_accepted'] = not_accepted_mask
    # 建立 misapplied 標記
    misapplied_mask = df['ru_status'] == 'misapplied'
    df['is_misapplied'] = misapplied_mask
    # 向量化分配
    misapplied_condition = (
        df['is_misapplied'] & 
        (df['taxon_name_id'] != df['accepted_name_id']) & 
        (~df['is_not_accepted'])
    )
    accepted_condition = df['taxon_name_id'] == df['accepted_name_id']
    df['taxon_status'] = np.select(
        [misapplied_condition, accepted_condition],
        ['misapplied', 'accepted'],
        default='not-accepted'
    )
    # 清理暫時欄位
    df.drop(['accepted_name_id', 'is_not_accepted', 'is_misapplied'], axis=1, inplace=True)
    return df


# 超級backbone：文獻類型 type=6
# 優先度：6>1,2,3>5>4


def select_latest_ru(df, ref_df, accepted_only=True, mark_on_original_df=None, return_conflict=False):
    """
    根據邏輯篩選每個 tmp_taxon_id 下的最新文獻，標記 is_latest 欄位。
    """
    # 保存原始 index
    # original_index = df.index.copy()
    df = df.copy()
    df = df.merge(ref_df[['type','publish_date', 'book_title', 'volume', 'issue']], right_index=True, left_on='reference_id')
    df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
    df['is_latest'] = False
    # Step 1: 只考慮 accepted
    df_accepted = df[df['ru_status'] == 'accepted'].copy() if accepted_only else df
    # Step 2: type 處理
    mask_type6 = df_accepted['type'] == 6
    has_type6 = df_accepted.groupby('tmp_taxon_id')['type'].transform(lambda x: (x == 6).any())
    df_filtered = df_accepted[~has_type6 | mask_type6]
    has_non4 = df_filtered.groupby('tmp_taxon_id')['type'].transform(lambda x: (x != 4).any())
    df_filtered = df_filtered[~((df_filtered['type'] == 4) & has_non4)]
    has_non5 = df_filtered.groupby('tmp_taxon_id')['type'].transform(lambda x: (x != 5).any())
    df_filtered = df_filtered[~((df_filtered['type'] == 5) & has_non5)]
    # Step 3: 年份 + 日期
    def keep_latest_by_year_and_date(grp):
        max_year = grp['publish_year'].max()
        grp_year = grp[grp['publish_year'] == max_year]
        if len(grp_year) == 1:
            return grp_year
        if grp_year['publish_date'].notna().any():
            max_date = grp_year['publish_date'].max()
            return grp_year[grp_year['publish_date'] == max_date]
        return grp_year
    df_filtered = df_filtered.groupby('tmp_taxon_id', group_keys=False).apply(keep_latest_by_year_and_date)
    # Step 4: 多章節處理
    # TODO 這邊應該可以改回用df了 因為前面有merge
    def resolve_chapters(grp):
        if grp['reference_id'].nunique() > 1:
            ref_subset = ref_df[ref_df.index.isin(grp['reference_id'])]
            if len(ref_subset) > 0 and ref_subset[['book_title', 'volume', 'issue']].nunique().max() == 1:
                return grp.head(1)
        return grp
    df_filtered = df_filtered.groupby('tmp_taxon_id', group_keys=False).apply(resolve_chapters)
    # Step 5: parent 關係
    conflict_list = []
    def resolve_conflict(grp):
        if len(grp) <= 1:
            return grp
        if grp['reference_id'].nunique() == 1 and grp['object_group'].nunique() == 1:
            ids = grp['taxon_name_id'].tolist()
            parents = grp['parent_taxon_name_id'].tolist()
            for i, t1 in enumerate(ids):
                for j, t2 in enumerate(ids):
                    if i != j and (t1 == parents[j] or t2 == parents[i]):
                        # conflict_list.append(grp.name if hasattr(grp, 'name') else grp['tmp_taxon_id'].iloc[0])
                        return grp
            return grp.loc[[grp['group'].idxmin()]]
        # else:
        #     conflict_list.append(grp.name if hasattr(grp, 'name') else grp['tmp_taxon_id'].iloc[0])
        #     return grp
    latest_accepted = df_filtered.groupby('tmp_taxon_id', group_keys=False).apply(resolve_conflict)
    # Step 6: 標記 is_latest
    # 使用三元組合：tmp_taxon_id + reference_id + accepted_taxon_name_id
    key_set = set(
        latest_accepted[['tmp_taxon_id', 'reference_id', 'accepted_taxon_name_id']].itertuples(index=False, name=None)
    )
    df['key_triplet'] = list(zip(df['tmp_taxon_id'], df['reference_id'], df['accepted_taxon_name_id']))
    df['is_latest'] = df['key_triplet'].isin(key_set)
    df.drop(columns=['type','publish_date', 'book_title', 'volume', 'issue','key_triplet'], inplace=True)
    # 若需要寫回主 df
    if mark_on_original_df is not None:
        if 'is_latest' not in mark_on_original_df.columns:
            mark_on_original_df['is_latest'] = False
        # 重置所有相關的 tmp_taxon_id 的 is_latest 為 False
        original_tmp_taxon_ids = df['tmp_taxon_id'].unique()
        mask = mark_on_original_df['tmp_taxon_id'].isin(original_tmp_taxon_ids)
        mark_on_original_df.loc[mask, 'is_latest'] = False
        # 使用三元組合來精確標記
        mark_on_original_df['temp_key'] = list(zip(
            mark_on_original_df['tmp_taxon_id'],
            mark_on_original_df['reference_id'], 
            mark_on_original_df['accepted_taxon_name_id']
        ))
        mark_on_original_df.loc[
            mark_on_original_df['temp_key'].isin(key_set), 'is_latest'
        ] = True
        mark_on_original_df.drop(columns='temp_key', inplace=True)
        return None
    return (df, conflict_list) if return_conflict else df



def select_global_latest_ru(df, ref_df, accepted_only=False):
    """
    將整個 DataFrame 視為單一 group，依據原本的邏輯篩選最新 ru 記錄。
    Parameters:
    - df: 欲處理的 DataFrame
    - accepted_only: 是否僅考慮 ru_status == 'accepted'
    - return_conflict: 是否回傳 conflict_list
    Returns:
    - 篩選後的 df（可能為一筆或多筆）
    - 若 return_conflict=True，回傳 tuple: (result_df, conflict_list)
    """
    df = df.copy()
    df = df.merge(ref_df[['type','publish_date', 'book_title', 'volume', 'issue']], right_index=True, left_on='reference_id')
    df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
    if accepted_only:
        df = df[df['ru_status'] == 'accepted']
    # Step 1: type 處理
    if (df['type'] == 6).any():
        df = df[df['type'] == 6]
    if (df['type'] == 4).any() and (df['type'] != 4).any():
        df = df[df['type'] != 4]
    if (df['type'] == 5).any() and (df['type'] != 5).any():
        df = df[df['type'] != 5]
    # Step 2: publish_year 和 publish_date 排序
    max_year = df['publish_year'].max()
    df = df[df['publish_year'] == max_year]
    if df['publish_date'].notna().any():
        max_date = df['publish_date'].max()
        df = df[df['publish_date'] == max_date]
    # Step 3: 多章節處理
    if df['reference_id'].nunique() > 1:
        # 只取當前 df 中存在的 reference_id
        # TODO 這邊應該可以改回用df了 因為前面有merge
        ref_subset = ref_df[ref_df.index.isin(df['reference_id'])]
        if ref_subset[['book_title', 'volume', 'issue']].nunique().max() == 1:
            df = df.head(1)
    # Step 4 / 5: parent 關係處理（與原邏輯一致）
    def resolve_conflict(grp):
        if len(grp) <= 1:
            return grp
        if grp['reference_id'].nunique() == 1 and grp['object_group'].nunique() == 1:
            ids = grp['taxon_name_id'].tolist()
            parents = grp['parent_taxon_name_id'].tolist()
            for i, t1 in enumerate(ids):
                for j, t2 in enumerate(ids):
                    if i != j and (t1 == parents[j] or t2 == parents[i]):
                        return grp  # 衝突，整組保留
            return grp.loc[[grp['group'].idxmin()]]  # 無衝突，取 group 最小
        else:
            return grp  # 不同 ref 或 group，保留整組
    result_df = resolve_conflict(df)
    return result_df.ru_id.to_list()



def get_check_obj_list(df, whitelist_list_1):
    return (df.loc[
        (df['ru_status'] != 'misapplied') &
        (~df['ru_id'].isin(whitelist_list_1)),
        ['object_group', 'tmp_taxon_id']
    ]
    .drop_duplicates(subset=['object_group', 'tmp_taxon_id'])
    .groupby('object_group')['tmp_taxon_id']
    .nunique()
    .loc[lambda x: x > 1]
    .index
    .tolist())



def get_multiple_accepted(df):
    # 找出 tmp_taxon_id, reference_id 中有多個 accepted 最新名
    multi_accept_tmp = (
        df[(df.is_latest) & (df.ru_status == 'accepted')]
        [['tmp_taxon_id', 'reference_id', 'accepted_taxon_name_id']]
        .drop_duplicates()
        .groupby(['tmp_taxon_id', 'reference_id'])['accepted_taxon_name_id']
        .nunique()
        .loc[lambda x: x > 1]
        .reset_index()['tmp_taxon_id']
        .unique()
        .tolist()
    )
    # 篩出所有這些 tmp_taxon_id 對應的最新接受名資料
    latest_accepted = df[
        (df.is_latest) & 
        (df.ru_status == 'accepted') & 
        (df.tmp_taxon_id.isin(multi_accept_tmp))
    ][['tmp_taxon_id', 'taxon_name_id', 'parent_taxon_name_id']]
    # 批量判斷 tmp_taxon_id 是否有上下階層關係
    def has_parent_relation(group):
        taxon_ids = set(group['taxon_name_id'])
        parent_ids = set(group['parent_taxon_name_id'])
        return len(taxon_ids.intersection(parent_ids)) > 0
    no_parent = (
        latest_accepted.groupby('tmp_taxon_id')
        .filter(has_parent_relation)
        ['tmp_taxon_id']
        .unique()
        .tolist()
    )
    return no_parent


def get_parent_not_accepted(df, sub_lin_ranks):
    sub_list = (
        df[
            (df.ru_status == 'accepted') &
            (df.is_latest) &
            (df.rank_id.isin(sub_lin_ranks))
        ]['tmp_taxon_id']
        .unique()
        .tolist()
    )
    no_parent = []
    for s in sub_list:
        rows = df[df.tmp_taxon_id == s]
        if rows['layer_count'].nunique() <= 1:
            continue
        # 抓當前最新的 accepted 種階層
        latest = rows[(rows.ru_status == 'accepted') & (rows.is_latest)]
        max_layer = latest['layer_count'].max()
        parent_id = latest[latest.layer_count == max_layer]['parent_taxon_name_id'].values[0]
        current_accepted_id = latest[latest.layer_count == max_layer]['accepted_taxon_name_id'].values[0]
        # 也要考慮 跨越兩層被組在一起的情況
        # 有上階層的接受學名使用 // 如果是上階層是分開taxon的無效名也可以
        # case 1：自己與自己的上階層被組在一起
        cond1 = rows[
            (rows.taxon_name_id == parent_id) &
            (rows.accepted_taxon_name_id != current_accepted_id) &
            (rows.ru_status != 'misapplied')
        ]
        # 如果上階層已經在其他分類群 在這邊可以忽略 因為有同物異名的關係 在後面會判斷誰要併入誰
        outside_cond1 = df[
            (~df.ru_id.isin(rows.ru_id)) &
            (df.taxon_name_id == parent_id) &
            (df.accepted_taxon_name_id != current_accepted_id) &
            (df.ru_status != 'misapplied')
        ]
        if len(cond1) and not len(outside_cond1):
            no_parent.append(s)
            continue
        # case 2：自己與上階層的上階層被組在一起
        # 先找到自己的上階層 & 自己的上階層的上階層
        p_parent_ids = df[
            (df.taxon_name_id == parent_id) &
            (df.ru_status != 'misapplied')
        ]['parent_taxon_name_id'].unique()
        cond2 = rows[
            (rows.taxon_name_id.isin(p_parent_ids)) &
            (rows.accepted_taxon_name_id != current_accepted_id) &
            (rows.ru_status != 'misapplied')
        ]
        # 如果上上階層已經在其他分類群 在這邊可以忽略 因為有同物異名的關係 在後面會判斷誰要併入誰
        outside_cond2 = df[
            (~df.ru_id.isin(rows.ru_id)) &
            (df.taxon_name_id.isin(p_parent_ids)) &
            (df.accepted_taxon_name_id != current_accepted_id) &
            (df.ru_status != 'misapplied')
        ]
        if len(cond2) and not len(outside_cond2):
            no_parent.append(s)
    return list(set(no_parent))



def get_check_name_list(df, whitelist_list_2):
    return (df[
        df['object_group'].isnull() &
        df['ru_status'].isin(['accepted', 'not-accepted'])
    ][['taxon_name_id', 'tmp_taxon_id']]
    .drop_duplicates()
    .groupby('taxon_name_id')['tmp_taxon_id']
    .nunique()
    .loc[lambda x: x > 1]
    .index.difference(whitelist_list_2)
    .tolist()
)


def get_check_autonyms(df):
    return (
        df[
            (df['ru_status'] == 'accepted') &
            (df['is_latest']) &
            (df['rank_id'] == 34) &
            (df['autonym_group'].notnull())
        ]['tmp_taxon_id']
        .drop_duplicates()
        .tolist()
    )


def determine_taxon_prop(df):
    """
    處理資料框中的分類屬性
    Parameters:
    - df: 輸入資料框
    - ref_df: 參考文獻資料框
    """
    # 建立 latest_accepted 映射
    latest_accepted_df = df[
        (df['taxon_status'] == 'accepted')
    ][['tmp_taxon_id', 'taxon_name_id']].drop_duplicates('tmp_taxon_id')
    # 獲取所有 tmp_taxon_id（排序以確保一致性）
    all_tmp_taxon_ids = np.sort(df['tmp_taxon_id'].unique())
    n_ids = len(all_tmp_taxon_ids)
    # 建立結果陣列
    result_data = {'tmp_taxon_id': all_tmp_taxon_ids}
    # 定義轉換函數
    def convert_to_numeric(value):
        """將 '0', '1', 0, 1 等值轉換為數字，None 保持為 None"""
        if value is None or pd.isna(value):
            return None
        if isinstance(value, str):
            if value == '1':
                return 1
            elif value == '0':
                return 0
        elif isinstance(value, (int, float, np.integer, np.floating)):
            if value == 1 or value == 1.0:
                return 1
            elif value == 0 or value == 0.0:
                return 0
        return value  # 其他情況保持原值
    # 建立位置映射
    id_to_pos = {tid: i for i, tid in enumerate(all_tmp_taxon_ids)}
    # 處理 common_names（保留所有資料）
    common_names_values = np.full(n_ids, None, dtype=object)
    common_names_mask = df['common_names'].notna()
    if common_names_mask.any():
        common_name_data = df[common_names_mask].copy()
        common_name_data = common_name_data.merge(
            latest_accepted_df.rename(columns={'taxon_name_id': 'latest_accepted_id'}),
            on='tmp_taxon_id',
            how='left'
        )
        common_name_data['is_latest_accepted'] = (
            common_name_data['taxon_name_id'] == common_name_data['latest_accepted_id']
        ) & common_name_data['latest_accepted_id'].notna()
        grouped = common_name_data.groupby('tmp_taxon_id')
        for tmp_taxon_id, group_data in grouped:
            group_data = group_data.sort_values(['is_latest_accepted', 'ru_order'], 
                                              ascending=[False, True])
            seen_names = set()
            all_names_ordered = []
            for _, row in group_data.iterrows():
                if pd.notna(row['common_names']):
                    names = [name.strip() for name in str(row['common_names']).split(',') if name.strip()]
                    for name in names:
                        if name not in seen_names:
                            all_names_ordered.append(name)
                            seen_names.add(name)
            pos = id_to_pos[tmp_taxon_id]
            if all_names_ordered:
                common_names_list = []
                for name in all_names_ordered:
                    cc_list = name.split('|')
                    common_names_list.append({
                            "area": cc_list[2],
                            "name": replace_char(cc_list[1]),
                            "language": cc_list[0]
                        })
                # 去重複，保持順序
                seen_combinations = set()
                unique_common_names = []
                for item in common_names_list:
                    # 使用 tuple 作為唯一識別
                    key = (item['language'], item['name'], item['area'])
                    if key not in seen_combinations:
                        seen_combinations.add(key)
                        unique_common_names.append(item)
                common_names_values[pos] = unique_common_names
    result_data['common_names'] = common_names_values
    # 處理 additional_fields, custom_fields, type_specimens（不限定地位）
    for field in ['additional_fields', 'custom_fields', 'type_specimens']:
        field_values = np.full(n_ids, None, dtype=object)
        field_mask = df[field].notna()
        if field_mask.any():
            field_data = df[field_mask].copy()
            grouped = field_data.groupby('tmp_taxon_id')
            for tmp_taxon_id, group_data in grouped:
                group_data = group_data.sort_values('ru_order')
                all_items = []
                for _, row in group_data.iterrows():
                    if pd.notna(row[field]):
                        try:
                            if field_items := json.loads(row[field]):
                                if field in ['additional_fields', 'custom_fields']:
                                    # 為每個項目添加來源資訊
                                    for item in field_items:
                                        item.update({
                                            'reference_id': row['reference_id'],
                                            'publish_year': row.get('publish_year'),
                                            'subtitle': row.get('subtitle'),
                                            'ru_id': row['ru_id']
                                        })
                                all_items.extend(field_items)
                        except (json.JSONDecodeError, TypeError):
                            pass
                pos = id_to_pos[tmp_taxon_id]
                if all_items:
                    if field in ['additional_fields', 'custom_fields']:
                        # 合併處理
                        df_items = pd.DataFrame(all_items)
                        group_key = 'field_name' if field == 'additional_fields' else 'field_name_en'
                        if group_key in df_items.columns:
                            df_items['formatted'] = df_items.apply(
                                lambda row: f"{row['field_value']} ({row.get('subtitle', '')})", axis=1
                            )
                            df_items = df_items.sort_values(by=[group_key, 'publish_year'])
                            grouped_items = df_items.groupby(group_key)['formatted'].apply('<br>'.join).reset_index()
                            grouped_items = grouped_items.rename(columns={'formatted': 'field_value'})
                            all_items = grouped_items.replace({np.nan: None}).to_dict('records')
                    field_values[pos] = all_items
        result_data[field] = field_values
    # 處理其他基本屬性
    basic_attributes = ['is_in_taiwan', 'alien_type', 'is_endemic', 'is_fossil', 'is_terrestrial', 
                       'is_freshwater', 'is_brackish', 'is_marine']
    for attr in basic_attributes:
        values = np.full(n_ids, None, dtype=object)
        attr_mask = df[attr].notna()
        if attr_mask.any():
            attr_data = df[attr_mask].copy()
            attr_data = attr_data.merge(
                latest_accepted_df.rename(columns={'taxon_name_id': 'latest_accepted_id'}),
                on='tmp_taxon_id',
                how='left'
            )
            attr_data['is_matched'] = (
                attr_data['taxon_name_id'] == attr_data['latest_accepted_id']
            ) & attr_data['latest_accepted_id'].notna()
            has_matched = attr_data.groupby('tmp_taxon_id')['is_matched'].any()
            attr_data['group_has_match'] = attr_data['tmp_taxon_id'].map(has_matched)
            keep_mask = (
                attr_data['latest_accepted_id'].isna() |
                ~attr_data['group_has_match'] |
                attr_data['is_matched']
            )
            filtered_data = attr_data[keep_mask]
            if len(filtered_data) > 0:
                min_records = filtered_data.loc[
                    filtered_data.groupby('tmp_taxon_id')['ru_order'].idxmin()
                ]
                for _, row in min_records.iterrows():
                    pos = id_to_pos[row['tmp_taxon_id']]
                    values[pos] = convert_to_numeric(row[attr])
        result_data[attr] = values
    return pd.DataFrame(result_data)



def get_prop_df(usage_list):
    query = """SELECT 
        id, taxon_name_id, reference_id, accepted_taxon_name_id, `group`,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_in_taiwan')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_in_taiwan')) 
        END as is_in_taiwan,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_endemic')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_endemic')) 
        END as is_endemic,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_fossil')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_fossil')) 
        END as is_fossil,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_terrestrial')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_terrestrial')) 
        END as is_terrestrial,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_freshwater')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_freshwater')) 
        END as is_freshwater,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_brackish')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_brackish')) 
        END as is_brackish,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_marine')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.is_marine')) 
        END as is_marine,
        CASE 
            WHEN JSON_UNQUOTE(JSON_EXTRACT(properties, '$.alien_type')) IN ('', 'null') 
            THEN NULL 
            ELSE JSON_UNQUOTE(JSON_EXTRACT(properties, '$.alien_type')) 
        END as alien_type,
        -- common_names 處理
        CASE 
            WHEN JSON_CONTAINS_PATH(properties, 'one', '$.common_names') 
                AND JSON_LENGTH(JSON_EXTRACT(properties, '$.common_names')) > 0
            THEN (
                SELECT GROUP_CONCAT(
                    CONCAT(common_name.language, '|', common_name.name_value, '|', COALESCE(common_name.area, '')) 
                    SEPARATOR ', '
                )
                FROM JSON_TABLE(
                    JSON_EXTRACT(properties, '$.common_names'),
                    '$[*]' COLUMNS (
                        language VARCHAR(10) PATH '$.language',
                        name_value VARCHAR(500) PATH '$.name',
                        area VARCHAR(100) PATH '$.area'
                    )
                ) as common_name
                WHERE common_name.name_value IS NOT NULL 
                    AND common_name.name_value NOT IN ('', 'null')
            )
            ELSE NULL
        END as common_names,
        JSON_EXTRACT(properties, '$.additional_fields'),
        JSON_EXTRACT(properties, '$.custom_fields'),
        -- indications 處理（去除重複）
        CASE 
            WHEN JSON_CONTAINS_PATH(properties, 'one', '$.indications') 
                AND JSON_LENGTH(JSON_EXTRACT(properties, '$.indications')) > 0
            THEN (
                SELECT GROUP_CONCAT(DISTINCT indication_item SEPARATOR ',')
                FROM JSON_TABLE(
                    JSON_EXTRACT(properties, '$.indications'),
                    '$[*]' COLUMNS (
                        indication_item VARCHAR(500) PATH '$'
                    )
                ) as indications_table
                WHERE indication_item IS NOT NULL 
                    AND indication_item NOT IN ('', 'null')
            )
            ELSE NULL
        END as indications,
        type_specimens,
        per_usages
    FROM reference_usages
    WHERE deleted_at IS NULL AND id IN %s ;"""
    conn = get_conn()
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, (usage_list,))
        df = pd.DataFrame(cursor.fetchall(), columns=[
            'ru_id', 'taxon_name_id', 'reference_id', 'accepted_taxon_name_id', 'group', 
            'is_in_taiwan', 'is_endemic', 'is_fossil', 'is_terrestrial', 
            'is_freshwater', 'is_brackish', 'is_marine', 'alien_type', 
            'common_names', 'additional_fields', 'custom_fields', 'indications', 'type_specimens', 'per_usages'
        ])
        # 對 reference_id = 95 的記錄，將特定欄位設為 None
        df.loc[df.reference_id==95, ['is_in_taiwan', 'is_endemic', 'is_fossil', 'is_terrestrial', 
            'is_freshwater', 'is_brackish', 'is_marine', 'alien_type', 
            ]] = [None, None, None, None, None, None, None, None]
    # 定義屬性欄位
    attributes = [k for k in df.keys() if k not in ['ru_id', 'taxon_name_id', 'reference_id', 'accepted_taxon_name_id', 'group']]
    # 排除所有屬性都是空的 row
    valid_mask = ~df[attributes].isna().all(axis=1)
    df_filtered = df[valid_mask].copy()
    # print(f"移除了 {(~valid_mask).sum()} 個所有屬性都是空值的 rows")
    return df_filtered


# 大約30秒左右
def select_priority_prop(df):
    """
    僅考慮accpted的usage，用於判斷各屬性的優先序
    優先順序：type(6 > 1,2,3 > 5 > 4) → 年份(降序) → 日期(降序) → 其他邏輯
    """
    df_work = df.copy()
    df_work['publish_date'] = pd.to_datetime(df_work['publish_date'], errors='coerce')
    df_work['ru_order'] = None
    # Step 2: 建立 type 優先順序映射（向量化）
    type_priority_map = {6: 1, 1: 2, 2: 2, 3: 2, 5: 3, 4: 4}
    df_work['type_priority'] = df_work['type'].map(type_priority_map)
    # Step 3: 建立排序鍵（完全向量化）
    df_work.loc[df_work.reference_id==95,'publish_date'] = None
    df_work['sort_year'] = df_work['publish_year'].fillna(-1)
    df_work['sort_date'] = df_work['publish_date'].fillna(pd.Timestamp('1700-01-01'))
    df_work['has_date'] = df_work['publish_date'].notna()
    # Step 4: 一次性排序（最關鍵的優化）
    df_work = df_work.sort_values([
        'type_priority',       # type 優先順序
        'sort_year',          # 年份降序
        'has_date',           # 有日期優先
        'sort_date',          # 日期降序
        'group'               # 最後按 group 排序
    ], ascending=[True, False, False, False, True])
    df_work['ru_order'] = range(1, len(df_work) + 1)
    # Step 8: 清理臨時欄位
    df_work = df_work.drop(columns=['type_priority', 'sort_year', 'sort_date', 'has_date'])
    return df_work



def check_deleted_usages():
    conn = get_conn()
    query = '''
            WITH deleted_taxon_names AS (
                SELECT id FROM taxon_names WHERE deleted_at IS NOT NULL
            ),
            deleted_references AS (
                SELECT id FROM `references` WHERE deleted_at IS NOT NULL
            )
            SELECT id, reference_id, taxon_name_id
            FROM reference_usages
            WHERE deleted_at IS NULL
            AND (
                taxon_name_id IN (SELECT id FROM deleted_taxon_names)
                OR accepted_taxon_name_id IN (SELECT id FROM deleted_taxon_names)
                OR reference_id IN (SELECT id FROM deleted_references)
            );
            '''
    with conn.cursor() as cursor:
        execute_line = cursor.execute(query)
        deleted_ids = cursor.fetchall()
        deleted_ru_ids = [i[0] for i in deleted_ids]
        if deleted_ru_ids:
            cursor.execute(
                "UPDATE reference_usages SET deleted_at = CURRENT_TIMESTAMP WHERE id IN %s",
                (tuple(deleted_ru_ids),) 
            )
            # 新增到import_usage_log
            log_query = """
                INSERT INTO import_usage_logs
                    (reference_id, action, user_id, reference_usage_id, taxon_name_id, created_at)
                VALUES (%s, 5, 5, %s, %s, CURRENT_TIMESTAMP)
                """
            log_values = [(r[1], r[0], r[2]) for r in deleted_ids]
            cursor.executemany(log_query, log_values)
            conn.commit()




def get_per_usages(taxon_name_id, rows, prop_df_, name_df, ref_df, conn, backbone_ref_ids):
    """
    取得特定 taxon_name_id 的 per_usages 資料
    Parameters:
    -----------
    taxon_name_id : int/str
        分類名稱ID
    rows : pandas.DataFrame
        包含 per_usages 欄位的資料框
    total_df : pandas.DataFrame
        總資料框，包含 taxon_name_id, ru_status, reference_id, ru_id 等欄位
    name_df : pandas.DataFrame
        名稱資料框，包含 taxon_name_id, name_reference_id 等欄位
    Returns:
    --------
    list
        per_usages 清單，每個項目包含 pro_parte, reference_id, including_usage_id 等欄位
    """
    per_usages = []
    # 4-1 相同taxon_name_id的per_usages
    for p in prop_df_[prop_df_.taxon_name_id == taxon_name_id].to_dict('records'):
        if now_usages := json.loads(p.get('per_usages')):
            per_usages += [{**item, 'including_usage_id': p.get('ru_id')} for item in now_usages]
    # 4-2 相同taxon_name_id的有效usage (轉成per_usages形式)
    for usage in rows[(rows.taxon_name_id == taxon_name_id) & 
                         (rows.ru_status == 'accepted')].to_dict('records'):
        per_usages.append({
            "pro_parte": False,
            "reference_id": usage.get('reference_id'),
            "including_usage_id": usage.get('ru_id')
        })
    # 4-3 taxon_name_id本身的發表文獻
    name_rows = name_df[name_df.taxon_name_id == taxon_name_id]
    if not name_rows.empty:
        name_reference_id = name_rows.name_reference_id.values[0]
        if name_reference_id and name_reference_id not in [pp['reference_id'] for pp in per_usages]:
            per_usages.append({
                "pro_parte": False,
                "reference_id": name_reference_id,
                "is_from_published_ref": True,
                "including_usage_id": None
            })
    if len(per_usages):
        per_usages = pd.DataFrame(per_usages)
        # 只取ref_df中有的reference_id
        per_usages = per_usages[per_usages.reference_id.isin(ref_df.index.to_list())]
        # 排除backbone
        per_usages = per_usages[~per_usages.reference_id.isin(backbone_ref_ids)]
        # 如果有reference_id重複時 要依including_usage_reference_id優先序選擇優先的那個
        duplicated_refs = per_usages[per_usages.reference_id.duplicated()].reference_id.unique()
        for ref in duplicated_refs:
            temp = rows[rows.ru_id.isin(per_usages[per_usages.reference_id==ref].including_usage_id.to_list())]
            chosen_ru_list = select_global_latest_ru(temp, ref_df, conn)
            removing_ru_id = [rr for rr in temp.ru_id.to_list() if rr not in chosen_ru_list]
            per_usages = per_usages[~per_usages.including_usage_id.isin(removing_ru_id)]
        per_usages = per_usages.replace({np.nan: None})
        per_usages = per_usages.drop(columns=['including_usage_id']).to_dict('records')
    return per_usages