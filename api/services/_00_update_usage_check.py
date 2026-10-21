from conf.settings import env
import pymysql
import pandas as pd
import numpy as np
from django.utils import timezone
from datetime import datetime, timedelta


db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "database": env('DB_DBNAME'),
}


def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def check_taxon_usage():
    """每日更新檢查usage - 優化版本"""
    
    conn = pymysql.connect(**db_settings)
    now = timezone.now() + timedelta(hours=8)
    
    try:
        usage_df = _get_reference_usage_data(conn)
        
        _check_deleted_fixed_usages(conn, now)  # Error type 1
        _check_autonym_object_group(conn, usage_df, now)  # Error type 2 (原 2+3)
        _check_multiple_accepted_same_object_group(conn, usage_df, now)  # Error type 4
        _check_usage_key_constraint(conn, usage_df, now)  # Error type 5 (原 5+10)
        _check_taxon_name_in_multiple_groups(conn, usage_df, now)  # Error type 6 (原 6+9)
        _check_multiple_accepted_names(conn, usage_df, now)  # Error type 7
        _check_no_accepted_names(conn, usage_df, now)  # Error type 8
              
        ref_df = _get_reference_data(conn)
        _check_cannot_decide_latest(conn, usage_df, ref_df, now) # 新增：Error Type 12
         
        _record_check_timestamp(conn, now)  # Error type 11

    finally:
        conn.close()
    
    return 'done!'


def _check_autonym_object_group(conn, df, now):
    """Error type 2 (原 2+3 合併): 檢查同模在同一篇文獻中有多個不同的accepted_taxon_name_id"""
    """ 2: 檢查autonym/同模：同一篇文獻中有多個not-accepted在不同分類群 """
    """ 3: 檢查autonym/同模：同一篇文獻在不同分類群同時出現accepted和not-accepted """

    filtered_df = df[df.ru_status != 'misapplied']
    
    # 按 (object_group, reference_id) 看 accepted_taxon_name_id 數量
    check_data = (filtered_df[['object_group', 'reference_id', 'accepted_taxon_name_id']]
                  .drop_duplicates()
                  .groupby(['reference_id', 'object_group'], as_index=False)
                  .nunique())
    
    problematic_refs = check_data[check_data.accepted_taxon_name_id > 1][['object_group', 'reference_id']]
    
    if problematic_refs.empty:
        return
    
    # 取得 not-accepted 的 accepted_taxon_name_id
    not_accepted_df = filtered_df[filtered_df.ru_status == 'not-accepted']
    
    not_accepted_merged = problematic_refs.merge(
        not_accepted_df[['object_group', 'reference_id', 'accepted_taxon_name_id']],
        on=['object_group', 'reference_id']
    )
    
    existing_names = df[['taxon_name_id', 'object_group']].drop_duplicates()
    
    check_result = not_accepted_merged.merge(
        existing_names,
        left_on=['accepted_taxon_name_id', 'object_group'],
        right_on=['taxon_name_id', 'object_group'],
        how='left',
        indicator=True
    )
    
    oo_to_check = check_result[check_result['_merge'] == 'left_only']['object_group'].unique()
    
    if len(oo_to_check) == 0:
        return
    
    rows_to_check = problematic_refs[problematic_refs.object_group.isin(oo_to_check)].merge(df)
    rows_to_check = rows_to_check.fillna(0)

    sub = rows_to_check[['ru_id', 'autonym_group', 'object_group']]
    batch = [
        (ru, ag, og, 0, 0, 0, 2, 1)
        for ru, ag, og in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _check_usage_key_constraint(conn, df, now):
    """Error type 5 (原 5+10 合併): 檢查同組(accepted_taxon_name_id, taxon_name_id, reference_id)是否有多個status或多個ru_id"""
    """ 5: 檢查accepted_taxon_name_id, taxon_name_id, reference_id是否只對到一個status """
    """ 10: 檢查一組reference_id, accepted_taxon_name_id, taxon_name_id對到多個ru_id """
    group_cols = ['accepted_taxon_name_id', 'taxon_name_id', 'reference_id']
    
    grouped = df.groupby(group_cols, as_index=False).agg(
        status_count=('ru_status', 'nunique'),
        ru_count=('ru_id', 'nunique')
    )
    
    problematic = grouped[(grouped.status_count > 1) | (grouped.ru_count > 1)]
    problematic = problematic.fillna(0)

    sub = problematic[['accepted_taxon_name_id', 'taxon_name_id', 'reference_id']]
    batch = [
        (0, 0, 0, a, t, r, 5, 3)
        for a, t, r in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _check_taxon_name_in_multiple_groups(conn, df, now):
    """Error type 6 (原 6+9 合併): 檢查同學名在同一篇文獻中出現在多個分類群（非誤用）"""

    """ 6: 檢查學名在同一篇文獻中被設定成兩個分類群的同物異名 """
    """ 9: 檢查同一個學名出現在同一篇文獻中的兩個分類群且不是誤用 """

    filtered_df = df[df.ru_status != 'misapplied'][
        ['accepted_taxon_name_id', 'reference_id', 'taxon_name_id']
    ].drop_duplicates()
    
    grouped = filtered_df.groupby(['reference_id', 'taxon_name_id'], as_index=False).count()
    problematic = grouped[grouped.accepted_taxon_name_id > 1]
    problematic = problematic.fillna(0)

    sub = problematic[['taxon_name_id', 'reference_id']]
    batch = [
        (0, 0, 0, 0, t, r, 6, 2)
        for t, r in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _get_reference_usage_data(conn):
    """獲取主要的reference usage數據"""
    query = """
        SELECT ru.id, ru.status, ru.accepted_taxon_name_id, ru.taxon_name_id, ru.reference_id, 
               tn.object_group, tn.autonym_group, r.properties ->> '$.check_list_type'
        FROM reference_usages ru 
        JOIN taxon_names tn ON tn.id = ru.taxon_name_id
        JOIN `references` r ON r.id = ru.reference_id
        WHERE ru.is_title != 1 
          AND ru.status NOT IN ("", "undetermined") 
          AND ru.deleted_at IS NULL 
          AND ru.accepted_taxon_name_id IS NOT NULL 
          AND ru.reference_id != 95
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        
    df = pd.DataFrame(data, columns=[
        'ru_id', 'ru_status', 'accepted_taxon_name_id', 'taxon_name_id',
        'reference_id', 'object_group', 'autonym_group', 'check_list_type'
    ])
    
    # 過濾和清理數據
    df = df[df.check_list_type != 4]  # !=4 寫在query裡會排除掉null
    df = df.drop_duplicates().reset_index(drop=True)
    df = df.replace({np.nan: None})
    df = df.drop(columns=['check_list_type'])
    
    return df


def _get_reference_data(conn):
    """獲取 reference 資料用於 Type  檢查"""
    query = """
        SELECT r.id, r.publish_year, r.type, ac.publish_date, 
               r.properties ->> '$.book_title' as book_title,
               r.properties ->> '$.volume' as volume,
               r.properties ->> '$.issue' as issue
        FROM `references` r 
        LEFT JOIN api_citations ac ON ac.reference_id = r.id
        WHERE r.is_publish = 1
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    df = pd.DataFrame(data, columns=['id', 'publish_year', 'type', 'publish_date', 'book_title', 'volume', 'issue'])
    df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
    df = df.set_index('id')
    return df


def _insert_or_update_usage_check_batch(conn, data_list, now):
    """批次插入或更新"""
    if not data_list:
        return

    query = """
        INSERT INTO api_usage_check (reference_usage_id, autonym_group, object_group,
                                   accepted_taxon_name_id, taxon_name_id, reference_id,
                                   error_type, whitelist_type, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)
    """

    params = [(*data, now) for data in data_list]

    with conn.cursor() as cursor:
        cursor.executemany(query, params)
        conn.commit()


def _check_deleted_fixed_usages(conn, now):
    """Error type 1: 檢查被刪除的fixed usage_id"""
    query = """
        SELECT fixed_reference_usage_id 
        FROM api_taxon 
        WHERE is_deleted = 0 
          AND fixed_reference_usage_id IN (
              SELECT id FROM reference_usages WHERE deleted_at IS NOT NULL
          )
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        deleted_fixed_usages = cursor.fetchall()
    
    batch = [(usage_id, 0, 0, 0, 0, 0, 1, None) for usage_id, in deleted_fixed_usages]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _check_multiple_accepted_same_object_group(conn, df, now):
    """Error type 4: 檢查同模（不包含autonym）：同一篇文獻中多個accepted"""
    filtered_df = df[
        (df.autonym_group.isnull()) &
        (df.object_group.notnull()) &
        (df.ru_status == 'accepted')
    ]
    
    check_data = (filtered_df[['object_group', 'taxon_name_id', 'reference_id']]
                  .drop_duplicates()
                  .groupby(['reference_id', 'object_group'], as_index=False)
                  .nunique())
    
    problematic_refs = check_data[check_data.taxon_name_id > 1]
    
    if problematic_refs.empty:
        return
    
    # 直接 merge 取代迴圈 concat
    rows_to_check = problematic_refs[['object_group', 'reference_id']].merge(
        df[df.ru_status == 'accepted']
    )
    rows_to_check = rows_to_check.fillna(0)

    sub = rows_to_check[['ru_id', 'autonym_group', 'object_group']]
    batch = [
        (ru, ag, og, 0, 0, 0, 4, 1)
        for ru, ag, og in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _check_multiple_accepted_names(conn, df, now):
    """Error type 7: 檢查同一個分類群有一個以上的接受名"""
    all_pairs = df[['accepted_taxon_name_id', 'reference_id']].drop_duplicates()
    accepted_pairs = df[df.ru_status == 'accepted'][
        ['accepted_taxon_name_id', 'reference_id', 'ru_status']
    ].drop_duplicates()

    grouped = accepted_pairs.groupby(['accepted_taxon_name_id', 'reference_id'],
                                     as_index=False).count()
    merged = all_pairs.merge(grouped, how='left')
    
    multiple_accepted = merged[merged.ru_status > 1]
    multiple_accepted = multiple_accepted.fillna(0)

    sub = multiple_accepted[['accepted_taxon_name_id', 'reference_id']]
    batch = [
        (0, 0, 0, a, 0, r, 7, 0)
        for a, r in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _check_no_accepted_names(conn, df, now):
    """Error type 8: 檢查同一個分類群裡面沒有任何接受名"""
    all_pairs = df[['accepted_taxon_name_id', 'reference_id']].drop_duplicates()
    accepted_pairs = df[df.ru_status == 'accepted'][
        ['accepted_taxon_name_id', 'reference_id', 'ru_status']
    ].drop_duplicates()

    grouped = accepted_pairs.groupby(['accepted_taxon_name_id', 'reference_id'],
                                     as_index=False).count()
    merged = all_pairs.merge(grouped, how='left')
    
    no_accepted = merged[merged.ru_status.isna()]
    no_accepted = no_accepted.fillna(0)

    sub = no_accepted[['accepted_taxon_name_id', 'reference_id']]
    batch = [
        (0, 0, 0, a, 0, r, 8, 0)
        for a, r in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)


def _record_check_timestamp(conn, now):
    """Error type 11: 記錄檢查的時間戳"""
    query = """
        INSERT INTO api_usage_check (error_type, updated_at) VALUES (11, %s)
        ON DUPLICATE KEY UPDATE updated_at = %s
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query, (now, now))
        conn.commit()


def _check_cannot_decide_latest(conn, df, ref_df, now):
    """Error Type 12: 同學名/同模有多個reference_id且無法決出最新"""
    
    if df.empty:
        return
    
    # 同模檢查（排除 misapplied）
    obj_df = df[(df.ru_status != 'misapplied') & (df.object_group.notnull())]
    _check_cannot_decide_by_dimension(conn, obj_df, ref_df, 'object_group', now)
    
    # 同學名檢查（排除 misapplied，只看 object_group 為 null）
    name_df = df[(df.ru_status != 'misapplied') & (df.object_group.isnull()) & (df.ru_status.isin(['accepted', 'not-accepted']))]
    _check_cannot_decide_by_dimension(conn, name_df, ref_df, 'taxon_name_id', now)


def _check_cannot_decide_by_dimension(conn, df, ref_df, dimension, now):
    """檢查指定維度是否有無法決出最新文獻的情況（向量化版）"""

    # 先找出有多個 reference_id 的分組
    multi_ref = df.groupby(dimension)['reference_id'].nunique()
    multi_ref = multi_ref[multi_ref > 1].index
    if len(multi_ref) == 0:
        return

    df_filtered = df[df[dimension].isin(multi_ref)].copy()
    df_filtered = df_filtered.merge(
        ref_df[['type', 'publish_date', 'publish_year', 'book_title', 'volume', 'issue']],
        right_index=True, left_on='reference_id', how='left'
    )

    # 所有篩選條件都只跟 reference_id 有關，先壓縮成「每個 (維度, reference_id) 一列」，
    # 大幅減少後續 groupby 的資料量，此時 nunique(reference_id) 等同於分組列數
    u = df_filtered[[dimension, 'reference_id', 'type', 'publish_year',
                     'publish_date', 'book_title', 'volume', 'issue']].drop_duplicates(
        subset=[dimension, 'reference_id'])

    def keep_multi(frame):
        """只保留仍有 >1 個 reference 的分組"""
        return frame[frame.groupby(dimension)['reference_id'].transform('size') > 1]

    # Step 1: type 優先性
    u['_is6'] = u['type'] == 6
    has6 = u.groupby(dimension)['_is6'].transform('max').astype(bool)
    u = u[(~has6) | (u['type'] == 6)]

    # 分組非全同 type 時才移除 4、5（順序：先移 4，再依移除後的狀態判斷是否移 5）
    tmin = u.groupby(dimension)['type'].transform('min')
    tmax = u.groupby(dimension)['type'].transform('max')
    u = u[~((u['type'] == 4) & ~((tmin == 4) & (tmax == 4)))]

    tmin = u.groupby(dimension)['type'].transform('min')
    tmax = u.groupby(dimension)['type'].transform('max')
    u = u[~((u['type'] == 5) & ~((tmin == 5) & (tmax == 5)))]

    u = keep_multi(u)
    if u.empty:
        return

    # Step 2: publish_year 取最大
    u = u[u['publish_year'] == u.groupby(dimension)['publish_year'].transform('max')]
    u = keep_multi(u)
    if u.empty:
        return

    # Step 3: 分組內有非空 publish_date 時，取最大
    has_date = u.groupby(dimension)['publish_date'].transform('count') > 0
    max_date = u.groupby(dimension)['publish_date'].transform('max')
    u = u[(~has_date) | (u['publish_date'] == max_date)]
    u = keep_multi(u)
    if u.empty:
        return

    # Step 4: 同書同卷號（book_title/volume/issue 皆唯一）視為可決出，排除之
    bt = u.groupby(dimension)['book_title'].transform('nunique')
    vol = u.groupby(dimension)['volume'].transform('nunique')
    iss = u.groupby(dimension)['issue'].transform('nunique')
    decidable = np.maximum(np.maximum(bt, vol), iss) == 1
    u = u[~decidable]
    if u.empty:
        return

    # u 內剩下的 (維度, reference_id) 即無法決出者，撈回原始 ru 列
    survivors = u[[dimension, 'reference_id']].drop_duplicates()
    result = df_filtered.merge(survivors, on=[dimension, 'reference_id'])

    result = result.fillna(0)
    sub = result[['ru_id', 'autonym_group', 'object_group',
                  'accepted_taxon_name_id', 'taxon_name_id', 'reference_id']]
    batch = [
        (ru, ag, og, a, t, r, 12, 0)
        for ru, ag, og, a, t, r in sub.itertuples(index=False, name=None)
    ]
    _insert_or_update_usage_check_batch(conn, batch, now)