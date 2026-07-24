# 202508 

# reference_usage_id 欄位先留著，但這個id有可能是會變動的，不能使用
# 改用taxon_name_id, accepted_taxon_name_id, reference_id組合為唯一值
# 實務上仍有可能會重複，若有重複情況則取集中一筆

import pandas as pd
from numpy import nan
from api.utils import get_whitelist, sub_lin_ranks, rank_order_map
from api.services.utils.common import get_conn
from api.services.utils.checklist import *

def build_total_df_default(usage_df, ref_df):

    # 202605 拿掉白名單機制
    whitelist_list_1, whitelist_list_2 = [], []

    total_df = usage_df
    total_df = total_df.drop_duplicates()
    total_df = total_df.replace({nan:None})
    total_df = total_df.reset_index(drop=True)

    # 決定誰是接受學名
    total_df, conflict_parent_list = select_latest_ru(total_df, ref_df, return_conflict=True)

    # 分類觀檢查

    # 2 同模異名檢查
    # 若分在多群，需檢查同模式學名的文獻優先性：
    # 沒有is_latest=TRUE/全部都是is_latest=FALSE的同模式：

    check_obj_list = get_check_obj_list(total_df,whitelist_list_1)

    cannot_decide = []

    loop_count = 0
    while len(check_obj_list):
        for ooo in check_obj_list:
            # 改用同模本身的usage判斷
            # 所有的同模式學名之學名使用，應併入文獻優先的學名使用對應的有效學名的分類群。
            # 整群併入 (accepted_name_id + reference_id相同的) 但要是同模accepted_name_id
            temp = total_df[(total_df.object_group==ooo)&(total_df.ru_status!='misapplied')]
            rows = total_df[total_df.tmp_taxon_id.isin(temp.tmp_taxon_id.unique())]
            newest_ru_id_list = select_global_latest_ru(temp, ref_df)
            if rows[rows.ru_id.isin(newest_ru_id_list)].tmp_taxon_id.nunique() == 1:
                # 併入的tmp_taxon_id
                merging_tmp_taxon_id = temp[temp.ru_id.isin(newest_ru_id_list)].tmp_taxon_id.values[0]
                # 所有同模accepted usages都併入同一個tmp_taxon_id 不管地位
                merging_keys = (
                    temp[(temp.ru_status == 'accepted') & (temp.tmp_taxon_id != merging_tmp_taxon_id)]
                    [['accepted_taxon_name_id', 'reference_id']]
                    .drop_duplicates()
                )
                # Step 2: 建 key_pair 欄位，加快對應速度
                merging_ru_ids = temp.ru_id.to_list() # 這邊就會包含單純的無效名 (接受名非同模)
                # 這邊會包含如果同模異名是accepted 併入整個無效名
                rows = rows.copy()
                rows['key_pair'] = list(zip(rows.accepted_taxon_name_id, rows.reference_id))
                merging_keys_set = set(zip(merging_keys.accepted_taxon_name_id, merging_keys.reference_id))
                merging_ru_ids += rows.loc[rows['key_pair'].isin(merging_keys_set), 'ru_id'].tolist()
                total_df.loc[total_df.ru_id.isin(merging_ru_ids),'tmp_taxon_id'] = merging_tmp_taxon_id
                reset_list = list(set(temp.tmp_taxon_id.unique()))
                select_latest_ru(total_df[total_df.tmp_taxon_id.isin(reset_list)], ref_df, mark_on_original_df=total_df)
            else:
                if temp['taxon_name_id'].nunique() > 1: # 排除掉同學名的情況 因為後面會在判斷 且有可能是whitelist
                    cannot_decide.append(ooo)
                else:
                    print(temp['taxon_name_id'].unique()[0])
        # 再檢查一次
        check_obj_list = get_check_obj_list(total_df,whitelist_list_1)
        check_obj_list = [c for c in check_obj_list if c not in cannot_decide]
        loop_count += 1


    # 同學名 
    # 2024-12-23 只考慮沒有同模關係的學名

    check_name_list = get_check_name_list(total_df, whitelist_list_2)

    # 學名之間可能會互相影響 
    cannot_decide = []
    # 若沒有把所有需要重新決定最新的分類群處理完 會造成後面有問題

    loop_count = 0
    while len(check_name_list):
        for ccc in check_name_list:
            # 用學名本身的usage判斷
            temp = total_df[(total_df.taxon_name_id==ccc)&(total_df.ru_status!='misapplied')]
            newest_ru_id_list = select_global_latest_ru(temp, ref_df)
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
                reset_list = list(set(temp.tmp_taxon_id.unique()))
                select_latest_ru(total_df[total_df.tmp_taxon_id.isin(reset_list)], ref_df, mark_on_original_df=total_df)
            else:
                cannot_decide.append(ccc)
        check_name_list = get_check_name_list(total_df, whitelist_list_2)
        check_name_list = [c for c in check_name_list if c not in cannot_decide]
        loop_count += 1

    # step 3. 若分類群中有兩筆最新接受名，且為上下階層的關係，將其獨立


    # NOTE 目前會被組在一起的情況
    # 1. 相同accepted_taxon_name_id的學名使用
    # 2. 承名種下的學名使用
    # 3. 接受名為同模式學名的學名使用（原始組合名本身、有相同的原始組合名、是對方的原始組合名）

    # 需要拆分的一定是 1. 承名種下的關係 2. 同模式學名關係
    # 先處理上下階層的關係

    # 上階層同為最新接受名的情況


    # 這邊會直接回傳：若分類群中有兩筆最新接受名，且為上下階層的關係
    check_multiple_accepted = get_multiple_accepted(total_df) 
    not_2_layer = []

    loop_count = 0
    while len(check_multiple_accepted):
        for s in check_multiple_accepted: # 214
            rows = total_df[total_df.tmp_taxon_id==s]
            # 限定最新接受名是種下階層
            # 有可能兩個都是種下 用max_layer_count來判斷誰是下階層
            rows_latest = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)&(rows.rank_id.isin(sub_lin_ranks))]
            max_layer_count = rows_latest.layer_count.max()
            # 需要確認是不是只差一層
            if not len(rows.layer_count.unique()) == 2 or not (rows.layer_count.max()-rows.layer_count.min()==1):
                not_2_layer.append(s)
            # 2024-12 這邊直接改成按照階層分
            # 給予下階層新的tmp_taxon_id
            new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
            sub_ru_ids = rows[rows.layer_count!=max_layer_count].ru_id.to_list()
            total_df.loc[total_df.ru_id.isin(sub_ru_ids),'tmp_taxon_id'] = new_tmp_taxon_id
            select_latest_ru(total_df[total_df.tmp_taxon_id.isin([s, new_tmp_taxon_id])], ref_df, mark_on_original_df=total_df)
        check_multiple_accepted = get_multiple_accepted(total_df)
        loop_count += 1


    # step 4. 若最新接受名是種下，需檢查種階層有沒有包含在裡面，有的話將其獨立
    # NOTE 確認剩下的check_tmp_taxon_id是不是都是承名種下
    # 是的話應該會一起出現在下方的no_parent中

    # 這邊好像不一定一定是種下 也有可能種被設定為最新接受名 因為group order的關係 -> 最新文獻為同一篇 但接受名不同 -> 移到 step 8
    # 處理種階層可能被包在種下的無效情況

    parent_not_accepted = get_parent_not_accepted(total_df, sub_lin_ranks) # 大概一分鐘

    # 處理上階層被合併在一起 但不是最新接受名的情況

    loop_count = 0

    while len(parent_not_accepted):
        for s in parent_not_accepted:
            rows = total_df[total_df.tmp_taxon_id==s]
            # 限定最新接受名是種下階層
            # 有可能兩個都是種下 用max_layer_count來判斷誰是下階層
            rows_latest = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)&(rows.rank_id.isin(sub_lin_ranks))]
            max_layer_count = rows_latest.layer_count.max()
            # 2024-12 這邊直接改成按照階層分
            # 給予下階層新的tmp_taxon_id
            new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
            sub_ru_ids = rows[rows.layer_count!=max_layer_count].ru_id.to_list()
            total_df.loc[total_df.ru_id.isin(sub_ru_ids),'tmp_taxon_id'] = new_tmp_taxon_id
            select_latest_ru(total_df[total_df.tmp_taxon_id.isin([s, new_tmp_taxon_id])], ref_df, mark_on_original_df=total_df)
        parent_not_accepted = get_parent_not_accepted(total_df, sub_lin_ranks)
        loop_count += 1


    # step 5. 若承名關係最新接受名為種，種與種下各自有有效的學名使用，且除backbone外沒有其他文獻指出他們為同物異名，將承名種下獨立出來


    check_autonyms = get_check_autonyms(total_df)


    # 承名種下有被非backbone的文獻設定成同物異名
    # sub_auto_is_syns = []

    for s in check_autonyms: # 1273
        rows = total_df[total_df.tmp_taxon_id==s]
        rows = rows.merge(ref_df[['type']], right_index=True, left_on='reference_id')
        latest = rows[(rows.ru_status=='accepted')&(rows.is_latest==True)].to_dict('records')[0]
        parent_auto_group = latest.get('autonym_group')
        max_layer_count = latest.get('layer_count')
        # parent_object_group = latest.get('object_group')
        parent_taxon_name_id = latest.get('taxon_name_id')
        # 先確定承名種下是不是在同一個taxon中 且有有效的學名使用
        sub_accepted_rows = rows[(rows.autonym_group==parent_auto_group)
                                &(rows.parent_taxon_name_id==parent_taxon_name_id)
                                &(rows.ru_status=='accepted')]
        if len(sub_accepted_rows):
            current_accepted_taxon_name_id = sub_accepted_rows.taxon_name_id.values[0]
            # 確認沒有除了backbone以外的同物異名關係
            # 種是種下的同物異名 / 種下是種的同物異名
            cond_1 = rows[(rows.accepted_taxon_name_id==parent_taxon_name_id)
                            &(rows.taxon_name_id==current_accepted_taxon_name_id)
                            &(rows.type!=4)&(rows.ru_status=='not-accepted')].empty 
            cond_2 = rows[(rows.accepted_taxon_name_id==current_accepted_taxon_name_id)
                            &(rows.taxon_name_id==parent_taxon_name_id)&(rows.type!=4)
                            &(rows.ru_status=='not-accepted')].empty
            if cond_1 and cond_2: # 兩個都是empty = 並沒有同物異名關係
                # 2024-12 這邊直接改成按照階層分
                # 給予下階層新的tmp_taxon_id
                new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
                sub_ru_ids = rows[rows.layer_count!=max_layer_count].ru_id.to_list()
                total_df.loc[total_df.ru_id.isin(sub_ru_ids),'tmp_taxon_id'] = new_tmp_taxon_id
                select_latest_ru(total_df[total_df.tmp_taxon_id.isin([s, new_tmp_taxon_id])], ref_df, mark_on_original_df=total_df)
            # else:
            #     sub_auto_is_syns.append(s)



    # step 10. 確認誤用在分類群的地位
    # 同時出現誤用與無效：若與同一分類群的有效名為同模，都改成無效。若與有效名非同模，判斷文獻優先性決定是誤用或無效，都改為判斷結果。
    # 誤用名若與同一分類群的有效名為同模式異名，需改為無效名。

    check_misapplied_list = total_df[total_df.ru_status=='misapplied'].tmp_taxon_id.unique()
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
                    latest_misapplied_ru = select_global_latest_ru(mm_rows, ref_df)
                    if len(latest_misapplied_ru) == 1:
                        latest_misapplied_ru = latest_misapplied_ru[0]
                        current_status = mm_rows[mm_rows.ru_id==latest_misapplied_ru].ru_status.values[0]
                        if current_status == 'misapplied':
                            # 應該要先確認誤用名是不是在其他獨立的taxon 且地位非誤用
                            # 如果是同模異名也不需要拿走
                            # 最新地位為誤用 且本身在這個分類群中有accepted的usage & 不是其他分類群的accepted or not-accepted
                            if len(total_df[(total_df.taxon_name_id==mm)&(total_df.ru_status=='accepted')&(total_df.tmp_taxon_id==t)]) and not len(total_df[(total_df.taxon_name_id==mm)&(total_df.ru_status!='misapplied')&(total_df.tmp_taxon_id!=t)]):
                                need_new_taxon_misapplied.append(mm)
                        total_df.loc[total_df.ru_id.isin(mm_rows[mm_rows.ru_status!='accepted'].ru_id.to_list()),'ru_status'] = current_status
            else: 
                # 只有誤用一種地位 -> 不對 可能會有誤用or接受
                # 誤用名若與同一分類群的有效名為同模式異名，需改為無效名。
                # 確認是不是同模
                if is_obj_syns:
                    total_df.loc[total_df.ru_id.isin(mm_rows[mm_rows.ru_status=='misapplied'].ru_id.to_list()),'ru_status'] = 'not-accepted'


    if len(check_misapplied_list):
        select_latest_ru(total_df[total_df.tmp_taxon_id.isin(check_misapplied_list)], ref_df, mark_on_original_df=total_df)

    for mm in need_new_taxon_misapplied:
        if len(total_df[(total_df.taxon_name_id==mm)&(total_df.ru_status!='misapplied')]):
            rows = total_df[total_df.accepted_taxon_name_id==mm]
            if len(rows) == 1:
                now_tmp_taxon_id = rows.tmp_taxon_id.values[0]
                new_tmp_taxon_id = total_df.tmp_taxon_id.max() + 1
                total_df.loc[total_df.ru_id.isin(rows.ru_id.to_list()), 'tmp_taxon_id'] = new_tmp_taxon_id  
                select_latest_ru(total_df[total_df.tmp_taxon_id.isin([now_tmp_taxon_id,new_tmp_taxon_id])], ref_df, mark_on_original_df=total_df)

    total_df['taxon_status'] = ''
    total_df = determine_taxon_status(total_df) 

    # 這邊在串回來的時候 要把accepted_taxon_name_id改回原本的 

    conn = get_conn()

    query = '''SELECT id, accepted_taxon_name_id, taxon_name_id, reference_id
                FROM reference_usages WHERE id IN %s'''

    with conn.cursor() as cursor:
        execute_line = cursor.execute(query, (total_df.ru_id.to_list(),))
        rus = cursor.fetchall()
        rus = pd.DataFrame(rus, columns=['ru_id','accepted_taxon_name_id', 'taxon_name_id','reference_id'])

    # 因為前面誤用有調整accepted_taxon_name_id 所以在這邊調整回來
    total_df = total_df.drop(columns=['accepted_taxon_name_id', 'taxon_name_id', 'reference_id'])
    total_df = total_df.merge(rus[['ru_id', 'accepted_taxon_name_id', 'taxon_name_id', 'reference_id']])


    check = total_df[(total_df.is_latest==1)&(total_df.taxon_status=='accepted')].drop_duplicates()

    check_tmp = check.groupby(['tmp_taxon_id']).ru_id.nunique()
    check_tmp[check_tmp>1]

    if len(check_tmp[check_tmp>1]):
        select_latest_ru(total_df[total_df.tmp_taxon_id.isin(check_tmp[check_tmp>1].index.to_list())], ref_df, mark_on_original_df=total_df)


    total_df = total_df.drop_duplicates()
    total_df = total_df.reset_index(drop=True)

    return total_df, usage_df


def build_total_df_taicol(usage_df, taxon_ids, only_in_taiwan, exclude_cultured):

    # taicol 分類觀：直接沿用 api_taxon_usages 既有的分群結果
    conn = get_conn()
    with conn.cursor() as cursor:
        query = '''
            SELECT atu.is_latest, atu.status, atu.taxon_id, atu.reference_usage_id
            FROM api_taxon_usages atu
            JOIN api_taxon t ON atu.taxon_id = t.taxon_id
            WHERE atu.is_deleted = 0
        '''
        if only_in_taiwan == 'yes':
            query += ' AND t.is_in_taiwan = 1'
        if exclude_cultured == 'yes':
            query += ' AND t.is_cultured = 0'
        query += ' AND t.taxon_id IN %s'
        cursor.execute(query, (list(taxon_ids),))
        taxon_df = pd.DataFrame(
            cursor.fetchall(),
            columns=['is_latest', 'taxon_status', 'taxon_id', 'ru_id'],
        )

    # 以 taxon_id 為分組基礎，給定流水號
    taxon_df['tmp_taxon_id'] = taxon_df.groupby('taxon_id', sort=False).ngroup() + 1

    # usage_df 已含自己的 tmp_taxon_id（assign_group 產生），移除避免欄位碰撞
    total_df = taxon_df.merge(usage_df.drop(columns=['tmp_taxon_id']), on='ru_id', how='inner')

    total_df = total_df[[
        'ru_id', 'ru_status', 'is_in_taiwan', 'parent_taxon_name_id', 'group',
        'rank_id', 'nomenclature_id', 'object_group', 'autonym_group',
        'layer_count', 'publish_year', 'group_id', 'tmp_taxon_id', 'is_latest',
        'taxon_status', 'accepted_taxon_name_id', 'taxon_name_id',
        'reference_id', 'taxon_id'
    ]]
    total_df = total_df.drop_duplicates().reset_index(drop=True)

    return total_df


def process_taxon_checklist(pairs, exclude_cultured, only_in_taiwan, references, classification_view, taxon_ids, completeness, usage_references):

    print(pairs, exclude_cultured, only_in_taiwan, references, classification_view, taxon_ids, completeness, usage_references)
    # 應該先判斷是不是有 accepted_taxon_name_id & taxon_name_id & reference_id 對應的usage需要被刪除
    check_deleted_usages()

    ref_df, usage_df, common_name_rus = get_dfs(pairs, exclude_cultured, only_in_taiwan) 
    usage_df = assign_group_and_tmp_taxon_ids(usage_df) # 初步分群

    if classification_view == 'taicol':
        # taicol 用另一套流程產生 total_df（自行加上 tmp_taxon_id）
        total_df = build_total_df_taicol(usage_df, taxon_ids, only_in_taiwan, exclude_cultured)
    else:
        total_df, usage_df = build_total_df_default(usage_df, ref_df)


    df_for_prop = total_df[['ru_id','is_latest','taxon_status','tmp_taxon_id', 'taxon_name_id', 'accepted_taxon_name_id', 'reference_id', 'ru_status']]

    # 接著處理階層

    # 處理 parent_taxon_name_id
    # 如果最新學名使用的parent_taxon_name_id為null 改為有資料的最新
    # 如果parent_taxon_name_id已經沒有usage的話 要先改成null

    # 注意這邊會不會漏掉 不會處理到全部

    parent_name_ids = usage_df.parent_taxon_name_id.dropna().unique()
    usage_name_ids_set = set(usage_df.taxon_name_id.unique())
    no_usage_name_ids = [p for p in parent_name_ids if p not in usage_name_ids_set]

    total_df.loc[total_df.parent_taxon_name_id.isin(no_usage_name_ids), 'parent_taxon_name_id'] = None
    usage_df.loc[usage_df.parent_taxon_name_id.isin(no_usage_name_ids), 'parent_taxon_name_id'] = None


    # taicol 分類觀的 parent 一律改由 api_taxon 解析（見下方 total_grouped 前的處理），
    # 以下回填與正規化只在其他分類觀使用
    if classification_view != 'taicol':
        # 這邊會有對到的parent_taxon_name_id為parent本身的not-accepted name
        df_for_parent = df_for_prop.merge(usage_df[['ru_id','parent_taxon_name_id','publish_year']], how='left')

        parent_null_taxon_ids = df_for_parent[(df_for_parent.is_latest==True)&(df_for_parent.taxon_status=='accepted')&(df_for_parent.parent_taxon_name_id.isnull())].tmp_taxon_id.unique()
        for pp in parent_null_taxon_ids:
            rows = df_for_parent[(df_for_parent.tmp_taxon_id==pp)&(df_for_parent.taxon_status=='accepted')&(df_for_parent.parent_taxon_name_id.notnull())]
            if len(rows):
                if len(rows) == 1:
                    newest_ru_id = rows.ru_id.values[0]
                else:
                    newest_ru_id_list = select_global_latest_ru(rows, ref_df)
                    newest_ru_id = newest_ru_id_list[0]
                newest_parent = rows[rows.ru_id==newest_ru_id].parent_taxon_name_id.values[0]
                # 把這個對應到的parent_taxon_name_id 補到最新接受的學名使用
                accepted_ru_id = df_for_parent[(df_for_parent.tmp_taxon_id==pp)&(df_for_parent.taxon_status=='accepted')&(df_for_parent.is_latest==1)].ru_id.values[0]
                total_df.loc[total_df.ru_id==accepted_ru_id,'parent_taxon_name_id'] = newest_parent
                usage_df.loc[usage_df.ru_id==accepted_ru_id,'parent_taxon_name_id'] = newest_parent


        # parent_taxon_name_id 正規化用的對照表
        # name -> tmp_taxon_id（排除 misapplied；排除一個 name 對到多個分類群的情況）
        name_taxon_map = df_for_prop[df_for_prop.taxon_status != 'misapplied'][
            ['taxon_name_id', 'tmp_taxon_id', 'taxon_status']].drop_duplicates()

        name_duplicates = name_taxon_map[name_taxon_map.taxon_status == 'not-accepted']
        name_duplicates = name_duplicates[name_duplicates.taxon_name_id.duplicated(keep=False)]
        exclude_pairs = set(zip(name_duplicates['tmp_taxon_id'], name_duplicates['taxon_name_id']))
        name_taxon_map = name_taxon_map[~name_taxon_map.apply(
            lambda r: (r['tmp_taxon_id'], r['taxon_name_id']) in exclude_pairs, axis=1)]

        # tmp_taxon_id -> 該群最新有效名
        accepted_name_map = df_for_prop[
            (df_for_prop.is_latest == True) & (df_for_prop.taxon_status == 'accepted')
        ].drop_duplicates('tmp_taxon_id').set_index('tmp_taxon_id')['taxon_name_id'].to_dict()

        name_to_tmp = dict(zip(name_taxon_map.taxon_name_id, name_taxon_map.tmp_taxon_id))
        parent_name_normalize = {
            n: (accepted_name_map.get(t), t) for n, t in name_to_tmp.items()
        }


    # # 處理屬性

    # 以下只處理ru_status = accepted
    prop_df = get_prop_df(total_df.ru_id.to_list()+common_name_rus)
    prop_df = prop_df.merge(ref_df[['publish_date','type','publish_year','subtitle']], right_index=True, left_on="reference_id", how='left')

    prop_df = select_priority_prop(prop_df)

    # merge全部的usage
    prop_df_ = prop_df.merge(df_for_prop)

    # common_name_rus（ref_95）在 inner merge 被丟掉，補回去以彙整 common_names
    if common_name_rus:
        name_to_tmp_taxon_id = (
            usage_df.drop_duplicates('taxon_name_id')
            .set_index('taxon_name_id')['tmp_taxon_id']
            .to_dict()
        )
        cn_rows = prop_df[prop_df.ru_id.isin(common_name_rus)].copy()
        cn_rows['tmp_taxon_id'] = cn_rows['taxon_name_id'].map(name_to_tmp_taxon_id)
        cn_rows = cn_rows[cn_rows['tmp_taxon_id'].notna()]
        if len(cn_rows):
            for col in prop_df_.columns:
                if col not in cn_rows.columns:
                    cn_rows[col] = None
            cn_rows = cn_rows[prop_df_.columns]
            prop_df_ = pd.concat([prop_df_, cn_rows], ignore_index=True)

    prop_df_['ru_order'] = prop_df_.groupby('tmp_taxon_id').cumcount() + 1


    all_prop_df = determine_taxon_prop(prop_df_)

    # 處理per_usages
    conn = get_conn()
    with conn.cursor() as cursor:
        # backbone_ref_ids
        query = 'SELECT id from `references` WHERE `type` IN (4, 6) AND deleted_at IS NULL;'
        execute_line = cursor.execute(query)
        backbone_ref_ids = cursor.fetchall()
        backbone_ref_ids = [b[0] for b in backbone_ref_ids]
        # name_df
        name_query = 'SELECT id, reference_id, `name`, type_specimens, original_taxon_name_id FROM taxon_names WHERE id IN %s'
        cursor.execute(name_query, (total_df.taxon_name_id.unique().tolist(),))
        name_df = pd.DataFrame(cursor.fetchall(), columns=['taxon_name_id', 'name_reference_id', 'name', 'type_specimens', 'original_taxon_name_id'])
        name_df = name_df.replace({np.nan: None})
        # tmp_checklist_id
        query = '''SELECT max(tmp_checklist_id) from tmp_namespace_usages;'''
        execute_line = cursor.execute(query)
        tmp_checklist_id = cursor.fetchone()[0]
        tmp_checklist_id = tmp_checklist_id + 1 if tmp_checklist_id else 1

    # (a) prop_df_ 依 taxon_name_id 分群(全部一次 to_dict 再分組,避免逐 group 呼叫)
    prop_records_by_name = {}
    for record in prop_df_.to_dict('records'):
        tnid = record['taxon_name_id']
        prop_records_by_name.setdefault(tnid, []).append(record)

    # (b) name_df 依 taxon_name_id 建索引
    name_by_id = {r['taxon_name_id']: r for r in name_df.to_dict('records')}

    # (c) references.publish_year lookup（取代 get_per_usages 內 N 次的 DB query）
    ref_publish_year_lookup = {}
    if 'publish_year' in ref_df.columns:
        py_series = pd.to_numeric(ref_df['publish_year'], errors='coerce')
        ref_publish_year_lookup = {
            k: (None if pd.isna(v) else int(v))
            for k, v in py_series.items()
        }

    # 補撈：所有可能被丟進 per_usages 的 reference_id
    extra_ref_ids = set()

    # (1) prop_df_.per_usages JSON 內的 reference_id（4-1 段會用到）
    for raw in prop_df_.per_usages.dropna().unique():
        try:
            for it in json.loads(raw):
                rid = it.get('reference_id')
                if rid is not None:
                    extra_ref_ids.add(rid)
        except (json.JSONDecodeError, TypeError):
            continue

    # (2) total_df 內 accepted usage 的 reference_id(4-2 段會用到)
    for rid in total_df[total_df.ru_status == 'accepted'].reference_id.dropna().unique():
        extra_ref_ids.add(int(rid))

    # (3) name_df.name_reference_id(4-3 段會用到)
    for rid in name_df.name_reference_id.dropna().unique():
        extra_ref_ids.add(int(rid))

    # 過濾掉已在 lookup 內的
    extra_ref_ids = {r for r in extra_ref_ids if r not in ref_publish_year_lookup}

    if extra_ref_ids:
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT id, publish_year FROM `references` WHERE id IN %s',
                (list(extra_ref_ids),)
            )
            for rid, py in cursor.fetchall():
                try:
                    ref_publish_year_lookup[rid] = int(py) if py is not None else None
                except (ValueError, TypeError):
                    ref_publish_year_lookup[rid] = None

    # (d) 預先批次撈「補原始名」可能用到的 taxon_names（取代迴圈內 N 次的 DB query）
    orig_lookup = {}
    if completeness in ('full', 'concise'):
        accepted_name_ids = total_df[
            (total_df.is_latest == True) & (total_df.taxon_status == 'accepted')
        ].taxon_name_id.unique()
        needed_orig_ids = set()
        for nm_id in accepted_name_ids:
            nm_row = name_by_id.get(nm_id)
            if nm_row is not None:
                orig = nm_row.get('original_taxon_name_id')
                if orig is not None and not pd.isna(orig):
                    needed_orig_ids.add(int(orig))
        if needed_orig_ids:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT id, reference_id, `name`, rank_id FROM taxon_names WHERE id IN %s',
                    (list(needed_orig_ids),)
                )
                for orig_id, orig_ref_id, orig_name, orig_rank_id in cursor.fetchall():
                    orig_lookup[orig_id] = (orig_ref_id, orig_name, orig_rank_id)

    if classification_view == 'taicol':
        parent_name_map = get_taicol_parent_name_map(total_df.taxon_id.unique())
        mask = (total_df.is_latest == True) & (total_df.taxon_status == 'accepted')
        total_df['parent_taxon_name_id'] = None
        total_df.loc[mask, 'parent_taxon_name_id'] = total_df.loc[mask, 'taxon_id'].map(parent_name_map)

    # (e) 預先 group by tmp_taxon_id（供主迴圈使用，避免 O(n²) 過濾）
    total_grouped = {k: v for k, v in total_df.groupby('tmp_taxon_id', sort=False)}
    prop_grouped = {k: v for k, v in all_prop_df.groupby('tmp_taxon_id', sort=False)}

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
    # group -> 分類群
    # order -> 排序（不管分類群的總排序）
    # per_usages
    # type_specimens
    # properties
    # 統一新增
    # tmp_checklist_id
    # updated_at

    final_usages = []
    references_set = set(references)

    def filter_per_usages(pu_list):
        if usage_references == 'none':
            return []
        if usage_references == 'original':
            return [u for u in pu_list if u.get('is_from_published_ref')]
        if usage_references == 'originalincluded':
            return [u for u in pu_list if u.get('is_from_published_ref') or u.get('reference_id') in references_set]
        return pu_list  # 'all'

    for nt, rows in total_grouped.items():
        try:
            acc_rows = rows[(rows['is_latest']==True) & (rows['taxon_status'] == 'accepted')]
            row = acc_rows.iloc[0]  # 等同於原本的 total_df.iloc[i]
            parent_taxon_name_id = row.parent_taxon_name_id

            if classification_view != 'taicol' and pd.notna(parent_taxon_name_id):
                mapped = parent_name_normalize.get(parent_taxon_name_id)
                if mapped:
                    mapped_name, mapped_tid = mapped
                    parent_taxon_name_id = None if mapped_tid == nt else mapped_name
            elif pd.isna(parent_taxon_name_id):
                parent_taxon_name_id = None

            # full / concise：取得最新有效學名對應的 original_taxon_name_id（可能為 None）
            original_taxon_name_id = None
            if completeness in ('full', 'concise'):
                acc_nm = name_df[name_df.taxon_name_id == row.taxon_name_id]
                if not acc_nm.empty and acc_nm.original_taxon_name_id.values[0] is not None:
                    original_taxon_name_id = int(acc_nm.original_taxon_name_id.values[0])

            now_prop = prop_grouped[nt][['common_names','additional_fields',
                'custom_fields', 'is_in_taiwan', 'alien_type',
                'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater',
                'is_brackish', 'is_marine']].to_dict('records')[0]

            if rank_order_map[row.rank_id] <= rank_order_map[30]: # 屬以上 顯示未知
                now_prop['is_in_taiwan'] = 2
            taxon_names = rows[['taxon_name_id','taxon_status','rank_id', 'nomenclature_id']].drop_duplicates().to_dict('records')
            for rrr in taxon_names:
                # none 模式：只保留 accepted
                if completeness == 'none' and rrr.get('taxon_status') != 'accepted':
                    continue
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
                    now_per_usages = get_per_usages(
                        rrr.get('taxon_name_id'), rows, prop_df_, name_df, ref_df, conn,
                        backbone_ref_ids, references, rrr.get('taxon_status'),
                        ref_publish_year_lookup=ref_publish_year_lookup,
                        prop_records_by_name=prop_records_by_name,
                        name_by_id=name_by_id)
                    now_dict['type_specimens'] = safe_json_dumps(deduplicate_type_specimens(
                        taxon_name_id=rrr.get('taxon_name_id'),
                        prop_df_=prop_df_,
                        name_df=name_df,
                        prop_records_by_name=prop_records_by_name,
                        name_by_id=name_by_id))
                else:
                    now_new_prop = {}
                    now_indications = []
                    # indications
                    if rrr.get('taxon_status') == 'misapplied':
                        if rrr.get('nomenclature_id') == 1: #動物
                            now_indications = ['not of']
                        elif rrr.get('nomenclature_id') == 2: #植物
                            now_indications = ['auct. non']
                        now_dict['type_specimens'] = '[]'
                    elif rrr.get('taxon_status') == 'not-accepted':
                        merged_indications = []
                        for rec in prop_records_by_name.get(rrr.get('taxon_name_id'), []):
                            if rec.get('ru_status') == 'not-accepted':
                                ii = rec.get('indications')
                                if ii:
                                    merged_indications += ii.split(',')
                        merged_indications = list(set(merged_indications))
                        now_indications = [m for m in merged_indications if m != 'syn. nov.']
                        now_dict['type_specimens'] = safe_json_dumps(deduplicate_type_specimens(
                            taxon_name_id=rrr.get('taxon_name_id'),
                            prop_df_=prop_df_,
                            name_df=name_df,
                            prop_records_by_name=prop_records_by_name,
                            name_by_id=name_by_id))
                    now_new_prop['indications'] = now_indications
                    now_dict['properties'] = safe_json_dumps(now_new_prop)
                    now_dict['parent_taxon_name_id'] = None
                    now_per_usages = get_per_usages(
                        rrr.get('taxon_name_id'), rows, prop_df_, name_df, ref_df, conn,
                        backbone_ref_ids, references, rrr.get('taxon_status'),
                        ref_publish_year_lookup=ref_publish_year_lookup,
                        prop_records_by_name=prop_records_by_name,
                        name_by_id=name_by_id)

                # concise 模式：accepted 一律保留；其餘依規則2（保護名）或規則1（per_usages 含 references 內的 ref）
                if completeness == 'concise' and rrr.get('taxon_status') != 'accepted':
                    keep = (rrr.get('taxon_name_id') == original_taxon_name_id)  # 規則2
                    if not keep:
                        ref_ids = {u.get('reference_id') for u in now_per_usages}  # 規則1
                        keep = bool(ref_ids & references_set)
                    if not keep:
                        continue

                # usage_references：決定最終要存哪些 per_usages
                now_dict['per_usages'] = safe_json_dumps(filter_per_usages(now_per_usages))
                final_usages.append(now_dict)

            # full / concise：若最新有效學名的 original_taxon_name_id 不在本群 rows 中，補一筆原始名的 not-accepted
            if completeness in ('full', 'concise') and original_taxon_name_id is not None \
                    and original_taxon_name_id not in rows.taxon_name_id.values:
                orig_row = orig_lookup.get(original_taxon_name_id)
                if orig_row:
                    orig_ref_id, orig_name, orig_rank_id = orig_row
                    orig_per_usages = []
                    if orig_ref_id:
                        orig_per_usages.append({
                            "pro_parte": False,
                            "reference_id": orig_ref_id,
                            "is_from_published_ref": True,
                            "including_usage_id": None
                        })
                    final_usages.append({
                        'tmp_taxon_id': nt,
                        'taxon_name_id': original_taxon_name_id,
                        'status': 'not-accepted',
                        'rank_id': orig_rank_id,
                        'parent_taxon_name_id': None,
                        'properties': safe_json_dumps({'indications': []}),
                        'type_specimens': '[]',
                        'per_usages': safe_json_dumps(filter_per_usages(orig_per_usages)),
                    })
                    # 補進 name_df 和 name_by_id，讓後續排序與查表能保留這筆合成列
                    if original_taxon_name_id not in name_df.taxon_name_id.values:
                        new_name_row = {
                            'taxon_name_id': original_taxon_name_id,
                            'name_reference_id': orig_ref_id,
                            'name': orig_name,
                            'type_specimens': None,
                            'original_taxon_name_id': None,
                        }
                        name_df = pd.concat([name_df, pd.DataFrame([new_name_row])], ignore_index=True)
                        name_by_id[original_taxon_name_id] = new_name_row

        except Exception as e:
            print('merging', e)
            raise

    # 排序

    # 科->科底下的屬->屬底下的種&種下
    # 除了科以外 應該可以直接用字母排
    # 先根據字母排 再根據自己的上階層排 
    # 先排有效名

    final_usages = pd.DataFrame(final_usages)
    final_usages['rank_order'] = final_usages['rank_id'].map(rank_order_map)

    # === 用 DFS 走樹狀結構排序 ===

    # 1. 取出所有 accepted 節點（每個 tmp_taxon_id 一筆）
    accepted_nodes = final_usages[final_usages.status == 'accepted'][
        ['taxon_name_id', 'parent_taxon_name_id', 'rank_id', 'tmp_taxon_id']
    ].drop_duplicates()
    accepted_nodes = accepted_nodes.merge(
        name_df[['taxon_name_id', 'name']].drop_duplicates(), how='left'
    )

    # 2. 正規化 NaN parent → None
    accepted_records = accepted_nodes.to_dict('records')
    for r in accepted_records:
        if pd.isna(r['parent_taxon_name_id']):
            r['parent_taxon_name_id'] = None

    # 3. 建立 parent -> children 對應
    #    parent 不在這次 accepted 名單裡的（例如資料只取局部）視為根
    all_name_ids = {r['taxon_name_id'] for r in accepted_records}
    children_map = {}
    for r in accepted_records:
        parent = r['parent_taxon_name_id']
        if parent is not None and parent not in all_name_ids:
            parent = None
        children_map.setdefault(parent, []).append(r)

    # 4. 同層子節點按 name 字母排序
    for parent in children_map:
        children_map[parent].sort(key=lambda x: (x['name'] or ''))

    # 5. iterative DFS 從根節點走出 tmp_taxon_id 順序
    ordered_tmp_taxon_ids = []
    visited = set()
    stack = list(reversed(children_map.get(None, [])))
    while stack:
        node = stack.pop()
        tid = node['tmp_taxon_id']
        if tid in visited:
            continue
        visited.add(tid)
        ordered_tmp_taxon_ids.append(tid)
        for child in reversed(children_map.get(node['taxon_name_id'], [])):
            stack.append(child)

    # 6. 保險：有 parent 鏈接不上的孤兒節點，補在最後（按 name 排）
    unvisited = sorted(
        [r for r in accepted_records if r['tmp_taxon_id'] not in visited],
        key=lambda x: (x['name'] or '')
    )
    for r in unvisited:
        # if r['tmp_taxon_id'] not in visited:
        visited.add(r['tmp_taxon_id'])
        ordered_tmp_taxon_ids.append(r['tmp_taxon_id'])

    # 7. 依順序組合：每個 tmp_taxon_id 先 accepted，再 not-accepted（按 name 排）
    final_usages_named = final_usages.merge(
        name_df[['taxon_name_id', 'name']].drop_duplicates(), how='left'
    )
    groups_by_tid = {tid: g for tid, g in final_usages_named.groupby('tmp_taxon_id', sort=False)}

    ordered_chunks = []
    for tid in ordered_tmp_taxon_ids:
        group = groups_by_tid.get(tid)
        if group is None:
            continue
        acc = group[group.status == 'accepted']
        non_acc = group[group.status != 'accepted'].sort_values('name', na_position='last')
        ordered_chunks.append(acc)
        if len(non_acc):
            ordered_chunks.append(non_acc)

    final_usage_df = pd.concat(ordered_chunks, ignore_index=True)
    final_usage_df['order'] = final_usage_df.index

    group_keys = final_usage_df['tmp_taxon_id'].drop_duplicates().reset_index(drop=True)
    group_id_map = {k: i+1 for i, k in enumerate(group_keys)}
    final_usage_df['group'] = final_usage_df['tmp_taxon_id'].map(group_id_map)

    # 存入資料庫
    final_usage_df['tmp_checklist_id'] = tmp_checklist_id
    final_usage_df = final_usage_df[['parent_taxon_name_id','tmp_checklist_id','taxon_name_id','status','group','order','per_usages','type_specimens','properties']]

    return final_usage_df, tmp_checklist_id