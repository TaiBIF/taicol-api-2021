# 202508 

# reference_usage_id 欄位先留著，但這個id有可能是會變動的，不能使用
# 改用taxon_name_id, accepted_taxon_name_id, reference_id組合為唯一值
# 實務上仍有可能會重複，若有重複情況則取集中一筆

import pandas as pd
from numpy import nan
from api.utils import get_whitelist, sub_lin_ranks, rank_order_map
from api.services.utils.common import get_conn
from api.services.utils.checklist import *
# import time




def process_taxon_checklist(pairs, exclude_cultured, only_in_taiwan, references):

    # conn = get_conn()

    # s = time.time()
    # 取得當前的白名單

    whitelist_list_1, whitelist_list_2, whitelist_list_3 = get_whitelist()

    # 應該先判斷是不是有 accepted_taxon_name_id & taxon_name_id & reference_id 對應的usage需要被刪除
    check_deleted_usages()

    ref_df, usage_df, common_name_rus = get_dfs(pairs, exclude_cultured, only_in_taiwan) 
    usage_df = assign_group_and_tmp_taxon_ids(usage_df) # 初步分群

    total_df = usage_df
    total_df = total_df.drop_duplicates()
    total_df = total_df.replace({nan:None})
    total_df = total_df.reset_index(drop=True)

    # print('q', time.time()-s)

    # s = time.time()

    # 決定誰是接受學名
    total_df, conflict_parent_list = select_latest_ru(total_df, ref_df, return_conflict=True)

    # print('w', time.time()-s)

    # s = time.time()


    # 分類觀檢查

    # 2 同模異名檢查
    # 若分在多群，需檢查同模式學名的文獻優先性：
    # 沒有is_latest=TRUE/全部都是is_latest=FALSE的同模式：

    check_obj_list = get_check_obj_list(total_df,whitelist_list_1)

    cannot_decide = []

    loop_count = 0
    while len(check_obj_list):
        # print(loop_count)
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
        # print(loop_count)
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
        # print(loop_count)
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
        # print(loop_count)
        for s in parent_not_accepted: # 179
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


    # print('e', time.time()-s)
    # s = time.time()


    total_df['taxon_status'] = ''
    total_df = determine_taxon_status(total_df) 

    # print('1', time.time()-s)
    # s = time.time()

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


    # 下面不應該從tmp_taxon_id處理 應該處理全部
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

    # print('2', time.time()-s)
    # s = time.time()

    # # 處理屬性

    # 以下只處理ru_status = accepted
    prop_df = get_prop_df(total_df.ru_id.to_list()+common_name_rus)
    prop_df = prop_df.merge(ref_df[['publish_date','type','publish_year','subtitle']], right_index=True, left_on="reference_id")

    # print('a', time.time()-s)
    # s = time.time()

    prop_df = select_priority_prop(prop_df)

    # print('b', time.time()-s)
    # s = time.time()

    # merge全部的usage
    prop_df_ = prop_df.merge(df_for_prop)
    prop_df_['ru_order'] = prop_df_.groupby('tmp_taxon_id').cumcount() + 1
    all_prop_df = determine_taxon_prop(prop_df_)
    # print('c', time.time()-s)

    # 處理per_usages
    # s = time.time()
    conn = get_conn()
    with conn.cursor() as cursor:
        # backbone_ref_ids
        query = 'SELECT id from `references` WHERE `type` IN (4, 6) AND deleted_at IS NULL;'
        execute_line = cursor.execute(query)
        backbone_ref_ids = cursor.fetchall()
        backbone_ref_ids = [b[0] for b in backbone_ref_ids]
        # name_df
        name_query = 'SELECT id, reference_id, `name` FROM taxon_names WHERE id IN %s'
        cursor.execute(name_query, (total_df.taxon_name_id.unique().tolist(),))
        name_df = pd.DataFrame(cursor.fetchall(), columns=['taxon_name_id', 'name_reference_id', 'name'])
        name_df = name_df.replace({np.nan: None})
        # tmp_checklist_id
        query = '''SELECT max(tmp_checklist_id) from tmp_namespace_usages;'''
        execute_line = cursor.execute(query)
        tmp_checklist_id = cursor.fetchone()[0]
        tmp_checklist_id = tmp_checklist_id + 1 if tmp_checklist_id else 1

    # print('3', time.time()-s)

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

    # s = time.time()

    final_usages = []

    for nt in total_df.tmp_taxon_id.unique():
        rows = total_df[total_df['tmp_taxon_id']==nt]
        try:
            i = rows[(rows['is_latest']==True) & (rows['taxon_status'] == 'accepted')].index[0] # 接受的row
            row = total_df.iloc[i] # 接受的row
            # accepted_taxon_name_id = row.accepted_taxon_name_id
            parent_taxon_name_id = row.parent_taxon_name_id
            now_prop = all_prop_df[all_prop_df.tmp_taxon_id==nt][['common_names','additional_fields',
                'custom_fields', 'is_in_taiwan', 'alien_type',
                'is_endemic', 'is_fossil', 'is_terrestrial', 'is_freshwater',
                'is_brackish', 'is_marine']].to_dict('records')[0]
            if rank_order_map[row.rank_id] <= rank_order_map[30]: # 屬以上 顯示未知
                now_prop['is_in_taiwan'] = 2
            # 所有屬性 資訊跟著有效名
            # 其他學名就存成無效 or 誤用
            # 要先在介面回傳預覽表，OK後才存進去my_namespace_usage
            # 還是先存入一個暫存的表 確定後再存入my_namespace_usage 匯入 or 選擇不匯入之後 再將這個暫時表的內容刪除
            # 在這邊整理要存入properties的欄位
            # 不管是什麼地位 學名都只保留一個
            taxon_names = rows[['taxon_name_id','taxon_status','rank_id', 'nomenclature_id']].drop_duplicates().to_dict('records')
            for rrr in taxon_names:
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
                    now_dict['per_usages'] = safe_json_dumps(get_per_usages(rrr.get('taxon_name_id'), rows, prop_df_, name_df, ref_df, conn, backbone_ref_ids, references))
                    now_dict['type_specimens'] = safe_json_dumps(get_type_specimens(rrr.get('taxon_name_id'), prop_df_))
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
                        # indications = prop_df_[(prop_df_.ru_status == 'not-accepted') & (prop_df_.taxon_name_id== rrr.get('taxon_name_id'))].indications.values
                        for ii in prop_df_[(prop_df_.ru_status == 'not-accepted') & (prop_df_.taxon_name_id== rrr.get('taxon_name_id'))].indications.values:
                            if ii:
                                merged_indications += ii.split(',')
                        merged_indications = list(set(merged_indications))
                        now_indications = [m for m in merged_indications if m != 'syn. nov.']
                        now_dict['type_specimens'] = safe_json_dumps(get_type_specimens(rrr.get('taxon_name_id'), prop_df_))
                    now_new_prop['indications'] = now_indications
                    now_dict['properties'] = safe_json_dumps(now_new_prop)
                    now_dict['parent_taxon_name_id'] = None
                    now_dict['per_usages'] = safe_json_dumps(get_per_usages(rrr.get('taxon_name_id'), rows, prop_df_, name_df, ref_df, conn, backbone_ref_ids, references))
                    # now_dict['type_specimens'] = '[]'
                final_usages.append(now_dict)
        except Exception as e: 
            print('merging', e)
            pass

    # print('4', time.time()-s)

    # 排序

    # 科->科底下的屬->屬底下的種&種下
    # 除了科以外 應該可以直接用字母排
    # 先根據字母排 再根據自己的上階層排 
    # 先排有效名
    

    # # s = time.time()
    final_usages = pd.DataFrame(final_usages)


    # 先取出屬 & 屬以下的
    final_usages['rank_order'] = final_usages['rank_id'].map(rank_order_map)
    accepted_usages = final_usages[(final_usages.status=='accepted')&(final_usages.rank_order>=rank_order_map[30])][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']].drop_duplicates()
    accepted_usages = accepted_usages.merge(name_df[['taxon_name_id','name']].drop_duplicates())
    # 直接按照字母排序
    accepted_usages = accepted_usages.sort_values('name')
    accepted_usages = accepted_usages.reset_index(drop=True)


    # 再把屬以上的加進去
    # 從下往上排
    family_usages = final_usages[(final_usages.status=='accepted')&(final_usages.rank_order<rank_order_map[30])][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']].drop_duplicates()
    family_usages = family_usages.merge(name_df[['taxon_name_id','name']].drop_duplicates()).sort_values(['rank_id','name'],ascending=False)

    for ff in family_usages.to_dict('records'):
        new_row = final_usages[(final_usages.tmp_taxon_id==ff.get('tmp_taxon_id'))&(final_usages.status=='accepted')][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']]
        if len(accepted_usages[accepted_usages.parent_taxon_name_id==ff.get('taxon_name_id')]):
            min_id = accepted_usages[accepted_usages.parent_taxon_name_id==ff.get('taxon_name_id')].index.min()
            accepted_usages = pd.concat([accepted_usages.iloc[:min_id], new_row, accepted_usages.iloc[min_id:]]).reset_index(drop=True)
        else:
            # 如果沒有的話就放在最前面 這邊也需要按照字母排
            accepted_usages = pd.concat([new_row, accepted_usages]).reset_index(drop=True)

    # 最後再把每個tmp_taxon_id的usage加進去
    other_usages = final_usages[final_usages.status!='accepted'][['taxon_name_id','parent_taxon_name_id','rank_id','tmp_taxon_id']]

    if len(other_usages):
        other_usages = other_usages.merge(name_df[['taxon_name_id','name']].drop_duplicates())
        other_usages = other_usages.sort_values('name')
        for oo in other_usages.tmp_taxon_id.unique():
            max_id = accepted_usages[accepted_usages.tmp_taxon_id==oo].index.max()+1
            new_rows = other_usages[other_usages.tmp_taxon_id==oo]
            accepted_usages = pd.concat([accepted_usages.iloc[:max_id], new_rows, accepted_usages.iloc[max_id:]]).reset_index(drop=True)

    final_usage_df = accepted_usages.merge(final_usages)
    final_usage_df = final_usage_df.reset_index(drop=True)

    final_usage_df['order'] = final_usage_df.index


    group_keys = final_usage_df['tmp_taxon_id'].drop_duplicates().reset_index(drop=True)
    group_id_map = {k: i+1 for i, k in enumerate(group_keys)}

    final_usage_df['group'] = final_usage_df['tmp_taxon_id'].map(group_id_map)

    # 存入資料庫
    final_usage_df['tmp_checklist_id'] = tmp_checklist_id
    final_usage_df = final_usage_df[['parent_taxon_name_id','tmp_checklist_id','taxon_name_id','status','group','order','per_usages','type_specimens','properties']]

    # print('5', time.time()-s)

    return final_usage_df, tmp_checklist_id