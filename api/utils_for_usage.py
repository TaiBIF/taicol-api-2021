import pandas as pd
import pymysql
import os
import json
from numpy import nan
import requests
import numpy as np
import re
from conf.settings import env

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)



def to_firstname_abbr(string):
    s_list = re.split(r'[\s|\-]', string)
    for i in range(len(s_list)):
        if len(s_list[i]) == 1 or re.match(r"(\w[\.]).*", s_list[i]):  # 本身只有一個字母或本身是縮寫
            c = s_list[i]
        else:
            c = re.sub(r"(\w).*", r'\1.', s_list[i])
        if i == 0:
            full_abbr = c
        else:
            full_abbr += '-' + c
    return full_abbr


def to_middlename_abbr(content):
    if re.match(r"(\w[\.]).*", content):  # 本身是縮寫
        return re.sub(r"(\w[\.]).*", r"\1", content)
    elif len(content) == 1:  # 本身只有一個字
        return content
    else:
        return re.sub(r"(\w).*", r"\1.", content)




def get_related_names(taxon_name_id, df, new_names, name_list, ref_group_pair_now, object_group, 
                    #   autonym_group, 
                      ref_group_pair_now_obj, 
                    #   ref_group_pair_now_autonym, 
                      ref_group_pair_now_misapplied):
    if len(df):
        misapplied_list = ref_group_pair_now_misapplied.merge(df).taxon_name_id.unique()
    else:
        misapplied_list = []
    new_names.remove(taxon_name_id)  # remove current taxon_name_id
    name_list.append(taxon_name_id)
    # 有相同的accepted_taxon_name_id
    ref_group_pair = ref_group_pair_now[ref_group_pair_now.accepted_taxon_name_id==taxon_name_id][['accepted_taxon_name_id','reference_id']]
    # 地位有效的同模異名
    if object_group:
        new_row = ref_group_pair_now_obj[~(ref_group_pair_now_obj.accepted_taxon_name_id.isin(misapplied_list))&(ref_group_pair_now_obj.object_group==object_group)]
        ref_group_pair = pd.concat([ref_group_pair, new_row[['accepted_taxon_name_id','reference_id']]],ignore_index=True)
    ref_group_pair = ref_group_pair[['reference_id','accepted_taxon_name_id']].drop_duplicates()
    # ref_group_pair = ref_group_pair_now.merge(ref_group_pair[['reference_id','accepted_taxon_name_id']])
    df = pd.concat([df, ref_group_pair], ignore_index=True)
    df = df.drop_duplicates().reset_index(drop=True)
    # get_names = ref_group_pair[['taxon_name_id','ru_status']].drop_duplicates().to_dict('records')
    get_names = df.accepted_taxon_name_id.unique()
    # get_names_list = [n['taxon_name_id'] for n in get_names if n['taxon_name_id'] not in name_list and n['ru_status'] == 'accepted']
    get_names_list = [n for n in get_names if n not in name_list]
    new_names += get_names_list
    # print(new_names)
    name_list = list(dict.fromkeys(name_list)) # drop duplicates
    new_names = list(dict.fromkeys(new_names)) # drop duplicates
    return new_names, df, name_list


def get_related_names_sub(taxon_name_id, df, new_names, name_list, ref_group_pair_now, object_group, ref_group_pair_now_obj):
    new_names.remove(taxon_name_id)  # remove current taxon_name_id
    name_list.append(taxon_name_id)
    # 有相同的accepted_taxon_name_id
    ref_group_pair = ref_group_pair_now[ref_group_pair_now.accepted_taxon_name_id==taxon_name_id][['accepted_taxon_name_id','reference_id']]
    # 地位有效的同模異名 
    # NOTE 這邊應該不用限定是地位有效了
    if object_group:
        ref_group_pair = pd.concat([ref_group_pair, ref_group_pair_now_obj[ref_group_pair_now_obj.object_group==object_group][['accepted_taxon_name_id','reference_id']]],ignore_index=True)
    ref_group_pair = ref_group_pair[['reference_id','accepted_taxon_name_id']].drop_duplicates()
    df = pd.concat([df, ref_group_pair], ignore_index=True)
    df = df.drop_duplicates().reset_index(drop=True)
    get_names = df.accepted_taxon_name_id.unique()
    get_names_list = [n for n in get_names if n not in name_list]
    new_names += get_names_list
    name_list = list(dict.fromkeys(name_list)) # drop duplicates
    new_names = list(dict.fromkeys(new_names)) # drop duplicates
    return new_names, df, name_list


def check_latest(temp, conn):
    latest_ru_id_list = []
    # 如果有super backbone 忽略其他
    if len(temp[temp['type']==6]):
        latest_ru_id_list += temp[temp['type'] == 6].ru_id.to_list()
    else:
        # 如果有文獻的話就忽略backbone
        ignore_backbone = False
        ignore_checklist = False
        if len(temp[(temp['type']!=4)]):
        # if not all(temp['type']==4):
            temp = temp[(temp['type']!=4)]
            ignore_backbone = True
        # 如果有非名錄文獻的話 忽略名錄文獻
        # if not all(temp['type']==5):
        if len(temp[(temp['type']!=5)]):
            temp = temp[(temp['type']!=5)]
            ignore_checklist = True
        # 如果都是backbone就直接比, 如果有大於一個reference_id, 比較年份
        yr = temp[['reference_id', 'publish_year']].drop_duplicates()
        max_yr = yr.publish_year.max()
        if len(yr[yr['publish_year'] == max_yr]) > 1:
            currently_cannot_decide = False
            temp = temp[(temp.publish_year==max_yr)]
            dt = temp[['reference_id', 'publish_date']].drop_duplicates()
            if len(dt[dt.publish_date!='']):
                max_dt = dt[dt.publish_date!=''].publish_date.max()
                if len(dt[dt['publish_date'] == max_dt]) > 1:
                    currently_cannot_decide = True
                else:
                    latest_ru_id_list += temp[temp['publish_date'] == max_dt].ru_id.to_list()
            else:
                currently_cannot_decide = True
            if currently_cannot_decide:
                ref_ids = dt.reference_id.to_list()
                query = '''SELECT JSON_EXTRACT(r.properties, "$.book_title"), 
                            JSON_EXTRACT(r.properties, "$.volume"), JSON_EXTRACT(r.properties, "$.issue") FROM `references` r
                            WHERE r.id in %s'''
                with conn.cursor() as cursor:
                    execute_line = cursor.execute(query, (ref_ids,))
                    ref_more_info = cursor.fetchall()
                    ref_more_info = pd.DataFrame(ref_more_info, columns=['book_title', 'volume', 'issue'])
                    if len(ref_more_info.drop_duplicates()) == 1:
                    # 判斷是同一期期刊的不同篇文章  擇一當作最新文獻
                        latest_ru_id_list += temp[temp['reference_id'] == ref_ids[0]].ru_id.to_list()
        else:
            if ignore_backbone and ignore_checklist:
                latest_ru_id_list += temp[(temp['publish_year'] == max_yr) & (temp['type'] != 5)  & (temp['type'] != 4)].ru_id.to_list()
            elif ignore_checklist and not ignore_backbone:
                latest_ru_id_list += temp[(temp['publish_year'] == max_yr) & (temp['type'] != 5)].ru_id.to_list()
            # 這裡也要排除backbone
            elif ignore_backbone and not ignore_checklist:
                latest_ru_id_list += temp[(temp['publish_year'] == max_yr) & (temp['type'] != 4)].ru_id.to_list()
            else:
                latest_ru_id_list += temp[(temp['publish_year'] == max_yr)].ru_id.to_list()
    # 如果最新的是同一篇文獻 且互為同模異名 但不是上下階層關係 要判斷usage中的group 來決定誰是最新
    is_obj_syns = False
    if len(latest_ru_id_list) > 1:
        temp_rows = temp[(temp.ru_id.isin(latest_ru_id_list))&(temp.object_group.notnull())]
        if len(temp_rows.object_group.unique()) == 1:
            is_obj_syns = True
        if is_obj_syns:
            current_parent = temp_rows.parent_taxon_name_id.unique()
            if not len(temp_rows[temp_rows.accepted_taxon_name_id.isin(current_parent)]):
                # 抓出group
                # group_min = temp_rus[temp_rus.ru_id.isin(latest_ru_id_list)].group.min()
                # latest_ru_id_list = temp_rus[(temp_rus.ru_id.isin(latest_ru_id_list))&(temp_rus.group==group_min)].ru_id.to_list()
                group_min = temp[temp.ru_id.isin(latest_ru_id_list)].group.min()
                latest_ru_id_list = temp[(temp.ru_id.isin(latest_ru_id_list))&(temp.group==group_min)].ru_id.to_list()
    return latest_ru_id_list

# 超級backbone：文獻類型 type=6
# 優先度：6>1,2,3>5>4

# 確定學名的最新學名使用是哪一個usage
def check_status_latest(temp, conn): # ru_id 只放taxon_name_id=誤用名的 不需放整個分類群
    # 如果有super backbone 忽略其他
    if len(temp[temp['type']==6]):
        newest_ru_id_list = temp[temp['type'] == 6].ru_id.to_list()
    else:
        # 如果有文獻的話就忽略backbone
        ignore_backbone = False
        ignore_checklist = False
        if not all(temp['type']==4):
            temp = temp[temp['type']!=4]
            ignore_backbone = True
        # 如果有非名錄文獻的話 忽略名錄文獻
        if not all(temp['type']==5):
            temp = temp[temp['type']!=5]
            ignore_checklist = True
        # 如果都是backbone就直接比, 如果有大於一個reference_id, 比較年份
        yr = temp[['reference_id', 'publish_year']].drop_duplicates()
        max_yr = yr.publish_year.max()
        if len(yr[yr['publish_year'] == max_yr]) > 1:
            currently_cannot_decide = False
            temp = temp[(temp.publish_year==max_yr)]
            dt = temp[['reference_id', 'publish_date']].drop_duplicates()
            if len(dt[dt.publish_date!='']):
                max_dt = dt[dt.publish_date!=''].publish_date.max()
                if len(dt[dt['publish_date'] == max_dt]) > 1:
                    currently_cannot_decide = True
                else:
                    newest_ru_id_list = temp[temp['publish_date'] == max_dt].ru_id.to_list()
            else:
                currently_cannot_decide = True
            if currently_cannot_decide:
                ref_ids = dt.reference_id.to_list()
                query = '''SELECT JSON_EXTRACT(r.properties, "$.book_title"), 
                            JSON_EXTRACT(r.properties, "$.volume"), JSON_EXTRACT(r.properties, "$.issue") FROM `references` r
                            WHERE r.id in %s'''
                with conn.cursor() as cursor:
                    execute_line = cursor.execute(query, (ref_ids,))
                    ref_more_info = cursor.fetchall()
                    ref_more_info = pd.DataFrame(ref_more_info, columns=['book_title', 'volume', 'issue'])
                    if len(ref_more_info.drop_duplicates()) == 1:
                    # 判斷是同一期期刊的不同篇文章  擇一當作最新文獻
                        newest_ru_id_list = temp[temp['reference_id'] == ref_ids[0]].ru_id.to_list()
                        # total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['reference_id'] == ref_ids[0]), 'is_latest'] = True
                    # rus = rus.rename(columns={ 0: 'parent_taxon_name_id', 1: 'properties', 2: 'is_in_taiwan', 3: 'ru_id'})
                    else:
                        newest_ru_id_list = []
        else:
            if ignore_backbone and ignore_checklist:
                newest_ru_id_list = temp[(temp['publish_year'] == max_yr) & (temp['type'] != 5)  & (temp['type'] != 4)].ru_id.to_list()
            elif ignore_checklist:
                newest_ru_id_list = temp[(temp['publish_year'] == max_yr) & (temp['type'] != 5)].ru_id.to_list()
            # 這裡也要排除backbone
            elif ignore_backbone:
                newest_ru_id_list = temp[(temp['publish_year'] == max_yr) & (temp['type'] != 4)].ru_id.to_list()
            else:
                newest_ru_id_list = temp.loc[(temp['publish_year'] == max_yr)].ru_id.to_list()
    # if len(newest_ru_id_list) == 1:
    #     newest_ru_id = newest_ru_id_list[0]
    # else:
    #     newest_ru_id = None
    return newest_ru_id_list




# 確定屬性的最新 應該規則可以放寬一點
def check_prop_status_latest(temp, conn): # ru_id 只放taxon_name_id=誤用名的 不需放整個分類群
    # 如果有super backbone 忽略其他
    if len(temp[temp['type']==6]):
        newest_ru_id_list = temp[temp['type'] == 6].ru_id.to_list()
    else:
        # 如果有文獻的話就忽略backbone
        ignore_backbone = False
        ignore_checklist = False
        if not all(temp['type']==4):
            temp = temp[temp['type']!=4]
            ignore_backbone = True
        # 如果有非名錄文獻的話 忽略名錄文獻
        if not all(temp['type']==5):
            temp = temp[temp['type']!=5]
            ignore_checklist = True
        # 如果都是backbone就直接比, 如果有大於一個reference_id, 比較年份
        yr = temp[['reference_id', 'publish_year']].drop_duplicates()
        max_yr = yr.publish_year.max()
        if len(yr[yr['publish_year'] == max_yr]) > 1:
            currently_cannot_decide = False
            temp = temp[(temp.publish_year==max_yr)]
            dt = temp[['reference_id', 'publish_date']].drop_duplicates()
            if len(dt[dt.publish_date!='']):
                max_dt = dt[dt.publish_date!=''].publish_date.max()
                if len(dt[dt['publish_date'] == max_dt]) > 1:
                    currently_cannot_decide = True
                else:
                    newest_ru_id_list = temp[temp['publish_date'] == max_dt].ru_id.to_list()
            else:
                currently_cannot_decide = True
            if currently_cannot_decide:
                ref_ids = dt.reference_id.to_list()
                query = '''SELECT JSON_EXTRACT(r.properties, "$.book_title"), 
                            JSON_EXTRACT(r.properties, "$.volume"), JSON_EXTRACT(r.properties, "$.issue") FROM `references` r
                            WHERE r.id in %s'''
                with conn.cursor() as cursor:
                    execute_line = cursor.execute(query, (ref_ids,))
                    ref_more_info = cursor.fetchall()
                    ref_more_info = pd.DataFrame(ref_more_info, columns=['book_title', 'volume', 'issue'])
                    if len(ref_more_info.drop_duplicates()) == 1:
                    # 判斷是同一期期刊的不同篇文章  擇一當作最新文獻
                        newest_ru_id_list = temp[temp['reference_id'] == ref_ids[0]].ru_id.to_list()
                        # total_df.loc[(total_df['tmp_taxon_id'] == t) & (total_df['reference_id'] == ref_ids[0]), 'is_latest'] = True
                    # rus = rus.rename(columns={ 0: 'parent_taxon_name_id', 1: 'properties', 2: 'is_in_taiwan', 3: 'ru_id'})
                    else:
                        newest_ru_id_list = temp[(temp.publish_year==max_yr)].ru_id.to_list()
        else:
            if ignore_backbone and ignore_checklist:
                newest_ru_id_list = temp[(temp['publish_year'] == max_yr) & (temp['type'] != 5)  & (temp['type'] != 4)].ru_id.to_list()
            elif ignore_checklist:
                newest_ru_id_list = temp[(temp['publish_year'] == max_yr) & (temp['type'] != 5)].ru_id.to_list()
            # 這裡也要排除backbone
            elif ignore_backbone:
                newest_ru_id_list = temp[(temp['publish_year'] == max_yr) & (temp['type'] != 4)].ru_id.to_list()
            else:
                newest_ru_id_list = temp.loc[(temp['publish_year'] == max_yr)].ru_id.to_list()
    return newest_ru_id_list




def reset_latest(total_df,reset_is_latest_list, conn):
    cannot_decide = []
    for t in reset_is_latest_list:
        temp = total_df[(total_df.tmp_taxon_id==t)&(total_df.ru_status=='accepted')]
        if len(temp):
            latest_ru_id_list = check_latest(temp=temp, conn=conn)
            if not len(latest_ru_id_list):
                cannot_decide.append(t)
            else:
                total_df.loc[total_df.tmp_taxon_id== t, 'is_latest'] = False
                total_df.loc[total_df.ru_id.isin(latest_ru_id_list), 'is_latest'] = True
        else:
            cannot_decide.append(t)
    return total_df, cannot_decide

# 全部都不需要特別排除taxon_status是誤用的學名
# custom_dict = {'accepted': 0, 'not-accepted': 1, 'misapplied': 2}

def determine_prop(conn, rows, accepted_taxon_name_id, tmp_ru_df, refs):    
    tmp_rows = tmp_ru_df.merge(refs)
    tmp_ru_df = tmp_ru_df.to_dict('records')
    # is 系列
    is_dict = {
        'is_in_taiwan' : None,
        'is_endemic': None, 
        'is_fossil': None, 
        'is_terrestrial': None, 
        'is_freshwater': None, 
        'is_brackish': None, 
        'is_marine': None,
    }
    n_list = []
    for n in tmp_ru_df:
        if prop := json.loads(n.get('properties')):
            for current_is in is_dict.keys():
                if prop.get(current_is) in [0,1]:
                    n_list.append({'is': current_is,'value':prop.get(current_is),'taxon_name_id': n.get('taxon_name_id'), 'ru_id': n.get('ru_id')})
    n_list = pd.DataFrame(n_list)
    if len(n_list):
        for current_is in is_dict.keys():
            # print(current_is)
            temp = []
            # 優先選擇和接受名相同的學名使用
            if len(n_list[(n_list['is']==current_is)&(n_list.taxon_name_id==accepted_taxon_name_id)]):
                temp = rows[rows.ru_id.isin(n_list[(n_list['is']==current_is)&(n_list.taxon_name_id==accepted_taxon_name_id)].ru_id.to_list())]
            elif len(n_list[n_list['is']==current_is]):
                temp = rows[rows.ru_id.isin(n_list[n_list['is']==current_is].ru_id.to_list())]
            if len(temp):
                newest_ru_id_list = check_prop_status_latest(temp, conn)
                newest_ru_id = newest_ru_id_list[0] # 若無法決定 也直接選其中一個 
                is_dict[current_is] = n_list[(n_list['is']==current_is)&(n_list.ru_id==newest_ru_id)]['value'].values[0]
    # 模式學名
    type_list = []
    for n in tmp_ru_df:
        if prop := json.loads(n.get('properties')):
            # for current_is in is_dict.keys():
            if prop.get('type_name'):
                type_list.append({'value': prop.get('type_name'),'taxon_name_id': n.get('taxon_name_id'), 'ru_id': n.get('ru_id')})
    type_list = pd.DataFrame(type_list)
    if len(type_list):
        temp = []
        # 優先選擇和接受名相同的學名使用
        if len(type_list[type_list.taxon_name_id==accepted_taxon_name_id]):
            temp = rows[rows.ru_id.isin(type_list[type_list.taxon_name_id==accepted_taxon_name_id].ru_id.to_list())]
        else:
            temp = rows[rows.ru_id.isin(type_list.ru_id.to_list())]
        if len(temp):
            newest_ru_id_list = check_prop_status_latest(temp, conn)
            newest_ru_id = newest_ru_id_list[0] # 若無法決定 也直接選其中一個 
            is_dict['type_name'] = type_list[type_list.ru_id==newest_ru_id]['value'].values[0]
    # 外來屬性
    alien_list = []
    is_cultured = 0
    has_accepted_name = False
    for n in tmp_ru_df:
        if prop:= json.loads(n.get('properties')):
            if prop.get('alien_type'):
                if n.get('taxon_name_id') == accepted_taxon_name_id:
                    has_accepted_name = True
                if prop.get('alien_type') == 'cultured':
                    is_cultured = 1
                alien_list.append({'alien_type': prop.get('alien_type'),'taxon_name_id': n.get('taxon_name_id'), 'reference_id': n.get('reference_id'), 'ru_id': n.get('ru_id'), 'status_note': prop.get('alien_status_note')})
    is_dict['is_cultured'] = is_cultured
    alien_list = pd.DataFrame(alien_list)
    main_alien_type = None
    if len(alien_list):
        # alien_list['is_latest'] = False
        if has_accepted_name:
            temp = tmp_rows[tmp_rows.ru_id.isin(alien_list[alien_list.taxon_name_id==accepted_taxon_name_id].ru_id.to_list())]
        else:
            temp = tmp_rows[tmp_rows.ru_id.isin(alien_list.ru_id.to_list())]
        newest_ru_id_list = check_prop_status_latest(temp, conn)
        newest_ru_id = newest_ru_id_list[0] # 若無法決定 也直接選其中一個 
        main_alien_type = alien_list[alien_list.ru_id==newest_ru_id].alien_type.values[0]
        is_dict['alien_type'] = main_alien_type
    if is_dict.get('is_in_taiwan') == None:
        is_dict['is_in_taiwan'] = 0
    return is_dict