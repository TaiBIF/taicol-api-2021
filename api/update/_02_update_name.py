import pandas as pd
import requests
from datetime import datetime
import json
from numpy import nan
import numpy as np
from api.update.utils import DatabaseManager, AuthorFormatter, setup_logging
import logging
from collections import defaultdict

class TaxonomicNameUpdater(DatabaseManager):
    """學名更新器"""
    
    def __init__(self, batch_size=1000, max_retries=3):
        super().__init__(max_retries)
        self.batch_size = batch_size
        setup_logging()
        
    
    def get_last_update_times(self):
        """獲取最後更新時間"""
        queries = {
            'api_names': "SELECT MAX(updated_at) FROM `api_names`", 
            'taxon_names': "SELECT MAX(updated_at) FROM `taxon_names` WHERE is_publish = 1"
        }
        
        results = {}
        for key, query in queries.items():
            result = self.execute_with_retry(query, fetch=True)
            results[key] = result[0][0] if result and result[0][0] else datetime(1900, 1, 1)
        
        return results
    
    def get_taxon_name_ids_from_person_ids(self, person_ids):
        """從 person_ids 獲取對應的 taxon_name_ids"""
        if not person_ids:
            return []
        
        if isinstance(person_ids, (list, tuple)) and len(person_ids) > 0:
            placeholders = ','.join(['%s'] * len(person_ids))
            query = f"""
                SELECT DISTINCT taxon_name_id 
                FROM person_taxon_name 
                WHERE person_id IN ({placeholders})
            """
            results = self.execute_with_retry(query, tuple(person_ids), fetch=True)
            return [r[0] for r in results] if results else []
        return []
    
    def get_taxon_name_ids_from_min_id(self, min_taxon_name_id=None):
        """從最小 ID 獲取對應的 taxon_name_ids"""
        if min_taxon_name_id is not None:
            # 直接從 taxon_names 表獲取 id >= min_taxon_name_id 的記錄
            query = """
                SELECT DISTINCT id 
                FROM taxon_names 
                WHERE id >= %s 
                AND rank_id <> 47 AND deleted_at IS NULL AND is_publish = 1
            """
            results = self.execute_with_retry(query, (min_taxon_name_id,), fetch=True)
            return [r[0] for r in results] if results else []
        
        return []
    
    def fetch_updated_taxon_names(self, last_updated=None, limit=None, taxon_name_ids=None, person_ids=None, min_taxon_name_id=None, update_all=False):
        """獲取需要更新的學名（直接提取 properties 中的特定欄位）"""
        base_query = """
            SELECT tn.id, tn.rank_id, tn.nomenclature_id, tn.name,
                   tn.original_taxon_name_id, tn.formatted_authors, tn.publish_year,
                   tn.properties ->> '$.authors_name' as authors_name,
                   tn.properties ->> '$.latin_name' as latin_name,
                   tn.properties ->> '$.latin_genus' as latin_genus,
                   tn.properties ->> '$.latin_s1' as latin_s1,
                   tn.properties ->> '$.is_hybrid' as is_hybrid,
                   tn.properties ->> '$.species_layers' as species_layers,
                   tn.properties ->> '$.species_id' as species_id,
                   tn.properties ->> '$.initial_year' as initial_year,
                   tn.properties ->> '$.is_approved_list' as is_approved_list,
                    tn.properties ->> '$.genus_taxon_name_id' as genus_taxon_name_id
            FROM taxon_names tn
            WHERE tn.rank_id <> 47 AND tn.deleted_at IS NULL AND tn.is_publish = 1
        """
        
        params = []
        
        # 根據不同的查詢條件調整 WHERE 子句
        if person_ids:
            # 先從 person_ids 獲取 taxon_name_ids
            # self.logger.info(f"從 {len(person_ids)} 個 person_ids 獲取對應的 taxon_name_ids")
            taxon_name_ids_from_persons = self.get_taxon_name_ids_from_person_ids(person_ids)
            
            if not taxon_name_ids_from_persons:
                # self.logger.info("沒有找到對應的 taxon_name_ids")
                return pd.DataFrame()
            
            # 使用獲取到的 taxon_name_ids，並且查詢 id 或 original_taxon_name_id 匹配的記錄
            placeholders = ','.join(['%s'] * len(taxon_name_ids_from_persons))
            base_query += f" AND (tn.id IN ({placeholders}) OR tn.original_taxon_name_id IN ({placeholders}))"
            params.extend(taxon_name_ids_from_persons * 2)  # 因為用了兩次
            
        elif taxon_name_ids:
            # 直接指定要更新的 taxon_name_ids，並且查詢 id 或 original_taxon_name_id 匹配的記錄
            if isinstance(taxon_name_ids, (list, tuple)) and len(taxon_name_ids) > 0:
                placeholders = ','.join(['%s'] * len(taxon_name_ids))
                base_query += f" AND (tn.id IN ({placeholders}) OR tn.original_taxon_name_id IN ({placeholders}))"
                params.extend(taxon_name_ids * 2)  # 因為用了兩次
            else:
                # 如果 taxon_name_ids 是空的，返回空 DataFrame
                return pd.DataFrame()
        elif min_taxon_name_id is not None:
            # 從最小 ID 獲取 taxon_name_ids
            # self.logger.info(f"從 taxon_name_id >= {min_taxon_name_id} 獲取對應的學名")
            
            taxon_name_ids_from_min = self.get_taxon_name_ids_from_min_id(min_taxon_name_id)
            
            if not taxon_name_ids_from_min:
                # self.logger.info("沒有找到對應的 taxon_name_ids")
                return pd.DataFrame()
            
            # 使用獲取到的 taxon_name_ids，並且查詢 id 或 original_taxon_name_id 匹配的記錄
            placeholders = ','.join(['%s'] * len(taxon_name_ids_from_min))
            base_query += f" AND (tn.id IN ({placeholders}) OR tn.original_taxon_name_id IN ({placeholders}))"
            params.extend(taxon_name_ids_from_min * 2)  # 因為用了兩次
        elif update_all:
            # 更新全部
            pass
        elif last_updated:
            # 根據更新時間查詢
            base_query += " AND tn.updated_at > %s"
            params.append(last_updated)
        else:
            # 如果既沒有指定 IDs 也沒有指定時間，返回空 DataFrame
            return pd.DataFrame()
        
        if limit:
            base_query += f" LIMIT {limit}"
        
        results = self.execute_with_retry(base_query, tuple(params), fetch=True)
        
        if not results:
            return pd.DataFrame()
        
        # # 在這裡記錄實際查詢到的結果數量
        # if person_ids:
        #     self.logger.info(f"從 person_ids 查詢到 {len(results)} 筆學名記錄")
        # elif taxon_name_ids:
        #     self.logger.info(f"從 taxon_name_ids 查詢到 {len(results)} 筆學名記錄")
        # elif min_taxon_name_id is not None:
        #     self.logger.info(f"從最小 ID 條件查詢到 {len(results)} 筆學名記錄")
        
        columns = ['taxon_name_id', 'rank_id', 'nomenclature_id', 'name',
                  'original_taxon_name_id', 'formatted_authors', 'publish_year', 'authors_name',
                  'latin_name', 'latin_genus', 'latin_s1', 'is_hybrid', 'species_layers', 
                  'species_id', 'initial_year', 'is_approved_list', 'genus_taxon_name_id']
        
        df = pd.DataFrame(results, columns=columns).replace({np.nan: None})
        
        # 處理 JSON 字串欄位
        if not df.empty:
            # 解析 species_layers JSON 字串
            df['species_layers'] = df['species_layers'].apply(
                lambda x: AuthorFormatter.safe_json_loads(x) if x else []
            )
            
            # 轉換布林值
            df['is_hybrid'] = df['is_hybrid'].apply(
                lambda x: x == 'true' if x is not None else False
            )
            
            df['is_approved_list'] = df['is_approved_list'].apply(
                lambda x: x == 'true' if x is not None else False
            )
            
            # 轉換數字欄位
            df['genus_taxon_name_id'] = pd.to_numeric(df['genus_taxon_name_id'], errors='coerce')
            df['species_id'] = pd.to_numeric(df['species_id'], errors='coerce')
            df['initial_year'] = pd.to_numeric(df['initial_year'], errors='coerce')
        
        return df
    
    def format_taxon_name(self, row):
        """格式化學名（使用已提取的欄位）"""
        rank_id = row['rank_id']
        nomenclature_id = row['nomenclature_id']
        name = row['name']
        latin_name = row['latin_name']
        latin_genus = row['latin_genus']
        latin_s1 = row['latin_s1']
        is_hybrid = row['is_hybrid']
        species_layers = row['species_layers'] or []
        species_id = row['species_id']
        genus_taxon_name_id = row['genus_taxon_name_id']

        # 屬以上
        if rank_id < 30 or (rank_id > 47 and rank_id <= 50):
            if nomenclature_id in [3, 4]:  # 細菌、古菌或病毒
                return f"<i>{latin_name or ''}</i>"
            else:
                return latin_name or ''
        
        # 屬 
        elif rank_id == 30:            
            if nomenclature_id == 2 and is_hybrid:  # 植物雜交
                return f"× <i>{latin_name or ''}</i>"
            else:
                return f"<i>{latin_name or ''}</i>"
            
        # 亞屬 / 組 / 亞組
        elif rank_id in [31, 32, 33]:
            if nomenclature_id == 2 and genus_taxon_name_id:
                names = name.split(' ')
                final_names = []
                for nn in names:
                    if nn in ['subgen.', 'sect.', 'subsect.']:
                        final_names.append(nn)
                    else:
                        final_names.append(f"<i>{nn}</i>")
                return f"{(' '.join(final_names))}"
            else:
                return f"<i>{latin_name or ''}</i>"
            
        # 種
        elif rank_id == 34:
            if nomenclature_id == 2 and is_hybrid:  # 植物雜交
                return f"<i>{latin_genus or ''}</i> × <i>{latin_s1 or ''}</i>"
            elif nomenclature_id == 2 and '×' in name and not is_hybrid:  # 植物雜交屬下的種
                return f"× <i>{latin_genus or ''}</i> <i>{latin_s1 or ''}</i>"
            elif nomenclature_id == 4:  # 病毒
                return f"<i>{latin_s1 or ''}</i>"
            else:
                return f"<i>{latin_genus or ''} {latin_s1 or ''}</i>"
        
        # 種下
        elif 34 < rank_id < 47:
            return self._format_subspecies_name(nomenclature_id, latin_genus, latin_s1, species_layers, species_id, name)
        
        else:
            return name
    
    def _format_subspecies_name(self, nomenclature_id, latin_genus, latin_s1, species_layers, species_id, name):
        """格式化種下學名（使用已提取的欄位）"""
        # 動物命名規約
        if nomenclature_id == 1:
            formatted_name = ""
            
            for count, layer in enumerate(species_layers):
                if count == 0:  # 種下rank不顯示

                    # 種下階層若為subsp.不用顯示，subsp.以外的種下階層需要顯示。(2025/8加入)
                    if not layer.get('rank_abbreviation') or layer.get('rank_abbreviation') == 'subsp.':
                        s2_rank = ''
                    else:
                        s2_rank = layer.get('rank_abbreviation') + ' '

                    if latin_genus and latin_s1:
                        formatted_name = f"<i>{latin_genus} {latin_s1} {s2_rank}{layer.get('latin_name', '')}</i>"

                    elif species_id:
                        # 需要查詢 species 資訊
                        species_prop = self._get_species_properties(species_id)
                        formatted_name = f"<i>{species_prop.get('latin_genus', '')} {species_prop.get('latin_s1', '')} {s2_rank}{layer.get('latin_name', '')}</i>"

                    else:
                        formatted_name = name
                else:  # 種下下rank需顯示
                    formatted_name += f" {layer.get('rank_abbreviation', '')} <i>{layer.get('latin_name', '')}</i>"
            
            return formatted_name
        
        # 植物或菌類命名規約
        elif nomenclature_id in [2, 3]:
            if latin_genus and latin_s1:
                formatted_name = f"<i>{latin_genus} {latin_s1}</i>"
            elif species_id:
                species_prop = self._get_species_properties(species_id)
                formatted_name = f"<i>{species_prop.get('latin_genus', '')} {species_prop.get('latin_s1', '')}</i>"
            else:
                formatted_name = name
            
            for layer in species_layers:
                formatted_name += f" {layer.get('rank_abbreviation', '')} <i>{layer.get('latin_name', '')}</i>"
            
            return formatted_name
        
        return name
    
    def _get_species_properties(self, species_id):
        """獲取種的屬性（只取需要的欄位）"""
        try:
            query = """
                SELECT properties ->> '$.latin_genus' as latin_genus,
                       properties ->> '$.latin_s1' as latin_s1
                FROM taxon_names WHERE id = %s
            """
            result = self.execute_with_retry(query, (species_id,), fetch=True)
            if result:
                return {
                    'latin_genus': result[0][0],
                    'latin_s1': result[0][1]
                }
        except Exception as e:
            self.logger.error(f"獲取種屬性失敗 (species_id: {species_id}): {e}")
        return {}
    
    def process_taxon_names(self, df):
        """處理學名"""
        if df.empty:
            return []
            
        rows = []
        failed_names = []
        
        for _, row in df.iterrows():
            try:
                formatted_name = self.format_taxon_name(row)
                rows.append([row['taxon_name_id'], formatted_name])
            except Exception as e:
                self.logger.error(f"❌ Name ID {row['taxon_name_id']}: 學名處理失敗 - {e}")
                failed_names.append(row['taxon_name_id'])
        
        if failed_names:
            self.logger.warning(f"⚠️  失敗的 Name IDs ({len(failed_names)}): {failed_names}")
        
        return rows
    
    def fetch_author_data(self, taxon_name_df):
        """獲取作者資料（只取得相關的 taxon_name_id）"""
        if taxon_name_df.empty:
            return pd.DataFrame(columns=['last_name', 'name_abbr', 'taxon_name_id', 'order', 'role'])
        
        # 收集所有需要查詢的 taxon_name_id
        taxon_name_ids = set()
        
        # 加入當前的 taxon_name_id
        taxon_name_ids.update(taxon_name_df['taxon_name_id'].dropna().astype(int).tolist())
        
        # 加入 original_taxon_name_id（如果存在）
        original_ids = taxon_name_df['original_taxon_name_id'].dropna().astype(int).tolist()
        taxon_name_ids.update(original_ids)
        
        if not taxon_name_ids:
            return pd.DataFrame(columns=['last_name', 'name_abbr', 'taxon_name_id', 'order', 'role'])
        
        # 準備 IN 查詢的佔位符
        placeholders = ','.join(['%s'] * len(taxon_name_ids))
        
        query = f"""
            SELECT p.last_name, p.abbreviation_name, ptn.taxon_name_id, ptn.order, ptn.role 
            FROM person_taxon_name ptn
            LEFT JOIN persons p ON ptn.person_id = p.id 
            WHERE ptn.taxon_name_id IN ({placeholders})
        """
        
        results = self.execute_with_retry(query, tuple(taxon_name_ids), fetch=True)
        
        if not results:
            return pd.DataFrame(columns=['last_name', 'name_abbr', 'taxon_name_id', 'order', 'role'])
        
        return pd.DataFrame(results, columns=['last_name', 'name_abbr', 'taxon_name_id', 'order', 'role'])

    def _batch_get_additional_data(self, df):
        """批次獲取所有需要的額外資料"""
        # 收集所有需要的 ID
        all_original_ids = set(df['original_taxon_name_id'].dropna().astype(int))
        
        if not all_original_ids:
            return {}, {}

        # 批次查詢發布年份和原始學名名稱
        publish_years = {}
        original_names = {}
        if all_original_ids:
            placeholders = ','.join(['%s'] * len(all_original_ids))
            query = f"SELECT id, publish_year, name FROM taxon_names WHERE id IN ({placeholders})"
            results = self.execute_with_retry(query, tuple(all_original_ids), fetch=True)
            for r in results:
                if r:
                    publish_years[r[0]] = r[1]
                    original_names[r[0]] = r[2]
        
        return publish_years, original_names

    def _create_author_lookup(self, author_df):
        """建立高效的作者查詢結構"""
        author_lookup = defaultdict(lambda: {
            'current_last_names': [],
            'current_abbr_names': [],
            'ex_last_names': [],
            'ex_abbr_names': []
        })
        
        # 按 taxon_name_id 分組並排序
        for taxon_name_id, group in author_df.groupby('taxon_name_id'):
            # 排序很重要，確保順序一致
            sorted_group = group.sort_values('order')
            
            # 分離一般作者和 ex 作者
            current_authors = sorted_group[sorted_group['role'] != 1]
            ex_authors = sorted_group[sorted_group['role'] == 1]
            
            author_lookup[taxon_name_id] = {
                'current_last_names': current_authors['last_name'].dropna().tolist(),
                'current_abbr_names': current_authors['name_abbr'].dropna().tolist(),
                'ex_last_names': ex_authors['last_name'].dropna().tolist(),
                'ex_abbr_names': ex_authors['name_abbr'].dropna().tolist()
            }
        
        return dict(author_lookup)

    def format_name_authors(self, df, author_df):
        """優化的學名作者格式化"""
        if df.empty:
            return df
        
        failed_names = []
        
        # 1. 預先建立高效查詢結構
        # self.logger.info("建立作者查詢索引...")
        author_lookup = self._create_author_lookup(author_df)
        
        # 2. 批次獲取額外資料（大幅簡化）
        # self.logger.info("批次獲取額外資料...")
        publish_years, original_names = self._batch_get_additional_data(df)
        
        # 3. 向量化處理（但保持相同邏輯）
        def process_single_row(row):
            try:
                return self._build_author_string(
                    row, author_lookup, publish_years, original_names
                )
            except Exception as e:
                self.logger.error(f"❌ Name ID {row.get('taxon_name_id', 'Unknown')}: 作者格式化失敗 - {e}")
                failed_names.append(row.get('taxon_name_id', 'Unknown'))
                return ""
        
        # 4. 使用 apply 進行向量化操作（比 iterrows 快）
        # self.logger.info("格式化作者...")
        df = df.copy()  # 避免 SettingWithCopyWarning
        df['formatted_author'] = df.apply(process_single_row, axis=1)
        
        if failed_names:
            self.logger.warning(f"⚠️  作者格式化失敗的 Name IDs ({len(failed_names)}): {failed_names}")
        
        return df

    def _build_author_string(self, row, author_lookup, publish_years, original_names):
        """優化的作者字串構建（使用已提取的欄位）"""
        nomenclature_id = row['nomenclature_id']
        
        # 動物命名規約
        if nomenclature_id == 1:
            return self._build_animal_author(row, author_lookup, publish_years, original_names)
        
        # 植物命名規約
        elif nomenclature_id == 2:
            return self._build_plant_author(row, author_lookup)
        
        # 細菌命名規約
        elif nomenclature_id == 3:
            return self._build_bacterial_author(row, author_lookup, publish_years)
        
        return ""

    def _build_animal_author(self, row, author_lookup, publish_years, original_names):
        """優化的動物命名規約作者"""
        names = []
        p_year = None
        
        if row['original_taxon_name_id']:
            # 從預建查詢表獲取作者
            original_authors = author_lookup.get(row['original_taxon_name_id'], {})
            names = original_authors.get('current_last_names', [])
            # 從批次查詢結果獲取年份
            p_year = publish_years.get(row['original_taxon_name_id'])
        else:
            current_authors = author_lookup.get(row['taxon_name_id'], {})
            names = current_authors.get('current_last_names', [])
            p_year = row['publish_year']
        
        author_str = AuthorFormatter.format_author_list(names, p_year, nomenclature_id=1)
        
        # 檢查是否需要加括號（使用預查詢的資料）
        if row['original_taxon_name_id'] and author_str:
            original_name = original_names.get(row['original_taxon_name_id'], "")
            if not self._is_same_genus(row['name'], original_name):
                author_str = f'({author_str})'
        
        return author_str

    def _build_plant_author(self, row, author_lookup):
        """優化的植物命名規約作者"""
        # 當前學名的作者
        current_authors = author_lookup.get(row['taxon_name_id'], {})
        names = current_authors.get('current_abbr_names', [])
        ex_names = current_authors.get('ex_abbr_names', [])
        
        author_str = AuthorFormatter.format_author_list(names, nomenclature_id=2)
        ex_author_str = AuthorFormatter.format_author_list(ex_names, nomenclature_id=2)
        
        # 組合 ex 作者
        if ex_author_str and author_str:
            author_str = f"{ex_author_str} ex {author_str}"
        
        # 處理基礎名
        if row['original_taxon_name_id']:
            ori_authors = author_lookup.get(row['original_taxon_name_id'], {})
            ori_names = ori_authors.get('current_abbr_names', [])
            ori_ex_names = ori_authors.get('ex_abbr_names', [])
            
            ori_author_str = AuthorFormatter.format_author_list(ori_names, nomenclature_id=2)
            ori_ex_author_str = AuthorFormatter.format_author_list(ori_ex_names, nomenclature_id=2)
            
            if ori_ex_author_str and ori_author_str:
                ori_combined = f"{ori_ex_author_str} ex {ori_author_str}"
            else:
                ori_combined = ori_author_str
            
            if ori_combined and author_str:
                author_str = f"({ori_combined}) {author_str}"
        
        return author_str

    def _build_bacterial_author(self, row, author_lookup, publish_years):
        """優化的細菌命名規約作者（使用已提取的欄位）"""
        # 當前學名的作者
        current_authors = author_lookup.get(row['taxon_name_id'], {})
        names = current_authors.get('current_last_names', [])
        ex_names = current_authors.get('ex_last_names', [])

        p_year = row['publish_year']
        # 直接使用已提取的前述名年份
        ex_p_year = row['initial_year']
        
        author_str = AuthorFormatter.format_author_list(names, p_year, nomenclature_id=3)
        ex_author_str = AuthorFormatter.format_author_list(ex_names, ex_p_year, nomenclature_id=3)

        # 處理原始組合名
        if not row['original_taxon_name_id']:  # 代表自己是原始組合名
            # 若有前述名 加上前述名
            if ex_author_str and author_str:
                author_str = f"(ex {ex_author_str}) {author_str}"
            elif ex_author_str:
                author_str = f"(ex {ex_author_str})"

        else:  # 若有原始組合名欄位 代表自己不是原始組合名
            # 非原始組合學名的基礎名如果有前述作者，不顯示前述作者，只顯示學名作者。
            ori_authors = author_lookup.get(row['original_taxon_name_id'], {})
            ori_names = ori_authors.get('current_last_names', [])
            ori_p_year = publish_years.get(row['original_taxon_name_id'])
            ori_author_str = AuthorFormatter.format_author_list(ori_names, ori_p_year, nomenclature_id=3)

            if ori_author_str and author_str:
                author_str = f"({ori_author_str}) {author_str}"
            elif ori_author_str:
                author_str = f"({ori_author_str})"

        # Approved Lists（直接使用已提取的欄位）
        if author_str and row['is_approved_list']:
            author_str += ' (Approved Lists 1980)'
        
        return author_str

    def _is_same_genus(self, current_name, original_name):
        """優化的同屬檢查（使用預查詢的資料）"""
        try:
            current_genus = current_name.split(' ')[0] if current_name else ""
            original_genus = original_name.split(' ')[0] if original_name else ""
            return current_genus == original_genus
        except Exception as e:
            self.logger.error(f"檢查同屬失敗: {e}")
            return False
    
    def process_hybrid_names(self, hybrid_name_ids=None):
        """處理雜交學名"""
        base_query = """
            WITH view as (
                SELECT tnhp.taxon_name_id, an.formatted_name 
                FROM taxon_name_hybrid_parent tnhp 
                JOIN api_names an ON tnhp.parent_taxon_name_id = an.taxon_name_id 
                JOIN taxon_names tn ON tnhp.taxon_name_id = tn.id
                WHERE tn.rank_id = 47 AND tn.is_publish = 1
        """
        
        params = []
        
        # 如果指定了 hybrid_name_ids，則只處理這些 ID
        if hybrid_name_ids and isinstance(hybrid_name_ids, (list, tuple)) and len(hybrid_name_ids) > 0:
            placeholders = ','.join(['%s'] * len(hybrid_name_ids))
            base_query += f" AND tnhp.taxon_name_id IN ({placeholders})"
            params = list(hybrid_name_ids)
        
        base_query += """
                ORDER BY tnhp.order
            ) 
            SELECT taxon_name_id, group_concat(formatted_name SEPARATOR ' × ') 
            FROM view 
            GROUP BY taxon_name_id
        """
        
        results = self.execute_with_retry(base_query, tuple(params), fetch=True)
        
        rows = []
        for r in results:
            rows.append([r[0], r[1]])
        
        return rows
    
    def prepare_names_for_db(self, rows, df, author_df, update_time):
        """準備學名資料"""
        if not rows:
            # self.logger.info("沒有學名資料需要更新")
            return {}
        
        # 使用優化版本格式化作者
        df_with_authors = self.format_name_authors(df, author_df)
        df_with_authors = df_with_authors.replace({nan: None})
        
        # 建立 taxon_name_id 到作者的映射
        author_mapping = {}
        for _, row in df_with_authors.iterrows():
            formatted_author = ""
            
            if hasattr(row, 'formatted_author') and row.formatted_author:
                formatted_author = row.formatted_author
            elif row['authors_name']:
                formatted_author = row['authors_name']
            
            author_mapping[row['taxon_name_id']] = formatted_author
        
        # 準備最終資料
        final_data = {}
        for row in rows:
            taxon_name_id = row[0]
            formatted_name = row[1]
            name_author = author_mapping.get(taxon_name_id, "")
            
            final_data[taxon_name_id] = {
                'taxon_name_id': taxon_name_id,
                'formatted_name': formatted_name,
                'name_author': name_author,
                'updated_at': update_time,
                'created_at': update_time
            }
        
        return final_data
    
    def batch_update_api_names(self, final_data, update_time):
        """批次更新 api_names 表"""
        if not final_data:
            # self.logger.info("沒有資料需要更新到 api_names")
            return
        
        # self.logger.info(f"準備批次更新 {len(final_data)} 筆 api_names 資料")
        
        # 完整更新（包含 formatted_name 和 name_author）
        # 只有在資料真的有變化時才更新 updated_at
        full_update_query = """
            INSERT INTO api_names (taxon_name_id, formatted_name, name_author, updated_at, created_at) 
            VALUES (%s, %s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE 
                updated_at = CASE 
                    WHEN COALESCE(formatted_name, '') != COALESCE(VALUES(formatted_name), '')
                        OR COALESCE(name_author, '') != COALESCE(VALUES(name_author), '')
                    THEN VALUES(updated_at)
                    ELSE updated_at
                END,
                formatted_name = VALUES(formatted_name),
                name_author = VALUES(name_author)
        """
        
        # 準備資料
        full_data = []
        for taxon_name_id, data in final_data.items():
            full_data.append((
                data['taxon_name_id'],
                data['formatted_name'],
                data['name_author'],
                data['updated_at'],
                data['created_at']
            ))
        
        # 執行批次更新
        if full_data:
            # self.logger.info(f"完整更新的記錄: {len(full_data)} 筆")
            self.batch_execute(full_update_query, full_data, self.batch_size)
        
    # ======================== 主要更新方法 ========================
    
    def run_update(self, custom_updated=None, limit=None, taxon_name_ids=None, person_ids=None, min_taxon_name_id=None, hybrid_name_ids=None, update_all=False):
        """執行學名更新"""
        # self.logger.info("開始更新學名資料...")
        
        # 如果是指定雜交學名模式，直接處理雜交學名
        if hybrid_name_ids:
            # self.logger.info(f"指定更新 {len(hybrid_name_ids)} 個雜交學名")
            
            # 獲取更新時間
            update_times = self.get_last_update_times()
            taxon_names_updated = update_times['taxon_names']
            
            # 直接處理指定的雜交學名
            hybrid_rows = self.process_hybrid_names(hybrid_name_ids)
            if hybrid_rows:
                # 雜交學名不需要作者處理，直接準備資料
                hybrid_data = {}
                for row in hybrid_rows:
                    taxon_name_id = row[0]
                    formatted_name = row[1]
                    
                    hybrid_data[taxon_name_id] = {
                        'taxon_name_id': taxon_name_id,
                        'formatted_name': formatted_name,
                        'name_author': "",  # 雜交學名沒有作者
                        'updated_at': taxon_names_updated,
                        'created_at': taxon_names_updated
                    }
                
                # self.logger.info(f"準備更新 {len(hybrid_rows)} 個雜交學名")
                self.batch_update_api_names(hybrid_data, taxon_names_updated)
                # self.logger.info(f"成功更新 {len(hybrid_data)} 筆雜交學名資料")
            # else:
                # self.logger.info("沒有找到指定的雜交學名")
            
            # self.logger.info("雜交學名更新完成!")
            return
        
        # 一般學名處理邏輯
        
        # 獲取更新時間
        update_times = self.get_last_update_times()
        last_names_updated = update_times['api_names']
        taxon_names_updated = update_times['taxon_names']
        
        # # 根據不同的更新方式顯示不同的日誌
        # if person_ids:
        #     self.logger.info(f"根據 {len(person_ids)} 個 person_ids 更新相關學名")
        # elif taxon_name_ids:
        #     self.logger.info(f"指定更新 {len(taxon_name_ids)} 個 taxon_name_ids 及其相關學名")
        # elif min_taxon_name_id is not None:
        #     self.logger.info(f"更新 taxon_name_id >= {min_taxon_name_id} 的相關學名")
        # elif custom_updated:
        #     self.logger.info(f"自定義更新時間: {custom_updated}")
        # else:
        #     self.logger.info(f"學名上次更新時間: {last_names_updated}")
        
        # 1. 獲取需要更新的學名資料（包含完整資訊）
        if person_ids:
            taxon_name_df = self.fetch_updated_taxon_names(
                person_ids=person_ids, 
                limit=limit
            )
        elif taxon_name_ids:
            taxon_name_df = self.fetch_updated_taxon_names(
                taxon_name_ids=taxon_name_ids, 
                limit=limit
            )
        elif update_all:
            taxon_name_df = self.fetch_updated_taxon_names(
                update_all=True, 
                limit=limit
            )
        elif custom_updated:
            taxon_name_df = self.fetch_updated_taxon_names(
                last_updated=custom_updated, 
                limit=limit
            )
        else:
            taxon_name_df = self.fetch_updated_taxon_names(
                last_updated=last_names_updated, 
                limit=limit
            )
        
        if not taxon_name_df.empty:
            # self.logger.info(f"找到 {len(taxon_name_df)} 個需要更新的學名")
            
            # 處理學名格式化
            rows = self.process_taxon_names(taxon_name_df)
            
            # 獲取作者資料（使用優化版本）
            # self.logger.info("處理學名作者...")
            author_df = self.fetch_author_data(taxon_name_df)
            
            # 準備完整資料（包含格式化名稱和作者）
            final_data = self.prepare_names_for_db(rows, taxon_name_df, author_df, taxon_names_updated)
            
            # 批次更新 api_names 表
            if final_data:
                self.batch_update_api_names(final_data, taxon_names_updated)
        #     else:
        #         self.logger.info("沒有資料需要更新")
        # else:
        #     self.logger.info("沒有需要更新的學名")
        
        # 2. 處理雜交學名（只有在非指定 ID 或 person_ids 模式下才處理）
        if not taxon_name_ids and not person_ids and min_taxon_name_id is None:
            # self.logger.info("處理雜交學名...")
            hybrid_rows = self.process_hybrid_names()
            if hybrid_rows:
                # 雜交學名不需要作者處理，直接準備資料
                hybrid_data = {}
                for row in hybrid_rows:
                    taxon_name_id = row[0]
                    formatted_name = row[1]
                    
                    hybrid_data[taxon_name_id] = {
                        'taxon_name_id': taxon_name_id,
                        'formatted_name': formatted_name,
                        'name_author': "",  # 雜交學名沒有作者
                        'updated_at': taxon_names_updated,
                        'created_at': taxon_names_updated
                    }
                
                # self.logger.info(f"準備更新 {len(hybrid_rows)} 個雜交學名")
                
                self.batch_update_api_names(hybrid_data, taxon_names_updated)
                # self.logger.info(f"成功更新 {len(hybrid_data)} 筆雜交學名資料")
        #     else:
        #         self.logger.info("沒有雜交學名需要更新")
        # else:
        #     # person修改不會影響雜交 最小id是由檔案匯入也不會有雜交
        #     self.logger.info("指定 ID、person_ids 或最小 ID 模式：跳過雜交學名處理")
        
        # self.logger.info("學名更新完成!")

# def main():
#     """主函數"""
#     # 一般執行
#     with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#         updater.run_update()
    
#     # 測試
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update()
    
#     # 限制數量 測試
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(limit=100)
    
#     # 更新全部
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(update_all=True)
    
#     # 自定義更新時間
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(custom_updated=datetime(2024, 1, 1))
    
#     # 根據 person_ids 更新
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(person_ids=[1, 2, 3])
    
#     # 根據 taxon_name_ids 更新（包含相關學名）
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(taxon_name_ids=[100, 200, 300])
    
#     # 根據最小 taxon_name_id 更新
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(min_taxon_name_id=1000)
    
#     # 根據指定雜交學名 IDs 更新
#     # with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     #     updater.run_update(hybrid_name_ids=[1001, 1002, 1003])

# if __name__ == "__main__":
#     main()


# 使用範例：
# from scripts.final._02_update_name import *
# 
# # 一般更新
# with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     updater.run_update()
#
# # 根據 person_ids 更新
# with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     updater.run_update(person_ids=[1, 2, 3])
#
# # 根據 taxon_name_ids 更新（包含相關學名）
# with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     updater.run_update(taxon_name_ids=[100, 200, 300])
#
# # 根據最小 taxon_name_id 更新
# with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     updater.run_update(min_taxon_name_id=1000)
#
#
# # 根據指定雜交學名 IDs 更新
# with TaxonomicNameUpdater(batch_size=1000, max_retries=3) as updater:
#     updater.run_update(hybrid_name_ids=[1001, 1002, 1003])