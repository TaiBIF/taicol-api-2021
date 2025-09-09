import pandas as pd
import requests
from datetime import datetime
import json
from numpy import nan
import numpy as np
from api.services.utils.common import DatabaseManager, to_firstname_abbr, to_middlename_abbr, setup_logging
import logging
import time

class CitationUpdater(DatabaseManager):
    """引用更新器"""
    
    def __init__(self, batch_size=1000, max_retries=3):
        """
        初始化 CitationUpdater
        
        Args:
            batch_size (int): 批次插入的大小，默認為 1000
            max_retries (int): 最大重試次數
        """
        super().__init__(max_retries)
        self.batch_size = batch_size
        setup_logging()
    
    def get_last_update_times(self):
        """獲取最後更新時間"""
        queries = {
            'api_citations': "SELECT MAX(updated_at) FROM `api_citations`",
            'references': "SELECT MAX(updated_at) FROM `references` WHERE is_publish = 1"
        }
        
        results = {}
        for key, query in queries.items():
            result = self.execute_with_retry(query, fetch=True)
            results[key] = result[0][0] if result and result[0][0] else datetime(1900, 1, 1)
        
        return results['api_citations'], results['references']
    
    def fetch_updated_references(self, last_updated=None, limit=None, update_all=False, reference_ids=None, person_ids=None, min_reference_id=None):
        """
        獲取需要更新的文獻資料，直接提取 properties 中的特定欄位
        
        Args:
            last_updated: 最後更新時間
            limit: 限制返回記錄數
            update_all: 是否更新全部
            reference_ids: 指定的 reference_id 列表
            person_ids: 指定的 person_id 列表
            min_reference_id: 最小 reference_id 條件
        """
        base_query = """
            SELECT p.last_name, p.first_name, p.middle_name, r.id as reference_id, pr.order, 
                   r.publish_year, r.type, r.title, 
                   r.properties ->> '$.doi' as doi,
                   r.properties ->> '$.article_title' as article_title,
                   r.properties ->> '$.book_title' as book_title,
                   r.properties ->> '$.volume' as volume,
                   r.properties ->> '$.issue' as issue,
                   r.properties ->> '$.article_number' as article_number,
                   r.properties ->> '$.pages_range' as pages_range,
                   r.properties ->> '$.edition' as edition,
                   r.properties ->> '$.chapter' as chapter,
                   ac.publish_date             
            FROM `references` r              
            LEFT JOIN person_reference pr ON r.id = pr.reference_id             
            LEFT JOIN persons p ON pr.person_id = p.id             
            LEFT JOIN api_citations ac ON ac.reference_id = r.id             
            WHERE r.is_publish = 1
        """

        # 根據不同的更新方式添加條件
        params = []
        
        if person_ids:
            # 使用指定的 person_id 列表
            if not isinstance(person_ids, (list, tuple)):
                person_ids = [person_ids]
            
            # 過濾無效的 ID
            valid_ids = [pid for pid in person_ids if pid is not None and str(pid).strip()]
            
            if not valid_ids:
                self.logger.warning("提供的 person_ids 列表為空或無效")
                return pd.DataFrame()
            
            placeholders = ','.join(['%s'] * len(valid_ids))
            base_query += f" AND EXISTS (SELECT 1 FROM person_reference pr_filter WHERE pr_filter.reference_id = r.id AND pr_filter.person_id IN ({placeholders}))"
            params.extend(valid_ids)
            
        elif reference_ids:
            # 使用指定的 reference_id 列表
            if not isinstance(reference_ids, (list, tuple)):
                reference_ids = [reference_ids]
            
            # 過濾無效的 ID
            valid_ids = [rid for rid in reference_ids if rid is not None and str(rid).strip()]
            
            if not valid_ids:
                self.logger.warning("提供的 reference_ids 列表為空或無效")
                return pd.DataFrame()
            
            placeholders = ','.join(['%s'] * len(valid_ids))
            base_query += f" AND r.id IN ({placeholders})"
            params.extend(valid_ids)
            
        elif min_reference_id is not None:
            # 使用最小 reference_id 條件
            base_query += " AND r.id >= %s"
            params.append(min_reference_id)
            
        elif last_updated:
            # 使用最後更新時間
            base_query += f""" AND (
                -- 條件1: references的updated_at
                r.updated_at > %s
                OR 
                -- 條件2: persons的updated_at 透過person_reference表關聯
                EXISTS (
                    SELECT 1 
                    FROM persons p2
                    INNER JOIN person_reference pr2 ON p2.id = pr2.person_id
                    WHERE pr2.reference_id = r.id 
                        AND p2.updated_at > %s
                )
            )
            """
            params.extend([last_updated, last_updated])
            
        elif update_all: 
            # 更新全部，不添加額外條件
            pass

        if limit:
            base_query += f" LIMIT {limit}"
        
        # 執行查詢
        if params:
            results = self.execute_with_retry(base_query, tuple(params), fetch=True)
        else:
            results = self.execute_with_retry(base_query, fetch=True)

        if not results:
            return pd.DataFrame()
        
        columns = ['last_name', 'first_name', 'middle_name', 'reference_id', 
                  'order', 'year', 'type', 'title', 'doi', 'article_title', 
                  'book_title', 'volume', 'issue', 'article_number', 'pages_range',
                  'edition', 'chapter', 'publish_date']
        
        df = pd.DataFrame(results, columns=columns).drop_duplicates().replace({np.nan: None})
        
        # 清理資料中的特殊字符
        if not df.empty:
            text_columns = ['article_title', 'book_title']
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: str(x).replace('\\{\\}', '').strip() if x else None)
        
        return df
    
    def build_author_strings(self, authors_data):
        """構建作者字串"""
        author_list = []
        short_author_list = []
        
        for _, row in authors_data.sort_values('order').iterrows():
            if row['last_name']:  # 確保有姓氏
                last_name = row['last_name']
                first_name = to_firstname_abbr(row['first_name'])
                middle_name = to_middlename_abbr(row['middle_name'])
                full_name = f"{last_name}, {first_name}{middle_name}"
                author_list.append(full_name)
                short_author_list.append(last_name)
        
        # 格式化作者字串
        if not author_list:
            return '', ''
        
        # 完整作者格式
        authors = {
            1: author_list[0],
            2: ' & '.join(author_list)
        }.get(len(author_list), ', '.join(author_list[:-1]) + ' & ' + author_list[-1])
        
        # 簡短作者格式  
        short_authors = {
            1: short_author_list[0],
            2: ' & '.join(short_author_list)
        }.get(len(short_author_list), short_author_list[0] + ' et al.')
        
        return authors, short_authors
    
    def generate_content(self, row):
        """根據文獻類型生成內容（使用已提取的欄位）"""
        content = row['title'] or ''
        
        content_generators = {
            3: self._generate_book_content,        # 書籍
            4: self._generate_catalog_content,     # 名錄  
            1: self._generate_journal_content,     # 期刊文章
            2: self._generate_chapter_content      # 書籍篇章
        }
        
        generator = content_generators.get(row['type'])
        if generator:
            return generator(content, row)
        
        return content
    
    def _generate_book_content(self, content, row):
        """生成書籍內容"""
        return content + '.' if content and not content.endswith('.') else content
    
    def _generate_catalog_content(self, content, row):
        """生成名錄內容"""
        return content  # 保持原樣
    
    def _generate_journal_content(self, content, row):
        """生成期刊文章內容（使用已提取的欄位）"""
        # 構建內容部分
        content_parts = []
        
        # 文章標題
        article_title = row['article_title']
        if article_title:
            content_parts.append(f"{article_title}.")
        
        # 期刊名稱和卷號部分
        journal_part = ""
        book_title = row['book_title']
        if book_title:
            journal_part += f"<i>{book_title}</i>"
        
        volume = str(row['volume']).strip() if row['volume'] else ""
        if volume:
            if journal_part:  # 如果前面有期刊名稱，加空格
                journal_part += f" {volume}"
            else:
                journal_part += volume
        
        # 期號 - 直接附加到卷號後面，不加空格
        issue = str(row['issue']).strip() if row['issue'] else ""
        if issue:
            journal_part += f"({issue})"
        
        # 頁碼或文章編號 - 直接附加冒號，不加空格
        if row['article_number']:
            journal_part += f": {row['article_number']}."
        elif row['pages_range']:
            journal_part += f": {row['pages_range']}."
        
        # 如果有期刊部分，加入內容
        if journal_part:
            content_parts.append(journal_part)
        
        # 最終組合
        return ' '.join(content_parts) if content_parts else content
    
    def _generate_chapter_content(self, content, row):
        """生成書籍篇章內容（使用已提取的欄位）"""
        article_title = row['article_title']
        book_title = row['book_title']

        content = f"{article_title}. In: {book_title}," if article_title and book_title else content
        
        # 版本、卷號、章節資訊
        extras = []
        if row['edition']:
            extras.append(f"{row['edition']} ed.")
        if row['volume']:
            extras.append(f"vol. {row['volume']}")
        elif row['chapter']:
            extras.append(f"ch. {row['chapter']}")
            
        if extras:
            content += ' ' + ', '.join(extras) + '.'
            
        if row['pages_range']:
            content += f" {row['pages_range']}."
            
        return content

    def process_citations(self, results, get_publish_date=True):
        """處理引用資料"""
        citation_data = []
        failed_references = []
        
        # 準備DOI對應的發布日期資料
        refs_dates = {}
        refs = results[['reference_id', 'doi', 'publish_date']].drop_duplicates()
        refs = refs.replace({'': None, nan: None})
        
        # 處理 DOI 發布日期
        if get_publish_date:
            # 先將所有已有的發布日期加入 refs_dates
            for _, row in refs.iterrows():
                refs_dates[row['reference_id']] = row['publish_date']
            
            # 篩選出需要從 DOI 獲取發布日期的記錄
            refs_need_doi = refs[(refs['publish_date'].isna()) & (refs['doi'].notna())]
            
            if not refs_need_doi.empty:
                
                for _, row in refs_need_doi.iterrows():
                    try:
                        reference_id = row['reference_id']
                        publish_date = self.fetch_doi_publish_date(row['doi'])
                        refs_dates[reference_id] = publish_date
                        
                    except Exception as e:
                        self.logger.error(f"❌ Reference ID {row.get('reference_id', 'Unknown')}: DOI 處理失敗 - {e}")
                        refs_dates[row.get('reference_id')] = None
        
        # 處理每個 reference_id 的引用資料
        for ref_id in results['reference_id'].unique():
            try:
                ref_rows = results[results['reference_id'] == ref_id]
                
                # 構建作者字串
                authors, short_authors = self.build_author_strings(ref_rows)
                
                # 獲取年份
                year = ref_rows['year'].iloc[0] if len(ref_rows) > 0 else ''
                
                # 生成內容
                content = self.generate_content(ref_rows.iloc[0])
                
                # 獲取發布日期
                if get_publish_date:
                    publish_date = refs_dates.get(ref_id)
                else:
                    publish_date = ref_rows['publish_date'].iloc[0]

                citation_data.append({
                    'reference_id': ref_id,
                    'author': f'{authors} ({year})' if authors else f'({year})',
                    'short_author': f'{short_authors}, {year}' if short_authors else str(year),
                    'content': content,
                    'publish_date': publish_date
                })
                
            except Exception as e:
                self.logger.error(f"❌ Reference ID {ref_id}: 處理失敗 - {e}")
                failed_references.append(ref_id)
                
                # 嘗試建立最基本的記錄作為後備
                try:
                    ref_rows = results[results['reference_id'] == ref_id]
                    basic_record = {
                        'reference_id': ref_id,
                        'author': f"({ref_rows['year'].iloc[0] if len(ref_rows) > 0 else ''})",
                        'short_author': str(ref_rows['year'].iloc[0] if len(ref_rows) > 0 else ''),
                        'content': ref_rows['title'].iloc[0] if len(ref_rows) > 0 else '',
                        'publish_date': None
                    }
                    citation_data.append(basic_record)
                except Exception as fallback_error:
                    self.logger.error(f"❌ Reference ID {ref_id}: 連基本記錄都無法建立 - {fallback_error}")
        
        # 報告處理結果
        if failed_references:
            self.logger.warning(f"⚠️  失敗的 Reference IDs ({len(failed_references)}): {failed_references}")
        
        return pd.DataFrame(citation_data)
    
    def update_citations_db(self, citation_df, update_time):
        """
        更新引用資料和發布日期到數據庫，使用優化的批次插入
        
        Args:
            citation_df (pd.DataFrame): 引用資料DataFrame
            update_time: 更新時間戳
        """
        if citation_df.empty:
            return
            
        # 確保欄位順序並添加時間戳
        citation_df = citation_df[['reference_id', 'author', 'short_author', 'content', 'publish_date']].copy()
        citation_df['updated_at'] = update_time
        citation_df['created_at'] = update_time
        
        # 使用 INSERT ... ON DUPLICATE KEY UPDATE 語法
        query = """
        INSERT INTO api_citations (reference_id, author, short_author, content, publish_date, updated_at, created_at) 
        VALUES (%s, %s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
            updated_at = CASE 
                WHEN COALESCE(author, '') != COALESCE(VALUES(author), '')
                     OR COALESCE(short_author, '') != COALESCE(VALUES(short_author), '')
                     OR COALESCE(content, '') != COALESCE(VALUES(content), '')
                     OR COALESCE(publish_date, '') != COALESCE(VALUES(publish_date), '')
                THEN VALUES(updated_at)
                ELSE updated_at
            END,
            author = VALUES(author), 
            short_author = VALUES(short_author), 
            content = VALUES(content), 
            publish_date = VALUES(publish_date)
        """
        
        # 轉換為元組列表
        all_data = [tuple(row) for row in citation_df.values]
                
        # 使用 DatabaseManager 的批次執行方法
        self.batch_execute(query, all_data, self.batch_size)
    
    def fetch_doi_publish_date(self, doi):
        """從DOI獲取發布日期"""
        if not doi:
            return None
        
        doi_clean = doi.replace('"', '').replace("'", '')
        if not doi_clean:
            return None
        
        try:
            url = f'https://api.crossref.org/works/{doi_clean}'
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                message = data.get('message', {})
                
                # 嘗試獲取印刷發布日期
                for date_type in ['published-print', 'published-online']:
                    try:
                        date_parts = message.get(date_type, {}).get('date-parts', [[]])[0]
                        if len(date_parts) >= 3:
                            return datetime(date_parts[0], date_parts[1], date_parts[2]).strftime("%Y-%m-%d")
                        elif len(date_parts) >= 2:
                            return datetime(date_parts[0], date_parts[1], 1).strftime("%Y-%m-%d")
                    except (IndexError, ValueError, TypeError):
                        continue
        except requests.RequestException:
            pass
        
        return None
    
    def set_batch_size(self, batch_size):
        """
        設定批次大小
        
        Args:
            batch_size (int): 新的批次大小
        """
        if batch_size <= 0:
            raise ValueError("批次大小必須大於 0")
        
        self.batch_size = batch_size
    
    def run_update(self, custom_updated=None, limit=None, get_publish_date=True, update_all=False, reference_ids=None, person_ids=None, min_reference_id=None):
        """
        執行完整更新流程
        
        Args:
            custom_updated: 自定義的更新時間
            limit: 限制處理的記錄數（用於測試）
            get_publish_date: 是否獲取發布日期
            update_all: 是否更新全部
            reference_ids: 指定的 reference_id 列表
            person_ids: 指定的 person_id 列表
            min_reference_id: 最小 reference_id 條件
        """

        # 1. 獲取更新時間
        last_updated_api_citation, last_updated_references = self.get_last_update_times()
        
        
        # 2. 獲取需要更新的資料
        results = self.fetch_updated_references(
            last_updated=custom_updated if not person_ids and not reference_ids and not update_all and min_reference_id is None else None,
            limit=limit,
            update_all=update_all and not person_ids and not reference_ids and min_reference_id is None,
            reference_ids=reference_ids if not person_ids and min_reference_id is None else None,
            person_ids=person_ids if min_reference_id is None else None,
            min_reference_id=min_reference_id
        )

        if results.empty:
            return
                
        # 3. 處理引用資料（包含發布日期）
        citation_df = self.process_citations(results, get_publish_date=get_publish_date)
        
        # 4. 更新引用資料和發布日期
        if not citation_df.empty:
            self.update_citations_db(citation_df, last_updated_references)
