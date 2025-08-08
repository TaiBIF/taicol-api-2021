# 批次優化的 Solr 更新器 v6 - 支援多個 taxon_id 批次更新

import requests
import json
import pymysql
import logging
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import os

class BatchOptimizedSolrTaxonUpdater:
    """
    批次優化的 Solr Taxon 更新器 v6
    - 支援多個 taxon_id 的批次更新
    - 同時支援 partial 和 full 更新
    - 大幅減少 commit 次數
    - 保持原有 API 介面
    """
    
    def __init__(self, solr_base_url: str, core_name: str):
        """
        初始化批次優化的 Solr 更新器
        
        Args:
            solr_base_url: Solr 基礎 URL
            core_name: Solr core 名稱
        """
        self.solr_base_url = solr_base_url.rstrip('/')
        self.core_name = core_name
        self.update_url = f"{self.solr_base_url}/{self.core_name}/update"
        self.select_url = f"{self.solr_base_url}/{self.core_name}/select"
        
        # 建立持久 HTTP 連接
        self.session = self._create_session()
        
        # 設定 logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 性能統計
        self.stats = {
            'total_updates': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'batch_count': 0,
            'total_time': 0
        }
    
    def _create_session(self) -> requests.Session:
        """建立優化的 HTTP 會話"""
        session = requests.Session()
        
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=3,
            # backoff_factor=0.3
        )
        
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        session.headers.update({
            'Content-Type': 'application/json; charset=utf-8',
            'Connection': 'keep-alive'
        })
        
        return session
    
    def batch_full_replace_by_taxon_ids(self, taxon_data_map: Dict[str, Any]) -> bool:
        """
        批次完整替換多個 taxon_id 的文檔
        
        Args:
            taxon_data_map: {taxon_id: documents} 的字典
            
        Returns:
            是否全部成功
        """
        if not taxon_data_map:
            self.logger.warning("沒有資料需要更新")
            return True
        
        start_time = time.time()
        
        try:
            self.logger.info(f"開始批次完整替換: {len(taxon_data_map)} 個 taxon_id")
            
            # 1. 批次刪除所有舊文檔
            delete_success = self._batch_delete_by_taxon_ids(list(taxon_data_map.keys()))
            if not delete_success:
                self.logger.error("批次刪除失敗")
                return False
            
            # 2. 批次新增所有新文檔
            all_documents = []
            for taxon_id, documents in taxon_data_map.items():
                if isinstance(documents, dict):
                    documents = [documents]
                
                # 確保每個文檔都有 taxon_id
                for doc in documents:
                    if 'taxon_id' not in doc:
                        doc['taxon_id'] = taxon_id
                
                all_documents.extend(documents)
            
            add_success = self._batch_add_documents(all_documents, auto_commit=True)
            
            if add_success:
                self.logger.info(f"批次完整替換成功: {len(taxon_data_map)} 個 taxon_id, {len(all_documents)} 個文檔")
                self.stats['successful_updates'] += len(taxon_data_map)
                self.stats['batch_count'] += 1
                return True
            else:
                self.logger.error("批次新增失敗")
                self.stats['failed_updates'] += len(taxon_data_map)
                return False
                
        except Exception as e:
            self.logger.error(f"批次完整替換失敗: {e}")
            self.stats['failed_updates'] += len(taxon_data_map)
            return False
        finally:
            elapsed = time.time() - start_time
            self.stats['total_time'] += elapsed
            self.stats['total_updates'] += len(taxon_data_map)
            self.logger.info(f"批次處理完成，耗時: {elapsed:.2f}s")
    
    def batch_partial_update_by_taxon_ids(self, taxon_updates_map: Dict[str, Dict[str, Any]]) -> bool:
        """
        批次部分更新多個 taxon_id 的文檔
        
        Args:
            taxon_updates_map: {taxon_id: {field: value}} 的字典
            
        Returns:
            是否全部成功
        """
        if not taxon_updates_map:
            self.logger.warning("沒有資料需要更新")
            return True
        
        start_time = time.time()
        
        try:
            self.logger.info(f"開始批次部分更新: {len(taxon_updates_map)} 個 taxon_id")
            
            # 收集所有需要更新的文檔 ID
            all_update_docs = []
            
            for taxon_id, update_fields in taxon_updates_map.items():
                # 查詢該 taxon_id 的所有文檔 ID
                document_ids = self._get_document_ids_by_taxon_id(taxon_id)
                
                if not document_ids:
                    self.logger.warning(f"找不到 taxon_id='{taxon_id}' 的文檔")
                    continue
                
                # 準備原子更新資料
                for doc_id in document_ids:
                    doc_update = {"id": doc_id}
                    
                    # 轉換為原子更新格式
                    for field_name, new_value in update_fields.items():
                        doc_update[field_name] = {"set": new_value}
                    
                    all_update_docs.append(doc_update)
            
            if not all_update_docs:
                self.logger.warning("沒有找到需要更新的文檔")
                return True
            
            # 執行批次原子更新
            success = self._batch_atomic_update(all_update_docs, auto_commit=True)
            
            if success:
                self.logger.info(f"批次部分更新成功: {len(taxon_updates_map)} 個 taxon_id, {len(all_update_docs)} 個文檔")
                self.stats['successful_updates'] += len(taxon_updates_map)
                self.stats['batch_count'] += 1
                return True
            else:
                self.logger.error("批次原子更新失敗")
                self.stats['failed_updates'] += len(taxon_updates_map)
                return False
                
        except Exception as e:
            self.logger.error(f"批次部分更新失敗: {e}")
            self.stats['failed_updates'] += len(taxon_updates_map)
            return False
        finally:
            elapsed = time.time() - start_time
            self.stats['total_time'] += elapsed
            self.stats['total_updates'] += len(taxon_updates_map)
            self.logger.info(f"批次處理完成，耗時: {elapsed:.2f}s")
    
    def _batch_delete_by_taxon_ids(self, taxon_ids: List[str]) -> bool:
        """批次刪除多個 taxon_id 的文檔"""
        try:
            if not taxon_ids:
                return True
            
            # 構建批次刪除查詢
            query_parts = [f'taxon_id:"{tid}"' for tid in taxon_ids]
            delete_query = " OR ".join(query_parts)
            
            delete_command = {
                "delete": {
                    "query": delete_query
                }
            }
            
            response = self.session.post(
                self.update_url,
                data=json.dumps(delete_command, ensure_ascii=False).encode('utf-8'),
                timeout=60
            )
            
            response.raise_for_status()
            self.logger.info(f"批次刪除成功: {len(taxon_ids)} 個 taxon_id")
            return True
            
        except Exception as e:
            self.logger.error(f"批次刪除失敗: {e}")
            return False
    
    def _batch_add_documents(self, documents: List[Dict], auto_commit: bool = True) -> bool:
        """批次新增文檔"""
        try:
            if not documents:
                return True
            
            params = {}
            if auto_commit:
                params['commit'] = 'true'
            else:
                params['softCommit'] = 'true'
            
            json_data = json.dumps(documents, ensure_ascii=False).encode('utf-8')
            
            response = self.session.post(
                self.update_url,
                params=params,
                data=json_data,
                timeout=300
            )
            
            response.raise_for_status()
            self.logger.info(f"批次新增成功: {len(documents)} 個文檔")
            return True
            
        except Exception as e:
            self.logger.error(f"批次新增失敗: {e}")
            return False
    
    def _batch_atomic_update(self, update_docs: List[Dict], auto_commit: bool = True) -> bool:
        """批次原子更新"""
        try:
            if not update_docs:
                return True
            
            params = {}
            if auto_commit:
                params['commit'] = 'true'
            else:
                params['softCommit'] = 'true'
            
            json_data = json.dumps(update_docs, ensure_ascii=False).encode('utf-8')
            
            response = self.session.post(
                self.update_url,
                params=params,
                data=json_data,
                timeout=300
            )
            
            response.raise_for_status()
            self.logger.info(f"批次原子更新成功: {len(update_docs)} 個文檔")
            return True
            
        except Exception as e:
            self.logger.error(f"批次原子更新失敗: {e}")
            return False
    
    def _get_document_ids_by_taxon_id(self, taxon_id: str, max_rows: int = 1000) -> List[str]:
        """查詢文檔 ID"""
        try:
            params = {
                'q': f'taxon_id:"{taxon_id}"',
                'fl': 'id',
                'rows': max_rows,
                'wt': 'json'
            }
            
            response = self.session.get(self.select_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            docs = data.get('response', {}).get('docs', [])
            
            return [doc['id'] for doc in docs if 'id' in doc]
            
        except Exception as e:
            self.logger.error(f"查詢文檔 ID 失敗: {e}")
            return []
    
    def get_stats(self) -> Dict[str, Any]:
        """獲取性能統計"""
        if self.stats['total_updates'] > 0:
            avg_time = self.stats['total_time'] / self.stats['batch_count'] if self.stats['batch_count'] > 0 else 0
            success_rate = self.stats['successful_updates'] / self.stats['total_updates']
        else:
            avg_time = 0
            success_rate = 0
        
        return {
            'total_updates': self.stats['total_updates'],
            'successful_updates': self.stats['successful_updates'],
            'failed_updates': self.stats['failed_updates'],
            'batch_count': self.stats['batch_count'],
            'success_rate': f"{success_rate:.2%}",
            'average_batch_time': f"{avg_time:.2f}s",
            'total_time': f"{self.stats['total_time']:.2f}s"
        }
    
    def __del__(self):
        """清理資源"""
        if hasattr(self, 'session'):
            self.session.close()


# # 修改後的主迴圈
# def optimized_main_loop_with_batch():
#     """
#     修改後的主迴圈 - 每10個一批處理
#     """
    
#     batch_size = 10  # 每10個為一批
#     current_batch = []
#     batch_count = 0
#     failed_batches = []
    
#     # 建立一個持久的 session 用於 API 呼叫
#     session = requests.Session()
#     adapter = requests.adapters.HTTPAdapter(
#         pool_connections=5,
#         pool_maxsize=10,
#         max_retries=2
#     )
#     session.mount('http://', adapter)
    
#     try:
#         for i, taxon_id in enumerate(tqdm(updating_taxon_ids, desc="處理 taxa")):
#             try:
#                 # 原有的資料處理邏輯
#                 (conservation_row, other_prop_row, tw_cultured_official_prop_row,
#                  common_name_rows, taxon_row, usage_rows, history_rows,
#                  parent_row, solr_rows) = process_final_taxon_data(
#                     taxon_id=taxon_id,
#                     conservation_df_map=conservation_df_map,
#                     common_name_map=common_name_map,
#                     other_prop_map=other_prop_map,
#                     tw_cultured_official_prop_map=tw_cultured_official_prop_map,
#                     taxon_latest_usage_map=taxon_latest_usage_map,
#                     parent_map=parent_map,
#                     taxon_rank_map=taxon_rank_map,
#                     hybrid_taxon_ids=hybrid_taxon_ids,
#                     last_updated=last_updated,
#                     new_taxon_id_list=new_taxon_id_list,
#                     total_df=total_df,
#                     history_df=history_df[history_df.note!='is_cultured'],
#                     tree_stat_df=tree_stat_df,
#                     df_alien_status_solr=df_alien_status_solr,
#                     df_path_solr=df_path_solr,
#                     created_map=created_map,
#                     name_map=name_map,
#                     df_name_solr=df_name_solr
#                 )
                
#                 # MySQL 更新 (保持原邏輯)
#                 db_manager = DatabaseManager()
#                 db_manager.conn.autocommit(False)
                
#                 create_insert_sql(db_manager=db_manager, table_name='api_common_name', 
#                                 taxon_id=taxon_id, data_list=common_name_rows, delete_first=True)
#                 create_insert_sql(db_manager=db_manager, table_name='api_taxon_usages', 
#                                 taxon_id=taxon_id, data_list=usage_rows, delete_first=True)
#                 create_insert_sql(db_manager=db_manager, table_name='api_taxon_history', 
#                                 taxon_id=taxon_id, data_list=history_rows)
#                 create_update_sql_on_duplicate(db_manager=db_manager, table_name='api_taxon_tree', 
#                                               taxon_id=taxon_id, data_list=[parent_row])
                
#                 if conservation_row:
#                     create_update_sql_on_duplicate(db_manager=db_manager, table_name='api_conservation', 
#                                                   taxon_id=taxon_id, data_list=[conservation_row])
                
#                 if taxon_id in old_taxon_id_list:
#                     create_update_sql(db_manager=db_manager, table_name='api_taxon', taxon_id=taxon_id, 
#                                     data_list=[taxon_row], update_columns=[k for k in taxon_row.keys() if k != 'taxon_id'])
#                 else:
#                     create_update_sql_on_duplicate(db_manager=db_manager, table_name='api_taxon', taxon_id=taxon_id, 
#                                                   data_list=[taxon_row], update_columns=[k for k in taxon_row.keys() if k != 'taxon_id'])
                
#                 create_insert_sql(db_manager=db_manager, table_name='api_for_solr', taxon_id=taxon_id, 
#                                  data_list=[{'taxon_id': taxon_id, 'content': json.dumps(solr_rows)}])
                
#                 db_manager.commit()
#                 db_manager.close()
                
#                 # 加入當前批次
#                 current_batch.append(taxon_id)
                
#                 # 當達到批次大小或是最後一個時，執行批次 Solr 更新
#                 if len(current_batch) >= batch_size or i == len(updating_taxon_ids) - 1:
#                     batch_count += 1
#                     print(f"\n處理批次 {batch_count}: {len(current_batch)} 個 taxon_id")
                    
#                     # 呼叫批次更新 API
#                     taxon_ids_str = ','.join(current_batch)
#                     resp = session.get(
#                         f'http://127.0.0.1:8005/update_solr_batch?update_type=full&taxon_ids={taxon_ids_str}',
#                         headers=update_solr_headers,
#                         timeout=300  # 5分鐘 timeout
#                     )
                    
#                     if resp.status_code == 200:
#                         result = resp.json()
#                         if result.get('status') == 'success':
#                             print(f"✅ 批次 {batch_count} 成功: {result.get('processing_time', 'N/A')}")
#                         else:
#                             print(f"❌ 批次 {batch_count} 失敗: {result.get('error', 'unknown')}")
#                             failed_batches.append({
#                                 'batch_num': batch_count,
#                                 'taxon_ids': current_batch.copy(),
#                                 'error': result.get('error', 'unknown')
#                             })
#                             break
#                     else:
#                         print(f"❌ 批次 {batch_count} HTTP 錯誤: {resp.status_code}")
#                         failed_batches.append({
#                             'batch_num': batch_count,
#                             'taxon_ids': current_batch.copy(),
#                             'error': f'HTTP {resp.status_code}'
#                         })
#                         break
                    
#                     # 清空當前批次
#                     current_batch = []
                    
#             except Exception as e:
#                 print(f"處理 {taxon_id} 時發生錯誤: {e}")
#                 # 可以選擇繼續或中斷
#                 break
        
#         # 顯示最終結果
#         print(f"\n=== 批次處理完成 ===")
#         print(f"總批次數: {batch_count}")
#         print(f"失敗批次數: {len(failed_batches)}")
        
#         if failed_batches:
#             print("失敗的批次:")
#             for batch_info in failed_batches:
#                 print(f"  批次 {batch_info['batch_num']}: {batch_info['error']}")
#                 print(f"    taxon_ids: {batch_info['taxon_ids']}")
        
#         return len(failed_batches) == 0
        
#     finally:
#         session.close()


# URL 路由配置 (加入到你的 urls.py)
"""
# 在 urls.py 中添加：
path('update_solr_batch', update_solr_batch, name='update_solr_batch'),

# 或者直接修改現有的 update_solr 來支援批次：
def update_solr_enhanced(request):
    taxon_ids_param = request.GET.get('taxon_ids', '')
    
    if taxon_ids_param:
        # 批次更新
        return update_solr_batch(request)
    else:
        # 單一更新 (原有邏輯)
        return update_solr_original(request)
"""