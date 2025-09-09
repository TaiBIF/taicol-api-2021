import pymysql
import json
import numpy as np
import pandas as pd
import time
import logging
from datetime import datetime
from contextlib import contextmanager
from conf.settings import env
from dotenv import load_dotenv

load_dotenv(override=True)

db_settings = {
    "host": env('DB_HOST'),
    "port": int(env('DB_PORT')),
    "user": env('DB_USER'),
    "password": env('DB_PASSWORD'),
    "db": env('DB_DBNAME'),
}


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def to_firstname_abbr(name):
    """將名字轉換為縮寫"""
    if not name:
        return ""
    
    import re
    # 分割但保留分隔符
    parts = re.split(r'(\s|-)', name.strip())
    
    result = ""
    for part in parts:
        if part and not part.isspace():
            if part == '-':
                result += '-'
            else:
                result += part[0] + '.'  # 移除 .upper()
        elif part.isspace():
            result += '-'  # 將空格替換成連字符
    
    return result


def to_middlename_abbr(name):
    """將中間名轉換為縮寫"""
    if not name:
        return ""
    
    # 分割名字並取首字母
    parts = name.strip().split()
    abbr = ""
    for part in parts:
        if part:
            abbr += part[0] + "."  # 移除 .upper()
    
    return abbr


class DatabaseManager:
    """資料庫管理基類"""
    
    def __init__(self, max_retries=3):
        self.max_retries = max_retries
        self.conn = None
        self.logger = logging.getLogger(__name__)
        self._connect()
    
    def _connect(self):
        """建立資料庫連接"""
        try:
            # 優化連接參數
            db_config = db_settings.copy()
            db_config.update({
                'autocommit': False,
                'charset': 'utf8mb4',
                'connect_timeout': 30,
                'read_timeout': 30,
                'write_timeout': 30,
                'max_allowed_packet': 1024 * 1024 * 16,  # 16MB
            })
            
            self.conn = pymysql.connect(**db_config)
                            
        except Exception as e:
            self.logger.error(f"資料庫連接失敗: {e}")
            raise
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def _is_connection_alive(self):
        """檢查連接是否還活著"""
        try:
            self.conn.ping(reconnect=False)
            return True
        except:
            return False
    
    def _ensure_connection(self):
        """確保連接可用，如果斷線則重新連接"""
        if not self.conn or not self._is_connection_alive():
            self.logger.warning("資料庫連接已斷開，正在重新連接...")
            self._reconnect()
    
    @contextmanager
    def get_cursor(self):
        """獲取游標的上下文管理器"""
        cursor = None
        try:
            # 確保連接可用
            self._ensure_connection()
            cursor = self.conn.cursor()
            yield cursor
        except Exception as e:
            self.logger.error(f"游標操作失敗: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute_with_retry(self, query, params=None, fetch=False):
        """執行SQL查詢，帶重試機制"""
        import time
        
        for attempt in range(self.max_retries):
            try:
                # 每次執行前都確保連接可用
                self._ensure_connection()
                
                with self.get_cursor() as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    
                    if fetch:
                        return cursor.fetchall()
                    return cursor.rowcount
                    
            except (pymysql.OperationalError, pymysql.InterfaceError, pymysql.err.MySQLError) as e:
                error_msg = str(e).lower()
                if "mysql server has gone away" in error_msg or "lost connection" in error_msg or "timed out" in error_msg:
                    self.logger.warning(f"資料庫連接問題 (嘗試 {attempt + 1}/{self.max_retries}): {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)  # 指數退避
                        self._reconnect()
                    else:
                        raise
                else:
                    # 非連接問題，直接拋出異常
                    self.logger.error(f"資料庫操作錯誤: {e}")
                    raise
            except Exception as e:
                self.logger.error(f"非連接錯誤: {e}")
                raise

    def _reconnect(self):
        """重新連接資料庫"""
        try:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass  # 忽略關閉時的錯誤
            self._connect()
            self.logger.info("資料庫重新連接成功")
        except Exception as e:
            self.logger.error(f"重新連接失敗: {e}")
            raise
    
    def batch_execute(self, query, data_list, batch_size=1000):
        """批次執行SQL語句，帶連接重試機制"""
        if not data_list:
            return
        
        total_records = len(data_list)
        processed_count = 0
        batch_count = 0
        failed_batches = []
        
        self.logger.info(f"準備批次執行 {total_records} 筆資料，批次大小: {batch_size}")
        
        # 使用重試機制執行整個批次操作
        for attempt in range(self.max_retries):
            try:
                # 確保連接可用
                self._ensure_connection()
                self.conn.autocommit(False)
                
                # 重置計數器（如果是重試）
                if attempt > 0:
                    processed_count = 0
                    batch_count = 0
                    failed_batches = []
                    self.logger.info(f"重試批次執行 (嘗試 {attempt + 1}/{self.max_retries})")
                
                for i in range(0, total_records, batch_size):
                    batch_data = data_list[i:i + batch_size]
                    batch_count += 1
                    current_batch_size = len(batch_data)
                    
                    try:
                        # 每個批次前都確保連接可用
                        self._ensure_connection()
                        
                        with self.get_cursor() as cursor:
                            cursor.executemany(query, batch_data)
                        
                        processed_count += current_batch_size
                        
                        # 每10個批次提交一次
                        if batch_count % 10 == 0:
                            self.conn.commit()
                            self.logger.info(f"已處理 {processed_count}/{total_records} 筆資料 (批次 {batch_count})")
                            
                    except Exception as batch_error:
                        error_msg = str(batch_error).lower()
                        if "mysql server has gone away" in error_msg or "lost connection" in error_msg or "timed out" in error_msg:
                            # 連接問題，重新拋出讓外層重試
                            raise batch_error
                        else:
                            # 其他錯誤，記錄但繼續
                            failed_batches.append(batch_count)
                            self.logger.error(f"❌ 批次 {batch_count} 失敗: {batch_error}")
                            continue
                
                # 最終提交
                self._ensure_connection()
                self.conn.commit()
                self.logger.info(f"批次執行完成: {processed_count} 筆資料，{batch_count} 個批次")
                
                if failed_batches:
                    self.logger.warning(f"⚠️  失敗的批次: {failed_batches}")
                
                # 成功完成，跳出重試循環
                break
                
            except Exception as e:
                error_msg = str(e).lower()
                if "mysql server has gone away" in error_msg or "lost connection" in error_msg or "timed out" in error_msg:
                    self.logger.warning(f"批次執行連接問題 (嘗試 {attempt + 1}/{self.max_retries}): {e}")
                    if attempt < self.max_retries - 1:
                        try:
                            self.conn.rollback()
                        except:
                            pass
                        time.sleep(2 ** attempt)  # 指數退避
                        self._reconnect()
                    else:
                        # 最後一次嘗試失敗
                        try:
                            self.conn.rollback()
                        except:
                            pass
                        self.logger.error(f"批次執行最終失敗: {e}")
                        raise
                else:
                    # 非連接問題
                    try:
                        self.conn.rollback()
                    except:
                        pass
                    self.logger.error(f"批次執行失敗: {e}")
                    raise
            finally:
                try:
                    self.conn.autocommit(True)
                except:
                    pass  # 忽略設定自動提交時的錯誤

    def commit(self):
        """手動提交事務"""
        try:
            self._ensure_connection()
            self.conn.commit()
            self.logger.info("事務已提交")
        except Exception as e:
            self.logger.error(f"提交失敗: {e}")
            raise
        
    def execute_query(self, query, params=None, fetch=False):
        """
        執行單一SQL查詢，不自動提交
        
        Args:
            query: SQL查詢語句
            params: 查詢參數
            fetch: 是否返回查詢結果
            
        Returns:
            如果 fetch=True，返回查詢結果；否則返回影響的行數
        """
        try:
            self._ensure_connection()
            
            with self.get_cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch:
                    return cursor.fetchall()
                return cursor.rowcount
                
        except Exception as e:
            self.logger.error(f"執行查詢失敗: {e}")
            raise
    

    
class AuthorFormatter:
    """作者格式化工具類"""
    
    @staticmethod
    def format_author_list(names, p_year=None, nomenclature_id=1):
        """
        格式化作者列表
        
        Args:
            names: 作者名稱列表
            p_year: 發布年份
            nomenclature_id: 命名規約ID (1: 動物, 2: 植物, 3: 細菌)
        """
        if not names:
            return ""
        
        # 動物命名規約
        if nomenclature_id == 1:
            if len(names) == 1:
                author_str = names[0]
            elif len(names) == 2:
                author_str = f'{names[0]} & {names[1]}'
            else:
                author_str = ', '.join(names[:-1]) + f' & {names[-1]}'
            
            if p_year and p_year not in [None, 0, '0', '']:
                author_str += f', {p_year}'
        
        # 植物命名規約
        elif nomenclature_id == 2:
            if len(names) == 1:
                author_str = names[0]
            elif len(names) == 2:
                author_str = f'{names[0]} & {names[1]}'
            else:
                author_str = ', '.join(names[:-1]) + f' & {names[-1]}'
        
        # 細菌命名規約
        elif nomenclature_id == 3:
            if len(names) == 1:
                author_str = names[0]
            elif len(names) == 2:
                author_str = f'{names[0]} & {names[1]}'
            else:
                author_str = f'{names[0]} et al.'
            
            if p_year and p_year not in [None, 0, '0', '']:
                author_str += f' {p_year}'
        
        else:
            author_str = ', '.join(names)
        
        return author_str
    
    @staticmethod
    def safe_json_loads(json_str):
        """安全的JSON解析"""
        try:
            return json.loads(json_str) if json_str else {}
        except (json.JSONDecodeError, TypeError):
            return {}

def setup_logging(level=logging.INFO):
    """設置日誌配置"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'system_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )


def safe_json_dumps(x):
    if x is None or (not isinstance(x, (dict, list)) and pd.isna(x)):
        return None
    try:
        return json.dumps(
            x,
            default=lambda o: o.item() if hasattr(o, 'item') else str(o)
        )
    except Exception as e:
        print(f"JSON 轉換失敗：{x}，錯誤：{e}")
        return None


def get_conn():
    conn = pymysql.connect(**db_settings)
    return conn

