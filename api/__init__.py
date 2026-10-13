import pandas as pd

# 提前採用 pandas 3.0 行為：fillna / replace / where 等不再自動降轉 object 欄位型別，
# 消除 "Downcasting object dtype arrays ... is deprecated" FutureWarning（pandas 3.0 起此為預設，選項已移除）
if int(pd.__version__.split('.')[0]) < 3:
    pd.set_option('future.no_silent_downcasting', True)
