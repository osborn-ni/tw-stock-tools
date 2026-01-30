import pandas as pd
import numpy as np
from FinMind.data import DataLoader

print(f"Pandas 版本: {pd.__version__}")
print(f"Numpy 版本: {np.__version__}")

# 試抓一筆資料看看（以台積電為例）
dl = DataLoader()
df = dl.taiwan_stock_daily(
    stock_id='2330',
    start_date='2025-01-01'
)

if not df.empty:
    print("成功抓取 FinMind 資料！")
    print(df.head())
else:
    print("資料抓取失敗，請檢查網路或 FinMind 狀態。")