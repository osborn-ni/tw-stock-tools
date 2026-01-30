import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime

# 設定參數
stock_id = '2330'
start_date = '2025-01-01'
# 自動取得今天日期作為 end_date
end_date = datetime.now().strftime('%Y-%m-%d')

print(f"正在抓取 {stock_id} 從 {start_date} 到 {end_date} 的資料...")

# 抓取資料
dl = DataLoader()
df = dl.taiwan_stock_daily(
    stock_id=stock_id,
    start_date=start_date,
    end_date=end_date
)

if not df.empty:
    # 產生檔名：2330_2025-01-01_2026-01-30.csv
    file_name = f"{stock_id}_{start_date}_{end_date}.csv"
    df.to_csv(file_name, index=False, encoding='utf-8-sig') # utf-8-sig 讓 Excel 打開不亂碼
    print(f"檔案已成功產生: {file_name}")
else:
    print("抓取失敗，沒有資料。")
