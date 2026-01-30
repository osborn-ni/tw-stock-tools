import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime
import os

# 1. 設定要抓取的股票清單 (你想加幾隻就加幾隻)
stock_list = ['2330', '2317', '2308', '2454', '2881', '2882', '3711', '2382', '2412', '2891']
start_date = '2025-01-01'
end_date = datetime.now().strftime('%Y-%m-%d')

# 建立存放資料的資料夾 (如果不存在的話)
folder_name = "data"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

dl = DataLoader()

print(f"🚀 開始批次抓取任務：{stock_list}")

# 2. 使用迴圈抓取每一隻股票
for stock_id in stock_list:
    print(f"正在抓取 {stock_id}...")
    
    try:
        df = dl.taiwan_stock_daily(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date
        )

        if not df.empty:
            # 存放到 data 資料夾下，檔名範例：data/tw_stock_data_2330_latest.csv
            file_path = os.path.join(folder_name, f"tw_stock_data_{stock_id}_latest.csv")
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"✅ {stock_id} 存檔成功！")
        else:
            print(f"⚠️ {stock_id} 沒有資料。")
            
    except Exception as e:
        print(f"❌ 抓取 {stock_id} 時發生錯誤: {e}")

print("\n✨ 所有任務執行完畢！")

