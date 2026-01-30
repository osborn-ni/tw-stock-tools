import pandas as pd
from FinMind.data import DataLoader
from datetime import datetime

# 1. 設定參數
stock_id = '2330'
start_date = '2025-01-01'  # 抓取起始日
end_date = datetime.now().strftime('%Y-%m-%d') # 自動抓到今天

# 固定檔名
output_filename = "tw_stock_data_latest.csv"

print(f"🚀 開始抓取股票: {stock_id} (從 {start_date} 到 {end_date})")

# 2. 抓取資料
dl = DataLoader()
df = dl.taiwan_stock_daily(
    stock_id=stock_id,
    start_date=start_date,
    end_date=end_date
)

# 3. 檢查資料並存檔
if not df.empty:
    # index=False 代表不要存 Pandas 的 0, 1, 2 索引
    # encoding='utf-8-sig' 確保 Excel 打開中文不亂碼
    df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    print(f"✅ 成功！資料已更新至固定檔案: {output_filename}")
    print(f"📊 目前共有 {len(df)} 筆交易資料。")
else:
    print("⚠️ 抓取失敗，請檢查 FinMind 服務或網路連線。")
    
