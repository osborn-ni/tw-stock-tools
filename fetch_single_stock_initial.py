"""
專案名稱：台灣股市自動化監控系統
程式名稱：fetch_single_stock_clean.py

【程式目的與核心邏輯說明】：
1. 抓取 (Extract)：
   - 同時向 FinMind 請求 6 種數據：股價、當沖、融資融券、外資持股、借券、三大法人買賣。
   
2. 清洗與轉換 (Transform) - 這是程式最聰明的地方：
   - 【數據去噪】：自動刪除備註欄(note)中的 HTML 標籤或亂碼，保持資料整潔。
   - 【借券加總】：借券 API 回傳的是明細，程式會自動「按日期」加總所有張數。
   - 【三大法人樞紐轉換】：這是關鍵！API 原始回傳是「一列一個法人」。
     本程式會：
     A. 自動算出「買賣超 (net)」 = 買進股數 - 賣出股數。
     B. 執行「樞紐分析 (Pivot)」，把法人的名字從「直的」變「橫的」。
     例如：原本三行資料(外資、投信、自營商)，會合併成一行，並新增「外資_net」、「投信_net」欄位。

3. 合併與存檔 (Load)：
   - 以「每日股價」為基底，把所有籌碼資訊「橫向拼接」。
   - 如果當天沒開盤(成交量為0)，會自動剔除，確保資料分析時不會有空洞。
   - 最終會產出各別的清洗檔以及一個「大總表 (all_data)」。
"""

import pandas as pd
from FinMind.data import DataLoader
import os

def fetch_and_process(stock_id, start_date, end_date):
    dl = DataLoader()
    
    tasks = [
        (dl.taiwan_stock_daily, "daily", "股價資訊"),
        (dl.taiwan_stock_day_trading, "day_trading", "當沖交易"),
        (dl.taiwan_stock_margin_purchase_short_sale, "margin", "融資融券"),
        (dl.taiwan_stock_shareholding, "shareholding", "外資持股"),
        (dl.taiwan_stock_securities_lending, "lending", "借券成交"),
        (dl.taiwan_stock_institutional_investors, "inst_investors", "三大法人")
    ]

    clean_dfs = {}

    print(f"--- 🚀 啟動「進階清洗並併」任務: {stock_id} ---")

    for api_func, suffix, label in tasks:
        try:
            df = api_func(stock_id=stock_id, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty:
                print(f"❌ {label:10}: 無數據")
                continue

            df['date'] = df['date'].astype(str)
            df['stock_id'] = df['stock_id'].astype(str)

            # --- 特定表清洗邏輯 ---
            
            # 1. 外資持股：移除 note 
            if suffix == "shareholding" and 'note' in df.columns:
                df = df.drop(columns=['note'])

            # 2. 借券成交：按日加總
            if suffix == "lending":
                q_col = next((c for c in ['volume', 'Quantity', 'quantity'] if c in df.columns), None)
                if q_col:
                    df = df.groupby(['date', 'stock_id'], as_index=False)[q_col].sum()
                    df.rename(columns={q_col: 'lending_total_vol'}, inplace=True)

            # 3. 三大法人：計算買賣超並進行樞紐轉換
            if suffix == "inst_investors":
                # A. 計算買賣超 (Net)
                df['net'] = df['buy'] - df['sell']
                
                # B. 樞紐分析：將「外資/投信/自營商」轉為橫向欄位
                # 這樣合併時才不會產生多餘的列
                df = df.pivot_table(
                    index=['date', 'stock_id'], 
                    columns='name', 
                    values='net'
                ).reset_index()
                
                # C. 重新命名欄位，加上後綴方便識別
                df.columns = [f"{c}_net" if c not in ['date', 'stock_id'] else c for c in df.columns]

            # 儲存清洗後的檔案
            clean_filename = f"{stock_id}_{suffix}_clean.csv"
            df.to_csv(clean_filename, index=False, encoding='utf-8-sig')
            print(f"💾 {label:10}: 已存檔")
            
            clean_dfs[suffix] = df
            
        except Exception as e:
            print(f"⚠️ {label:10}: 錯誤 -> {e}")

    # --- 合併總表 ---
    if "daily" in clean_dfs:
        print(f"\n--- 🔗 正在合併精煉總表 ---")
        all_data = clean_dfs["daily"]
        
        for key in [k for k in clean_dfs.keys() if k != "daily"]:
            all_data = pd.merge(all_data, clean_dfs[key], on=['date', 'stock_id'], how='left')

        if 'Trading_Volume' in all_data.columns:
            all_data = all_data[all_data['Trading_Volume'] > 0]
        
        final_output = f"{stock_id}_all_data.csv"
        all_data.to_csv(final_output, index=False, encoding='utf-8-sig')
        print(f"🎉 任務完成！最終筆數: {len(all_data)}")
    else:
        print("\n❌ 錯誤：缺少 daily 資料")

if __name__ == "__main__":
    fetch_and_process("2330", "2010-01-01", "2025-12-31")