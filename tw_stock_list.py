"""
專案名稱：台灣股市自動化監控系統 (Taiwan Stock Automation)
程式名稱：tw_stock_list.py
程式版本：V1.2 (修正 StringIO 警告與優化輸出)
作者：Osborn (與 Gemini 協作)

【程式功能說明】：
1. 自動對接證交所 (TWSE) 與櫃買中心 (TPEx) 官方數據源。
2. 採用國際標準 CFI Code (ESVUFR) 精確過濾「普通股」。
3. 產出 4 份標準化檔案：
   - twse_stock_list.csv (上市清單)
   - tpex_stock_list.csv (上櫃清單)
   - tw_stock_list.csv   (全市場總表，依代碼排序)
   - tw_stock_list_summary.csv (各產業上市/上櫃公司數量統計表)

【技術筆記】：
- 使用 io.StringIO 處理 HTML 字串，避開 Pandas 未來版本警告。
- 使用 utf-8-sig 編碼儲存 CSV，確保 Excel 跨平台讀取中文不亂碼。
"""

import os
import requests
import pandas as pd
import urllib3
import io

# 關閉公司環境常見的 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_stock_list_by_market(market_name, url):
    """抓取單一市場清單並清洗資料"""
    print(f"正在從 {market_name} 獲取最新清單...")
    try:
        # SSL 容錯處理 (適應公司電腦環境)
        try:
            res = requests.get(url, timeout=15)
        except requests.exceptions.SSLError:
            res = requests.get(url, verify=False, timeout=15)
        
        res.encoding = 'ms950' # 證交所網頁使用 Big5 編碼
        
        # 使用 io.StringIO 包裹文字流，修正 FutureWarning
        dfs = pd.read_html(io.StringIO(res.text))
        df = dfs[0]
        
        # 清洗表格：設定第一列為標題
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        
        # 欄位重新命名，確保後續處理一致性
        df = df.rename(columns={
            '有價證券代號及名稱': 'sid_name',
            'CFICode': 'cfi_code',
            '產業別': 'industry'
        })
        
        # 拆分「代碼」與「名稱」 (例如: "2330　台積電" -> "2330", "台積電")
        # n=1 代表只拆分第一個遇到的全形空格
        df[['stock_id', 'stock_name']] = df['sid_name'].str.split('　', n=1, expand=True)
        
        # 核心過濾：代碼必須為 4 位 且 CFI Code 為普通股 (ESVUFR)
        df = df[(df['stock_id'].str.len() == 4) & (df['cfi_code'] == 'ESVUFR')]
        
        # 標記市場別並選取最終需要的欄位
        df['market_type'] = market_name
        clean_df = df[['stock_id', 'stock_name', 'market_type', 'industry', 'cfi_code']].copy()
        
        return clean_df
            
    except Exception as e:
        print(f"處理 {market_name} 時發生錯誤: {e}")
        return pd.DataFrame()

def main():
    # 官方數據源網址
    targets = {
        "twse": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", # 上市
        "tpex": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"  # 上櫃
    }
    
    # 1. 執行抓取
    twse_df = get_stock_list_by_market("上市", targets["twse"])
    tpex_df = get_stock_list_by_market("上櫃", targets["tpex"])
    
    # 2. 儲存個別市場清單
    if not twse_df.empty:
        twse_df.to_csv("twse_stock_list.csv", index=False, encoding='utf-8-sig')
        print(f"✅ 已存檔：twse_stock_list.csv ({len(twse_df)} 筆)")

    if not tpex_df.empty:
        tpex_df.to_csv("tpex_stock_list.csv", index=False, encoding='utf-8-sig')
        print(f"✅ 已存檔：tpex_stock_list.csv ({len(tpex_df)} 筆)")
    
    # 3. 合併、排序與產出統計表
    if not twse_df.empty or not tpex_df.empty:
        # 合併兩者
        combined_df = pd.concat([twse_df, tpex_df], ignore_index=True)
        
        # 依照 stock_id 全域排序
        combined_df = combined_df.sort_values(by='stock_id').reset_index(drop=True)
        combined_df.to_csv("tw_stock_list.csv", index=False, encoding='utf-8-sig')
        print(f"✅ 已存檔：tw_stock_list.csv (共 {len(combined_df)} 筆)")

        # 4. 產業統計分析 (Summary)
        # 統計各產業在上市/上櫃的家數
        summary_df = combined_df.groupby(['industry', 'market_type']).size().unstack(fill_value=0)
        
        # 計算總計並降冪排序 (從最多的產業排到最少)
        summary_df['總計'] = summary_df.sum(axis=1)
        summary_df = summary_df.sort_values(by='總計', ascending=False)
        
        # 儲存統計 CSV
        summary_df.to_csv("tw_stock_list_summary.csv", encoding='utf-8-sig')
        print(f"✅ 已存檔：tw_stock_list_summary.csv")
        
        print("-" * 30)
        print("🚀 [任務完成] 4 份數據地圖已準備就緒！")
        print("-" * 30)
    else:
        print("❌ 失敗：未能從官網獲取數據，請檢查網路連線。")

if __name__ == "__main__":
    main()