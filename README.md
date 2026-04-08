# HKJC 賽馬全端數據分析系統 (HKJC Analytics System)

這是一個針對香港賽馬 (HKJC) 設計的每場賽事獨立計分排名系統。本系統專注於數據抓取、多維度計分以及即時排名輸出。

## 專案結構 (Phase 1)

```
hkjc_analytics/
├── data_scraper/          # 資料抓取模組 (歷史、排位、晨操、賠率)
├── database/              # 資料庫模型與連線管理
│   ├── models.py          # SQLAlchemy ORM 模型 (馬/騎/練/賽/計分因子)
│   └── connection.py      # 資料庫引擎與 Session 配置
├── scoring_engine/        # 核心計分引擎 (17 個獨立計分函數)
├── web_ui/                # Streamlit 前端介面
├── utils/                 # 工具模組 (日誌、設定檔、限速器)
├── backtest/              # 回測與權重優化模組
├── config/                # YAML 設定檔
├── logs/                  # 日誌檔案
├── data/                  # SQLite 資料庫儲存路徑
├── scripts/               # 維護腳本 (初始化資料庫等)
├── requirements.txt       # 專案依賴
└── .env                   # 環境變數 (資料庫連線字串等)
```

## 專案進度

- [x] **階段 1：資料庫 Schema 設計 + 初始化腳本**
- [x] **階段 2：數據抓取模組 (History + Real-time Entry Table + Morning Track + Odds)**
- [x] **階段 3：計分引擎核心 (`score_horse_in_race()` 函數，每條件獨立)**
- [x] **階段 4：每場排名計算 + Streamlit 介面**
- [ ] **階段 5：回測模組 + 權重優化工具**

## 第四階段功能說明 (Streamlit UI)

1. **互動式 Dashboard (web_ui/app.py)**：
   - **賽事選擇**：左側選單可依日期與場次切換數據。
   - **專業排名表**：顯示排名、馬號、馬名、總分、勝率及系統建議（如首選、價值等）。
   - **動態權重配置**：使用者可透過滑桿即時調整 17 個計分因子的權重，系統會立即重新計算排名。
   - **雷達圖分析**：視覺化展示前三名馬匹在各維度（如騎練合作、晨操表現、SpeedPRO 等）的戰力對比。

## 如何使用 (Updated)

1. **安裝依賴與 Playwright**：
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

2. **初始化資料庫**：
   ```bash
   python scripts/init_db.py
   ```

3. **執行抓取 (可選)**：
   ```bash
   python scripts/run_scraper.py
   ```

4. **啟動視覺化介面**：
   ```bash
   streamlit run web_ui/app.py
   ```
   啟動後，瀏覽器會自動打開 `http://localhost:8501`。

---
*開發者：資深 HKJC 全端數據工程師*
