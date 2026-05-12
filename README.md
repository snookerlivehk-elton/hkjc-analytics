# HKJC 賽馬全端數據分析系統 (HKJC Analytics System)

接手流程：
1) 先讀 OPERATIONS.md（Railway services / cron / 排障）與 CHANGELOG.md（最近改動與口徑）
2) 再讀本 README 了解資料表/流程/入口
3) 任何修改先用後台「監察面板→Job 狀態」或對應 service logs 驗收

這是一個針對香港賽馬 (HKJC) 設計的每場賽事獨立計分排名系統。本系統專注於數據抓取、多維度計分以及即時排名輸出。

## 核心概念

- **獨立條件 (factor)**：單一計分條件（例如檔位偏差、近期狀態等），各自生成每場 Top5 以便做「條件本身」的命中統計。
- **會員組合 (preset)**：會員儲存的多條件加權組合，生成每場 Top5 以便做「組合表現」統計。
- **Top5 快照（PredictionTop5）**：把「某賽日、某場次、某條件/組合」的 Top5 預測落庫，作為之後命中統計/對外輸出的唯一來源。
- **命中結算**：賽果入庫後，會用 Top5 快照 + 賽果 Top5 計算命中（獨贏/位置/正Q/PQ/三重/四連），並回寫到快照 meta。

## 專案結構

```
hkjc_analytics/
├── data_scraper/          # 資料抓取模組 (歷史、排位、晨操、賠率)
├── database/              # 資料庫模型與連線管理
│   ├── models.py          # SQLAlchemy ORM 模型 (馬/騎/練/賽/計分因子)
│   └── connection.py      # 資料庫引擎與 Session 配置
├── scoring_engine/        # 核心計分引擎 (多個獨立計分函數)
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

## 主要資料表（摘要）

- `races`：賽日/場次主檔（含 `post_time_hk` 作每場開跑時間基準）
- `race_entries`：排位/出賽馬匹（horse/jockey/trainer/draw/rating…）
- `scoring_factors`：計分因子明細（可作診斷/透明化）
- `prediction_top5`：Top5 快照（統計與貼士的核心來源）
- `odds_history`：賠率快照/歷史（`odds_type` 支援 `PRE_*`、`Live` 等）
- `system_configs`：系統快照/狀態/queue（例如 SpeedPRO、job queue、fixture、runpos…）
- `search_documents`：全站搜尋索引（統一 AND 搜索入口）

## 服務架構（Railway）

- Web（Streamlit）：`web_ui/app.py`
- Worker（常駐）：`python scripts/job_worker.py`
  - 後台按鈕會 enqueue job（存於 `system_configs`），由 worker claim 後執行
- Cron（定時）：跑 `scripts/cron_*.py`
  - 例：fixture/draw 檢查、SpeedPRO 追齊、Final Snapshot、賽果補抓、01:00 賽前賠率快照

## 模式 3（資料未齊不生成快照）

當 SpeedPRO（EA/SR）覆蓋未達門檻時：
- `daily_update_pipeline` 的 `snapshot` step 會寫入 `step_skipped ... speedpro_not_ready`，並不生成/覆蓋快照
- `Final Snapshot` cron 亦只會在「開跑前約 6 小時窗口」且覆蓋達標時 enqueue `rescore+snapshot`

## Streamlit UI（摘要）

1. **互動式 Dashboard (web_ui/app.py)**：
   - **賽事選擇**：左側選單可依日期與場次切換數據。
   - **場次按鈕顯示規則**：只會顯示「當日有排位資料（race_entries）」的場次，避免資料不完整時出現多餘場次按鈕。
   - **專業排名表**：顯示排名、馬號、馬名、跑法（近6）、總分、勝率及系統建議，並提供各時段賠率欄（24H/30M/15M/10M/5M/即時；格式=獨贏/位置）。
   - **動態權重配置**：使用者可透過滑桿即時調整計分因子的權重，系統會立即重新計算排名。
   - **會員組合**：每位會員最多可儲存 3 組權重配置，並可累積命中率統計。
2. **數據管理後台 (web_ui/pages/1_數據管理.py)**：
   - 抓取排位/即時數據、回填馬匹往績、重算所選賽日、生成 Top5 快照。
   - 抓取賽果/派彩後會自動結算：會員組合命中率 + Top5 快照命中（回寫 hits/actual_top5）。
3. **命中統計總覽 (web_ui/pages/3_命中統計.py)**：
   - 查看「獨立條件」與「會員組合」的命中率彙總（可按日期範圍）。
   - 反向統計（淘汰 BottomN%）：支援按地點（沙田/跑馬地）、草/泥、距離、班次等分桶，並可用作篩選條件輸出個別表現。

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

## 運維與更新日誌

- 運維/排程/常見問題：見 [OPERATIONS.md](file:///c:/Users/User/.trae/hkjc_analytics/OPERATIONS.md)
- 更新日誌（便於下次接手/回溯）：見 [CHANGELOG.md](file:///c:/Users/User/.trae/hkjc_analytics/CHANGELOG.md)

## Odds 分段（供 UI 顯示）

- `odds_history.odds_type`：
  - `PRE_24H`：賽日第 1 場開跑前 24 小時快照
  - `PRE_30M / PRE_15M / PRE_10M / PRE_5M`：臨場里程碑快照
  - `Live`：即時賠率（run_scraper/Live）

## Top5 快照與命中結算（手動）

以 `2026/04/08` 為例：

1. 後台依序完成：抓排位 → 回填往績 → 重算 → 生成 Top5 快照  
   - 推薦直接用「⚡ 一鍵完整更新」按鈕（會順序等待每步完成）。
2. 抓取賽果/派彩後，系統會自動結算 Top5 快照命中（並寫回 hits/actual_top5）。

## 對外 API（FastAPI）

- 啟動（本機）：
  ```bash
  uvicorn api_server:app --host 0.0.0.0 --port 8000
  ```
- 主要輸出（分組 Top5；factor + preset 都在同一格式內）：
  - `GET /api/hkjc/base?date=YYYY-MM-DD`
  - 兼容：`GET /api/v1/like?date=YYYY-MM-DD`
- 健康檢查：
  - `GET /health`

---
*開發者：資深 HKJC 全端數據工程師*
