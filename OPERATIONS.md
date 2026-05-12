# 運維手冊（Railway）

本文件用於：避免下次接手時「唔知邊個 service 做緊乜」、快速排障（queued 不動/資料未齊/誤寫賽果等）。

## 1) Services（應該存在）

- **Web（Streamlit）**：`streamlit run web_ui/app.py`
- **Worker（常駐）**：`python scripts/job_worker.py`
  - 必須：Cron Schedule = 無；Serverless = 關；Replicas = 1
  - 作用：claim job queue，執行後台排程（抓排位/回填/重算/快照/賽果/SpeedPRO job）
- **Cron（定時）**：跑 `scripts/cron_*.py`（跑完 teardown 省錢）

## 2) Job queue（點解後台按鈕唔郁）

- 後台按鈕通常只會 enqueue job
- job 會存在 `system_configs`：
  - `job_queue_v1`：queued list
  - `job:{uuid}`：job 詳情
- 如果「全部 queued 不動」：
  - 先睇 `job-worker` Logs 有無 `claimed job_id=...`
  - 確認 `job-worker` 同 Web 用同一個 `DATABASE_URL`
  - 後台「監察面板」可用「修復 queue」

## 3) SpeedPRO / EA / SR（追齊策略）

- service：`secure-speedpro`（cron）
- 建議 schedule：每小時（外層）
- 腳本：`python scripts/cron_speedpro_fetch.py`
- 內建時間窗：T-60h → T-20h（以當日 R1 `post_time_hk` 作 anchor）
- 重試間距：`SPEEDPRO_RETRY_MINUTES`（建議 60–120 分）

## 4) 模式 3（資料未齊不生成快照）

- `daily_update_pipeline` 的 `snapshot` step 會先檢查 SpeedPRO 覆蓋率（EA+SR）
  - 未達門檻：skip，不覆蓋快照（Job log 會列出每場 coverage）
- `Final Snapshot`（cron）：
  - 每小時跑一次
  - 只會在「開跑前約 6 小時窗口」且 SpeedPRO 覆蓋達標時 enqueue `rescore+snapshot`

## 5) 賽果抓取（防止未開賽誤寫）

- `scripts/fetch_race_results.py` 已加入「頁面日期不符就略過」保護
- 如曾誤寫：後台「維護工具（高風險）」有「清除該日賽果/派彩/走位」

## 6) 賽前 24H 賠率快照（PRE_24H）

- service：`pre-odds-24h`（cron）
- Start command：`python scripts/cron_pre_odds_24h.py`（舊 `cron_pre_odds_0100.py` 仍可用但已改為同一邏輯）
- 建議 schedule（UTC）：`*/5 * * * *`（每 5 分鐘巡邏一次，腳本只會在「R1 開跑前 24 小時」窗口內實際落庫）
- 腳本窗口：以賽日 R1 `post_time_hk` 作 anchor，觸發時間＝R1 開跑前 24 小時（可用 `PRE_ODDS_24H_WINDOW_MINUTES` 調整容錯窗口）
- 落庫：
  - `odds_history.odds_type = PRE_24H`
  - `race_pool_snapshots.snapshot_type = PRE_24H`
  - 系統會以 `PRE_*` 作「賽前賠率」來源（`pre_race_latest`）

## 7) 檢查「每場開跑時間」是否入庫

- `races.post_time_hk` 由排位爬蟲（racecard）抽取
- 若為空：重新跑一次「抓排位」即可補回
