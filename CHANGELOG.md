# 更新日誌（Changelog）

此文件記錄「可影響運作/排程/資料口徑」的重要更新，方便下次接手快速理解現況。

## 2026-05-11

- Worker 化後台批量操作：後台按鈕 enqueue job，由 `job-worker` 執行；新增 job 狀態/日誌與 queue 修復。
- 修復 worker claim：跳過 stale queue ids，避免 queued 永遠不動。
- 模式 3 Ready Gate：SpeedPRO（EA+SR）覆蓋未達門檻時不生成/不覆蓋快照；Final Snapshot cron 只在開跑前約 6 小時窗口且達標時 enqueue `rescore+snapshot`。
- SpeedPRO 時間窗：改為 T-60h → T-20h（以賽日 R1 `post_time_hk` 作 anchor），重試預設 90 分鐘。
- 新增 `races.post_time_hk`：排位爬蟲由 racecard 抽取每場開跑時間，供 per-day anchor 與後續即場數據抓取使用。
- 賽果抓取保護：若 localresults 回傳頁面日期不符，略過不寫入；並提供後台清除誤寫賽果/派彩/走位工具。
- 新增 01:00 賽前賠率快照：`cron_pre_odds_0100.py` 寫入 `odds_history.odds_type=PRE_0100`；統計/貼士的 `pre_race_latest` 支援 `PRE_*`。

## 2026-05-12

- 賽前賠率快照由固定 01:00 改為「賽日 R1 開跑前 24 小時」觸發：新增 `cron_pre_odds_24h.py`（`odds_history.odds_type=PRE_24H`，`race_pool_snapshots.snapshot_type=PRE_24H`）；監察面板顯示改為「賽前賠率（24H）」並兼容舊 `PRE_0100`。

## 2026-05-13

- 修復會員頁「未有賽果仍顯示舊賽果」：未到開跑時間時不顯示賽果/派彩區塊，避免錯寫資料滲出。
- 加固賽果抓取入庫：未到開跑時間、無有效完成時間、頁面日期無法確認（當日/未來）或路程不符時，略過不寫入，降低誤寫舊賽果風險。
- 專業排名表：跑法只顯示近6；加入各時段賠率欄（24H/30M/15M/10M/5M/即時）。
- 本場各組合 Top5：加入各時段 Top5 賠率顯示。
- 左側場次按鈕：改為只顯示當日「有排位資料（race_entries）」的場次，避免出現多餘場次按鈕。
