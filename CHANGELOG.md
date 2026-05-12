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
