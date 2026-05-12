# Canonicalization（Staging → Canonical）落地規格（v1）

目的：把外部數據來源先落「原始快照（Staging）」；再由「對齊器（Canonicalizer）」產生一致、可驗證的 canonical tables（races / race_entries / odds_history / results / weather…），從根源避免假場次、重覆 entries、來源互相覆蓋導致 UI/統計出錯。

本規格以「不影響日常數據收集及系統運作」為首要原則：任何改動先用 shadow mode 跑；可隨時回滾到現況。

## 0) 已確認決策（2026-05-13）

- A. 假場次處理：標 `races.is_valid=false`（不硬刪）
- B. 唯一鍵：願意做 DB migration 加 `race_entries(race_id, horse_no)` unique
- C. Raw snapshots：只保最新（省，不保存歷史 events）

## 1) 現況痛點（為何要做）

- racecard 偶發 redirect/回傳重覆內容，導致 DB 誤建 R10–R12 或重覆 races/entries
- entries 缺少強制唯一鍵，容易出現同一場 duplicate entry（後續 odds/results 對唔上）
- UI 被迫用各種 workaround（以 race_entries count、signature 去重、甚至參考 HKJC RaceNo），仍容易踩中「DB 無資料」或「假場次」情況
- 未來加入更多來源（WP odds、Windtracker、SpeedPRO、賽後報告…）會令一致性問題倍增

## 2) 核心原則（必守）

- **雙軌期（Zero-downtime）**：舊 cron/job 照舊寫 canonical；新流程先 shadow 寫 raw + runs，唔改 UI/業務讀路徑
- **Idempotent**：同一日同一來源重跑，結果一致；用 hash/exists guard 去重
- **可回滾**：任何時候可關 `ENABLE_CANONICALIZER` 或切回 UI 舊讀法
- **品質閘（DQ Gate）**：寧願 skip/標 invalid，都唔把疑似錯數據寫入 canonical

## 3) 分層設計

### 3.1 Layer 1：Staging（RawSnapshot：只保最新）

沿用 `raw_snapshots`（已有 unique constraint：`(source, entity_type, entity_key)`）。

**entity_key 規範（統一）**
- `entity_type="race"`：`YYYY/MM/DD:VENUE:RACE_NO`
  - 例：`2026/05/13:ST:9`
- `entity_type="race_day"`：`YYYY/MM/DD:VENUE`
  - 例：`2026/05/13:ST`

**source 枚舉（建議）**
- `HKJC_RACECARD`
- `HKJC_LOCALRESULTS`
- `HKJC_WP_ODDS`
- `HKJC_WINDTRACKER`

**meta 最低要求**
- `schema`：例如 `racecard:v2`
- `fetched_at`：UTC ISO
- `url`
- `hash`：payload canonical json hash
- `parse_ok`：bool
- `errors`：list（可選）

寫入策略：
- 只保最新一份 payload（同 key 覆蓋更新）
- 當 hash 未變，canonicalizer 可 skip（減少負載）

### 3.2 Layer 2：Canonical（權威層）

仍以現有業務表為主（`races/race_entries/...`），只新增「有效性」與「可觀測性」。

**races（新增欄位）**
- `is_valid BOOLEAN NOT NULL DEFAULT TRUE`
- `invalid_reason VARCHAR(120) NULL`
- `canonical_updated_at TIMESTAMP NULL`（可選）

**race_entries（新增唯一鍵）**
- unique：`(race_id, horse_no)`（建議做 partial：`horse_no > 0`）

**canonical_runs（新增表）**
用途：追蹤每個 domain 對齊狀態、輸入 hash、輸出統計、錯誤摘要；供運維/儀表板使用。

欄位建議：
- `id SERIAL PRIMARY KEY`
- `date_day DATE NOT NULL`
- `venue VARCHAR(10) NOT NULL`
- `domain VARCHAR(20) NOT NULL`（racecard/odds/results/weather）
- `status VARCHAR(10) NOT NULL`（ok/skipped/failed）
- `input_hashes JSON`
- `stats JSON`
- `error TEXT NULL`
- `started_at TIMESTAMP`
- `finished_at TIMESTAMP`

## 4) Canonicalizer（對齊器）設計

### 4.1 腳本與調度

新增 scripts（建議）：
- `scripts/canonicalize_racecard_day.py`
- `scripts/canonicalize_results_day.py`
- `scripts/canonicalize_odds_day.py`
- `scripts/canonicalize_weather_day.py`
- `scripts/canonicalize_all_day.py`（順序跑）

環境變數（feature flags）：
- `ENABLE_CANONICALIZER=0/1`（預設 0）
- `CANONICALIZER_SHADOW_MODE=1`（預設 1）
  - 1：只做檢查與寫 `canonical_runs`；可選擇只標 `races.is_valid/invalid_reason`
  - 0：允許修復性寫入（例如清理 duplicate entries、覆蓋 canonical 欄位）
- `CANONICALIZER_MIN_ENTRIES=6`（預設 6）

調度（不影響日常）：
- 新增一個 cron（例如每 10 分鐘），只要 `ENABLE_CANONICALIZER=1` 才跑
- 初期只跑 `racecard` domain，其他 domain 先保持 shadow report

### 4.2 Domain：Racecard canonicalization（v1）

**輸入**
- `raw_snapshots`：`source=HKJC_RACECARD`、`entity_type=race`、指定 `date_day + venue`

**輸出（shadow mode 建議先做）**
- 寫 `canonical_runs`
- 依規則標記假場次：`races.is_valid=false`（不刪）

**核心算法（canon:v1）**
1) 對每場建立 `entry_signature`
   - 建議用 `(horse_no, horse_code)` 排序後串接
   - 若 horse_code 缺失，fallback 用 `(horse_no)`（但會降低去重能力）
2) 偵測重覆場次（假場次）
   - 同日同 venue 若 signature 重覆：
     - 保留 race_no 最細者為 valid
     - 其餘設 `is_valid=false`、`invalid_reason="duplicate_signature_of_R{min_race_no}"`
3) 偵測明顯無效場
   - entries 數量 < `CANONICALIZER_MIN_ENTRIES`：標 invalid（reason=`insufficient_entries`）
4) 輸出 stats
   - valid_races / invalid_races / duplicates_detected / races_missing_raw

### 4.3 Domain：Results canonicalization（v1）

**輸入**
- `raw_snapshots source=HKJC_LOCALRESULTS entity_type=race`

**品質閘（必守）**
- meta 的 race_date/race_no/venue/distance 必須同 canonical race 一致
- 至少一匹馬 `finish_time` 可 parse 成秒，否則只保留 raw，不寫 `RaceResult`

**輸出（初期 shadow）**
- 寫 `canonical_runs` 報告：會寫/會 skip 的原因、coverage

### 4.4 Domain：Odds canonicalization（v1）

**輸入**
- `raw_snapshots source=HKJC_WP_ODDS entity_type=race`
- meta 帶入 `odds_type`（`PRE_24H/PRE_30M/.../Live`）

**輸出**
- `OddsHistory`：以 `(entry_id, odds_type, captured_at_bucket_5m)` 去重
- `RacePoolSnapshot`：同理

**品質閘**
- 覆蓋不足可照寫，但 meta 標 `partial=true`，避免 UI 誤判

### 4.5 Domain：Weather canonicalization（v1）

**輸入**
- `raw_snapshots source=HKJC_WINDTRACKER entity_type=race_day`（5 分鐘 bucket）

**輸出**
- `RaceDayWeather`（最新 bucket 或按需要保留 history）

## 5) DB Migration（安全流程）

### 5.1 新增 races 欄位（安全）
- `ALTER TABLE races ADD COLUMN IF NOT EXISTS is_valid BOOLEAN NOT NULL DEFAULT TRUE;`
- `ALTER TABLE races ADD COLUMN IF NOT EXISTS invalid_reason VARCHAR(120);`
- `ALTER TABLE races ADD COLUMN IF NOT EXISTS canonical_updated_at TIMESTAMP;`

### 5.2 加 unique 之前的「清理 duplicate entries」（必做）

目標：確保同一 `race_id` 內不會有重覆 `horse_no`。

保留策略（建議）：
- 優先保留資料較齊全者（horse_id/jockey_id/trainer_id/draw/rating/weight）
- 同分時保留 `id` 最細

清理後再加 unique index。

### 5.3 加 unique index（建議 concurrently）
- `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_race_entries_race_id_horse_no ON race_entries(race_id, horse_no) WHERE horse_no IS NOT NULL AND horse_no > 0;`

### 5.4 建 canonical_runs 表
- `CREATE TABLE IF NOT EXISTS canonical_runs (...)`

## 6) 漸進切換（Zero downtime）

Phase 0（只寫 raw + runs）：
- scrapers/cron 照舊
- canonicalizer shadow mode：只寫 `canonical_runs`（可選標 invalid）

Phase 1（只改 UI 顯示邏輯）：
- UI 場次按鈕：只顯示 `races.is_valid=true` 且有 entries 的場次
- 其他功能不改

Phase 2（逐 domain cutover）：
- results/odds/weather 逐一改成：先寫 raw，再由 canonicalizer 寫 canonical

回滾：
- 關 `ENABLE_CANONICALIZER`
- UI 切回舊查詢（不讀 is_valid）

## 7) 運維與驗收

最低驗收：
- 指定賽日：UI 場次按鈕數 == HKJC 當日場數（或至少不會出現多餘假場次）
- 所有顯示場次都有 entries，按鈕一定可點
- canonical_runs 能清楚列出 invalid 原因與重覆 signature 統計

建議監控：
- 今日各 domain 最後 `canonical_runs` status、finished_at
- invalid_races 數量、duplicate_signature 數量

## 8) 待確認事項（明天落地前）

- 是否允許 shadow mode 先「只標 is_valid/invalid_reason」（建議允許；不改任何核心欄位，且 UI 可選擇是否使用）
- signature 用 horse_code 還是 horse_id 作主（建議先 horse_code，避免 horse_id 未齊）

