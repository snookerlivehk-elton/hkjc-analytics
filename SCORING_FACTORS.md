# HKJC Analytics - 獨立計分條件與評分邏輯清單

本文件詳細列出系統中計分條件的所需數據來源、評分邏輯，以及目前的開發狀態。系統透過將各項原始數據標準化為 0-10 分，並依據權重計算總分，以產出每場賽事的專業排名。

> 提示：新增/更新計分邏輯後，如要套用到既有賽事資料，需在「數據管理」頁面執行 Batch Rescore 才會更新舊結果。

---

## ✅ 已開發完成（已上線）

| 條件（UI 名稱） | 內部代號 | 所需數據來源 | 評分邏輯摘要 |
| :--- | :--- | :--- | :--- |
| **騎師＋練馬師合作 (綜合)** | `jockey_trainer_bond` | 歷史往績 (`HorseHistory`: jockey_name / trainer_name / horse_id / rank / race_date) | 同時計算「全庫合作」與「本駒合作」的勝/上名率，依可調權重與可調樣本範圍加權合併；同場再做百分位標準化成 0–10 分。 |
| **檔位偏差 (官方 Draw Statistics)** | `draw_stats` | HKJC 當日檔位統計（抓取後暫存於 `SystemConfig`）、排位表檔位 (`RaceEntry.draw`) | 以當日該場次各檔位的勝率/上名率作基礎；以「最高勝率/最高上名率」為基準，計算相對強度（勝率 70% + 上名率 30%）；同場再標準化成 0–10 分。 |
| **負磅／評分表現** | `weight_rating_perf` | 排位表 (`RaceEntry.rating` / `RaceEntry.actual_weight`)、歷史往績 (`HorseHistory.distance/rank/rating/weight`) | 找出同路程歷史勝仗中「最高可贏評分」（取 `rank=1` 且 `rating` 最大）；目前評分低於該值則加分，並輔以同程勝仗負磅差（同程勝仗負磅 - 今日負磅）作小幅加分；同場再標準化成 0–10 分。 |
| **班次表現** | `class_performance` | 本場班次 (`Race.race_class`)、歷史往績 (`HorseHistory.race_class`) | 現階段專注「降班訊號」：只在 3→4 / 4→5 時加分（透過解析「第X班/Class X」字串）；同場再標準化成 0–10 分。 |
| **近期狀態 (Last 6 Runs)** | `recent_form` | 歷史往績 (`HorseHistory.rank` / `race_date`)、可調權重 (`SystemConfig.recent_form_weights`) | 取最近 6 仗有效名次，使用時間衰減權重計算加權平均名次；以「負的加權平均名次」作為 raw（名次越好 raw 越高），同場再標準化成 0–10 分。 |

---

## 🟡 資料已具備/部分具備（待實作真實邏輯）

| 條件（UI 名稱） | 內部代號 | 現況 | 建議方向 |
| :--- | :--- | :--- | :--- |
| **場地＋路程專長** | `venue_dist_specialty` | 目前為 placeholder | 可用 `HorseHistory.venue + distance` 聚合統計同場地同路程上名率/勝率，並加入樣本不足降權。 |
| **場地狀況專長 (Going)** | `going_specialty` | 目前為 placeholder；且賽前 Going 可能未公布 | Going 已可能存入 `Race.going`，若缺失可先用草/泥作 fallback；可用歷史在相同 Going（或相同跑道）表現聚合。 |
| **初出／長休後表現** | `debut_long_rest` | 目前為 placeholder | 僅用 `HorseHistory.race_date` 即可計算休息天數（例如 >90 天）；再比較該馬歷史「長休後復出」表現。 |

---

## 🚀 未來系統升級計畫

1. **透明化原始數據 (Raw Data Display)**
   - 在資料庫 `ScoringFactor` 表中新增 `raw_data_display` 欄位。
   - 在 UI 的「獨立條件分析」分頁中，除了顯示標準化後的 0-10 分，還將直接顯示文字化的原始數據（如：「同程勝率 45%」、「降班出戰」），讓計分邏輯完全透明化。
2. **實作第一類條件的 Python 演算法**
   - 逐步將 `scoring_engine/factors.py` 中的隨機數佔位符，替換為上述第一類 11 個條件的真實 SQL 查詢與數學運算。
3. **擴充第二類爬蟲與數據表**
   - 針對賠率、晨操、分段時間等進階數據，建立新的 Scraper 與 SQLAlchemy Models，實現全方位的自動化收集。

---

## 🔴 需要擴充爬蟲/資料來源（目前無法準確落地）

| 條件（UI 名稱） | 內部代號 | 缺少的關鍵資料 |
| :--- | :--- | :--- |
| **馬匹分段時間＋完成時間 (同路程歷史)** | `horse_time_perf` | 需要賽果頁的分段時間/完成時間（目前資料庫 `RaceResult.sectional_times` 尚未穩定填入）。 |
| **投注額變動 (早盤 vs 即時)** | `odds_movement` | 需要早盤/臨場多時間點賠率或投注額序列（目前未建立完整時間序列）。 |
| **晨操／試閘表現** | `morning_trial_perf` | 需要晨操/試閘頁面爬取與入庫（目前 `Workout` 尚未穩定填入）。 |
| **配備變化** | `gear_change` | 需要在排位爬蟲中抓取並寫入 `RaceEntry.gear`（目前未完整入庫）。 |
| **配速分析 (步速匹配度)** | `pace_analysis` | 需要詳細賽果的走位/步速資料（目前缺）。 |
| **HKJC SpeedPRO 能量分** | `speedpro_energy` | 需要 SpeedPRO 官方頁面數據爬取與入庫（目前缺）。 |
| **獸醫報告／休息天數** | `vet_rest_days` | 需要獸醫報告資料爬取與入庫（目前 `VetReport` 尚未穩定填入）。 |
