# HKJC Analytics - 獨立計分條件與評分邏輯清單

本文件詳細列出系統中計分條件的所需數據來源、評分邏輯，以及目前的開發狀態。系統透過將各項原始數據標準化為 0-10 分，並依據權重計算總分，以產出每場賽事的專業排名。

> 提示：新增/更新計分邏輯後，如要套用到既有賽事資料，需在「數據管理」頁面執行 Batch Rescore 才會更新舊結果。

## Top5 快照與命中統計（重要）

系統會把每場預測「落庫成快照」，再以快照作為命中統計與對外輸出來源：

- **PredictionTop5（Top5 快照）**
  - 每筆代表：某賽日某場次、某條件/某會員組合的 Top5 預測馬號。
  - `predictor_type=factor`：獨立條件（單一因子）Top5
  - `predictor_type=preset`：會員儲存組合（多因子加權）Top5
- **結算日與流程**
  - 排位/即時數據更新 + 計分後：生成 Top5 快照（建議使用後台「⚡ 一鍵完整更新」確保資料完整）
  - 賽果/派彩入庫後：以「Top5 快照」對比賽果 Top5 計算命中（WIN/P/Q1/PQ/T3E/T3/F4/F4Q/B5W/B5P），並回寫到快照 meta.hits / meta.actual_top5

建議順序（以某賽日為單位）：

1. 抓取排位表（建立 Race/RaceEntry 等）
2. 回填該日涉及馬匹往績（HorseHistory）
3. 重算該日所有場次（ScoringEngine.score_race）
4. 生成 Top5 快照（factor + preset）
5. 抓取賽果/派彩並結算命中（回寫 hits）

---

## ✅ 已開發完成（已上線）

| 條件（UI 名稱） | 內部代號 | 所需數據來源 | 評分邏輯摘要 |
| :--- | :--- | :--- | :--- |
| **騎師＋練馬師合作 (綜合)** | `jockey_trainer_bond` | 歷史往績 (`HorseHistory`: jockey_name / trainer_name / horse_id / rank / race_date) | 同時計算「全庫合作」與「本駒合作」的勝/上名率，依可調權重與可調樣本範圍加權合併；同場再做百分位標準化成 0–10 分。 |
| **馬匹分段時間＋完成時間 (同路程歷史)** | `horse_time_perf` | 排位表 (`Race.track_type` / `Race.distance`)、歷史往績 (`HorseHistory.finish_time/venue/surface/distance/race_date`) | v1 先以「完成時間」作速度指標：同路程下取歷史最佳完成時間（track_type→草/泥→同程 fallback），時間越短越好；加入樣本下限與可信度降權，缺乏賽績則以中性處理；同場再標準化成 0–10 分。 |
| **場地＋路程專長** | `venue_dist_specialty` | 排位表 (`Race.track_type` / `Race.distance`)、歷史往績 (`HorseHistory.venue/surface/distance/rank/race_date`) | 以「同跑道資訊（場地+草/泥/賽道）＋同路程」的勝率/上名率計分；勝/上名率使用半衰期做時間衰減，並加入可信度降權（樣本越少降權越多）；可調時間窗、半衰期、樣本下限、可信度滿分樣本與勝/上名權重；同場再標準化成 0–10 分。 |
| **檔位偏差 (官方 Draw Statistics)** | `draw_stats` | HKJC 當日檔位統計（抓取後暫存於 `SystemConfig`）、排位表檔位 (`RaceEntry.draw`) | 以當日該場次各檔位的勝率/上名率作基礎；以「最高勝率/最高上名率」為基準，計算相對強度（勝率 70% + 上名率 30%）；同場再標準化成 0–10 分。 |
| **負磅／評分表現** | `weight_rating_perf` | 排位表 (`RaceEntry.rating` / `RaceEntry.actual_weight`)、歷史往績 (`HorseHistory.distance/rank/rating/weight/race_date`) | 主訊號為「同路程勝仗評分差」：在可調時間窗內找同程勝仗的最高可贏評分，計算與現評分差並加入同程勝磅差；輔助訊號為「同程上名率(前3)」並以半衰期做時間衰減，且需達同程樣本下限 N 才生效；最終按可調入圍權重合成 raw，再同場標準化成 0–10 分。 |
| **班次表現** | `class_performance` | 本場班次 (`Race.race_class`)、歷史往績 (`HorseHistory.race_class`) | 現階段專注「降班訊號」：只在 3→4 / 4→5 時加分（透過解析「第X班/Class X」字串）；同場再標準化成 0–10 分。 |
| **近期狀態 (Last 6 Runs)** | `recent_form` | 歷史往績 (`HorseHistory.rank` / `race_date`)、可調權重 (`SystemConfig.recent_form_weights`) | 取最近 6 仗有效名次，使用時間衰減權重計算加權平均名次；以「負的加權平均名次」作為 raw（名次越好 raw 越高），同場再標準化成 0–10 分。 |
| **初出／長休後表現** | `debut_long_rest` | 歷史往績 (`HorseHistory.race_date/rank`) | 以可調「休息天數門檻」判斷本場是否屬長休復出；若是，回看該馬歷史上每次「休息≥門檻」後的復出賽績（勝/入位）並疊加加分；同場再標準化成 0–10 分。 |

---

## ⛔ 暫停使用（未完成開發，已禁制參與評分/權重配置）

| 條件（UI 名稱） | 內部代號 | 原因 |
| :--- | :--- | :--- |
| **配備變化** | `gear_change` | 目前未完整入庫（缺 `RaceEntry.gear`），先禁用避免誤導總分。 |
| **場地狀況專長 (Going)** | `going_specialty` | 目前為 placeholder，且賽前 Going 可能未公布，先禁用避免重複/誤判。 |
| **HKJC SpeedPRO 能量分** | `speedpro_energy` | 需要 SpeedPRO 官方頁面數據爬取與入庫（目前缺），先禁用。 |
| **獸醫報告／休息天數** | `vet_rest_days` | 需要獸醫報告資料爬取與入庫（目前 `VetReport` 尚未穩定填入），先禁用。 |

---

## 🟡 資料已具備/部分具備（待實作真實邏輯）

| 條件（UI 名稱） | 內部代號 | 現況 | 建議方向 |
| :--- | :--- | :--- | :--- |
| （暫無） |  |  |  |

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
| **馬匹分段時間（同路程歷史）** | `horse_time_perf` | v1 已以往績完成時間落地；如要加入「分段時間」則仍需賽果頁分段時間入庫（目前 `RaceResult.sectional_times` 尚未穩定填入）。 |
| **晨操／試閘表現** | `morning_trial_perf` | 需要晨操/試閘頁面爬取與入庫（目前 `Workout` 尚未穩定填入）。 |
