# SECTIONAL_STYLE_LABEL_SPEC

日期：2026-07-31

## 1. 目的

本文件定義「分段時間 -> 預計跑法 -> 馬匹屬性標籤」的統計層規格。

此模組的定位是：

- 由結構化數據直接產出
- 作為統計引擎的一部分
- 提供 AI 簡報、賽事分析、反思報告引用
- 不依賴 AI 作核心判定

## 2. 核心原則

1. 跑法判定屬於統計層，不屬於 AI 推測層
2. 屬性標籤屬於統計層，不屬於 AI 自由命名層
3. 所有結果必須可回測、可解釋、可調閾值
4. 優先使用同程歷史數據，再做同場相對比較
5. 原始時間不可直接硬比，需做同條件標準化

## 3. 輸入數據

每匹馬至少需要：

1. 同程歷史完成時間
2. 同程歷史各段分段時間
3. 歷史沿途位置
4. 歷史賽事條件
   - 場地
   - 路程
   - 場地狀態
   - 班次
5. 同場參賽馬的相同欄位

建議最小欄位：

- `horse_id`
- `race_id`
- `distance`
- `surface`
- `going`
- `race_class`
- `finish_time_sec`
- `sectional_times`
- `run_position_early`
- `run_position_mid`
- `run_position_late`

## 4. 標準化原則

分段與完成時間應盡量按以下維度分桶比較：

1. 同場地
2. 同路程
3. 同場地狀態
4. 同班次或相近班次

若精準樣本不足，fallback 次序建議如下：

1. 同場地 + 同路程 + 同場地狀態 + 同班次
2. 同場地 + 同路程 + 同場地狀態
3. 同場地 + 同路程
4. 同路程
5. 全局同距離基準

## 5. 特徵工程

第一版建議先生成以下特徵：

### 完成時間類

- `finish_avg_same_distance`
- `finish_best_same_distance`
- `finish_rank_pct_same_distance`

### 分段時間類

- `early_split_avg`
- `mid_split_avg`
- `late_split_avg`
- `early_split_rank_pct`
- `mid_split_rank_pct`
- `late_split_rank_pct`

### 變化率類

- `early_to_mid_delta`
- `mid_to_late_delta`
- `closing_gain_index`
- `fade_risk_index`
- `stamina_index`

### 位置 / 走勢類

- `early_position_avg`
- `mid_position_avg`
- `late_position_avg`
- `position_gain_late`
- `position_loss_late`

## 6. 預計跑法分類

第一版建議固定 5 類：

1. `放頭型`
2. `前置型`
3. `跟前型`
4. `中後追型`
5. `後上型`

## 7. 跑法判定規則

### 7.1 放頭型

典型條件：

- `early_split_rank_pct` 高
- `early_position_avg` 靠前
- 長期早段守前成功率高

### 7.2 前置型

典型條件：

- 早段速度偏快
- `early_position_avg` 前列但非極前
- 中段可守位

### 7.3 跟前型

典型條件：

- 早段不極快
- 中段位置穩定
- 末段保持力較好

### 7.4 中後追型

典型條件：

- 早段位置中後
- 中後段開始推進
- `position_gain_late` 正值

### 7.5 後上型

典型條件：

- `early_position_avg` 靠後
- `late_split_rank_pct` 高
- `closing_gain_index` 高
- 末段追回能力穩定

## 8. 跑法信心分數

建議輸出：

- `predicted_runstyle`
- `runstyle_confidence`
- `runstyle_reason_codes`

信心分數可由以下因素組成：

1. 樣本數
2. 分類一致性
3. 同程穩定性
4. 最近 6 仗是否一致

簡化公式建議：

`confidence = sample_score * consistency_score * recency_score`

其中各分量限制在 `0 ~ 1`。

## 9. 馬匹屬性標籤

第一版建議分為 3 類：

### 9.1 風格標籤

- `出閘快`
- `放頭穩定`
- `跟前穩定`
- `後上極佳`

### 9.2 強項標籤

- `末段爆發強`
- `中段保速佳`
- `耐力特強`
- `長途續航佳`

### 9.3 風險標籤

- `末段易乏力`
- `步速受限型`
- `中段易失位`
- `早段搶口風險`

## 10. 標籤生成規則

每個標籤必須至少有：

- `label_code`
- `label_name`
- `label_strength`
- `sample_size`
- `reason_codes`

### 範例：後上極佳

建議條件：

1. `late_split_rank_pct` 長期處於高位
2. `position_gain_late` 穩定為正
3. 後段追回名次表現穩定
4. 樣本數達最低門檻

### 範例：耐力特強

建議條件：

1. 長途同程 `fade_risk_index` 低
2. `stamina_index` 長期高
3. 末段掉速幅度低
4. 樣本數達最低門檻

## 11. 樣本門檻

第一版建議：

- 跑法判定最低樣本：`5`
- 標籤生效最低樣本：`5 ~ 8`
- 強標籤建議樣本：`8+`

若樣本不足：

- 允許輸出低信心跑法
- 不建議輸出強標籤

## 12. 對 AI 的輸出接口

本模組應提供以下字段給 AI 使用：

- `predicted_runstyle`
- `runstyle_confidence`
- `runstyle_reason_codes`
- `horse_style_labels`
- `horse_strength_labels`
- `horse_risk_labels`

AI 的角色是：

1. 引用這些結果寫分析簡報
2. 引用這些結果寫賽事分析
3. 引用這些結果寫反思報告

AI 不應：

1. 自行推翻統計層的跑法分類
2. 自行發明新的核心標籤
3. 在沒有對應規則的情況下修改標籤口徑

## 13. 對統計模型的用途

這層結果可供以下模組使用：

1. 勝率模型
   - 作為新因子或子因子

2. 落敗率模型
   - 作為風險訊號

3. 黃金法則
   - 作為規則觸發條件

4. AI 報告
   - 作為語義骨架

## 14. 驗證與回測

需要驗證 3 件事：

1. 跑法分類是否準確
2. 屬性標籤是否穩定
3. 跑法 / 標籤是否真能提升勝敗率模型

建議檢查指標：

- 跑法命中率
- 標籤穩定率
- 同類標籤馬匹的實際表現差異
- 加入此模組前後的勝率模型提升幅度

## 15. 會議決策點

開會時建議先拍板以下事項：

1. 第一版跑法是否固定 5 類
2. 標籤是否先限制在 10 個以內
3. 最低樣本門檻取多少
4. 是否先只做同程，不做跨程推估
5. 哪些標籤可進入黃金法則
6. 哪些標籤只供 AI 報告引用

## 16. 建議的第一版交付

第一版建議只交付以下能力：

1. 同程分段時間特徵
2. 5 類預計跑法
3. 4 至 8 個核心標籤
4. 跑法信心分數
5. AI 可引用輸出字段

這樣可以最快落地，也最容易回測與修正。
