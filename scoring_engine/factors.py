import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from database.models import RaceResult, RaceEntry, OddsHistory, Workout, VetReport
from typing import Dict, Any

class FactorCalculator:
    """17 個獨立計分條件的具體計算邏輯"""

    def __init__(self, session: Session, df: pd.DataFrame):
        self.session = session
        self.df = df # 本場賽事的參賽馬匹 DataFrame

    def calculate(self, factor_name: str):
        """根據因子名稱調用相應的計算函數，返回 (原始分數 Series, 原始數據顯示 Series)"""
        method_name = f"_calculate_{factor_name}"
        if hasattr(self, method_name):
            return getattr(self, method_name)()
        else:
            # 預設回傳 0.0 (中性分數) 與空字串
            return pd.Series(0.0, index=self.df.index), pd.Series("無數據", index=self.df.index)

    # 1. 騎師＋練馬師合作 (J/T Bond)
    def _calculate_jockey_trainer_bond(self):
        # 範例邏輯：查詢該騎練組合在過去 6-10 場的勝率
        # (這裡簡化，實際應用中需查詢資料庫)
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 2. 馬匹分段時間＋完成時間 (Horse Time Perf)
    def _calculate_horse_time_perf(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 3. 投注額變動 (Odds Movement)
    def _calculate_odds_movement(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 4. 場地＋路程專長 (Venue/Dist Specialty)
    def _calculate_venue_dist_specialty(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 5. 檔位偏差 (Draw Stats) - 真實邏輯：內檔在短途通常有優勢
    def _calculate_draw_stats(self):
        # 簡單邏輯：檔位越小，分數越高 (1檔 10分, 14檔 1分)
        raw_scores = 11 - self.df["draw"].clip(1, 10)
        display = self.df["draw"].apply(lambda x: f"第 {x} 檔")
        return raw_scores, display

    # 6. 負磅／評分表現 (Weight/Rating Perf) - 真實邏輯：高評分馬通常實力較強
    def _calculate_weight_rating_perf(self):
        # 評分越高，分數越高
        raw_scores = self.df["rating"] / 10
        display = self.df["rating"].apply(lambda x: f"評分 {x}")
        return raw_scores, display

    # 7. 晨操／試閘表現 (Morning/Trial Perf)
    def _calculate_morning_trial_perf(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 8. 騎師＋馬匹組合 (Jockey/Horse Bond)
    def _calculate_jockey_horse_bond(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 9. 練馬師＋馬匹組合 (Trainer/Horse Bond)
    def _calculate_trainer_horse_bond(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 10. 配備變化 (Gear Change)
    def _calculate_gear_change(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 11. 配速分析 (Pace Analysis)
    def _calculate_pace_analysis(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 12. 班次表現 (Class Performance) - 真實邏輯：負磅越輕壓力越小
    def _calculate_class_performance(self):
        # 負磅越輕，分數越高 (135磅 0分, 115磅 10分)
        raw_scores = 145 - self.df["weight"]
        display = self.df["weight"].apply(lambda x: f"負 {x} 磅")
        return raw_scores, display

    # 13. 場地狀況專長 (Going Specialty)
    def _calculate_going_specialty(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 14. HKJC SpeedPRO 能量分 (SpeedPRO)
    def _calculate_speedpro_energy(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 15. 近期狀態 (Recent Form - Last 6 Runs) - 真實邏輯：計算過去 6 場的平均名次
    def _calculate_recent_form(self):
        from database.models import HorseHistory, Horse
        scores = []
        displays = []
        for _, row in self.df.iterrows():
            # 查詢該馬匹最近 6 場往績
            history = self.session.query(HorseHistory)\
                .join(Horse)\
                .filter(Horse.code == row["horse_code"])\
                .order_by(HorseHistory.race_date.desc())\
                .limit(6).all()
            
            if not history:
                scores.append(5.0) # 無數據給中位分
                displays.append("無往績紀錄")
                continue
            
            # 計算平均名次 (名次越小，分數越高)
            ranks = [h.rank for h in history if h.rank > 0]
            if not ranks:
                scores.append(5.0)
                displays.append("近期無有效名次")
                continue
            
            avg_rank = sum(ranks) / len(ranks)
            # 轉換為 0-10 分：1名 10分, 14名 0分
            score = max(0, min(10, (14 - avg_rank) * 0.75))
            scores.append(score)
            
            recent_str = "/".join(str(r) for r in ranks[:6])
            displays.append(f"近仗: {recent_str} (均 {avg_rank:.1f})")
            
        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 16. 獸醫報告／休息天數 (Vet/Rest Days)
    def _calculate_vet_rest_days(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 17. 初出／長休後表現 (Debut/Long Rest)
    def _calculate_debut_long_rest(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display
