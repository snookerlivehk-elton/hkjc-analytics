import pandas as pd
import numpy as np
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from database.models import RaceEntry, ScoringFactor, ScoringWeight
from utils.logger import logger

from scoring_engine.utils import calculate_relative_percentile, estimate_win_probability
from scoring_engine.constants import DISABLED_FACTORS

class ScoringEngine:
    """每場賽事獨立計分排名系統"""

    def __init__(self, session: Session):
        self.session = session
        self.weights = self._load_weights()

    def _load_weights(self) -> Dict[str, float]:
        """從資料庫載入權重配置"""
        from scoring_engine.factors import get_available_factors

        available = get_available_factors()
        weights = (
            self.session.query(ScoringWeight)
            .filter(ScoringWeight.is_active == True)
            .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
            .all()
        )
        # 確保資料庫中已有預設權重，否則初始化失敗
        out: Dict[str, float] = {}
        for w in weights:
            name = str(w.factor_name or "").strip()
            if not name:
                continue
            if name not in available:
                logger.warning(f"已啟用但未實作的因子將被略過：{name}")
                continue
            out[name] = float(w.weight or 0.0)
        return out

    def score_race(self, race_id: int):
        """核心函數：對單場賽事的所有出賽馬匹進行獨立計分、相對排名"""
        # 1. 獲取本場所有參賽馬匹 (Entries)
        entries = self.session.query(RaceEntry).filter_by(race_id=race_id).all()
        if not entries:
            logger.warning(f"賽事 ID {race_id} 無參賽馬匹數據")
            return

        # 2. 準備數據框 (DataFrame) 以進行批量相對排名計算
        horse_data = []
        for entry in entries:
            horse_data.append({
                "race_id": race_id,
                "entry_id": entry.id,
                "horse_id": entry.horse_id,
                "horse_code": entry.horse.code if entry.horse else "",
                "horse_no": entry.horse_no,
                "jockey_name": entry.jockey.name_ch if entry.jockey else "",
                "trainer_name": entry.trainer.name_ch if entry.trainer else "",
                "draw": entry.draw,
                "rating": entry.rating,
                "weight": entry.actual_weight,
            })
        
        df = pd.DataFrame(horse_data)
        
        # 3. 計算所有啟用的獨立計分條件的「原始分 (Raw Value)」
        from scoring_engine.factors import FactorCalculator
        calculator = FactorCalculator(self.session, df)
        
        factor_raw_scores = {}
        factor_displays = {}
        for factor_name in self.weights.keys():
            raw_scores, displays = calculator.calculate(factor_name)
            factor_raw_scores[factor_name] = raw_scores
            factor_displays[factor_name] = displays

        # 4. 核心功能：進行相對百分位排名 (0-10 分)
        # 確保所有分數都在本場馬匹內做相對比較
        scored_df = df.copy()
        for factor_name, raw_vals in factor_raw_scores.items():
            scored_df[f"{factor_name}_raw"] = raw_vals
            scored_df[f"{factor_name}_score"] = calculate_relative_percentile(raw_vals, score_range=(0, 10))
            scored_df[f"{factor_name}_display"] = factor_displays[factor_name]

        # 5. 計算加權總分與排名
        total_score = np.zeros(len(scored_df))
        for factor_name, weight in self.weights.items():
            total_score += scored_df[f"{factor_name}_score"] * weight
        
        scored_df["total_score"] = total_score
        # 總分越高，名次越前 (rank 1)
        scored_df["rank_in_race"] = scored_df["total_score"].rank(ascending=False, method='min').astype(int)
        
        # 6. 估計勝出概率 (Win Probability)
        scored_df["win_probability"] = estimate_win_probability(scored_df["total_score"])

        # 7. 將結果持久化到資料庫
        self._save_results(scored_df)
        
        logger.info(f"賽事 ID {race_id} 計分排名完成 (成功計算 {len(scored_df)} 匹馬)")
        return scored_df

    def _save_results(self, df: pd.DataFrame):
        """儲存計分結果與因子得分"""
        for _, row in df.iterrows():
            entry = self.session.get(RaceEntry, row["entry_id"])
            if entry:
                entry.total_score = row["total_score"]
                entry.rank_in_race = row["rank_in_race"]
                entry.win_probability = row["win_probability"]
                
                # 儲存個別因子得分 (ScoringFactor 表)
                for factor_name in self.weights.keys():
                    sf = self.session.query(ScoringFactor).filter_by(
                        entry_id=entry.id, factor_name=factor_name
                    ).first()
                    if not sf:
                        sf = ScoringFactor(entry_id=entry.id, factor_name=factor_name)
                        self.session.add(sf)
                    
                    raw_val = row.get(f"{factor_name}_raw")
                    if raw_val is None or (isinstance(raw_val, float) and np.isnan(raw_val)):
                        sf.raw_value = None
                    else:
                        try:
                            sf.raw_value = float(raw_val)
                        except Exception:
                            sf.raw_value = None
                    sf.score = row[f"{factor_name}_score"]
                    sf.raw_data_display = row[f"{factor_name}_display"]
                    sf.weight_at_time = self.weights[factor_name]
        
        self.session.commit()
