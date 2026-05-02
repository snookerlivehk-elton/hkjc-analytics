import sys
from pathlib import Path
import pandas as pd

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race, Horse, RaceEntry, ScoringWeight
from scoring_engine.core import ScoringEngine
from utils.logger import logger

def setup_dummy_race():
    """建立一場測試用的賽事與 12 匹馬"""
    session = get_session()
    init_db()
    
    # 1. 檢查是否已有預設權重
    if session.query(ScoringWeight).count() == 0:
        from scripts.init_db import populate_default_weights
        populate_default_weights()
    
    # 2. 建立賽事
    race = Race(
        race_date=pd.to_datetime("2024-04-10"),
        venue="ST",
        race_no=1,
        race_id="TEST-RACE-1"
    )
    session.add(race)
    session.flush()
    
    # 3. 建立 12 匹馬與 Entry
    for i in range(1, 13):
        horse = Horse(code=f"H{100+i}", name_ch=f"測試馬{i}")
        session.add(horse)
        session.flush()
        
        entry = RaceEntry(
            race_id=race.id,
            horse_id=horse.id,
            horse_no=i,
            draw=i,
            rating=60 + i,
            actual_weight=120 + i
        )
        session.add(entry)
    
    session.commit()
    logger.info(f"測試賽事已建立，ID: {race.id}")
    return race.id

def test_scoring_engine(race_id):
    """測試計分引擎"""
    session = get_session()
    engine = ScoringEngine(session)
    
    logger.info("=== 開始階段 3 驗證測試 (計分引擎) ===")
    scored_df = engine.score_race(race_id)
    
    if scored_df is not None:
        # 輸出簡化排名表
        output_cols = ["horse_no", "horse_code", "total_score", "rank_in_race", "win_probability"]
        # 也加入幾個因子的得分來觀察
        for col in scored_df.columns:
            if "_score" in col and len(output_cols) < 8:
                output_cols.append(col)
                
        print("\n--- 測試賽事排名表 ---")
        print(scored_df[output_cols].sort_values("rank_in_race").to_string(index=False))
        print("\n✅ 計分引擎運作正常")
    else:
        logger.error("❌ 計分引擎未能產出結果")

if __name__ == "__main__":
    race_id = setup_dummy_race()
    test_scoring_engine(race_id)
