import sys
import os
from pathlib import Path

# 將專案根目錄加入路徑，以便導入 database 模組
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import init_db, get_session
from database.models import ScoringWeight

def populate_default_weights():
    """初始化計分條件的預設權重"""
    session = get_session()
    
    factors = [
        ("jockey_trainer_bond", "騎師＋練馬師合作 (綜合)", 1.0, True),
        ("horse_time_perf", "馬匹分段時間＋完成時間 (同路程歷史)", 1.5, True),
        ("venue_dist_specialty", "場地＋路程專長", 1.0, True),
        ("draw_stats", "檔位偏差 (官方 Draw Statistics)", 0.8, True),
        ("weight_rating_perf", "負磅／評分表現", 0.7, True),
        ("morning_trial_perf", "晨操／試閘表現", 0.0, False),
        ("gear_change", "配備變化", 0.6, False),
        ("class_performance", "班次表現", 1.0, True),
        ("going_specialty", "場地狀況專長 (Going)", 0.8, False),
        ("speedpro_energy", "HKJC SpeedPRO 能量分", 1.2, True),
        ("recent_form", "近期狀態 (Last 6 Runs)", 1.4, True),
        ("vet_rest_days", "獸醫報告／休息天數", 0.5, False),
        ("debut_long_rest", "初出／長休後表現", 0.7, True)
    ]
    
    print("正在寫入預設權重配置...")
    desired_factor_names = {name for name, _, _, _ in factors}
    for name, desc, weight, is_active in factors:
        # 檢查是否已存在
        existing = session.query(ScoringWeight).filter_by(factor_name=name).first()
        if not existing:
            sw = ScoringWeight(factor_name=name, description=desc, weight=weight, is_active=bool(is_active))
            session.add(sw)
        else:
            if existing.description != desc:
                existing.description = desc
            if existing.is_active != bool(is_active):
                existing.is_active = bool(is_active)

    existing_weights = session.query(ScoringWeight).all()
    for w in existing_weights:
        if w.factor_name not in desired_factor_names:
            session.delete(w)
    
    try:
        session.commit()
        print("權重配置寫入完成。")
    except Exception as e:
        session.rollback()
        print(f"寫入權重時發生錯誤: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    print("正在初始化資料庫...")
    # 確保 data 目錄存在
    os.makedirs("data", exist_ok=True)
    
    init_db()
    print("資料庫表結構建立完成。")
    
    populate_default_weights()
    print("初始化流程結束。")
