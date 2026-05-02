import json
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Dict, Any, List

from database.models import SystemConfig, Race, RaceEntry, RaceResult
from scoring_engine.member_stats import _actual_topk_for_race

def calculate_ai_hit_stats(session: Session) -> Dict[str, Any]:
    # Fetch all AI race reports
    reports = session.query(SystemConfig).filter(SystemConfig.key.like("ai_race_report:%")).all()
    
    stats = {
        "hit": {
            "races": 0,
            "w1": 0, "q2": 0, "pq2": 0, "t3": 0, "f4": 0,
            "top1_in_top4": 0, "top2_in_top4": 0, "top3_in_top4": 0, "top4_in_top4": 0, "top5_in_top4": 0
        },
        "elim": {
            "races": 0,
            "pred": 0, "tn": 0, "fp": 0
        }
    }
    
    for r in reports:
        val = r.value
        if not isinstance(val, dict):
            continue
            
        top5 = val.get("top5_horse_nos", [])
        elim = val.get("eliminated_horse_nos", [])
        
        if not top5 and not elim:
            continue
            
        parts = r.key.split(":")
        if len(parts) < 3:
            continue
        date_str = parts[1]
        race_no = parts[2]
        
        # Get actual results for this race
        date_obj = datetime.strptime(date_str, "%Y/%m/%d").date()
        race = session.query(Race).filter(func.date(Race.race_date) == date_obj, Race.race_no == race_no).first()
        if not race:
            continue
            
        act_top4 = _actual_topk_for_race(session, race.id, 4)
        if len(act_top4) < 4:
            continue
            
        act_set = set(act_top4)
        
        # Hit stats
        if top5 and len(top5) >= 5:
            stats["hit"]["races"] += 1
            
            p1, p2, p3, p4, p5 = top5[:5]
            if p1 in act_top4 and act_top4.index(p1) == 0: stats["hit"]["w1"] += 1
            
            # Q2 (1st and 2nd in predictions match 1st and 2nd in actual, any order)
            if p1 in act_top4[:2] and p2 in act_top4[:2]: stats["hit"]["q2"] += 1
            
            # PQ2 (Any 2 of top 3 predictions match any 2 of actual top 3)
            pred_top3 = set([p1, p2, p3])
            act_top3 = set(act_top4[:3])
            if len(pred_top3 & act_top3) >= 2: stats["hit"]["pq2"] += 1
            
            # T3 (Top 3 predictions match Top 3 actual, any order)
            if len(pred_top3 & act_top3) == 3: stats["hit"]["t3"] += 1
            
            # F4 (Top 4 predictions match Top 4 actual, any order)
            pred_top4 = set([p1, p2, p3, p4])
            if len(pred_top4 & act_set) == 4: stats["hit"]["f4"] += 1
            
            # Individual positions in Top 4
            if p1 in act_set: stats["hit"]["top1_in_top4"] += 1
            if p2 in act_set: stats["hit"]["top2_in_top4"] += 1
            if p3 in act_set: stats["hit"]["top3_in_top4"] += 1
            if p4 in act_set: stats["hit"]["top4_in_top4"] += 1
            if p5 in act_set: stats["hit"]["top5_in_top4"] += 1
            
        # Elim stats
        if elim:
            stats["elim"]["races"] += 1
            pred_set = set(elim)
            stats["elim"]["pred"] += len(pred_set)
            
            # False Positive: predicted to be eliminated (not in top4), but actually was in top4
            fp = len(pred_set & act_set)
            # True Negative: predicted to be eliminated, and actually not in top4
            tn = len(pred_set - act_set)
            
            stats["elim"]["fp"] += fp
            stats["elim"]["tn"] += tn

    # Save to SystemConfig
    cfg = session.query(SystemConfig).filter_by(key="ai_overall_stats").first()
    if not cfg:
        cfg = SystemConfig(key="ai_overall_stats", description="AI 整體命中與淘汰統計")
        session.add(cfg)
    
    cfg.value = {
        "stats": stats,
        "updated_at": datetime.utcnow().isoformat()
    }
    session.commit()
    
    return stats

