import json
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Dict, Any, List

from database.models import SystemConfig, Race, RaceEntry, RaceResult
from scoring_engine.member_stats import HIT_METRICS, _actual_topk_for_race, _calc_hits

def _try_parse_date(s: str):
    t = str(s or "").strip()
    if not t:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt).date()
        except Exception:
            continue
    return None

def calculate_ai_hit_stats(session: Session) -> Dict[str, Any]:
    # Fetch all AI race reports
    reports = session.query(SystemConfig).filter(SystemConfig.key.like("ai_race_report:%")).all()
    
    hit = {"races": 0}
    for k in list(HIT_METRICS):
        hit[str(k)] = 0
    hit.update(
        {
            "top3_2in_top4": 0,
            "top1_in_top4": 0,
            "top2_in_top4": 0,
            "top3_in_top4": 0,
            "top4_in_top4": 0,
            "top5_in_top4": 0,
        }
    )

    stats = {
        "hit": hit,
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
        try:
            top5 = [int(x) for x in list(top5 or []) if int(x or 0) > 0]
        except Exception:
            top5 = []
        try:
            elim = [int(x) for x in list(elim or []) if int(x or 0) > 0]
        except Exception:
            elim = []
        
        if not top5 and not elim:
            continue
            
        parts = r.key.split(":")
        if len(parts) < 3:
            continue
        date_str = parts[1]
        try:
            race_no = int(parts[2])
        except Exception:
            continue
        
        # Get actual results for this race
        date_obj = _try_parse_date(date_str)
        if not date_obj:
            continue
        race = session.query(Race).filter(func.date(Race.race_date) == date_obj, Race.race_no == int(race_no)).first()
        if not race:
            continue
            
        act_top5 = _actual_topk_for_race(session, race.id, 5)
        if len(act_top5) < 5:
            continue
            
        act_top4 = act_top5[:4]
        act_set = set(act_top4)
        
        # Hit stats
        if top5 and len(top5) >= 5:
            stats["hit"]["races"] += 1
            hits = _calc_hits(list(top5), list(act_top5))
            for k in list(HIT_METRICS):
                if k in hits:
                    stats["hit"][k] = int(stats["hit"].get(k) or 0) + int(hits.get(k) or 0)

            # Backward-compatible alias:
            # top3_2in_top4 used to mean (pred_top3 ∩ actual_top4) >= 2.
            # The system's optimization target is now PQ(3), so keep this key aligned with pq3.
            stats["hit"]["top3_2in_top4"] += int(hits.get("pq3") or 0)

            p1 = int(top5[0]) if len(top5) > 0 else 0
            p2 = int(top5[1]) if len(top5) > 1 else 0
            p3 = int(top5[2]) if len(top5) > 2 else 0
            p4 = int(top5[3]) if len(top5) > 3 else 0
            p5 = int(top5[4]) if len(top5) > 4 else 0
            if p1 and p1 in act_set:
                stats["hit"]["top1_in_top4"] += 1
            if p2 and p2 in act_set:
                stats["hit"]["top2_in_top4"] += 1
            if p3 and p3 in act_set:
                stats["hit"]["top3_in_top4"] += 1
            if p4 and p4 in act_set:
                stats["hit"]["top4_in_top4"] += 1
            if p5 and p5 in act_set:
                stats["hit"]["top5_in_top4"] += 1
            
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
