import json
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict, Any, List

from database.models import Race, RaceEntry, SystemConfig
from scoring_engine.ai_advisor import load_ai_settings, load_ai_api_key, call_chat_completions

def get_learned_rules(session: Session) -> List[str]:
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    if cfg and isinstance(cfg.value, list):
        return cfg.value
    return []

def save_learned_rules(session: Session, new_rules: List[str]):
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    if not cfg:
        cfg = SystemConfig(key="ai_learned_rules", description="AI 賽後反思學習到的法則")
        session.add(cfg)
        existing = []
    else:
        existing = cfg.value if isinstance(cfg.value, list) else []
    
    # Merge and deduplicate, keep last 20 to avoid prompt getting too large
    all_rules = existing + new_rules
    seen = set()
    deduped = []
    for r in all_rules:
        if r not in seen:
            seen.add(r)
            deduped.append(r)
            
    cfg.value = deduped[-20:]
    session.commit()

def generate_race_reflection(session: Session, race_id: int) -> Dict[str, Any]:
    race = session.query(Race).get(race_id)
    if not race:
        return {"ok": False, "reason": "race_not_found"}
        
    date_str = race.race_date.strftime("%Y/%m/%d")
    race_no = race.race_no
    
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    top_4 = []
    for e in entries:
        if e.result and e.result.rank and e.result.rank <= 4:
            horse_name = e.horse.name_ch if e.horse else "?"
            top_4.append({
                "rank": e.result.rank,
                "horse_no": e.horse_no,
                "horse_name": horse_name
            })
            
    if not top_4:
        return {"ok": False, "reason": "no_results"}
        
    top_4.sort(key=lambda x: x["rank"])
    actual_results_str = ", ".join([f"第{x['rank']}名: [{x['horse_no']}] {x['horse_name']}" for x in top_4])
    
    report_key = f"ai_race_report:{date_str}:{race_no}"
    report_cfg = session.query(SystemConfig).filter_by(key=report_key).first()
    if not report_cfg or not isinstance(report_cfg.value, dict) or "report" not in report_cfg.value:
        return {"ok": False, "reason": "no_pre_race_report"}
        
    pre_race_report = report_cfg.value["report"]
    
    reflection_key = f"ai_race_reflection:{date_str}:{race_no}"
    ref_cfg = session.query(SystemConfig).filter_by(key=reflection_key).first()
    if ref_cfg:
        return {"ok": True, "reason": "already_reflected", "reflection": ref_cfg.value.get("reflection")}
        
    settings = load_ai_settings(session)
    api_key_info = load_ai_api_key(session)
    api_key = api_key_info.get("env") or api_key_info.get("stored")
    if not api_key:
        return {"ok": False, "reason": "missing_api_key"}
        
    system_prompt = (
        "你是專業賽馬 AI 檢討專家。以下是你在賽前寫的分析報告，以及該場賽事最終的真實 Top 4 賽果。\n"
        "請檢視你的預測與實際結果的落差。找出你可能漏看的盲點（例如：高估了某種走勢、低估了檔位或負磅的影響、忽視了特定意外紀錄等）。\n"
        "請將『檢討分析過程』的字數嚴格控制在 200 到 400 字以內，精簡扼要。\n"
        "請總結出 1-2 條簡潔、通用、可供未來參考的『賽事預測黃金法則』。\n\n"
        "請務必嚴格以 JSON 格式輸出，格式如下：\n"
        "{\n"
        "  \"reflection_analysis\": \"你的檢討分析過程 (200-400字內)...\",\n"
        "  \"learned_rules\": [\"法則1\", \"法則2\"]\n"
        "}\n"
        "不要包含任何 markdown block 標籤，直接輸出純 JSON。"
    )
    
    user_text = f"【賽前分析報告】\n{pre_race_report}\n\n【實際賽果 Top 4】\n{actual_results_str}"
    
    resp = call_chat_completions(
        endpoint=settings["endpoint"],
        api_key=api_key,
        model_id=settings["model_id"],
        system_prompt=system_prompt,
        user_text=user_text,
        timeout_sec=60
    )
    
    if resp.get("ok"):
        try:
            text = resp.get("text", "").strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.endswith("```"):
                text = text[:-3]
            parsed = json.loads(text)
            
            new_ref_cfg = SystemConfig(key=reflection_key, description=f"AI 賽後反思（racedate={date_str} R{race_no}）")
            session.add(new_ref_cfg)
            new_ref_cfg.value = {
                "actual_results": actual_results_str,
                "reflection": parsed.get("reflection_analysis", ""),
                "learned_rules": parsed.get("learned_rules", []),
                "created_at": datetime.utcnow().isoformat()
            }
            
            new_rules = parsed.get("learned_rules", [])
            if new_rules:
                save_learned_rules(session, new_rules)
                
            session.commit()
            return {"ok": True, "reflection": parsed.get("reflection_analysis"), "learned_rules": new_rules}
        except Exception as e:
            session.rollback()
            return {"ok": False, "reason": "json_parse_error", "error": str(e)}
            
    return {"ok": False, "reason": "api_error", "error": resp.get("error")}
