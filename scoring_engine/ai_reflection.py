import json
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Optional, Tuple

from database.models import Race, RaceEntry, SystemConfig
from scoring_engine.ai_advisor import load_ai_settings, load_ai_api_key, call_chat_completions

def _normalize_rule_items(val: Any) -> List[Dict[str, Any]]:
    if not isinstance(val, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in val:
        if isinstance(item, str) and item.strip():
            out.append({"rule": item.strip(), "enabled": True, "created_at": None, "source": None})
        elif isinstance(item, dict) and str(item.get("rule") or "").strip():
            out.append(
                {
                    "rule": str(item.get("rule") or "").strip(),
                    "enabled": bool(item.get("enabled") is not False),
                    "created_at": item.get("created_at"),
                    "source": item.get("source"),
                }
            )
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for it in out:
        r = str(it.get("rule") or "").strip()
        if not r or r in seen:
            continue
        seen.add(r)
        deduped.append(it)
    return deduped


def get_learned_rule_items(session: Session) -> List[Dict[str, Any]]:
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    return _normalize_rule_items(cfg.value if cfg else None)


def get_learned_rules(session: Session, include_disabled: bool = False) -> List[str]:
    items = get_learned_rule_items(session)
    if include_disabled:
        return [str(x.get("rule") or "").strip() for x in items if str(x.get("rule") or "").strip()]
    return [str(x.get("rule") or "").strip() for x in items if bool(x.get("enabled") is not False) and str(x.get("rule") or "").strip()]


def save_learned_rule_items(session: Session, items: List[Dict[str, Any]]) -> None:
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    if not cfg:
        cfg = SystemConfig(key="ai_learned_rules", description="AI 賽後反思學習到的法則")
        session.add(cfg)
    norm = _normalize_rule_items(items)
    cfg.value = norm[-30:]
    session.commit()


def save_learned_rules(session: Session, new_rules: List[str], source: Optional[str] = None):
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    if not cfg:
        cfg = SystemConfig(key="ai_learned_rules", description="AI 賽後反思學習到的法則")
        session.add(cfg)
        existing_items: List[Dict[str, Any]] = []
    else:
        existing_items = _normalize_rule_items(cfg.value)

    now = datetime.utcnow().isoformat()
    by_rule = {str(x.get("rule") or "").strip(): x for x in existing_items if str(x.get("rule") or "").strip()}
    for r in new_rules or []:
        rr = str(r or "").strip()
        if not rr:
            continue
        if rr in by_rule:
            continue
        by_rule[rr] = {"rule": rr, "enabled": True, "created_at": now, "source": str(source or "").strip() or None}

    merged = list(by_rule.values())
    cfg.value = merged[-30:]
    session.commit()


def _actual_top4(session: Session, race_id: int) -> List[int]:
    entries = session.query(RaceEntry).filter_by(race_id=int(race_id)).all()
    rows = []
    for e in entries:
        try:
            rk = int(getattr(getattr(e, "result", None), "rank", 0) or 0)
        except Exception:
            rk = 0
        if rk and rk <= 4:
            try:
                rows.append((rk, int(getattr(e, "horse_no", 0) or 0)))
            except Exception:
                rows.append((rk, 0))
    rows.sort(key=lambda x: x[0])
    return [hn for _, hn in rows if int(hn or 0) > 0][:4]


def _report_key(date_str: str, race_no: int) -> str:
    return f"ai_race_report:{str(date_str)}:{int(race_no)}"

def _corunning_key(date_str: str, race_no: int) -> str:
    return f"race_corunning:{str(date_str)}:{int(race_no)}"

def _build_corunning_excerpt(
    session: Session,
    date_str: str,
    race_no: int,
    focus_horse_nos: Optional[List[int]] = None,
    max_items: int = 12,
    max_comment_len: int = 180,
) -> str:
    cfg = session.query(SystemConfig).filter_by(key=_corunning_key(date_str, int(race_no))).first()
    if not cfg or not isinstance(cfg.value, dict):
        return ""
    items = cfg.value.get("items")
    if not isinstance(items, dict) or not items:
        return ""

    picked: List[Tuple[int, Dict[str, Any]]] = []
    if isinstance(focus_horse_nos, list) and focus_horse_nos:
        for hn in focus_horse_nos:
            k = str(int(hn))
            v = items.get(k)
            if isinstance(v, dict):
                picked.append((int(hn), v))
    else:
        for k, v in items.items():
            if not isinstance(v, dict):
                continue
            try:
                hn = int(k)
            except Exception:
                continue
            picked.append((hn, v))

    if not picked:
        return ""

    seen = set()
    out_lines = []
    for hn, v in picked:
        if hn in seen:
            continue
        seen.add(hn)
        name = str(v.get("horse_name") or "").strip()
        comment = str(v.get("commentary") or "").strip()
        if not comment:
            continue
        if max_comment_len and len(comment) > int(max_comment_len):
            comment = comment[: int(max_comment_len)].rstrip() + "..."
        prefix = f"[{hn}]"
        if name:
            prefix = f"[{hn}] {name}"
        out_lines.append(f"- {prefix}: {comment}")
        if max_items and len(out_lines) >= int(max_items):
            break

    if not out_lines:
        return ""
    return "\n".join(out_lines)


def list_reflection_candidates(
    session: Session,
    date_str: Optional[str] = None,
    only_unreflected: bool = True,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    q = session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc()).limit(int(limit or 200))
    races = q.all()
    out: List[Dict[str, Any]] = []
    for r in races:
        ds = r.race_date.strftime("%Y/%m/%d") if hasattr(r.race_date, "strftime") else str(r.race_date)
        if date_str and str(date_str) != ds:
            continue
        rep_key = _report_key(ds, int(r.race_no or 0))
        rep_cfg = session.query(SystemConfig).filter_by(key=rep_key).first()
        if not rep_cfg or not isinstance(rep_cfg.value, dict):
            continue
        if only_unreflected:
            ref_key = f"ai_race_reflection:{ds}:{int(r.race_no or 0)}"
            if session.query(SystemConfig).filter_by(key=ref_key).first():
                continue

        act = _actual_top4(session, int(r.id))
        if len(act) < 4:
            continue

        top5 = rep_cfg.value.get("top5_horse_nos")
        elim = rep_cfg.value.get("eliminated_horse_nos")
        top5 = top5 if isinstance(top5, list) else []
        elim = elim if isinstance(elim, list) else []
        try:
            pred = [int(x) for x in top5 if str(x).strip().isdigit()]
        except Exception:
            pred = []
        try:
            elim2 = [int(x) for x in elim if str(x).strip().isdigit()]
        except Exception:
            elim2 = []

        act_set = set(act)
        pred_set = set(pred)
        elim_set = set(elim2)

        hits = len(act_set & pred_set)
        fp = len(act_set & elim_set)
        score = (4 - hits) + (fp * 2)
        if not pred:
            score += 2
        out.append(
            {
                "race_id": int(r.id),
                "date": ds,
                "race_no": int(r.race_no or 0),
                "score": float(score),
                "hits_in_top4": int(hits),
                "false_elim": int(fp),
                "has_pred": bool(len(pred) > 0),
                "has_elim": bool(len(elim2) > 0),
            }
        )
    out.sort(key=lambda x: (-float(x.get("score") or 0.0), int(x.get("race_no") or 0)))
    return out


def batch_reflect_worst(
    session: Session,
    date_str: str,
    top_n: int = 3,
) -> Dict[str, Any]:
    cand = list_reflection_candidates(session, date_str=str(date_str), only_unreflected=True, limit=500)
    picked = cand[: int(top_n or 0)]
    results = []
    for c in picked:
        rid = int(c.get("race_id") or 0)
        if not rid:
            continue
        res = generate_race_reflection(session, rid)
        results.append({"race_id": rid, "date": c.get("date"), "race_no": c.get("race_no"), "res": res, "score": c.get("score")})
    return {"ok": True, "picked": picked, "results": results}

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
    pred_top5 = report_cfg.value.get("top5_horse_nos")
    elim = report_cfg.value.get("eliminated_horse_nos")
    pred_top5 = pred_top5 if isinstance(pred_top5, list) else []
    elim = elim if isinstance(elim, list) else []
    focus = []
    try:
        focus.extend([int(x) for x in pred_top5 if str(x).strip().isdigit()])
    except Exception:
        pass
    try:
        focus.extend([int(x) for x in elim if str(x).strip().isdigit()])
    except Exception:
        pass
    try:
        focus.extend([int(x.get("horse_no") or 0) for x in top_4 if int(x.get("horse_no") or 0) > 0])
    except Exception:
        pass
    focus = [int(x) for x in focus if int(x or 0) > 0]
    seen_focus = set()
    focus2 = []
    for x in focus:
        if x in seen_focus:
            continue
        seen_focus.add(x)
        focus2.append(x)
    corunning_excerpt = _build_corunning_excerpt(session, date_str=date_str, race_no=int(race_no), focus_horse_nos=focus2)
    
    reflection_key = f"ai_race_reflection:{date_str}:{race_no}"
    ref_cfg = session.query(SystemConfig).filter_by(key=reflection_key).first()
    if ref_cfg:
        return {"ok": True, "reason": "already_reflected", "reflection": ref_cfg.value.get("reflection"), "learned_rules": ref_cfg.value.get("learned_rules")}
        
    settings = load_ai_settings(session)
    api_key_info = load_ai_api_key(session)
    api_key = api_key_info.get("env") or api_key_info.get("stored")
    if not api_key:
        return {"ok": False, "reason": "missing_api_key"}
        
    system_prompt = (
        "你是專業賽馬 AI 檢討專家。以下是你在賽前寫的分析報告，以及該場賽事最終的真實 Top 4 賽果。\n"
        "如果提供了賽後『沿途走勢評述』，請優先用它來判斷賽事關鍵事件（例如出閘快慢、受阻、走外疊、步速形勢、留放策略），並反推你賽前分析有哪些盲點。\n"
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
    if corunning_excerpt:
        user_text += f"\n\n【沿途走勢評述（賽後）】\n{corunning_excerpt}"
    
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
                "corunning_excerpt": corunning_excerpt or "",
                "corunning_used": bool(corunning_excerpt),
                "created_at": datetime.utcnow().isoformat()
            }
            
            new_rules = parsed.get("learned_rules", [])
            if new_rules:
                save_learned_rules(session, new_rules, source=f"{date_str}:R{race_no}")
                
            session.commit()
            return {"ok": True, "reflection": parsed.get("reflection_analysis"), "learned_rules": new_rules}
        except Exception as e:
            session.rollback()
            return {"ok": False, "reason": "json_parse_error", "error": str(e)}
            
    return {"ok": False, "reason": "api_error", "error": resp.get("error")}
