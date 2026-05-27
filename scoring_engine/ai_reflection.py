import os
import json
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Optional

from database.models import Race, RaceEntry, SystemConfig
from scoring_engine.ai_advisor import load_ai_settings, load_ai_api_key, call_chat_completions
from scoring_engine.config_value import build_meta, unwrap_value, wrap_value

DEFAULT_RULES_WAREHOUSE_KEEP = 200
DEFAULT_RULES_MAX_ENABLED = 30
DEFAULT_RULES_AUTO_CURATE_MAX_CHANGES = 3
DEFAULT_RULES_AUTO_CURATE_COOLDOWN_SEC = 600


def _parse_dt_iso(v: Any) -> Optional[datetime]:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _infer_created_at_from_source(source: Any) -> Optional[str]:
    s = str(source or "").strip()
    if not s:
        return None
    try:
        if ":" in s:
            s = s.split(":", 1)[0]
        dt = datetime.strptime(s, "%Y/%m/%d")
        return dt.strftime("%Y-%m-%dT00:00:00")
    except Exception:
        return None


def _format_rule_date_ymd(created_at: Any, source: Any) -> str:
    dt = _parse_dt_iso(created_at)
    if not dt:
        inferred = _infer_created_at_from_source(source)
        dt = _parse_dt_iso(inferred)
    return dt.strftime("%Y-%m-%d") if dt else "-"


def _get_rules_limits_from_env() -> Dict[str, int]:
    def _to_int(v: Any, default: int) -> int:
        try:
            n = int(str(v).strip())
            return n
        except Exception:
            return default

    keep = _to_int(os.environ.get("AI_RULES_WAREHOUSE_KEEP"), DEFAULT_RULES_WAREHOUSE_KEEP)
    max_enabled = _to_int(os.environ.get("AI_RULES_MAX_ENABLED"), DEFAULT_RULES_MAX_ENABLED)
    max_changes = _to_int(os.environ.get("AUTO_RULE_CURATE_MAX_CHANGES"), DEFAULT_RULES_AUTO_CURATE_MAX_CHANGES)
    cooldown = _to_int(os.environ.get("AUTO_RULE_CURATE_COOLDOWN_SEC"), DEFAULT_RULES_AUTO_CURATE_COOLDOWN_SEC)
    keep = max(30, min(500, keep))
    max_enabled = max(5, min(100, max_enabled))
    max_changes = max(1, min(5, max_changes))
    cooldown = max(0, min(3600, cooldown))
    return {"keep": keep, "max_enabled": max_enabled, "max_changes": max_changes, "cooldown": cooldown}


def _enforce_rule_limits(items: List[Dict[str, Any]], *, keep: int, max_enabled: int) -> List[Dict[str, Any]]:
    norm = _normalize_rule_items(items)

    def _sort_key(it: Dict[str, Any]):
        dt = _parse_dt_iso(it.get("created_at")) or _parse_dt_iso(_infer_created_at_from_source(it.get("source"))) or datetime.min
        return dt

    norm.sort(key=_sort_key)
    if len(norm) > int(keep):
        norm = norm[-int(keep) :]

    enabled_items = [it for it in norm if bool(it.get("enabled") is not False)]
    if len(enabled_items) > int(max_enabled):
        enabled_items.sort(key=_sort_key)  # oldest first
        disable_n = len(enabled_items) - int(max_enabled)
        disable_set = set(str(it.get("rule") or "").strip() for it in enabled_items[:disable_n] if str(it.get("rule") or "").strip())
        out2 = []
        for it in norm:
            rt = str(it.get("rule") or "").strip()
            it2 = dict(it)
            if rt and rt in disable_set:
                it2["enabled"] = False
            out2.append(it2)
        norm = _normalize_rule_items(out2)

    return norm


def apply_rules_outcome(
    session: Session,
    *,
    rules_used: List[str],
    hits_in_top4: int,
    false_elim: int,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    rules_used = [str(x or "").strip() for x in (rules_used or []) if str(x or "").strip()]
    if not rules_used:
        return {"ok": False, "reason": "no_rules_used"}

    items = get_learned_rule_items(session)
    by_rule = {str(x.get("rule") or "").strip(): dict(x) for x in items if str(x.get("rule") or "").strip()}

    now = datetime.utcnow().isoformat()
    hits_in_top4 = int(hits_in_top4 or 0)
    false_elim = int(false_elim or 0)
    delta = float(hits_in_top4 - (2 * false_elim))
    good = (hits_in_top4 >= 3) and (false_elim <= 0)
    bad = (hits_in_top4 <= 1) or (false_elim >= 1)

    updated = 0
    for r in rules_used:
        it = by_rule.get(r)
        if not it:
            continue
        try:
            it["used_count"] = int(it.get("used_count") or 0) + 1
        except Exception:
            it["used_count"] = 1
        it["last_used_at"] = now
        try:
            it["impact_score"] = float(it.get("impact_score") or 0.0) + float(delta)
        except Exception:
            it["impact_score"] = float(delta)
        if good:
            try:
                it["good_count"] = int(it.get("good_count") or 0) + 1
            except Exception:
                it["good_count"] = 1
        if bad:
            try:
                it["bad_count"] = int(it.get("bad_count") or 0) + 1
            except Exception:
                it["bad_count"] = 1
        if source and (not it.get("source")):
            it["source"] = str(source)
        by_rule[r] = it
        updated += 1

    limits = _get_rules_limits_from_env()
    final_items = _enforce_rule_limits(list(by_rule.values()), keep=int(limits["keep"]), max_enabled=int(limits["max_enabled"]))
    save_learned_rule_items(session, final_items)
    return {"ok": True, "updated": int(updated), "delta": float(delta), "good": bool(good), "bad": bool(bad)}

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
                    "used_count": item.get("used_count"),
                    "last_used_at": item.get("last_used_at"),
                    "impact_score": item.get("impact_score"),
                    "good_count": item.get("good_count"),
                    "bad_count": item.get("bad_count"),
                }
            )
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for it in out:
        r = str(it.get("rule") or "").strip()
        if not r or r in seen:
            continue
        seen.add(r)
        if not it.get("created_at"):
            inferred = _infer_created_at_from_source(it.get("source"))
            if inferred:
                it = dict(it)
                it["created_at"] = inferred
        try:
            it2 = dict(it)
            it2["used_count"] = int(it2.get("used_count") or 0)
            it2["impact_score"] = float(it2.get("impact_score") or 0.0)
            it2["good_count"] = int(it2.get("good_count") or 0)
            it2["bad_count"] = int(it2.get("bad_count") or 0)
            it2["last_used_at"] = it2.get("last_used_at") or None
            it = it2
        except Exception:
            it = dict(it)
            it["used_count"] = 0
            it["impact_score"] = 0.0
            it["good_count"] = 0
            it["bad_count"] = 0
            it["last_used_at"] = it.get("last_used_at") or None
        deduped.append(it)
    return deduped


def get_learned_rule_items(session: Session) -> List[Dict[str, Any]]:
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    val, _ = unwrap_value(cfg.value) if cfg else (None, {})
    return _normalize_rule_items(val)


def get_learned_rules(session: Session, include_disabled: bool = False) -> List[str]:
    items = get_learned_rule_items(session)
    if include_disabled:
        return [str(x.get("rule") or "").strip() for x in items if str(x.get("rule") or "").strip()]
    return [str(x.get("rule") or "").strip() for x in items if bool(x.get("enabled") is not False) and str(x.get("rule") or "").strip()]


def save_learned_rule_items(session: Session, items: List[Dict[str, Any]]) -> None:
    limits = _get_rules_limits_from_env()
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    if not cfg:
        cfg = SystemConfig(key="ai_learned_rules", description="AI 賽後反思學習到的法則")
        session.add(cfg)
    norm = _enforce_rule_limits(items, keep=int(limits["keep"]), max_enabled=int(limits["max_enabled"]))
    cfg.value = wrap_value(norm, build_meta(source="AI_REFLECTION", fetched_at=datetime.utcnow().isoformat(), schema="ai_learned_rules:v2"))
    try:
        from scoring_engine.search_index import index_system_config_doc

        index_system_config_doc(session, "ai_learned_rules", doc_type="ai_learned_rules", title="ai_learned_rules")
    except Exception:
        pass
    session.commit()


def save_learned_rules(session: Session, new_rules: List[str], source: Optional[str] = None):
    limits = _get_rules_limits_from_env()
    cfg = session.query(SystemConfig).filter_by(key="ai_learned_rules").first()
    if not cfg:
        cfg = SystemConfig(key="ai_learned_rules", description="AI 賽後反思學習到的法則")
        session.add(cfg)
        existing_items: List[Dict[str, Any]] = []
    else:
        val0, _ = unwrap_value(cfg.value)
        existing_items = _normalize_rule_items(val0)

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
    final_items = _enforce_rule_limits(merged, keep=int(limits["keep"]), max_enabled=int(limits["max_enabled"]))
    cfg.value = wrap_value(final_items, build_meta(source="AI_REFLECTION", fetched_at=datetime.utcnow().isoformat(), schema="ai_learned_rules:v2"))
    try:
        from scoring_engine.search_index import index_system_config_doc

        index_system_config_doc(session, "ai_learned_rules", doc_type="ai_learned_rules", title="ai_learned_rules")
    except Exception:
        pass
    session.commit()

def _apply_rule_actions(
    items: List[Dict[str, Any]],
    actions: Dict[str, Any],
    *,
    max_changes: int = 5,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    max_changes = int(max_changes or 0)
    if max_changes <= 0:
        return {"items": items, "applied": {"total": 0, "enable": 0, "disable": 0, "delete": 0, "add": 0}}

    norm = _normalize_rule_items(items)
    by_rule = {str(x.get("rule") or "").strip(): dict(x) for x in norm if str(x.get("rule") or "").strip()}
    now = datetime.utcnow().isoformat()

    def _norm_list(v: Any) -> List[str]:
        if isinstance(v, list):
            out = []
            for x in v:
                s = str(x or "").strip()
                if s:
                    out.append(s)
            return out
        if isinstance(v, str) and v.strip():
            return [v.strip()]
        return []

    to_delete = _norm_list(actions.get("delete"))
    to_disable = _norm_list(actions.get("disable"))
    to_enable = _norm_list(actions.get("enable"))
    to_add = _norm_list(actions.get("add"))

    applied = {"total": 0, "enable": 0, "disable": 0, "delete": 0, "add": 0}

    for r in to_delete:
        if applied["total"] >= max_changes:
            break
        if r in by_rule:
            del by_rule[r]
            applied["delete"] += 1
            applied["total"] += 1

    for r in to_disable:
        if applied["total"] >= max_changes:
            break
        it = by_rule.get(r)
        if not it:
            continue
        if bool(it.get("enabled") is not False):
            it["enabled"] = False
            by_rule[r] = it
            applied["disable"] += 1
            applied["total"] += 1

    for r in to_enable:
        if applied["total"] >= max_changes:
            break
        it = by_rule.get(r)
        if not it:
            continue
        if bool(it.get("enabled") is False):
            it["enabled"] = True
            by_rule[r] = it
            applied["enable"] += 1
            applied["total"] += 1

    for r in to_add:
        if applied["total"] >= max_changes:
            break
        if r in by_rule:
            continue
        by_rule[r] = {"rule": r, "enabled": True, "created_at": now, "source": str(source or "").strip() or None}
        applied["add"] += 1
        applied["total"] += 1

    merged = _normalize_rule_items(list(by_rule.values()))
    return {"items": merged, "applied": applied}


def curate_learned_rules(
    session: Session,
    *,
    max_changes: int = 5,
    keep: int = DEFAULT_RULES_WAREHOUSE_KEEP,
    max_enabled: int = DEFAULT_RULES_MAX_ENABLED,
) -> Dict[str, Any]:
    items = get_learned_rule_items(session)

    settings = load_ai_settings(session)
    api_key_info = load_ai_api_key(session)
    api_key = api_key_info.get("env") or api_key_info.get("stored")
    if not api_key:
        return {"ok": False, "reason": "missing_api_key"}

    max_changes = max(1, min(5, int(max_changes or 5)))
    keep = max(30, min(500, int(keep or DEFAULT_RULES_WAREHOUSE_KEEP)))
    max_enabled = max(5, min(100, int(max_enabled or DEFAULT_RULES_MAX_ENABLED)))

    enabled_lines = []
    disabled_lines = []
    for x in items:
        rule = str(x.get("rule") or "").strip()
        if not rule:
            continue
        date_ymd = _format_rule_date_ymd(x.get("created_at"), x.get("source"))
        src = str(x.get("source") or "").strip()
        try:
            used_cnt = int(x.get("used_count") or 0)
        except Exception:
            used_cnt = 0
        try:
            impact = float(x.get("impact_score") or 0.0)
        except Exception:
            impact = 0.0
        prefix = f"[{date_ymd}]"
        if src:
            prefix += f" ({src})"
        line = f"{prefix} used={used_cnt} impact={impact:.1f}｜{rule}"
        if bool(x.get("enabled") is not False):
            enabled_lines.append(line)
        else:
            disabled_lines.append(line)

    system_prompt = (
        "你是賽馬 AI 的知識庫管理員。你要整理「賽後反思黃金法則」清單。\n"
        f"目標：法則倉庫最多保留 {int(keep)} 條；其中「啟用」最多 {int(max_enabled)} 條（啟用才會注入下一次賽前預測）。\n"
        "你應盡量用 disable 代替 delete（除非明顯無用/重複/錯誤）。\n"
        f"你只能做最多 {int(max_changes)} 個改動（enable/disable/delete/add 的總和）。\n"
        "請優先做「線性、漸進式」改動：不要一次大幅重寫。\n"
        "輸出必須是純 JSON，格式：\n"
        "{\n"
        "  \"delete\": [\"法則全文\"],\n"
        "  \"disable\": [\"法則全文\"],\n"
        "  \"enable\": [\"法則全文\"],\n"
        "  \"add\": [\"新增法則全文\"]\n"
        "}\n"
        "每個陣列都可以是空。不要輸出 markdown。\n"
    )

    user_text = (
        f"【目前啟用法則（{len(enabled_lines)}）】\n"
        + "\n".join([f"- {x}" for x in enabled_lines])
        + f"\n\n【目前停用法則（{len(disabled_lines)}）】\n"
        + "\n".join([f"- {x}" for x in disabled_lines])
    )

    resp = call_chat_completions(
        endpoint=settings["endpoint"],
        api_key=api_key,
        model_id=settings["model_id"],
        system_prompt=system_prompt,
        user_text=user_text,
        timeout_sec=60,
    )

    if not resp.get("ok"):
        return {"ok": False, "reason": "api_error", "error": resp.get("error")}

    try:
        text = str(resp.get("text") or "").strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.endswith("```"):
            text = text[:-3]
        parsed = json.loads(text)
    except Exception as e:
        return {"ok": False, "reason": "json_parse_error", "error": str(e)}

    parsed = parsed if isinstance(parsed, dict) else {}
    res = _apply_rule_actions(items, parsed, max_changes=int(max_changes), source="AI_RULE_CURATOR")
    final_items = _enforce_rule_limits(_normalize_rule_items(res.get("items")), keep=int(keep), max_enabled=int(max_enabled))
    save_learned_rule_items(session, final_items)
    return {"ok": True, "applied": res.get("applied"), "count": len(final_items)}


def maybe_auto_curate_rules(session: Session, *, source: Optional[str] = None) -> Dict[str, Any]:
    enabled = str(os.environ.get("ENABLE_AUTO_RULE_CURATION") or "0").strip().lower() in ("1", "true", "yes")
    if not enabled:
        return {"ok": False, "reason": "disabled"}

    limits = _get_rules_limits_from_env()
    cooldown = int(limits["cooldown"])
    key = "auto_ai_rules_curated:last"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if cfg and cfg.value:
        try:
            val, _ = unwrap_value(cfg.value)
        except Exception:
            val = cfg.value
        if isinstance(val, dict):
            last_at = _parse_dt_iso(val.get("at"))
            if last_at and cooldown > 0:
                if (datetime.utcnow() - last_at).total_seconds() < float(cooldown):
                    return {"ok": False, "reason": "cooldown"}

    res = curate_learned_rules(
        session,
        max_changes=int(limits["max_changes"]),
        keep=int(limits["keep"]),
        max_enabled=int(limits["max_enabled"]),
    )
    if res.get("ok"):
        if not cfg:
            cfg = SystemConfig(key=key, description="自動法則整理：最近一次執行紀錄（避免頻繁重覆）")
            session.add(cfg)
        cfg.value = wrap_value(
            {"at": datetime.utcnow().isoformat(), "source": str(source or "").strip() or None, "result": res},
            build_meta(source="AI_RULE_CURATOR", fetched_at=datetime.utcnow().isoformat(), schema="auto_ai_rules_curated:v1"),
        )
        session.commit()
    return res


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

def _event_report_key(date_str: str, race_no: int) -> str:
    return f"race_event_report:{str(date_str)}:{int(race_no)}"

def _build_event_report_excerpt(
    session: Session,
    date_str: str,
    race_no: int,
    max_items: int = 12,
    max_desc_len: int = 220,
) -> str:
    cfg = session.query(SystemConfig).filter_by(key=_event_report_key(date_str, int(race_no))).first()
    if not cfg:
        return ""
    payload, _ = unwrap_value(cfg.value)
    if not isinstance(payload, dict):
        return ""
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return ""
    out_lines = []
    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            hn = int(it.get("horse_no") or 0)
        except Exception:
            hn = 0
        if hn <= 0 or hn in seen:
            continue
        seen.add(hn)
        name = str(it.get("horse_name") or "").strip()
        desc = str(it.get("desc") or "").strip()
        if not desc:
            continue
        if max_desc_len and len(desc) > int(max_desc_len):
            desc = desc[: int(max_desc_len)].rstrip() + "..."
        prefix = f"[{hn}]"
        if name:
            prefix = f"[{hn}] {name}"
        out_lines.append(f"- {prefix}: {desc}")
        if max_items and len(out_lines) >= int(max_items):
            break
    return "\n".join(out_lines) if out_lines else ""

def list_reflection_candidates(
    session: Session,
    date_str: Optional[str] = None,
    only_unreflected: bool = True,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    q = session.query(Race)
    if date_str:
        try:
            d0 = datetime.strptime(str(date_str), "%Y/%m/%d")
            d1 = d0 + timedelta(days=1)
            q = q.filter(Race.race_date >= d0, Race.race_date < d1)
        except Exception:
            pass
    q = q.order_by(Race.race_date.desc(), Race.race_no.asc()).limit(int(limit or 200))
    races = q.all()
    out: List[Dict[str, Any]] = []
    for r in races:
        ds = r.race_date.strftime("%Y/%m/%d") if hasattr(r.race_date, "strftime") else str(r.race_date)
        if date_str and str(date_str) != ds:
            continue
        rep_key = _report_key(ds, int(r.race_no or 0))
        rep_cfg = session.query(SystemConfig).filter_by(key=rep_key).first()
        if not rep_cfg:
            continue
        rep_val, _ = unwrap_value(rep_cfg.value)
        if not isinstance(rep_val, dict):
            continue
        if only_unreflected:
            ref_key = f"ai_race_reflection:{ds}:{int(r.race_no or 0)}"
            if session.query(SystemConfig).filter_by(key=ref_key).first():
                continue

        act = _actual_top4(session, int(r.id))
        if len(act) < 4:
            continue

        top5 = rep_val.get("top5_horse_nos")
        elim = rep_val.get("eliminated_horse_nos")
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

def batch_reflect_day(
    session: Session,
    date_str: str,
    mode: str = "all",
    only_unreflected: bool = True,
) -> Dict[str, Any]:
    mode2 = str(mode or "").strip().lower() or "all"
    cand = list_reflection_candidates(session, date_str=str(date_str), only_unreflected=bool(only_unreflected), limit=500)
    picked = cand
    if mode2 in {"miss_only", "miss", "unhit"}:
        picked = [x for x in cand if (int(x.get("hits_in_top4") or 0) < 4) or (int(x.get("false_elim") or 0) > 0)]

    results = []
    for c in picked:
        rid = int(c.get("race_id") or 0)
        if not rid:
            continue
        save_rules = (int(c.get("hits_in_top4") or 0) < 4) or (int(c.get("false_elim") or 0) > 0)
        res = generate_race_reflection(session, rid, save_rules=bool(save_rules))
        results.append(
            {
                "race_id": rid,
                "date": c.get("date"),
                "race_no": c.get("race_no"),
                "res": res,
                "score": c.get("score"),
                "hits_in_top4": c.get("hits_in_top4"),
                "false_elim": c.get("false_elim"),
            }
        )
    return {"ok": True, "mode": mode2, "picked": picked, "results": results}


def generate_race_reflection(session: Session, race_id: int, save_rules: bool = True) -> Dict[str, Any]:
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
    if not report_cfg:
        return {"ok": False, "reason": "no_pre_race_report"}
    report_val, _ = unwrap_value(report_cfg.value)
    if not isinstance(report_val, dict) or "report" not in report_val:
        return {"ok": False, "reason": "no_pre_race_report"}
        
    pre_race_report = report_val["report"]
    pred_top5 = report_val.get("top5_horse_nos")
    elim = report_val.get("eliminated_horse_nos")
    pred_top5 = pred_top5 if isinstance(pred_top5, list) else []
    elim = elim if isinstance(elim, list) else []
    
    reflection_key = f"ai_race_reflection:{date_str}:{race_no}"
    ref_cfg = session.query(SystemConfig).filter_by(key=reflection_key).first()
    if ref_cfg:
        ref_val, _ = unwrap_value(ref_cfg.value)
        if isinstance(ref_val, dict):
            return {"ok": True, "reason": "already_reflected", "reflection": ref_val.get("reflection"), "learned_rules": ref_val.get("learned_rules")}
        return {"ok": True, "reason": "already_reflected"}
        
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
    event_report_excerpt = _build_event_report_excerpt(session, date_str=date_str, race_no=int(race_no))
    if event_report_excerpt:
        user_text += f"\n\n【競賽事件報告（賽後）】\n{event_report_excerpt}"
    
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
            payload_ref = {
                "actual_results": actual_results_str,
                "reflection": parsed.get("reflection_analysis", ""),
                "learned_rules": parsed.get("learned_rules", []),
                "event_report_excerpt": event_report_excerpt or "",
                "event_report_used": bool(event_report_excerpt),
                "created_at": datetime.utcnow().isoformat()
            }
            meta = build_meta(
                source="AI_REFLECTION",
                fetched_at=datetime.utcnow().isoformat(),
                schema="ai_race_reflection:v1",
                extra={
                    "race_id": int(race_id),
                    "date": str(date_str),
                    "race_no": int(race_no),
                    "sources": {
                        "system_config_keys": [report_key, _event_report_key(date_str, int(race_no))],
                        "model_id": str(settings.get("model_id") or ""),
                    },
                },
            )
            new_ref_cfg.value = wrap_value(payload_ref, meta)
            try:
                from scoring_engine.search_index import index_system_config_doc

                index_system_config_doc(session, str(reflection_key), doc_type="ai_reflection", title=f"{date_str} R{race_no} AI reflection")
            except Exception:
                pass
            
            new_rules = parsed.get("learned_rules", [])
            try:
                actual_hns = []
                for x in top_4:
                    try:
                        actual_hns.append(int(x.get("horse_no") or 0))
                    except Exception:
                        pass
                actual_hns = [x for x in actual_hns if int(x or 0) > 0]
                pred_top5_hns = [int(x or 0) for x in (pred_top5 or []) if int(x or 0) > 0]
                elim_hns = [int(x or 0) for x in (elim or []) if int(x or 0) > 0]
                hits_in_top4 = len(set(actual_hns).intersection(set(pred_top5_hns)))
                false_elim = len(set(actual_hns).intersection(set(elim_hns)))
                rules_used = report_val.get("rules_used") if isinstance(report_val, dict) else None
                if not (isinstance(rules_used, list) and rules_used):
                    rules_used = get_learned_rules(session)
                apply_rules_outcome(
                    session,
                    rules_used=list(rules_used or []),
                    hits_in_top4=int(hits_in_top4),
                    false_elim=int(false_elim),
                    source=f"{date_str}:R{race_no}",
                )
            except Exception:
                pass
            if save_rules and new_rules:
                save_learned_rules(session, new_rules, source=f"{date_str}:R{race_no}")
                try:
                    maybe_auto_curate_rules(session, source=f"{date_str}:R{race_no}")
                except Exception:
                    pass
                
            session.commit()
            return {
                "ok": True,
                "reflection": parsed.get("reflection_analysis"),
                "learned_rules": new_rules,
                "event_report_used": bool(event_report_excerpt),
            }
        except Exception as e:
            session.rollback()
            return {"ok": False, "reason": "json_parse_error", "error": str(e)}
            
    return {"ok": False, "reason": "api_error", "error": resp.get("error")}
