import json
import os
import re
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from sqlalchemy import func
from sqlalchemy.orm import Session

from database.models import Race, RaceEntry, RaceResult, ScoringFactor, ScoringWeight, SystemConfig
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.weight_tuning import build_topk_training_frame, tune_weights_topk


def default_ai_system_prompt() -> str:
    return (
        "你是資深數據分析顧問，任務是根據輸入的系統統計（命中率、因子缺失、因子重要性）提出可執行的建議。"
        "只輸出 JSON，必須符合指定 schema。"
        "建議以省資源、可逐步驗證為前提，不要要求大型重訓或高成本資料工程。"
        "嚴禁提供任何投注/賭博/提高博彩收益的建議；只可就資料品質、因子工程、權重調整與統計驗證提出建議。"
    )


def default_ai_schema_hint() -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "recommendations": [
            {
                "action": "add|optimize|remove|reduce_weight|increase_weight|data_fix",
                "factor_name": "",
                "priority": "P0|P1|P2",
                "reason": "",
                "evidence": {
                    "missing_rate": None,
                    "coef_score": None,
                    "coef_missing": None,
                    "lift_top20": None,
                    "notes": [],
                },
                "proposal": {
                    "weight_delta": None,
                    "new_factor_spec": None,
                    "data_fix_steps": [],
                },
                "expected_impact": "",
                "risk": "",
                "validation": "",
            }
        ],
        "summary": "",
        "next_questions": [],
    }


def _strip_json_fence(s: str) -> str:
    t = str(s or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(json)?\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()

def _extract_json_obj(text: str) -> Dict[str, Any]:
    t = _strip_json_fence(text)
    i = t.find("{")
    j = t.rfind("}")
    if i < 0 or j < 0 or j <= i:
        return {"ok": False, "data": None, "error": "no_json_object_found"}
    cand = t[i : j + 1].strip()
    try:
        obj = json.loads(cand)
        return {"ok": True, "data": obj, "error": None}
    except Exception as e:
        return {"ok": False, "data": None, "error": str(e)}


def parse_json_response(text: str) -> Dict[str, Any]:
    t = _strip_json_fence(text)
    try:
        obj = json.loads(t)
        return {"ok": True, "data": obj, "error": None}
    except Exception as e:
        return {"ok": False, "data": None, "error": str(e)}


def load_ai_settings(session: Session) -> Dict[str, Any]:
    cfg = session.query(SystemConfig).filter_by(key="ai_llm_settings").first()
    val = cfg.value if cfg and isinstance(cfg.value, dict) else {}
    out = {
        "endpoint": str(val.get("endpoint") or "").strip(),
        "model_id": str(val.get("model_id") or "").strip(),
        "system_prompt": str(val.get("system_prompt") or "").strip(),
    }
    if not out["endpoint"]:
        out["endpoint"] = "https://api.openai.com/v1/chat/completions"
    if not out["model_id"]:
        out["model_id"] = "gpt-4.1-mini"
    if not out["system_prompt"]:
        out["system_prompt"] = default_ai_system_prompt()
    return out


def save_ai_settings(session: Session, endpoint: str, model_id: str, system_prompt: str) -> None:
    cfg = session.query(SystemConfig).filter_by(key="ai_llm_settings").first()
    if not cfg:
        cfg = SystemConfig(key="ai_llm_settings", description="AI LLM 設定（endpoint/model/system_prompt）")
        session.add(cfg)
    cfg.value = {
        "endpoint": str(endpoint or "").strip(),
        "model_id": str(model_id or "").strip(),
        "system_prompt": str(system_prompt or "").strip(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    session.commit()


def load_ai_api_key(session: Session) -> Dict[str, Any]:
    env_key = os.environ.get("AI_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
    cfg = session.query(SystemConfig).filter_by(key="ai_api_key").first()
    stored = str(cfg.value or "") if cfg and isinstance(cfg.value, str) else ""
    return {"env": env_key, "stored": stored}


def save_ai_api_key(session: Session, api_key: str) -> None:
    cfg = session.query(SystemConfig).filter_by(key="ai_api_key").first()
    if not cfg:
        cfg = SystemConfig(key="ai_api_key", description="AI API Key（不建議存 DB，優先使用環境變數）")
        session.add(cfg)
    cfg.value = str(api_key or "")
    session.commit()


def _is_missing_display(x: Any) -> bool:
    s = str(x or "").strip()
    return s == "" or s == "無數據"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _spearman(a: pd.Series, b: pd.Series) -> Optional[float]:
    try:
        aa = pd.to_numeric(a, errors="coerce")
        bb = pd.to_numeric(b, errors="coerce")
        ok = aa.notna() & bb.notna()
        if int(ok.sum()) < 30:
            return None
        return float(aa[ok].corr(bb[ok], method="spearman"))
    except Exception:
        return None


def _lift_top20(score: pd.Series, y: pd.Series) -> Optional[float]:
    try:
        s = pd.to_numeric(score, errors="coerce")
        yy = pd.to_numeric(y, errors="coerce")
        ok = s.notna() & yy.notna()
        if int(ok.sum()) < 50:
            return None
        q = float(s[ok].quantile(0.8))
        top = yy[ok & (s >= q)]
        base = yy[ok]
        if len(top) < 20 or len(base) < 50:
            return None
        base_rate = float(base.mean())
        if base_rate <= 0.0:
            return None
        return float(top.mean() / base_rate)
    except Exception:
        return None


def build_factor_snapshot(
    session: Session,
    d1: date,
    d2: date,
    top_k: int = 5,
    max_suggest_weight: float = 3.0,
) -> Dict[str, Any]:
    w_rows = (
        session.query(ScoringWeight.factor_name, ScoringWeight.weight, ScoringWeight.description)
        .filter(ScoringWeight.is_active == True)
        .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
        .order_by(ScoringWeight.factor_name.asc())
        .all()
    )
    factor_names = [str(r[0]) for r in w_rows if r and str(r[0] or "").strip()]
    factor_desc = {str(fn): str(desc or fn) for fn, _, desc in w_rows if fn}
    current_w = {str(fn): float(w or 0.0) for fn, w, _ in w_rows if fn}

    tune = tune_weights_topk(session, d1=d1, d2=d2, top_k=int(top_k), factor_names=factor_names, max_suggest_weight=float(max_suggest_weight))
    train = build_topk_training_frame(session, d1=d1, d2=d2, top_k=int(top_k), factor_names=factor_names)

    miss_rows2 = (
        session.query(ScoringFactor.factor_name, ScoringFactor.raw_data_display)
        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
        .join(Race, Race.id == RaceEntry.race_id)
        .filter(ScoringFactor.factor_name.in_(factor_names))
        .filter(func.date(Race.race_date) >= d1.isoformat())
        .filter(func.date(Race.race_date) <= d2.isoformat())
        .all()
    )
    miss_cnt = {}
    total_cnt = {}
    for fn, disp in miss_rows2:
        k = str(fn or "").strip()
        if not k:
            continue
        total_cnt[k] = int(total_cnt.get(k) or 0) + 1
        if _is_missing_display(disp):
            miss_cnt[k] = int(miss_cnt.get(k) or 0) + 1

    factors = []
    if not train.empty:
        y = train["y"].astype(int)
        for fn in factor_names:
            s_col = f"{fn}__score"
            m_col = f"{fn}__missing"
            if s_col not in train.columns:
                continue
            s = train[s_col].fillna(5.0)
            missing_rate = None
            if m_col in train.columns:
                try:
                    missing_rate = float(pd.to_numeric(train[m_col], errors="coerce").fillna(0.0).mean())
                except Exception:
                    missing_rate = None
            if missing_rate is None:
                t0 = int(total_cnt.get(fn) or 0)
                if t0:
                    missing_rate = float(int(miss_cnt.get(fn) or 0) / t0)
            factors.append(
                {
                    "factor_name": fn,
                    "factor_label": factor_desc.get(fn, fn),
                    "current_weight": float(current_w.get(fn) or 0.0),
                    "missing_rate": missing_rate,
                    "coef_score": _safe_float((tune.get("coef_score") or {}).get(fn) if isinstance(tune, dict) else None),
                    "coef_missing": _safe_float((tune.get("coef_missing") or {}).get(fn) if isinstance(tune, dict) else None),
                    "suggested_weight": _safe_float((tune.get("suggested_weights") or {}).get(fn) if isinstance(tune, dict) else None),
                    "spearman_score_y": _spearman(s, y),
                    "lift_top20": _lift_top20(s, y),
                }
            )
    else:
        for fn in factor_names:
            t0 = int(total_cnt.get(fn) or 0)
            mr = float(int(miss_cnt.get(fn) or 0) / t0) if t0 else None
            factors.append(
                {
                    "factor_name": fn,
                    "factor_label": factor_desc.get(fn, fn),
                    "current_weight": float(current_w.get(fn) or 0.0),
                    "missing_rate": mr,
                    "coef_score": _safe_float((tune.get("coef_score") or {}).get(fn) if isinstance(tune, dict) else None),
                    "coef_missing": _safe_float((tune.get("coef_missing") or {}).get(fn) if isinstance(tune, dict) else None),
                    "suggested_weight": _safe_float((tune.get("suggested_weights") or {}).get(fn) if isinstance(tune, dict) else None),
                    "spearman_score_y": None,
                    "lift_top20": None,
                }
            )

    races = (
        session.query(Race.id)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(func.date(Race.race_date) >= d1.isoformat())
        .filter(func.date(Race.race_date) <= d2.isoformat())
        .distinct()
        .order_by(Race.id.asc())
        .all()
    )
    race_ids = [int(r[0]) for r in races if r and r[0] is not None]

    fq_keys = [f"factor_quality:{rid}" for rid in race_ids[:5000]]
    fq_rows = []
    if fq_keys:
        fq_rows = session.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.in_(fq_keys)).all()
    fq_by_rid = {}
    for k, v in fq_rows:
        kk = str(k or "")
        if ":" not in kk:
            continue
        try:
            rid = int(kk.split(":", 1)[1])
        except Exception:
            continue
        if isinstance(v, dict):
            fq_by_rid[rid] = v

    reasons_by_factor: Dict[str, Dict[str, int]] = {}
    races_with_fq = 0
    for rid, fv in fq_by_rid.items():
        races_with_fq += 1
        factors_v = fv.get("factors") if isinstance(fv.get("factors"), dict) else {}
        for fn, item in factors_v.items():
            if not isinstance(item, dict):
                continue
            reasons = item.get("reasons") if isinstance(item.get("reasons"), dict) else {}
            if not reasons:
                continue
            agg = reasons_by_factor.get(str(fn)) or {}
            for rk, n in reasons.items():
                try:
                    agg[str(rk)] = int(agg.get(str(rk)) or 0) + int(n or 0)
                except Exception:
                    continue
            reasons_by_factor[str(fn)] = agg

    top_reasons = {}
    for fn, agg in reasons_by_factor.items():
        pairs = sorted([(k, int(v or 0)) for k, v in agg.items()], key=lambda x: (-x[1], x[0]))[:3]
        top_reasons[fn] = [{"reason": k, "missing": int(v)} for k, v in pairs]

    out = {
        "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
        "objective": {"top_k": int(top_k)},
        "tune_summary": {
            "ok": bool(isinstance(tune, dict) and tune.get("ok") is True),
            "rows": int(tune.get("rows") or 0) if isinstance(tune, dict) else 0,
            "pos_rate": _safe_float(tune.get("pos_rate") if isinstance(tune, dict) else None),
            "auc": _safe_float(tune.get("auc") if isinstance(tune, dict) else None),
            "log_loss": _safe_float(tune.get("log_loss") if isinstance(tune, dict) else None),
        },
        "factor_quality": {
            "races_total": int(len(race_ids)),
            "races_with_snapshot": int(races_with_fq),
            "coverage_pct": float(races_with_fq / len(race_ids) * 100.0) if race_ids else 0.0,
        },
        "factors": factors,
        "top_missing_reasons": top_reasons,
    }
    return out


def _mask_key(k: str) -> str:
    s = str(k or "").strip()
    if len(s) <= 8:
        return "*" * len(s)
    return ("*" * (len(s) - 4)) + s[-4:]


def run_ai_race_summary(
    session: Session,
    race_id: int,
    going_code_override: str = "",
    scenario_tag: str = "",
    save_as_scenario: bool = False,
) -> Dict[str, Any]:
    from database.models import Race, RaceEntry, SystemConfig, PredictionTop5
    
    race = session.query(Race).filter(Race.id == race_id).first()
    if not race:
        return {"ok": False, "reason": "race_not_found"}
        
    date_str = race.race_date.strftime("%Y/%m/%d") if hasattr(race.race_date, "strftime") else str(race.race_date)[:10].replace("-", "/")
    race_no = race.race_no

    def _venue_label(venue: Any, track_type: Any) -> str:
        v = str(venue or "").strip().upper()
        t = str(track_type or "").strip()
        if v == "HV" or ("跑馬地" in t):
            return "跑馬地"
        if v == "ST" or ("沙田" in t):
            return "沙田"
        return str(venue or "").strip() or "-"

    def _surface_label(surface: Any, track_type: Any) -> str:
        s = str(surface or "").strip()
        if s:
            return s
        t = str(track_type or "").strip().upper()
        if any(x in t for x in ["ALL WEATHER", "A/W", "AW"]):
            return "泥地(全天候)"
        if "TURF" in t:
            return "草地"
        return "-"

    def _race_prefix(r: Any, date_s: str) -> str:
        loc = _venue_label(getattr(r, "venue", None), getattr(r, "track_type", None))
        surface = _surface_label(getattr(r, "surface", None), getattr(r, "track_type", None))
        course = str(getattr(r, "course_type", "") or "").strip()
        dist = int(getattr(r, "distance", 0) or 0)
        cls = str(getattr(r, "race_class", "") or "").strip()

        parts = []
        if loc and loc != "-":
            parts.append(loc)
        if surface and surface != "-":
            parts.append(surface)
        if course:
            parts.append(f"跑道{course}")
        if dist > 0:
            parts.append(f"{dist}米")
        if cls:
            parts.append(cls)
        meta = "｜".join(parts)
        meta = f"｜{meta}" if meta else ""
        extra = ""
        if str(scenario_tag or "").strip():
            extra = f"｜情境：{str(scenario_tag).strip()}"
        elif str(going_code_override or "").strip():
            extra = f"｜情境：{str(going_code_override).strip()}"
        return f"# J18.HK AI 賽事前瞻分析\n**賽事：{date_s} 第 {int(getattr(r, 'race_no', 0) or 0)} 場{meta}{extra}**\n\n"
    
    fg_key = f"speedpro_formguide:{date_str}:{race_no}"
    cfg = session.query(SystemConfig).filter_by(key=fg_key).first()
    if not cfg or not isinstance(cfg.value, dict) or not cfg.value:
        return {"ok": False, "reason": "no_formguide_data"}
        
    # Get AI settings
    settings = load_ai_settings(session)
    api_key_info = load_ai_api_key(session)
    api_key = api_key_info.get("env") or api_key_info.get("stored")
    if not api_key:
        return {"ok": False, "reason": "missing_api_key"}
        
    # Get custom prompt from DB or use default
    prompt_cfg = session.query(SystemConfig).filter_by(key="ai_race_summary_prompt").first()
    if prompt_cfg and isinstance(prompt_cfg.value, dict) and "prompt" in prompt_cfg.value:
        base_prompt = prompt_cfg.value["prompt"]
    else:
        base_prompt = (
            "你是專業香港賽馬分析師。現在我提供這場賽事各匹馬的近期走勢評述（FormGuide），以及系統量化出來的客觀數據（包含檔位、負磅、評分、SpeedPRO能量分、騎練合作分、近期狀態分等）。\n"
            "請根據這些質化與量化數據進行深度綜合分析。\n\n"
            "請務必包含以下兩個版本：\n\n"
            "### 【簡潔版分析】\n"
            "- 使用列點方式，直接給出 1-5 匹你認為最值得留意的馬匹（寧缺勿濫），以及你真正有把握淘汰的馬匹。若賽事形勢極度混亂均勢，可不勉強推介，並重點於形勢上作出解說。\n"
            "- 必須標明 `[馬號] 馬名`。\n"
            "- 每匹馬用一句話總結原因（結合客觀因子與走勢評述）。\n\n"
            "### 【完整版分析】\n"
            "包含以下四個部分：\n"
            "1. **👀 焦點馬匹點評**：挑選出狀態正在回勇，或上仗因「意外/受困/走位差/不利步速」而落敗的「可原諒馬匹/黑馬」。必須標明 `[馬號] 馬名`，並結合其客觀因子進行解釋。\n"
            "2. **⚠️ 淘汰風險馬匹分析**：挑選出 1-2 匹你認為今場沒太大可能入圍的馬匹（反向分析）。例如：近期走勢持續疲弱、今仗面對極端不利檔位/步速、或能量數值與評述皆差的馬匹，並解釋為何看淡。\n"
            "3. **🏇 預期賽事形勢**：綜合各駒近仗步速與跑法，預測今場的步速偏快或偏慢？哪幾匹馬可能放頭？\n"
            "4. **💡 綜合結論與觀賽焦點**：給出整體的賽事定調與客觀的數據觀察建議。\n\n"
            "請注意：本報告純屬數據統計與學術研究，絕不構成任何博彩或投注建議。請務必以客觀中立的數據分析師口吻撰寫。\n"
            "請用繁體中文以清晰的 Markdown 格式輸出，直接給出分析，不要包含任何 json 或 markdown code block 標籤。"
        )
        
    # Fetch Objective factors from DB for this race (Req 4)
    from database.models import ScoringFactor
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    factors_by_horse = {}
    for e in entries:
        hno = str(e.horse_no)
        factors_by_horse[hno] = {
            "draw": e.draw,
            "weight": e.actual_weight,
            "rating": e.rating,
            "total_score": e.total_score,
        }
        
    # Fetch computed scores for key factors
    s_factors = session.query(ScoringFactor).join(RaceEntry).filter(RaceEntry.race_id == race_id).all()
    for f in s_factors:
        hno = str(f.entry.horse_no)
        if f.factor_name == "speedpro_energy": factors_by_horse[hno]["speedpro"] = f.score
        elif f.factor_name == "jockey_trainer_bond": factors_by_horse[hno]["jt"] = f.score
        elif f.factor_name == "recent_form": factors_by_horse[hno]["recent"] = f.score
        
    # Build the input text
    fg_data = cfg.value
    input_lines = [
        f"賽事：{date_str} 第 {race_no} 場",
        "以下是各匹馬的近期走勢評述與紀錄：\n"
    ]

    try:
        from database.models import RaceTrackCondition
        tc = session.query(RaceTrackCondition).filter_by(race_id=int(race.id)).first()
        going_code = str(going_code_override or "").strip()
        if not going_code:
            going_code = str(getattr(tc, "going_code", "") or "").strip()
        if not going_code:
            from scoring_engine.track_conditions import normalize_going
            _, going_code2 = normalize_going(str(getattr(race, "going", "") or ""))
            going_code = str(going_code2 or "").strip()
        if going_code:
            if str(going_code_override or "").strip():
                input_lines.insert(1, f"### 假設場地狀態（going_code）：{going_code}\n")
            from scoring_engine.track_profile import load_track_profile
            prof = load_track_profile(
                session,
                venue=str(getattr(race, "venue", "") or ""),
                going_code=going_code,
                course_type=str(getattr(race, "course_type", "") or ""),
                distance=int(getattr(race, "distance", 0) or 0),
            )
            if isinstance(prof, dict) and int(prof.get("n_races") or 0) > 0:
                input_lines.insert(
                    1,
                    "\n".join(
                        [
                            "### 跑道/場地狀態統計（歷史樣本摘要）",
                            f"- 分組：{prof.get('venue')}｜{prof.get('going_code')}｜跑道{prof.get('course_type')}｜距離分桶{prof.get('dist_bucket')}｜樣本 {int(prof.get('n_races') or 0)} 場",
                            f"- 入圍跑法（早段）：{json.dumps(prof.get('top4_style_early_pct') or prof.get('top4_style_pct') or {}, ensure_ascii=False)}",
                            f"- 入圍跑法（中段）：{json.dumps(prof.get('top4_style_mid_pct') or {}, ensure_ascii=False)}",
                            f"- 入圍跑法（末段）：{json.dumps(prof.get('top4_style_late_pct') or {}, ensure_ascii=False)}",
                            f"- 入圍跑法（綜合）：{json.dumps(prof.get('top4_style_composite_pct') or {}, ensure_ascii=False)}",
                            f"- 勝出跑法（綜合）：{json.dumps(prof.get('winner_style_composite_pct') or prof.get('winner_style_pct') or {}, ensure_ascii=False)}",
                            f"- 步速分布（勝出/入圍；樣本={int(prof.get('pace_races') or 0)}）：{json.dumps({'勝出': (prof.get('winner_pace_pct') or {}), '入圍Top4': (prof.get('top4_pace_pct') or {})}, ensure_ascii=False)}",
                            f"- 勝出 WinOdds（平均/中位）：{prof.get('winner_win_odds_avg')} / {prof.get('winner_win_odds_median')}",
                            f"- Top4 WinOdds（平均/中位）：{prof.get('top4_win_odds_avg')} / {prof.get('top4_win_odds_median')}",
                            "",
                        ]
                    ),
                )
    except Exception:
        pass
    
    for horse_no, h_data in sorted(fg_data.items(), key=lambda x: int(x[0])):
        if not isinstance(h_data, dict):
            continue
        h_name = h_data.get("horse_name", "")
        history = h_data.get("history", [])
        
        # Add basic factors
        f_info = factors_by_horse.get(str(horse_no), {})
        draw = f_info.get("draw", "?")
        weight = f_info.get("weight", "?")
        rating = f_info.get("rating", "?")
        
        input_lines.append(f"### [{horse_no}] {h_name} (檔位: {draw}, 負磅: {weight}, 評分: {rating})")
        
        # Add specific factor scores if available
        f_scores = []
        if "speedpro" in f_info: f_scores.append(f"SpeedPRO能量: {f_info['speedpro']:.1f}分")
        if "jt" in f_info: f_scores.append(f"騎練合作: {f_info['jt']:.1f}分")
        if "recent" in f_info: f_scores.append(f"近期狀態: {f_info['recent']:.1f}分")
        if f_scores:
            input_lines.append("系統量化因子: " + ", ".join(f_scores))
            
        if not history:
            input_lines.append("無近期紀錄\n")
            continue
            
        for i, rec in enumerate(history[:3]):  # Limit to last 3 runs to save tokens
            r_date = rec.get("racedate", "")
            dist = rec.get("dist", "")
            going = rec.get("going", "")
            fp = rec.get("fp", "")
            pace = rec.get("pace", "")
            wide = rec.get("wide", "")
            incident = rec.get("incident", "")
            comments = rec.get("comments", "")
            
            line = f"- {r_date} ({dist} {going}): 名次 {fp}"
            if pace: line += f", 步速: {pace}"
            if wide: line += f", 走位: {wide}"
            if incident: line += f", 意外: {incident}"
            line += f" | 評述: {comments}"
            
            input_lines.append(line)
        input_lines.append("")
        
    user_text = "\n".join(input_lines)
    
    # Inject learned rules
    from scoring_engine.ai_reflection import get_learned_rules
    learned_rules = get_learned_rules(session)
    rules_text = ""
    if learned_rules:
        rules_text = "\n### 【系統過往學習到的賽事法則】\n請在分析時，務必參考以下你過往自我檢討得出的法則（若適用於本場）：\n"
        for i, rule in enumerate(learned_rules, 1):
            rules_text += f"- {rule}\n"
            
    # Append JSON output instruction
    num_elim = max(1, int(len(entries) * 0.35))
    system_prompt = f"{base_prompt}\n\n{rules_text}"
    system_prompt += (
        "\n\n====================\n"
        "【輸出格式要求】\n"
        "請務必以 JSON 格式輸出，不要包含任何 markdown code block (如 ```json) 標籤。格式必須完全符合以下結構：\n"
        "{\n"
        "  \"top5_horse_nos\": [整數, ...], // 推薦馬匹 1-5 匹（按優先順序；寧缺勿濫）\n"
        f"  \"eliminated_horse_nos\": [整數, ...], // 最多 {num_elim} 匹淘汰馬。注意：若該場次勢均力敵，請只列出你【真正有把握】淘汰的馬匹，寧缺勿濫，數量可少於此數甚至留空 []\n"
        "  \"report\": \"你撰寫的完整 Markdown 分析報告內容 (請將簡潔版與完整版內容放在此字串中，並使用 \\n 換行)\"\n"
        "}\n"
    )
    
    resp = call_chat_completions(
        endpoint=settings["endpoint"],
        api_key=api_key,
        model_id=settings["model_id"],
        system_prompt=system_prompt,
        user_text=user_text,
        timeout_sec=90
    )
    
    if resp.get("ok"):
        raw_text = str(resp.get("text") or "")
        parsed_res = _extract_json_obj(raw_text)
        if parsed_res.get("ok") is True and isinstance(parsed_res.get("data"), dict):
            parsed = parsed_res["data"]
            report_text = parsed.get("report", "報告解析失敗")

            prefix = _race_prefix(race, date_str)
            top5 = parsed.get("top5_horse_nos", [])
            elim = parsed.get("eliminated_horse_nos", [])

            top5_original = top5
            top5_rerank_debug = None
            try:
                from scoring_engine.ai_rerank import rerank_top5
                rrk = rerank_top5(
                    session,
                    int(race_id),
                    top5,
                    factors_by_horse=factors_by_horse,
                    going_code_override=str(going_code_override or "").strip() or None,
                )
                top5_reranked = rrk.get("top5") if isinstance(rrk, dict) else None
                if isinstance(top5_reranked, list) and top5_reranked:
                    top5 = top5_reranked
                if isinstance(rrk, dict):
                    top5_rerank_debug = rrk.get("debug")
            except Exception:
                top5_original = top5
                top5_rerank_debug = None

            name_map = {}
            try:
                q_entries = (
                    session.query(RaceEntry.horse_no, RaceEntry)
                    .filter(RaceEntry.race_id == int(race_id))
                    .all()
                )
                for hn, e in q_entries:
                    try:
                        hn_i = int(hn or 0)
                    except Exception:
                        continue
                    nm = ""
                    try:
                        nm = str(getattr(getattr(e, "horse", None), "name_ch", "") or "").strip()
                    except Exception:
                        nm = ""
                    if hn_i > 0:
                        name_map[hn_i] = nm
            except Exception:
                name_map = {}

            def _fmt_horses(xs):
                out = []
                for x in xs or []:
                    try:
                        hn = int(x)
                    except Exception:
                        continue
                    if hn <= 0:
                        continue
                    nm = str(name_map.get(hn) or "").strip()
                    out.append(f"[{hn}] {nm}".strip() if nm else f"[{hn}]")
                return out

            top5_fmt = _fmt_horses(top5)
            elim_fmt = _fmt_horses(elim)
            summary_lines = []
            summary_lines.append("## ✅ AI 推薦名單（按優先）")
            if top5_fmt:
                for i, s in enumerate(top5_fmt, 1):
                    summary_lines.append(f"{i}. {s}")
            else:
                summary_lines.append("- （本場勢均力敵或資訊不足，AI 沒有給出明確推介）")

            summary_lines.append("")
            summary_lines.append("## ⚠️ AI 淘汰名單（僅列出有把握）")
            if elim_fmt:
                for s in elim_fmt:
                    summary_lines.append(f"- {s}")
            else:
                summary_lines.append("- （無）")

            summary_block = "\n".join(summary_lines).strip() + "\n\n"
            report_text = prefix + summary_block + str(report_text)

            # Save to DB for historical viewing (Req 2)
            if save_as_scenario:
                tag = str(scenario_tag or "").strip() or str(going_code_override or "").strip() or "SCENARIO"
                tag = tag.replace(":", "_")
                report_key = f"ai_race_report_scenario:{date_str}:{race_no}:{tag}"
            else:
                report_key = f"ai_race_report:{date_str}:{race_no}"
            report_cfg = session.query(SystemConfig).filter_by(key=report_key).first()
            if not report_cfg:
                report_cfg = SystemConfig(key=report_key, description=f"AI 賽事分析報告（racedate={date_str} R{race_no}）")
                session.add(report_cfg)

            report_cfg.value = {
                "report": report_text,
                "top5_horse_nos": top5,
                "top5_horse_nos_original": top5_original,
                "top5_rerank_debug": top5_rerank_debug,
                "eliminated_horse_nos": elim,
                "created_at": datetime.utcnow().isoformat(),
                "race_id": int(race_id),
                "scenario": (str(scenario_tag or "").strip() or str(going_code_override or "").strip() or None),
                "going_code_override": (str(going_code_override or "").strip() or None),
            }

            if not save_as_scenario:
                # Update prediction snapshots so AI predictions appear in member stats and hit stats tables
                top5_key = f"top5_snapshot:{date_str}:{race_no}"
                t5_cfg = session.query(SystemConfig).filter_by(key=top5_key).first()
                if not t5_cfg:
                    t5_cfg = SystemConfig(key=top5_key, description=f"Top 5 預測快照（racedate={date_str} R{race_no}）")
                    session.add(t5_cfg)
                t5_val = t5_cfg.value if isinstance(t5_cfg.value, dict) else {}
                t5_val["🤖 AI 賽事前瞻"] = top5
                t5_cfg.value = t5_val

                elim_key = f"elim_snapshot:{date_str}:{race_no}"
                el_cfg = session.query(SystemConfig).filter_by(key=elim_key).first()
                if not el_cfg:
                    el_cfg = SystemConfig(key=elim_key, description=f"反向預測淘汰快照（racedate={date_str} R{race_no}）")
                    session.add(el_cfg)
                el_val = el_cfg.value if isinstance(el_cfg.value, dict) else {}
                el_val["🤖 AI 賽事前瞻"] = elim
                el_cfg.value = el_val

                session.commit()

                # Update AI stats
                from scoring_engine.ai_stats import calculate_ai_hit_stats
                calculate_ai_hit_stats(session)
            else:
                session.commit()

            return {"ok": True, "summary": report_text}

        # JSON parsing failed: still save report for viewing, and keep raw for diagnostics
        report_text = _race_prefix(race, date_str) + raw_text
        report_key = f"ai_race_report:{date_str}:{race_no}"
        report_cfg = session.query(SystemConfig).filter_by(key=report_key).first()
        if not report_cfg:
            report_cfg = SystemConfig(key=report_key, description=f"AI 賽事分析報告（racedate={date_str} R{race_no}）")
            session.add(report_cfg)
        report_cfg.value = {
            "report": report_text,
            "created_at": datetime.utcnow().isoformat(),
            "parse_error": str(parsed_res.get("error") or ""),
        }
        session.commit()
        return {"ok": True, "summary": report_text, "reason": "json_parse_failed"}
    else:
        return {"ok": False, "reason": "api_error", "error": resp.get("error")}

def _hash_payload(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return sha256(raw).hexdigest()[:12]


def call_chat_completions(
    endpoint: str,
    api_key: str,
    model_id: str,
    system_prompt: str,
    user_text: str,
    timeout_sec: int = 60,
) -> Dict[str, Any]:
    ep = str(endpoint or "").strip()
    if not ep:
        ep = "https://api.openai.com/v1/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {str(api_key or '').strip()}"}
    body = {
        "model": str(model_id or "").strip(),
        "messages": [
            {"role": "system", "content": str(system_prompt or "").strip()},
            {"role": "user", "content": str(user_text or "").strip()},
        ],
        "temperature": 0.2,
    }
    r = requests.post(ep, headers=headers, json=body, timeout=int(timeout_sec or 60))
    try:
        data = r.json()
    except Exception:
        data = None
    if int(r.status_code) >= 400:
        return {"ok": False, "status": int(r.status_code), "error": str(data or r.text), "text": None, "raw": data}
    try:
        content = str(((data or {}).get("choices") or [])[0]["message"]["content"])
    except Exception:
        content = None
    return {"ok": True, "status": int(r.status_code), "error": None, "text": content, "raw": data}


def build_ai_user_prompt(payload: Dict[str, Any], extra_instructions: str = "") -> str:
    schema = default_ai_schema_hint()
    brief = {
        "date_range": payload.get("date_range"),
        "objective": payload.get("objective"),
        "tune_summary": payload.get("tune_summary"),
        "factor_quality": payload.get("factor_quality"),
        "factors": payload.get("factors"),
        "top_missing_reasons": payload.get("top_missing_reasons"),
    }
    lines = [
        "請根據以下 JSON 輸入，提出可執行的因子建議（新增/優化/刪減/調權/補數據）。",
        "輸出必須為 JSON，必須符合 schema_hint 的結構（可省略不適用欄位，但 key 名稱要一致）。",
        "每條建議都要包含 evidence、proposal、validation，並控制建議數量在 10 條以內。",
        "注意：請勿提供投注/賭博/博彩策略或回報相關建議；只針對資料品質、缺失原因、因子設計與權重調整提出建議。",
    ]
    if str(extra_instructions or "").strip():
        lines.append(str(extra_instructions).strip())
    return "\n".join(lines) + "\n\nschema_hint:\n" + json.dumps(schema, ensure_ascii=False, indent=2) + "\n\ninput:\n" + json.dumps(brief, ensure_ascii=False)


def run_ai_factor_advice(
    session: Session,
    d1: date,
    d2: date,
    top_k: int,
    max_suggest_weight: float,
    endpoint: str,
    model_id: str,
    system_prompt: str,
    api_key: str,
    extra_instructions: str = "",
) -> Dict[str, Any]:
    if not str(api_key or "").strip():
        return {"ok": False, "reason": "missing_api_key"}
    payload = build_factor_snapshot(session, d1=d1, d2=d2, top_k=int(top_k), max_suggest_weight=float(max_suggest_weight))
    user_text = build_ai_user_prompt(payload, extra_instructions=extra_instructions)
    resp = call_chat_completions(endpoint=endpoint, api_key=api_key, model_id=model_id, system_prompt=system_prompt, user_text=user_text)
    if resp.get("ok") is not True:
        return {
            "ok": False,
            "reason": "api_error",
            "status": resp.get("status"),
            "error": resp.get("error"),
            "meta": {"endpoint": str(endpoint or "").strip(), "model_id": str(model_id or "").strip(), "api_key": _mask_key(api_key)},
        }
    text = str(resp.get("text") or "")
    parsed = parse_json_response(text)
    out = {
        "ok": True,
        "created_at": datetime.utcnow().isoformat(),
        "request": {
            "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
            "top_k": int(top_k),
            "max_suggest_weight": float(max_suggest_weight),
            "endpoint": str(endpoint or "").strip(),
            "model_id": str(model_id or "").strip(),
            "payload_hash": _hash_payload(payload),
        },
        "payload": payload,
        "response_text": text,
        "parsed": parsed,
    }
    return out
