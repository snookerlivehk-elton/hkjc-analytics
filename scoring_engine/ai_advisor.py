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
