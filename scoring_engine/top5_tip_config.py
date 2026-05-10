from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from scoring_engine.config_value import unwrap_value, wrap_value


TIP_CONFIG_KEY = "top5_tip_config"


def default_tip_config() -> Dict[str, Any]:
    return {
        "enabled": False,
        "stats_days": 180,
        "min_samples": 10,
        "min_place_rate": 0.40,
        "min_win_rate": 0.20,
        "positions": [1, 2, 3, 4, 5],
        "odds_buckets": ["LT7", "B7_10", "B10_15", "B15_20", "B20_35", "GE35", "UNKNOWN"],
        "predictor_types": ["preset", "factor", "ai"],
        "odds_source": "pre_race_latest",
        "max_tips": 20,
    }


def load_tip_config(session: Session) -> Dict[str, Any]:
    from database.models import SystemConfig

    cfg = session.query(SystemConfig).filter_by(key=TIP_CONFIG_KEY).first()
    base = default_tip_config()
    if not cfg:
        return base
    payload, _ = unwrap_value(cfg.value)
    if not isinstance(payload, dict):
        return base
    out = dict(base)
    for k, v in payload.items():
        out[str(k)] = v
    return out


def save_tip_config(session: Session, config: Dict[str, Any]) -> None:
    from database.models import SystemConfig

    cfg = session.query(SystemConfig).filter_by(key=TIP_CONFIG_KEY).first()
    if not cfg:
        cfg = SystemConfig(key=TIP_CONFIG_KEY, description="Top5 貼士提示設定")
        session.add(cfg)
    cfg.value = wrap_value(dict(config or {}), {"source": "TIP_CONFIG"})
    session.commit()

