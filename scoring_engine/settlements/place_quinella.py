from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from scoring_engine.settlements.registry import register


def _normalize_pool(s: str) -> str:
    x = str(s or "").strip().lower()
    x = re.sub(r"\s+", " ", x)
    x = x.replace("：", ":")
    return x


def _is_place_quinella_pool(pool: str) -> bool:
    p = _normalize_pool(pool)
    if "place quinella" in p:
        return True
    if "位置q" in p:
        return True
    if p in {"pq", "位置q"}:
        return True
    return False


def _pair_key(a: int, b: int) -> str:
    x = int(a)
    y = int(b)
    if x <= 0 or y <= 0:
        return ""
    lo, hi = (x, y) if x <= y else (y, x)
    return f"{lo}-{hi}"


def _parse_combo_to_pair_key(s: str) -> str:
    nums = re.findall(r"\d+", str(s or ""))
    if len(nums) < 2:
        return ""
    try:
        a = int(nums[0])
        b = int(nums[1])
    except Exception:
        return ""
    return _pair_key(a, b)


def _dividend_map(dividends: Optional[List[Dict[str, Any]]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for item in dividends or []:
        if not isinstance(item, dict):
            continue
        if not _is_place_quinella_pool(str(item.get("pool") or "")):
            continue
        k = _parse_combo_to_pair_key(str(item.get("combination") or ""))
        if not k:
            continue
        v = item.get("dividend")
        try:
            out[k] = float(v)
        except Exception:
            continue
    return out


def _pairs_from_top3(top3: List[int]) -> List[Tuple[int, int]]:
    if len(top3) < 3:
        return []
    a, b, c = int(top3[0]), int(top3[1]), int(top3[2])
    return [(a, b), (a, c), (b, c)]


class PlaceQuinellaPQ3V1:
    plugin_key = "hkjc.place_quinella.pq3_v1"

    def __init__(self, *, stake_per_bet: float = 10.0):
        self.stake_per_bet = float(stake_per_bet)

    def settle(
        self,
        *,
        race_id: int,
        pred_top5: List[int],
        actual_top5: List[int],
        dividends: Optional[List[Dict[str, Any]]],
        settled_at: str,
    ) -> Optional[Dict[str, Any]]:
        pred_top3 = [int(x) for x in (pred_top5 or [])[:3] if int(x or 0) > 0]
        act_top3 = [int(x) for x in (actual_top5 or [])[:3] if int(x or 0) > 0]
        if len(pred_top3) < 3 or len(act_top3) < 3:
            return None

        act_set = set(act_top3)
        div_map = _dividend_map(dividends)

        bets = []
        payout = 0.0
        hit_count = 0
        missing_dividend = 0

        for a, b in _pairs_from_top3(pred_top3):
            key = _pair_key(a, b)
            if not key:
                continue
            hit = (a in act_set) and (b in act_set)
            d = div_map.get(key)
            if hit:
                hit_count += 1
                if d is None:
                    missing_dividend += 1
                else:
                    payout += float(d) * (self.stake_per_bet / 10.0)
            bets.append({"pair": key, "hit": bool(hit), "dividend": (float(d) if d is not None else None)})

        cost = float(self.stake_per_bet) * 3.0
        profit = payout - cost
        roi = (profit / cost) if cost > 0 else None

        return {
            "pool": "place_quinella",
            "metric": "pq3",
            "race_id": int(race_id),
            "pred_top3": pred_top3,
            "actual_top3": act_top3,
            "bets": bets,
            "hit_count": int(hit_count),
            "stake_per_bet": float(self.stake_per_bet),
            "bets_per_race": 3,
            "cost": float(cost),
            "payout": float(round(payout, 6)),
            "profit": float(round(profit, 6)),
            "roi": (float(round(roi, 8)) if isinstance(roi, float) else None),
            "missing_dividend_bets": int(missing_dividend),
            "settled_at": str(settled_at),
        }


register(PlaceQuinellaPQ3V1())

