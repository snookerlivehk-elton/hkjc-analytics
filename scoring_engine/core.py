import pandas as pd
import numpy as np
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
from database.models import RaceEntry, ScoringFactor, ScoringWeight, SystemConfig, Race, HorseHistory
from utils.logger import logger

from scoring_engine.utils import calculate_relative_percentile, estimate_win_probability
from scoring_engine.constants import DISABLED_FACTORS

class ScoringEngine:
    """每場賽事獨立計分排名系統"""

    def __init__(self, session: Session):
        self.session = session
        self.weights = self._load_weights()

    def _load_weights(self) -> Dict[str, float]:
        """從資料庫載入權重配置"""
        from scoring_engine.factors import get_available_factors

        available = get_available_factors()
        weights = (
            self.session.query(ScoringWeight)
            .filter(ScoringWeight.is_active == True)
            .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
            .all()
        )
        # 確保資料庫中已有預設權重，否則初始化失敗
        out: Dict[str, float] = {}
        rows_by_name: Dict[str, ScoringWeight] = {}
        for w in weights:
            name = str(w.factor_name or "").strip()
            if not name:
                continue
            if name not in available:
                logger.warning(f"已啟用但未實作的因子將被略過：{name}")
                continue
            rows_by_name[name] = w
            out[name] = float(w.weight or 0.0)

        if out:
            s = 0.0
            for v in out.values():
                try:
                    s += abs(float(v or 0.0))
                except Exception:
                    continue
            if s < 1e-9:
                defaults = {
                    "jockey_trainer_bond": 1.0,
                    "horse_time_perf": 1.5,
                    "venue_dist_specialty": 1.0,
                    "draw_stats": 0.8,
                    "weight_rating_perf": 0.7,
                    "gear_change": 0.6,
                    "class_performance": 1.0,
                    "going_specialty": 0.8,
                    "speedpro_energy": 1.2,
                    "recent_form": 1.4,
                    "vet_rest_days": 0.5,
                    "debut_long_rest": 0.7,
                    "morning_trial_perf": 0.0,
                }
                for name in list(out.keys()):
                    w = rows_by_name.get(name)
                    if not w:
                        continue
                    new_w = float(defaults.get(name, 1.0))
                    w.weight = new_w
                    out[name] = new_w
                try:
                    self.session.commit()
                    logger.warning("偵測到全局權重合計為 0，已自動回復預設權重。")
                except Exception:
                    self.session.rollback()
        return out

    def _load_factor_quality_policy(self) -> Dict[str, Any]:
        cfg = self.session.query(SystemConfig).filter_by(key="factor_quality_policy").first()
        if cfg and isinstance(cfg.value, dict):
            return cfg.value
        return {"default": {"action": "warn", "min_coverage": 0.7}, "overrides": {}}

    def _upsert_system_config(self, key: str, value: Any, description: str = ""):
        cfg = self.session.query(SystemConfig).filter_by(key=key).first()
        if not cfg:
            cfg = SystemConfig(key=key, description=str(description or "").strip() or None)
            self.session.add(cfg)
        cfg.value = value

    def _missing_reason(
        self,
        factor_name: str,
        display: str,
        row: Dict[str, Any],
        speedpro_state: Dict[str, Any],
        horse_has_history: Dict[int, bool],
    ) -> str:
        fn = str(factor_name or "").strip()
        d = str(display or "").strip()
        if fn == "speedpro_energy":
            if not speedpro_state.get("has_data"):
                if speedpro_state.get("had_retry"):
                    return "SpeedPRO未追到"
                return "SpeedPRO未抓取/未公佈"
            if speedpro_state.get("last_error"):
                return "SpeedPRO抓取失敗"
            return "SpeedPRO缺馬/缺欄位"

        draw = row.get("draw")
        rating = row.get("rating")
        weight = row.get("weight")
        if draw is None or rating is None or weight is None:
            return "排位/基本資料缺失"

        horse_id = row.get("horse_id")
        try:
            hid = int(horse_id) if horse_id is not None else 0
        except Exception:
            hid = 0
        if hid and horse_has_history.get(hid) is False:
            return "往績未回填"

        if "樣本不足" in d or "不足" in d:
            return "樣本不足"
        if "未公佈" in d or "更新中" in d:
            return "未公佈/待更新"
        if "不適用" in d:
            return "不適用"
        if d in {"", "無數據"}:
            return "無數據"
        return "其他/未知"

    def score_race(self, race_id: int):
        """核心函數：對單場賽事的所有出賽馬匹進行獨立計分、相對排名"""
        # 1. 獲取本場所有參賽馬匹 (Entries)
        entries = self.session.query(RaceEntry).filter_by(race_id=race_id).all()
        if not entries:
            logger.warning(f"賽事 ID {race_id} 無參賽馬匹數據")
            return

        # 2. 準備數據框 (DataFrame) 以進行批量相對排名計算
        horse_data = []
        for entry in entries:
            horse_data.append({
                "race_id": race_id,
                "entry_id": entry.id,
                "horse_id": entry.horse_id,
                "horse_code": entry.horse.code if entry.horse else "",
                "horse_no": entry.horse_no,
                "jockey_name": entry.jockey.name_ch if entry.jockey else "",
                "trainer_name": entry.trainer.name_ch if entry.trainer else "",
                "draw": entry.draw,
                "rating": entry.rating,
                "weight": entry.actual_weight,
            })
        
        df = pd.DataFrame(horse_data)
        
        # 3. 計算所有啟用的獨立計分條件的「原始分 (Raw Value)」
        from scoring_engine.factors import FactorCalculator
        calculator = FactorCalculator(self.session, df)
        
        factor_raw_scores = {}
        factor_displays = {}
        for factor_name in self.weights.keys():
            raw_scores, displays = calculator.calculate(factor_name)
            factor_raw_scores[factor_name] = raw_scores
            factor_displays[factor_name] = displays

        # 4. 核心功能：進行相對百分位排名 (0-10 分)
        # 確保所有分數都在本場馬匹內做相對比較
        scored_df = df.copy()
        for factor_name, raw_vals in factor_raw_scores.items():
            scored_df[f"{factor_name}_raw"] = raw_vals
            scored_df[f"{factor_name}_score"] = calculate_relative_percentile(raw_vals, score_range=(0, 10))
            scored_df[f"{factor_name}_display"] = factor_displays[factor_name]

        # 5. 計算加權總分與排名
        policy = self._load_factor_quality_policy()
        default_policy = policy.get("default") if isinstance(policy.get("default"), dict) else {}
        overrides = policy.get("overrides") if isinstance(policy.get("overrides"), dict) else {}
        weights_at_time = dict(self.weights)
        factor_quality: Dict[str, Any] = {}
        n_field = int(len(scored_df))
        race = self.session.get(Race, int(race_id)) if race_id else None
        date_str = None
        race_no = int(getattr(race, "race_no", 0) or 0) if race else 0
        try:
            rd = getattr(race, "race_date", None)
            if rd is not None and hasattr(rd, "date"):
                date_str = rd.date().strftime("%Y/%m/%d")
        except Exception:
            date_str = None

        speedpro_state: Dict[str, Any] = {"has_data": False, "had_retry": False, "last_error": None}
        if date_str and race_no:
            sp = self.session.query(SystemConfig).filter_by(key=f"speedpro_energy:{date_str}:{race_no}").first()
            if sp and isinstance(sp.value, dict) and sp.value:
                speedpro_state["has_data"] = True
            rr = self.session.query(SystemConfig).filter_by(key=f"speedpro_retry:{date_str}:{race_no}").first()
            rv = rr.value if rr and isinstance(rr.value, dict) else {}
            if rv:
                speedpro_state["had_retry"] = True
                speedpro_state["last_error"] = rv.get("last_error")

        horse_ids = []
        try:
            horse_ids = [int(x) for x in scored_df["horse_id"].tolist() if x is not None]
        except Exception:
            horse_ids = []
        horse_ids = sorted(set([x for x in horse_ids if x > 0]))
        horse_has_history: Dict[int, bool] = {hid: False for hid in horse_ids}
        if horse_ids:
            rows_h = (
                self.session.query(HorseHistory.horse_id, func.count(HorseHistory.id))
                .filter(HorseHistory.horse_id.in_(horse_ids))
                .group_by(HorseHistory.horse_id)
                .all()
            )
            for hid, cnt in rows_h:
                try:
                    horse_has_history[int(hid)] = int(cnt or 0) > 0
                except Exception:
                    continue

        for factor_name in list(self.weights.keys()):
            disp = scored_df.get(f"{factor_name}_display")
            if disp is None:
                missing_cnt = n_field
            else:
                d = disp.fillna("").astype(str).str.strip()
                missing_cnt = int(((d == "") | (d == "無數據")).sum())
            coverage = (1.0 - (missing_cnt / float(n_field))) if n_field else 0.0

            ov = overrides.get(factor_name) if isinstance(overrides.get(factor_name), dict) else {}
            action = str((ov.get("action") if isinstance(ov, dict) else None) or default_policy.get("action") or "warn").strip().lower()
            min_cov = (ov.get("min_coverage") if isinstance(ov, dict) else None)
            if min_cov is None:
                min_cov = default_policy.get("min_coverage")
            try:
                min_cov = float(min_cov if min_cov is not None else 0.0)
            except Exception:
                min_cov = 0.0
            if min_cov > 1.0:
                min_cov = min_cov / 100.0
            if min_cov < 0.0:
                min_cov = 0.0
            if min_cov > 1.0:
                min_cov = 1.0

            ignored = False
            if action == "ignore" and coverage < min_cov:
                weights_at_time[factor_name] = 0.0
                ignored = True
            elif action == "warn" and coverage < min_cov:
                logger.warning(f"因子資料覆蓋不足：{factor_name} 覆蓋率 {coverage:.1%} (< {min_cov:.0%})")

            reason_counts: Dict[str, int] = {}
            if disp is None:
                reason_counts["無數據"] = int(n_field or 0)
            else:
                dd = disp.fillna("").astype(str).str.strip()
                for i in range(len(dd)):
                    if str(dd.iloc[i] or "").strip() not in {"", "無數據"}:
                        continue
                    row = scored_df.iloc[i].to_dict()
                    r = self._missing_reason(
                        factor_name=factor_name,
                        display=str(dd.iloc[i] or ""),
                        row=row,
                        speedpro_state=speedpro_state,
                        horse_has_history=horse_has_history,
                    )
                    reason_counts[r] = int(reason_counts.get(r) or 0) + 1

            factor_quality[factor_name] = {
                "field_size": n_field,
                "missing": missing_cnt,
                "coverage": coverage,
                "action": action,
                "min_coverage": min_cov,
                "weight": float(self.weights.get(factor_name) or 0.0),
                "effective_weight": float(weights_at_time.get(factor_name) or 0.0),
                "ignored": bool(ignored),
                "reasons": reason_counts,
            }

        factor_quality_meta: Dict[str, Any] = {}
        eff_sum = 0.0
        base_sum = 0.0
        for v in weights_at_time.values():
            try:
                eff_sum += abs(float(v or 0.0))
            except Exception:
                continue
        for v in self.weights.values():
            try:
                base_sum += abs(float(v or 0.0))
            except Exception:
                continue
        if base_sum > 0.0 and eff_sum < 1e-9 and factor_quality:
            weights_at_time = dict(self.weights)
            for fn, info in factor_quality.items():
                if isinstance(info, dict):
                    info["effective_weight"] = float(weights_at_time.get(fn) or 0.0)
                    info["ignored"] = False
            factor_quality_meta = {"fallback": "all_factors_ignored"}
            logger.warning("本場因子資料不足策略導致全因子被忽略，已自動回退為只提示（避免 total_score 全為 0）。")

        total_score = np.zeros(len(scored_df))
        for factor_name, weight in weights_at_time.items():
            total_score += scored_df[f"{factor_name}_score"] * float(weight or 0.0)
        
        scored_df["total_score"] = total_score
        # 總分越高，名次越前 (rank 1)
        scored_df["rank_in_race"] = scored_df["total_score"].rank(ascending=False, method='min').astype(int)
        
        # 6. 估計勝出概率 (Win Probability)
        try:
            from scoring_engine.calibration import load_winprob_temperature

            t = load_winprob_temperature(self.session)
        except Exception:
            t = None
        scored_df["win_probability"] = estimate_win_probability(scored_df["total_score"], temperature=float(t) if t else 1.0)

        # 7. 將結果持久化到資料庫
        self._save_results(scored_df, weights_at_time=weights_at_time)
        try:
            self._upsert_system_config(
                key=f"factor_quality:{int(race_id)}",
                value={"race_id": int(race_id), "field_size": n_field, "factors": factor_quality, "meta": factor_quality_meta},
                description="因子資料完整度（按場次）",
            )
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.warning(f"寫入因子資料完整度失敗: {e}")
        
        logger.info(f"賽事 ID {race_id} 計分排名完成 (成功計算 {len(scored_df)} 匹馬)")
        return scored_df

    def _save_results(self, df: pd.DataFrame, weights_at_time: Dict[str, float]):
        """儲存計分結果與因子得分"""
        for _, row in df.iterrows():
            entry = self.session.get(RaceEntry, row["entry_id"])
            if entry:
                entry.total_score = row["total_score"]
                entry.rank_in_race = row["rank_in_race"]
                entry.win_probability = row["win_probability"]
                
                # 儲存個別因子得分 (ScoringFactor 表)
                for factor_name in self.weights.keys():
                    sf = self.session.query(ScoringFactor).filter_by(
                        entry_id=entry.id, factor_name=factor_name
                    ).first()
                    if not sf:
                        sf = ScoringFactor(entry_id=entry.id, factor_name=factor_name)
                        self.session.add(sf)
                    
                    raw_val = row.get(f"{factor_name}_raw")
                    if raw_val is None or (isinstance(raw_val, float) and np.isnan(raw_val)):
                        sf.raw_value = None
                    else:
                        try:
                            sf.raw_value = float(raw_val)
                        except Exception:
                            sf.raw_value = None
                    sf.score = row[f"{factor_name}_score"]
                    sf.raw_data_display = row[f"{factor_name}_display"]
                    sf.weight_at_time = float(weights_at_time.get(factor_name) or 0.0)
        
        self.session.commit()
