import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from database.models import RaceResult, RaceEntry, OddsHistory, Workout, VetReport
from typing import Dict, Any

class FactorCalculator:
    """獨立計分條件的具體計算邏輯"""

    def __init__(self, session: Session, df: pd.DataFrame):
        self.session = session
        self.df = df # 本場賽事的參賽馬匹 DataFrame

    def calculate(self, factor_name: str):
        """根據因子名稱調用相應的計算函數，返回 (原始分數 Series, 原始數據顯示 Series)"""
        method_name = f"_calculate_{factor_name}"
        if hasattr(self, method_name):
            return getattr(self, method_name)()
        else:
            # 預設回傳 0.0 (中性分數) 與空字串
            return pd.Series(0.0, index=self.df.index), pd.Series("無數據", index=self.df.index)


    def _to_int(self, v, default=0):
        try:
            if v is None:
                return default
            return int(v)
        except (ValueError, TypeError):
            return default

    def _race_cutoff_dt(self):
        from datetime import datetime, time
        from database.models import Race

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        race_date = getattr(race, "race_date", None) if race else None
        if isinstance(race_date, datetime):
            d = race_date.date()
        else:
            d = datetime.now().date()
        return datetime.combine(d, time.min)

    def _parse_class_num(self, s: str):
        import re

        if not s:
            return None

        m = re.search(r'Class\s*([0-9]+)', s, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None

        m = re.search(r'第\s*([0-9]+)\s*班', s)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None

        m = re.search(r'第\s*([三四五])\s*班', s)
        if m:
            return {"三": 3, "四": 4, "五": 5}.get(m.group(1))

        if str(s).strip() in {"3", "4", "5"}:
            return int(str(s).strip())

        return None

    # 1. 騎師＋練馬師合作 (J/T Bond)
    def _calculate_jockey_trainer_bond(self):
        from datetime import datetime
        from database.models import HorseHistory, SystemConfig, Race
        scores = []
        displays = []

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {
            "global_window": 0,
            "global_win_w": 0.7,
            "global_place_w": 0.3,
            "horse_window": 0,
            "horse_win_w": 0.7,
            "horse_place_w": 0.3,
            "global_weight": 0.5,
            "horse_weight": 0.5,
            "prior_strength_global": 12.0,
            "prior_strength_horse": 8.0,
            "prior_win_rate": 0.08,
            "prior_place_rate": 0.28,
            "confidence_runs_global": 12.0,
            "confidence_runs_horse": 8.0,
            "horse_weight_full_runs": 8.0,
        }

        try:
            config = self.session.query(SystemConfig).filter_by(key="jt_bond_combined_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                for k in cfg.keys():
                    if k in v:
                        cfg[k] = type(cfg[k])(v[k])
            else:
                # 嘗試讀取舊設定
                old_cfg = self.session.query(SystemConfig).filter_by(key="jt_bond_config").first()
                if old_cfg and isinstance(old_cfg.value, dict):
                    cfg["global_window"] = int(old_cfg.value.get("window", 0))
                    cfg["global_win_w"] = float(old_cfg.value.get("win", 0.7))
                    cfg["global_place_w"] = float(old_cfg.value.get("place", 0.3))
        except Exception:
            pass

        # 正規化權重
        for prefix in ["global_", "horse_"]:
            ww = cfg[prefix + "win_w"]
            pw = cfg[prefix + "place_w"]
            if ww < 0: ww = 0.0
            if pw < 0: pw = 0.0
            tw = ww + pw
            if tw <= 0:
                ww, pw, tw = 0.7, 0.3, 1.0
            cfg[prefix + "win_w"] = ww / tw
            cfg[prefix + "place_w"] = pw / tw

        gw = cfg["global_weight"]
        hw = cfg["horse_weight"]
        if gw < 0: gw = 0.0
        if hw < 0: hw = 0.0
        thw = gw + hw
        if thw <= 0:
            gw, hw, thw = 0.5, 0.5, 1.0
        gw /= thw
        hw /= thw

        def _clip01(x):
            try:
                v = float(x)
            except Exception:
                v = 0.0
            if v < 0.0:
                v = 0.0
            if v > 1.0:
                v = 1.0
            return v

        def _safe_pos(x, default=0.0):
            try:
                v = float(x)
            except Exception:
                v = float(default)
            if v < 0.0:
                v = 0.0
            return v

        prior_win = _clip01(cfg.get("prior_win_rate"))
        prior_place = _clip01(cfg.get("prior_place_rate"))
        ps_g = _safe_pos(cfg.get("prior_strength_global"), default=12.0)
        ps_h = _safe_pos(cfg.get("prior_strength_horse"), default=8.0)
        cr_g = _safe_pos(cfg.get("confidence_runs_global"), default=12.0)
        cr_h = _safe_pos(cfg.get("confidence_runs_horse"), default=8.0)
        hw_full = _safe_pos(cfg.get("horse_weight_full_runs"), default=8.0)
        if hw_full < 1.0:
            hw_full = 1.0

        for _, row in self.df.iterrows():
            jockey = row.get("jockey_name", "")
            trainer = row.get("trainer_name", "")
            horse_id = row.get("horse_id", None)
            
            if not jockey or not trainer:
                scores.append(0.0)
                displays.append("無騎練資料")
                continue
                
            # --- 1. 計算全庫合作 ---
            q_global = self.session.query(HorseHistory).filter(
                HorseHistory.jockey_name == jockey,
                HorseHistory.trainer_name == trainer
            ).filter(HorseHistory.race_date < cutoff_dt).order_by(HorseHistory.race_date.desc())
            
            if cfg["global_window"] > 0:
                q_global = q_global.limit(cfg["global_window"])

            try:
                hist_global = q_global.all()
            except Exception:
                hist_global = []
                
            runs_global = len(hist_global)
            w_g = sum(1 for h in hist_global if h.rank == 1)
            p_g = sum(1 for h in hist_global if h.rank in (1, 2, 3))
            wr_g = (w_g / runs_global) if runs_global > 0 else None
            pr_g = (p_g / runs_global) if runs_global > 0 else None
            swr_g = (float(w_g) + prior_win * ps_g) / (float(runs_global) + ps_g) if (runs_global > 0 or ps_g > 0) else 0.0
            spr_g = (float(p_g) + prior_place * ps_g) / (float(runs_global) + ps_g) if (runs_global > 0 or ps_g > 0) else 0.0
            score_global_raw = (swr_g * float(cfg["global_win_w"])) + (spr_g * float(cfg["global_place_w"]))
            conf_g = ((float(runs_global) + ps_g) / (float(runs_global) + ps_g + cr_g)) if (cr_g > 0) else 1.0
            score_global = float(score_global_raw) * float(conf_g)
            if runs_global > 0 and wr_g is not None and pr_g is not None:
                str_global = f"全({runs_global}次,勝{wr_g*100:.0f}%,位{pr_g*100:.0f}%)→平滑(勝{swr_g*100:.0f}%,位{spr_g*100:.0f}%)×信{conf_g:.2f}"
            else:
                str_global = f"全庫0→先驗(勝{prior_win*100:.0f}%,位{prior_place*100:.0f}%)×信{conf_g:.2f}"

            # --- 2. 計算本駒合作 ---
            score_horse = 0.0
            str_horse = "本駒0"
            runs_horse = 0
            
            if horse_id:
                q_horse = self.session.query(HorseHistory).filter(
                    HorseHistory.horse_id == int(horse_id),
                    HorseHistory.jockey_name == jockey,
                    HorseHistory.trainer_name == trainer
                ).filter(HorseHistory.race_date < cutoff_dt).order_by(HorseHistory.race_date.desc())
                
                if cfg["horse_window"] > 0:
                    q_horse = q_horse.limit(cfg["horse_window"])

                try:
                    hist_horse = q_horse.all()
                except Exception:
                    hist_horse = []
                    
                runs_horse = len(hist_horse)
                w_h = sum(1 for h in hist_horse if h.rank == 1)
                p_h = sum(1 for h in hist_horse if h.rank in (1, 2, 3))
                wr_h = (w_h / runs_horse) if runs_horse > 0 else None
                pr_h = (p_h / runs_horse) if runs_horse > 0 else None
                swr_h = (float(w_h) + prior_win * ps_h) / (float(runs_horse) + ps_h) if (runs_horse > 0 or ps_h > 0) else 0.0
                spr_h = (float(p_h) + prior_place * ps_h) / (float(runs_horse) + ps_h) if (runs_horse > 0 or ps_h > 0) else 0.0
                score_horse_raw = (swr_h * float(cfg["horse_win_w"])) + (spr_h * float(cfg["horse_place_w"]))
                conf_h = ((float(runs_horse) + ps_h) / (float(runs_horse) + ps_h + cr_h)) if (cr_h > 0) else 1.0
                score_horse = float(score_horse_raw) * float(conf_h)
                if runs_horse > 0 and wr_h is not None and pr_h is not None:
                    str_horse = f"本({runs_horse}次,勝{wr_h*100:.0f}%,位{pr_h*100:.0f}%)→平滑(勝{swr_h*100:.0f}%,位{spr_h*100:.0f}%)×信{conf_h:.2f}"
                else:
                    str_horse = f"本駒0→先驗×信{conf_h:.2f}"

            # 綜合計分
            scale_hw = min(1.0, (float(runs_horse) / float(hw_full))) if runs_horse > 0 else 0.0
            hw_eff = float(hw) * float(scale_hw)
            if hw_eff < 0.0:
                hw_eff = 0.0
            if hw_eff > 1.0:
                hw_eff = 1.0
            gw_eff = 1.0 - hw_eff
            bond_score = (float(score_global) * float(gw_eff)) + (float(score_horse) * float(hw_eff))
            scores.append(bond_score)
            displays.append(f"{str_global} | {str_horse} | 合併(g={gw_eff:.2f},h={hw_eff:.2f})")
            
        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)
    # 2. 馬匹分段時間＋完成時間 (Horse Time Perf)
    def _calculate_horse_time_perf(self):
        from datetime import datetime, timedelta
        import math
        import re
        from database.models import HorseHistory, Race, SystemConfig

        def parse_finish_time_to_seconds(s: str):
            v = str(s or "").strip()
            if not v:
                return None
            v = v.replace(" ", "")
            v = v.replace("．", ".").replace("：", ":")
            v = re.sub(r"[^0-9:\.]", "", v)
            if not v:
                return None

            if ":" in v:
                parts = v.split(":")
                if len(parts) != 2:
                    return None
                try:
                    m = int(parts[0])
                except ValueError:
                    return None
                sec_str = parts[1]
                try:
                    sec = float(sec_str)
                except ValueError:
                    return None
                if sec < 0:
                    return None
                return m * 60.0 + sec

            if v.count(".") >= 2:
                p = v.split(".")
                try:
                    m = int(p[0])
                    s2 = int(p[1])
                    frac = int(p[2])
                except ValueError:
                    return None
                if m < 0 or s2 < 0 or s2 >= 60:
                    return None
                return m * 60.0 + s2 + (frac / (100.0 if frac >= 10 else 10.0))

            try:
                sec = float(v)
            except ValueError:
                return None
            if sec <= 0:
                return None
            return sec

        def norm_track(s: str):
            return str(s or "").strip().replace(" ", "")

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None

        distance = self._to_int(getattr(race, "distance", 0), default=0) if race else 0
        track_key = str(getattr(race, "track_type", "") if race else "").strip()
        track_key_norm = norm_track(track_key)

        surface = ""
        if ("全天候" in track_key) or ("泥地" in track_key):
            surface = "泥地"
        elif "草地" in track_key:
            surface = "草地"
        else:
            g = getattr(race, "going", "") if race else ""
            if g in ("草地", "泥地"):
                surface = g

        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {
            "min_samples": 3,
            "confidence_runs": 12.0,
            "prior_strength": 12.0,
            "fallback_strategy": "A_B_C",
            "window_days": 720,
            "use_quantile": 0.2,
            "pct_tau": 0.012,
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="horse_time_perf_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "min_samples" in v:
                    cfg["min_samples"] = int(v["min_samples"])
                if "confidence_runs" in v:
                    cfg["confidence_runs"] = float(v["confidence_runs"])
                if "prior_strength" in v:
                    cfg["prior_strength"] = float(v["prior_strength"])
                if "fallback_strategy" in v:
                    cfg["fallback_strategy"] = str(v["fallback_strategy"])
                if "window_days" in v:
                    cfg["window_days"] = int(v["window_days"])
                if "use_quantile" in v:
                    cfg["use_quantile"] = float(v["use_quantile"])
                if "pct_tau" in v:
                    cfg["pct_tau"] = float(v["pct_tau"])
        except Exception:
            pass

        if cfg["min_samples"] < 0:
            cfg["min_samples"] = 0
        if float(cfg["confidence_runs"] or 0.0) <= 0:
            cfg["confidence_runs"] = 1.0
        if float(cfg["prior_strength"] or 0.0) < 0:
            cfg["prior_strength"] = 0.0
        if cfg["window_days"] < 0:
            cfg["window_days"] = 0
        if cfg["fallback_strategy"] not in ("A_B_C", "B_C", "C"):
            cfg["fallback_strategy"] = "A_B_C"
        try:
            qv = float(cfg["use_quantile"])
        except Exception:
            qv = 0.2
        if qv < 0.0:
            qv = 0.0
        if qv > 1.0:
            qv = 1.0
        cfg["use_quantile"] = qv
        try:
            pct_tau = float(cfg["pct_tau"])
        except Exception:
            pct_tau = 0.012
        if pct_tau <= 0:
            pct_tau = 0.012
        cfg["pct_tau"] = pct_tau

        def _quantile(sorted_vals, q: float):
            n = len(sorted_vals or [])
            if n <= 0:
                return None
            if n == 1:
                return float(sorted_vals[0])
            pos = float(q) * float(n - 1)
            lo = int(math.floor(pos))
            hi = int(math.ceil(pos))
            if lo < 0:
                lo = 0
            if hi >= n:
                hi = n - 1
            if lo == hi:
                return float(sorted_vals[lo])
            w = pos - float(lo)
            return float(sorted_vals[lo]) * (1.0 - float(w)) + float(sorted_vals[hi]) * float(w)

        cutoff_date = (race_date - timedelta(days=cfg["window_days"])) if cfg["window_days"] > 0 else None

        rows = []
        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)
            rows.append(horse_id)

        cached = {}
        best_secs = [None] * len(rows)
        best_n = [0] * len(rows)
        best_mode = [""] * len(rows)

        for i, horse_id in enumerate(rows):
            if not horse_id or not distance:
                continue

            q = (
                self.session.query(HorseHistory.finish_time, HorseHistory.race_date, HorseHistory.venue, HorseHistory.surface)
                .filter(
                    HorseHistory.horse_id == horse_id,
                    HorseHistory.distance == distance,
                    HorseHistory.rank > 0,
                    HorseHistory.race_date < cutoff_dt
                )
                .order_by(HorseHistory.race_date.desc())
            )
            if cutoff_date:
                q = q.filter(HorseHistory.race_date >= cutoff_date)
            hist = q.all()

            times_A = []
            times_B = []
            times_C = []
            for ft, _dt, v, sf in hist:
                sec = parse_finish_time_to_seconds(ft)
                if sec is None:
                    continue
                times_C.append(sec)
                if surface and sf == surface:
                    times_B.append(sec)
                if track_key_norm and norm_track(v) == track_key_norm:
                    times_A.append(sec)

            modes = []
            if cfg["fallback_strategy"] == "A_B_C":
                modes = [("A", times_A), ("B", times_B), ("C", times_C)]
            elif cfg["fallback_strategy"] == "B_C":
                modes = [("B", times_B), ("C", times_C)]
            else:
                modes = [("C", times_C)]

            chosen = None
            chosen_n = 0
            chosen_mode = ""
            for m, ts in modes:
                if len(ts) >= cfg["min_samples"]:
                    tss = sorted([float(x) for x in ts])
                    chosen = _quantile(tss, float(cfg["use_quantile"]))
                    chosen_n = len(ts)
                    chosen_mode = m
                    break

            if chosen is not None:
                best_secs[i] = chosen
                best_n[i] = chosen_n
                best_mode[i] = chosen_mode

        sig = f"Q{int(float(cfg['use_quantile']) * 100)}|N{cfg['min_samples']}|PS{float(cfg['prior_strength']):.0f}|C{float(cfg['confidence_runs']):.0f}|T{float(cfg['pct_tau']):.4f}|{cfg['fallback_strategy']}"

        avail = [v for v in best_secs if v is not None]
        if not avail:
            raw_scores = pd.Series([0.0] * len(rows), index=self.df.index)
            display = pd.Series([f"無賽績參考 | {sig}"] * len(rows), index=self.df.index)
            return raw_scores, display

        t_min = min(avail)

        raw_vals = []
        displays = []
        for i, horse_id in enumerate(rows):
            t = best_secs[i]
            if t is None:
                raw_vals.append(None)
                displays.append(f"無賽績參考 | {sig}")
                continue

            gap = float(t) - float(t_min)
            gap_pct = (gap / float(t_min)) if float(t_min) > 0 else 0.0
            if gap_pct < 0.0:
                gap_pct = 0.0
            base = math.exp(-float(gap_pct) / float(cfg["pct_tau"])) if float(cfg["pct_tau"]) > 0 else 1.0
            n_eff = float(best_n[i] or 0)
            ps = float(cfg["prior_strength"] or 0.0)
            cr = float(cfg["confidence_runs"] or 1.0)
            conf = ((n_eff + ps) / (n_eff + ps + cr)) if (n_eff + ps + cr) > 0 else 0.0
            raw = float(base) * float(conf)
            raw_vals.append(raw)

            head = track_key if best_mode[i] == "A" else (surface if best_mode[i] == "B" else "同程")
            displays.append(
                f"{head}{distance}m | p{int(float(cfg['use_quantile'])*100)}={t:.2f}s | gap+{gap:.2f}s({gap_pct*100:.2f}%) | n{best_n[i]} | conf{conf:.2f} | {best_mode[i]} | {sig}"
            )

        non_missing = [v for v in raw_vals if v is not None]
        mid = float(pd.Series(non_missing).median()) if non_missing else 0.0
        raw_vals = [mid if v is None else v for v in raw_vals]

        return pd.Series(raw_vals, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 4. 場地＋路程專長 (Venue/Dist Specialty)
    def _calculate_venue_dist_specialty(self):
        from datetime import datetime, timedelta
        import math
        from database.models import HorseHistory, Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None

        distance = self._to_int(getattr(race, "distance", 0), default=0) if race else 0
        surface = ""
        tt = str(getattr(race, "track_type", "") if race else "")
        track_key = tt.strip()
        track_key_norm = track_key.replace(" ", "")
        if ("全天候" in tt) or ("泥地" in tt):
            surface = "泥地"
        elif "草地" in tt:
            surface = "草地"
        else:
            g = getattr(race, "going", "") if race else ""
            if g in ("草地", "泥地"):
                surface = g

        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {
            "window_days": 720,
            "half_life_days": 365,
            "min_samples": 3,
            "confidence_runs": 12.0,
            "prior_strength": 12.0,
            "prior_win_rate": 0.08,
            "prior_place_rate": 0.28,
            "win_w": 0.6,
            "place_w": 0.4,
            "fallback_strategy": "A_B_C",
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="venue_dist_specialty_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "window_days" in v:
                    cfg["window_days"] = int(v["window_days"])
                if "half_life_days" in v:
                    cfg["half_life_days"] = int(v["half_life_days"])
                if "min_samples" in v:
                    cfg["min_samples"] = int(v["min_samples"])
                if "confidence_runs" in v:
                    cfg["confidence_runs"] = float(v["confidence_runs"])
                if "prior_strength" in v:
                    cfg["prior_strength"] = float(v["prior_strength"])
                if "prior_win_rate" in v:
                    cfg["prior_win_rate"] = float(v["prior_win_rate"])
                if "prior_place_rate" in v:
                    cfg["prior_place_rate"] = float(v["prior_place_rate"])
                if "win_w" in v:
                    cfg["win_w"] = float(v["win_w"])
                if "place_w" in v:
                    cfg["place_w"] = float(v["place_w"])
                if "fallback_strategy" in v:
                    cfg["fallback_strategy"] = str(v["fallback_strategy"])
        except Exception:
            pass

        if cfg["window_days"] < 0:
            cfg["window_days"] = 0
        if cfg["half_life_days"] < 0:
            cfg["half_life_days"] = 0
        if cfg["min_samples"] < 0:
            cfg["min_samples"] = 0
        if float(cfg["confidence_runs"] or 0.0) <= 0:
            cfg["confidence_runs"] = 1.0
        if float(cfg["prior_strength"] or 0.0) < 0:
            cfg["prior_strength"] = 0.0
        try:
            pw = float(cfg["prior_win_rate"])
        except Exception:
            pw = 0.08
        if pw < 0.0:
            pw = 0.0
        if pw > 1.0:
            pw = 1.0
        cfg["prior_win_rate"] = pw
        try:
            pp = float(cfg["prior_place_rate"])
        except Exception:
            pp = 0.28
        if pp < 0.0:
            pp = 0.0
        if pp > 1.0:
            pp = 1.0
        cfg["prior_place_rate"] = pp
        if cfg["win_w"] < 0:
            cfg["win_w"] = 0.0
        if cfg["place_w"] < 0:
            cfg["place_w"] = 0.0
        tw = cfg["win_w"] + cfg["place_w"]
        if tw <= 0:
            cfg["win_w"], cfg["place_w"], tw = 0.6, 0.4, 1.0
        cfg["win_w"] /= tw
        cfg["place_w"] /= tw
        if cfg["fallback_strategy"] not in ("A_B_C", "B_C", "C"):
            cfg["fallback_strategy"] = "A_B_C"

        cutoff_date = (race_date - timedelta(days=cfg["window_days"])) if cfg["window_days"] > 0 else None

        scores = []
        displays = []

        cached = {}

        def norm_track(s: str):
            return str(s or "").strip().replace(" ", "")

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)
            if not horse_id or not distance:
                scores.append(None)
                displays.append("無數據")
                continue

            key = (horse_id, track_key_norm, surface, distance, cfg["window_days"], cfg["half_life_days"], cfg["fallback_strategy"])
            if key in cached:
                eff_runs, win_rate_w, place_rate_w, last_days, chosen_mode = cached[key]
            else:
                q = (
                    self.session.query(HorseHistory.rank, HorseHistory.race_date, HorseHistory.venue, HorseHistory.surface)
                    .filter(
                        HorseHistory.horse_id == horse_id,
                        HorseHistory.distance == distance,
                        HorseHistory.rank > 0,
                        HorseHistory.race_date < cutoff_dt
                    )
                    .order_by(HorseHistory.race_date.desc())
                )
                if cutoff_date:
                    q = q.filter(HorseHistory.race_date >= cutoff_date)
                hist = q.all()

                mode_A = []
                mode_B = []
                mode_C = []
                for rnk, dt, v, sf in hist:
                    mode_C.append((rnk, dt))
                    if surface and sf == surface:
                        mode_B.append((rnk, dt))
                    if track_key_norm and norm_track(v) == track_key_norm:
                        mode_A.append((rnk, dt))

                if cfg["fallback_strategy"] == "A_B_C":
                    modes = [("A", mode_A), ("B", mode_B), ("C", mode_C)]
                elif cfg["fallback_strategy"] == "B_C":
                    modes = [("B", mode_B), ("C", mode_C)]
                else:
                    modes = [("C", mode_C)]

                chosen_mode = ""
                chosen = []
                for m, arr in modes:
                    if len(arr) >= int(cfg["min_samples"] or 0):
                        chosen_mode = m
                        chosen = arr
                        break
                if not chosen:
                    best_m = ""
                    best_n = -1
                    for m, arr in modes:
                        if len(arr) > best_n:
                            best_m = m
                            best_n = len(arr)
                            chosen = arr
                            chosen_mode = best_m

                last_days = None
                if chosen and isinstance(chosen[0][1], datetime):
                    last_days = max((race_date - chosen[0][1]).days, 0)

                if chosen and cfg["half_life_days"] > 0:
                    sum_w = 0.0
                    sum_win_w = 0.0
                    sum_place_w = 0.0
                    for rnk, dt in chosen:
                        if isinstance(dt, datetime):
                            days = max((race_date - dt).days, 0)
                        else:
                            days = 0
                        w = math.exp(-days / float(cfg["half_life_days"]))
                        sum_w += w
                        if rnk == 1:
                            sum_win_w += w
                        if rnk in (1, 2, 3):
                            sum_place_w += w
                    eff_runs = float(sum_w)
                    win_rate_w = (sum_win_w / sum_w) if sum_w > 0 else 0.0
                    place_rate_w = (sum_place_w / sum_w) if sum_w > 0 else 0.0
                else:
                    runs = len(chosen or [])
                    wins = sum(1 for rnk, _ in (chosen or []) if rnk == 1)
                    places = sum(1 for rnk, _ in (chosen or []) if rnk in (1, 2, 3))
                    eff_runs = float(runs)
                    win_rate_w = (wins / runs) if runs else 0.0
                    place_rate_w = (places / runs) if runs else 0.0

                cached[key] = (eff_runs, win_rate_w, place_rate_w, last_days, chosen_mode)

            eff = float(eff_runs or 0.0)
            ps = float(cfg["prior_strength"] or 0.0)
            pw = float(cfg["prior_win_rate"] or 0.0)
            pp = float(cfg["prior_place_rate"] or 0.0)
            denom = eff + ps
            if denom > 0:
                swr = (float(win_rate_w) * eff + pw * ps) / denom
                spr = (float(place_rate_w) * eff + pp * ps) / denom
            else:
                swr = pw
                spr = pp

            raw0 = (float(swr) * float(cfg["win_w"])) + (float(spr) * float(cfg["place_w"]))
            cr = float(cfg["confidence_runs"] or 1.0)
            conf = ((eff + ps) / (eff + ps + cr)) if (eff + ps + cr) > 0 else 0.0
            raw = float(raw0) * float(conf)

            if eff <= 0.0:
                scores.append(None)
            else:
                scores.append(raw)

            param_label = f"W{cfg['window_days']}d | HL{cfg['half_life_days']}d | N{cfg['min_samples']} | PS{ps:.0f} | C{cr:.0f} | WW{cfg['win_w']:.2f} | PW{cfg['place_w']:.2f} | {cfg['fallback_strategy']}"
            last_label = f"@{last_days}d" if last_days is not None else ""
            head = track_key if chosen_mode == "A" else (surface if chosen_mode == "B" else "同程")
            displays.append(
                f"{param_label} | {head}{distance}m | 勝{win_rate_w*100:.1f}%→{swr*100:.1f}% | 位{place_rate_w*100:.1f}%→{spr*100:.1f}% | eff{eff:.1f} | conf{conf:.2f}{last_label}"
            )

        non_missing = [v for v in scores if v is not None]
        mid = float(pd.Series(non_missing).median()) if non_missing else 0.0
        out_scores = [mid if v is None else float(v) for v in scores]
        return pd.Series(out_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 5. 檔位偏差 (Draw Stats) - 基於當日檔位統計
    def _calculate_draw_stats(self):
        from database.models import SystemConfig, Race
        import math
        scores = []
        displays = []
        
        # 取得當前賽事資訊
        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        try:
            race_id = int(race_id) if race_id is not None else None
        except (ValueError, TypeError):
            race_id = None
        race_no = None
        race_date_str = None
        if race_id:
            race = self.session.get(Race, race_id)
            if race:
                race_no = race.race_no
                if hasattr(race.race_date, 'strftime'):
                    race_date_str = race.race_date.strftime("%Y/%m/%d")
                else:
                    race_date_str = str(race.race_date)[:10].replace("-", "/")
                    
        # 讀取當日檔位統計 (從 SystemConfig)
        draw_stats_dict = {}
        if race_date_str:
            config_key = f"draw_stats_{race_date_str}"
            config = self.session.query(SystemConfig).filter_by(key=config_key).first()
            if config and isinstance(config.value, dict):
                # value 格式應為 { "1": [{"draw": 1, "win_rate": 8.0, ...}, ...], ... }
                # 注意 JSON 儲存後 key 可能變成字串
                str_race_no = str(race_no)
                if str_race_no in config.value:
                    stats_list = config.value[str_race_no]
                elif race_no in config.value:
                    stats_list = config.value[race_no]
                else:
                    stats_list = []

                if isinstance(stats_list, list):
                    for item in stats_list:
                        draw_stats_dict[item["draw"]] = item

        cfg = {
            "win_w": 0.4,
            "place_w": 0.6,
            "confidence_runs": 50.0,
            "prior_strength": 50.0,
            "prior_win_rate": 8.0,
            "prior_place_rate": 28.0,
            "use_top4_if_available": True,
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="draw_stats_factor_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "win_w" in v:
                    cfg["win_w"] = float(v["win_w"])
                if "place_w" in v:
                    cfg["place_w"] = float(v["place_w"])
                if "confidence_runs" in v:
                    cfg["confidence_runs"] = float(v["confidence_runs"])
                if "prior_strength" in v:
                    cfg["prior_strength"] = float(v["prior_strength"])
                if "prior_win_rate" in v:
                    cfg["prior_win_rate"] = float(v["prior_win_rate"])
                if "prior_place_rate" in v:
                    cfg["prior_place_rate"] = float(v["prior_place_rate"])
                if "use_top4_if_available" in v:
                    cfg["use_top4_if_available"] = bool(v["use_top4_if_available"])
        except Exception:
            pass

        if cfg["win_w"] < 0:
            cfg["win_w"] = 0.0
        if cfg["place_w"] < 0:
            cfg["place_w"] = 0.0
        tw = float(cfg["win_w"]) + float(cfg["place_w"])
        if tw <= 0:
            cfg["win_w"], cfg["place_w"], tw = 0.4, 0.6, 1.0
        cfg["win_w"] = float(cfg["win_w"]) / tw
        cfg["place_w"] = float(cfg["place_w"]) / tw
        if float(cfg["confidence_runs"] or 0.0) <= 0:
            cfg["confidence_runs"] = 1.0
        if float(cfg["prior_strength"] or 0.0) < 0:
            cfg["prior_strength"] = 0.0
        if float(cfg["prior_win_rate"] or 0.0) < 0:
            cfg["prior_win_rate"] = 0.0
        if float(cfg["prior_place_rate"] or 0.0) < 0:
            cfg["prior_place_rate"] = 0.0
        
        # 如果有讀取到檔位統計，則使用統計數據；否則回退到預設邏輯
        if draw_stats_dict:
            use_top4 = False
            if cfg["use_top4_if_available"]:
                for item in draw_stats_dict.values():
                    if "top4_rate" in item:
                        use_top4 = True
                        break

            per_draw = {}
            for d, item in draw_stats_dict.items():
                try:
                    dd = int(d)
                except Exception:
                    continue
                runs = float(item.get("total_runs", 0.0) or 0.0)
                win_rate = float(item.get("win_rate", 0.0) or 0.0)
                if use_top4:
                    place_rate = float(item.get("top4_rate", 0.0) or 0.0)
                else:
                    place_rate = float(item.get("place_rate", 0.0) or 0.0)
                ps = float(cfg["prior_strength"] or 0.0)
                denom = runs + ps
                sw = ((win_rate * runs) + (float(cfg["prior_win_rate"]) * ps)) / denom if denom > 0 else float(cfg["prior_win_rate"])
                sp = ((place_rate * runs) + (float(cfg["prior_place_rate"]) * ps)) / denom if denom > 0 else float(cfg["prior_place_rate"])
                conf = ((runs + ps) / (runs + ps + float(cfg["confidence_runs"]))) if (runs + ps + float(cfg["confidence_runs"])) > 0 else 0.0
                per_draw[dd] = {"runs": runs, "win": win_rate, "place": place_rate, "sw": sw, "sp": sp, "conf": conf}

            best_sw = max([v["sw"] for v in per_draw.values()]) if per_draw else 0.0
            best_sp = max([v["sp"] for v in per_draw.values()]) if per_draw else 0.0
            if best_sw <= 0:
                best_sw = 1.0
            if best_sp <= 0:
                best_sp = 1.0
            
            for _, row in self.df.iterrows():
                try:
                    draw = int(row.get("draw", 0))
                except (ValueError, TypeError):
                    draw = 0
                    
                if draw in per_draw:
                    st = per_draw[draw]
                    base = (float(cfg["win_w"]) * (st["sw"] / best_sw)) + (float(cfg["place_w"]) * (st["sp"] / best_sp))
                    if base < 0.0:
                        base = 0.0
                    score = 10.0 * float(base) * float(st["conf"])
                    scores.append(score)
                    lab_p = "Top4" if use_top4 else "上名"
                    displays.append(
                        f"第{draw}檔 | 勝{st['win']:.1f}%→{st['sw']:.1f}% | {lab_p}{st['place']:.1f}%→{st['sp']:.1f}% | n{st['runs']:.0f} | conf{st['conf']:.2f}"
                    )
                else:
                    scores.append(None)
                    displays.append(f"第{draw}檔 | 無統計數據")
        else:
            # 預設簡單邏輯：檔位越小，分數越高 (1檔 10分, 14檔 1分)
            max_draw = 0
            for _, row in self.df.iterrows():
                try:
                    d = int(row.get("draw", 0))
                except (ValueError, TypeError):
                    d = 0
                if d > max_draw:
                    max_draw = d
            if max_draw <= 1:
                max_draw = 14
            for _, row in self.df.iterrows():
                try:
                    draw = int(row.get("draw", 0))
                except (ValueError, TypeError):
                    draw = 0
                if draw > 0:
                    score = 10.0 * float(max_draw - draw) / float(max_draw - 1)
                else:
                    score = 0.0
                scores.append(score)
                displays.append(f"第 {draw} 檔 (未載入官方統計)")

        non_missing = [v for v in scores if v is not None]
        mid = float(pd.Series(non_missing).median()) if non_missing else 0.0
        out_scores = [mid if v is None else float(v) for v in scores]
        return pd.Series(out_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 6. 負磅／評分表現 (Weight/Rating Perf) - 真實邏輯：高評分馬通常實力較強
    def _calculate_weight_rating_perf(self):
        from datetime import datetime, timedelta
        import math
        from database.models import HorseHistory, Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        current_distance = self._to_int(getattr(race, "distance", 0), default=0) if race else 0
        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {
            "window_days": 365,
            "half_life_days": 180,
            "min_samples": 5,
            "place_weight": 0.2,
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="weight_rating_perf_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "window_days" in v:
                    cfg["window_days"] = int(v["window_days"])
                if "half_life_days" in v:
                    cfg["half_life_days"] = int(v["half_life_days"])
                if "min_samples" in v:
                    cfg["min_samples"] = int(v["min_samples"])
                if "place_weight" in v:
                    cfg["place_weight"] = float(v["place_weight"])
        except Exception:
            pass

        if cfg["window_days"] < 0:
            cfg["window_days"] = 0
        if cfg["half_life_days"] < 0:
            cfg["half_life_days"] = 0
        if cfg["min_samples"] < 0:
            cfg["min_samples"] = 0
        if cfg["place_weight"] < 0:
            cfg["place_weight"] = 0.0
        if cfg["place_weight"] > 1:
            cfg["place_weight"] = 1.0

        win_weight = 1.0 - cfg["place_weight"]
        cutoff_date = (race_date - timedelta(days=cfg["window_days"])) if cfg["window_days"] > 0 else None

        raw_scores = []
        displays = []

        cached_best_same_dist = {}
        cached_stats_same_dist = {}

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)
            current_rating = self._to_int(row.get("rating", 0), default=0)
            current_weight = self._to_int(row.get("weight", 0), default=0)

            best_win_rating = None
            best_win_weight = None
            best_win_days = None
            if horse_id and current_distance:
                key = (horse_id, current_distance)
                if key in cached_best_same_dist:
                    best_win_rating, best_win_weight, best_win_days = cached_best_same_dist[key]
                else:
                    rec = (
                        self.session.query(HorseHistory.rating, HorseHistory.weight, HorseHistory.race_date)
                        .filter(
                            HorseHistory.horse_id == horse_id,
                            HorseHistory.distance == current_distance,
                            HorseHistory.rank == 1,
                            HorseHistory.rating > 0,
                            HorseHistory.race_date < cutoff_dt
                        )
                        .order_by(HorseHistory.rating.desc())
                    )
                    if cutoff_date:
                        rec = rec.filter(HorseHistory.race_date >= cutoff_date)
                    rec = rec.first()
                    if rec:
                        best_win_rating = self._to_int(rec[0], default=0) or None
                        best_win_weight = self._to_int(rec[1], default=0) or None
                        if isinstance(rec[2], datetime):
                            best_win_days = max((race_date - rec[2]).days, 0)
                        else:
                            best_win_days = None
                    cached_best_same_dist[key] = (best_win_rating, best_win_weight, best_win_days)

            ref_w = None
            ref_w_days = None
            ref_w_label = None
            if horse_id and current_distance:
                q_ref = (
                    self.session.query(HorseHistory.weight, HorseHistory.race_date, HorseHistory.rank)
                    .filter(
                        HorseHistory.horse_id == horse_id,
                        HorseHistory.distance == current_distance,
                        HorseHistory.rank.in_((1, 2, 3)),
                        HorseHistory.weight > 0,
                        HorseHistory.race_date < cutoff_dt
                    )
                    .order_by(HorseHistory.race_date.desc())
                )
                if cutoff_date:
                    q_ref = q_ref.filter(HorseHistory.race_date >= cutoff_date)
                rec_ref = q_ref.first()
                if rec_ref:
                    ref_w = self._to_int(rec_ref[0], default=0) or None
                    if isinstance(rec_ref[1], datetime):
                        ref_w_days = max((race_date - rec_ref[1]).days, 0)
                    ref_w_label = "勝" if rec_ref[2] == 1 else "入"

            total_runs = 0
            weighted_place_rate = None
            if horse_id and current_distance:
                key = (horse_id, current_distance, cfg["window_days"], cfg["half_life_days"])
                if key in cached_stats_same_dist:
                    total_runs, weighted_place_rate = cached_stats_same_dist[key]
                else:
                    q = (
                        self.session.query(HorseHistory.rank, HorseHistory.race_date)
                        .filter(
                            HorseHistory.horse_id == horse_id,
                            HorseHistory.distance == current_distance,
                            HorseHistory.rank > 0,
                            HorseHistory.race_date < cutoff_dt
                        )
                        .order_by(HorseHistory.race_date.desc())
                    )
                    if cutoff_date:
                        q = q.filter(HorseHistory.race_date >= cutoff_date)
                    hist = q.all()
                    total_runs = len(hist)
                    if total_runs > 0:
                        if cfg["half_life_days"] > 0:
                            sum_w = 0.0
                            sum_place_w = 0.0
                            for rnk, dt in hist:
                                if isinstance(dt, datetime):
                                    days = max((race_date - dt).days, 0)
                                else:
                                    days = 0
                                w = math.exp(-days / float(cfg["half_life_days"]))
                                sum_w += w
                                if rnk in (1, 2, 3):
                                    sum_place_w += w
                            weighted_place_rate = (sum_place_w / sum_w) if sum_w > 0 else 0.0
                        else:
                            places = sum(1 for rnk, _ in hist if rnk in (1, 2, 3))
                            weighted_place_rate = places / total_runs
                    cached_stats_same_dist[key] = (total_runs, weighted_place_rate)

            decay = 1.0
            if cfg["half_life_days"] > 0 and best_win_days is not None:
                decay = math.exp(-best_win_days / float(cfg["half_life_days"]))

            delta_rating = (best_win_rating - current_rating) if (best_win_rating is not None and current_rating) else 0
            delta_weight = (ref_w - current_weight) if (ref_w is not None and current_weight) else 0

            win_component = 0.0
            if delta_rating > 0:
                win_component += min(delta_rating, 15) / 5.0
            win_component *= decay

            weight_component = 0.0
            weight_decay = 1.0
            if cfg["half_life_days"] > 0 and ref_w_days is not None:
                weight_decay = math.exp(-ref_w_days / float(cfg["half_life_days"]))
            if delta_weight > 0:
                weight_component = (min(delta_weight, 10) / 40.0) * weight_decay
            win_component += weight_component

            place_component = 0.0
            if weighted_place_rate is not None and total_runs >= cfg["min_samples"]:
                place_component = float(weighted_place_rate) * 4.0

            score = (win_weight * win_component) + (cfg["place_weight"] * place_component)
            score = round(score / 0.05) * 0.05

            raw_scores.append(score)

            parts = []
            parts.append(f"W{cfg['window_days']}d")
            parts.append(f"HL{cfg['half_life_days']}d")
            parts.append(f"N{cfg['min_samples']}")
            parts.append(f"PW{cfg['place_weight']:.2f}")
            if current_distance:
                parts.append(f"同程{current_distance}m")

            if best_win_rating is not None:
                dr = best_win_rating - current_rating
                if best_win_days is not None:
                    parts.append(f"同程可贏評{best_win_rating}({dr:+d})@{best_win_days}d")
                else:
                    parts.append(f"同程可贏評{best_win_rating}({dr:+d})")
            else:
                parts.append("同程無勝仗")

            if ref_w is not None and current_weight and ref_w_label:
                dw = ref_w - current_weight
                if ref_w_days is not None:
                    parts.append(f"同程{ref_w_label}磅{ref_w}({dw:+d})@{ref_w_days}d")
                else:
                    parts.append(f"同程{ref_w_label}磅{ref_w}({dw:+d})")

            if weighted_place_rate is not None:
                if total_runs < cfg["min_samples"]:
                    parts.append(f"同程上名率{weighted_place_rate*100:.1f}%({total_runs}<{cfg['min_samples']})")
                else:
                    parts.append(f"同程上名率{weighted_place_rate*100:.1f}%({total_runs})")

            if cfg["half_life_days"] > 0 and best_win_days is not None:
                parts.append(f"decay{decay:.2f}")

            parts.append(f"現評{current_rating}")
            parts.append(f"負磅{current_weight}")

            displays.append(" | ".join(parts) if parts else "無數據")

        return pd.Series(raw_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 7. 晨操／試閘表現 (Morning/Trial Perf)
    def _calculate_morning_trial_perf(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 10. 配備變化 (Gear Change)
    def _calculate_gear_change(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 12. 班次表現 (Class Performance)
    def _calculate_class_performance(self):
        from datetime import datetime
        from database.models import HorseHistory, Race

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        current_class_str = getattr(race, "race_class", "") if race else ""
        current_class_num = self._parse_class_num(current_class_str)
        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        raw_scores = []
        displays = []

        cached_prev_class = {}

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)

            prev_class_num = None
            prev_class_str = ""
            if horse_id:
                if horse_id in cached_prev_class:
                    prev_class_num, prev_class_str = cached_prev_class[horse_id]
                else:
                    hist = (
                        self.session.query(HorseHistory.race_class, HorseHistory.race_date)
                        .filter(HorseHistory.horse_id == horse_id)
                        .filter(HorseHistory.race_date < cutoff_dt)
                        .order_by(HorseHistory.race_date.desc())
                        .limit(10)
                        .all()
                    )
                    for rc, _ in hist:
                        n = self._parse_class_num(rc or "")
                        if n is not None:
                            prev_class_num = n
                            prev_class_str = rc or ""
                            break
                    cached_prev_class[horse_id] = (prev_class_num, prev_class_str)

            class_drop = False
            if current_class_num in (4, 5) and prev_class_num in (3, 4):
                class_drop = (current_class_num == prev_class_num + 1)

            score = 1.0 if class_drop else 0.0
            raw_scores.append(score)

            parts = []
            if current_class_str:
                parts.append(f"今班{current_class_str}")
            if prev_class_str:
                parts.append(f"上次班{prev_class_str}")
            if class_drop and prev_class_num and current_class_num:
                parts.append(f"降班{prev_class_num}→{current_class_num}")
            else:
                parts.append("無降班")
            displays.append(" | ".join(parts) if parts else "無數據")

        return pd.Series(raw_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 13. 場地狀況專長 (Going Specialty)
    def _calculate_going_specialty(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 14. HKJC SpeedPRO 能量分 (SpeedPRO)
    def _calculate_speedpro_energy(self):
        from datetime import datetime
        from database.models import Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        if not race_id:
            return pd.Series(0.0, index=self.df.index), pd.Series("無數據", index=self.df.index)

        race = self.session.get(Race, race_id) if race_id else None
        race_no = int(getattr(race, "race_no", 0) or 0) if race else 0
        date_str = None
        try:
            rd = getattr(race, "race_date", None)
            if isinstance(rd, datetime):
                date_str = rd.date().strftime("%Y/%m/%d")
        except Exception:
            date_str = None

        data_map = {}
        if date_str and race_no:
            cfg2 = self.session.query(SystemConfig).filter_by(key=f"speedpro_energy:{date_str}:{race_no}").first()
            if cfg2 and isinstance(cfg2.value, dict):
                data_map = cfg2.value
        if not data_map:
            cfg = self.session.query(SystemConfig).filter_by(key=f"speedpro_energy:{race_id}").first()
            data_map = cfg.value if cfg and isinstance(cfg.value, dict) else {}
        if not isinstance(data_map, dict) or not data_map:
            return pd.Series(0.0, index=self.df.index), pd.Series("無數據", index=self.df.index)

        priority_cfg = self.session.query(SystemConfig).filter_by(key="speedpro_energy_sort_priority").first()
        priority = priority_cfg.value if priority_cfg and isinstance(priority_cfg.value, list) else None
        if not isinstance(priority, list) or not priority:
            priority = ["energy_required", "status_rating", "energy_assess"]
        priority = [str(x) for x in priority if str(x) in {"energy_required", "status_rating", "energy_assess", "energy_diff"}]
        if not priority:
            priority = ["energy_required", "status_rating", "energy_assess"]
        priority = priority[:3]

        def _get_metric(hn: int):
            v = data_map.get(str(hn))
            if v is None:
                v = data_map.get(int(hn)) if isinstance(list(data_map.keys())[0], int) else None
            return v if isinstance(v, dict) else {}

        def _num(v):
            try:
                if v is None:
                    return None
                return float(v)
            except Exception:
                return None

        def _key_for(hn: int):
            m = _get_metric(hn)
            er = _num(m.get("energy_required"))
            sr = _num(m.get("status_rating"))
            ea = _num(m.get("energy_assess"))
            ed = _num(m.get("energy_diff"))

            def asc(x):
                return x if x is not None else 1e18

            def desc(x):
                return -(x if x is not None else -1e18)

            out = []
            for p in priority:
                if p == "energy_required":
                    out.append(asc(er))
                elif p == "status_rating":
                    out.append(desc(sr))
                elif p == "energy_assess":
                    out.append(desc(ea))
                elif p == "energy_diff":
                    out.append(desc(ed))
            out.append(int(hn))
            return tuple(out)

        horse_nos = [self._to_int(x, default=0) for x in self.df["horse_no"].tolist()] if "horse_no" in self.df.columns else []
        horse_nos = [hn for hn in horse_nos if hn > 0]
        sorted_hn = sorted(set(horse_nos), key=_key_for)
        rank_map = {hn: i + 1 for i, hn in enumerate(sorted_hn)}
        n_total = len(sorted_hn) if sorted_hn else 0

        raw_scores = []
        displays = []
        for _, row in self.df.iterrows():
            hn = self._to_int(row.get("horse_no"), default=0)
            m = _get_metric(hn) if hn else {}
            er = m.get("energy_required")
            sr = m.get("status_rating")
            ea = m.get("energy_assess")
            ed = m.get("energy_diff")
            rnk = rank_map.get(hn)
            if not rnk or not n_total:
                raw_scores.append(0.0)
                displays.append("無數據")
            else:
                raw_scores.append(float(n_total - rnk))
                displays.append(f"需{er}｜評{sr}｜評估{ea}｜差{ed}｜排{rnk}")

        return pd.Series(raw_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 15. 近期狀態 (Recent Form - Last 6 Runs) - 真實邏輯：加權計算過去 6 場的平均名次
    def _calculate_recent_form(self):
        from datetime import datetime
        from database.models import HorseHistory, Horse, SystemConfig, Race
        scores = []
        displays = []

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()
        
        # 讀取自訂權重參數 (如果沒有則使用預設值 [6, 5, 4, 3, 2, 1])
        config = self.session.query(SystemConfig).filter_by(key="recent_form_weights").first()
        if config and isinstance(config.value, list) and len(config.value) == 6:
            default_weights = config.value
        else:
            default_weights = [6, 5, 4, 3, 2, 1]
        
        for _, row in self.df.iterrows():
            # 查詢該馬匹最近 6 場往績
            history = self.session.query(HorseHistory)\
                .join(Horse)\
                .filter(Horse.code == row["horse_code"])\
                .filter(HorseHistory.race_date < cutoff_dt)\
                .order_by(HorseHistory.race_date.desc())\
                .limit(6).all()
            
            if not history:
                scores.append(-7.0) # 無數據給中位分 (假設平均第7名)
                displays.append("無往績紀錄")
                continue
            
            # 過濾出有效名次 (>0)，忽略退出等異常紀錄
            ranks = [h.rank for h in history if h.rank > 0]
            if not ranks:
                scores.append(-7.0)
                displays.append("近期無有效名次")
                continue
            
            # 反轉排序：確保第一筆是最近的賽事 (history 是按時間降序 order_by desc)
            # 所以 ranks[0] 就是最近一場
            
            # 根據有效名次的數量截取對應的權重
            n = len(ranks)
            weights = default_weights[:n]
            total_weight = sum(weights)
            
            if total_weight == 0:
                scores.append(-7.0)
                displays.append("權重總和為0")
                continue
                
            weighted_sum = sum(r * w for r, w in zip(ranks, weights))
            weighted_avg_rank = weighted_sum / total_weight
            
            # 為了給後端排序使用，我們把 raw_scores 設為負的加權平均名次
            scores.append(-weighted_avg_rank)
            
            # 組合顯示字串
            recent_str = "-".join(str(r) for r in ranks)
            displays.append(f"近仗: {recent_str} (加權均名次 {weighted_avg_rank:.1f})")
            
        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 16. 獸醫報告／休息天數 (Vet/Rest Days)
    def _calculate_vet_rest_days(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 17. 初出／長休後表現 (Debut/Long Rest)
    def _calculate_debut_long_rest(self):
        from datetime import datetime
        from database.models import HorseHistory, Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None

        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {"rest_days": 90, "win_points": 1.0, "place_points": 0.5}
        try:
            config = self.session.query(SystemConfig).filter_by(key="debut_long_rest_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "rest_days" in v:
                    cfg["rest_days"] = int(v["rest_days"])
                if "win_points" in v:
                    cfg["win_points"] = float(v["win_points"])
                if "place_points" in v:
                    cfg["place_points"] = float(v["place_points"])
        except Exception:
            pass

        if cfg["rest_days"] < 0:
            cfg["rest_days"] = 0
        if cfg["win_points"] < 0:
            cfg["win_points"] = 0.0
        if cfg["place_points"] < 0:
            cfg["place_points"] = 0.0

        scores = []
        displays = []

        cached = {}

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)
            if not horse_id:
                scores.append(0.0)
                displays.append("無數據")
                continue

            if horse_id in cached:
                current_rest, comeback_n, win_n, place_n, points, samples = cached[horse_id]
            else:
                hist = (
                    self.session.query(HorseHistory.race_date, HorseHistory.rank)
                    .filter(HorseHistory.horse_id == horse_id, HorseHistory.rank > 0, HorseHistory.race_date < cutoff_dt)
                    .order_by(HorseHistory.race_date.asc())
                    .all()
                )

                if not hist:
                    current_rest = None
                    comeback_n = 0
                    win_n = 0
                    place_n = 0
                    points = 0.0
                    samples = []
                else:
                    last_hist_date = hist[-1][0] if isinstance(hist[-1][0], datetime) else None
                    current_rest = max((race_date - last_hist_date).days, 0) if last_hist_date else None

                    comeback_n = 0
                    win_n = 0
                    place_n = 0
                    points = 0.0
                    samples = []

                    prev_date = None
                    for dt, rnk in hist:
                        if not isinstance(dt, datetime):
                            prev_date = None
                            continue
                        if prev_date is not None:
                            gap = (dt - prev_date).days
                            if gap >= cfg["rest_days"]:
                                comeback_n += 1
                                if rnk == 1:
                                    win_n += 1
                                    points += cfg["win_points"]
                                    samples.append(f"W@{gap}d")
                                elif rnk in (2, 3):
                                    place_n += 1
                                    points += cfg["place_points"]
                                    samples.append(f"P@{gap}d")
                                else:
                                    samples.append(f"-@{gap}d")
                        prev_date = dt

                cached[horse_id] = (current_rest, comeback_n, win_n, place_n, points, samples[:6])

            if current_rest is None:
                scores.append(0.0)
                displays.append(f"初出/無往績 | R{cfg['rest_days']}d")
                continue

            if current_rest < cfg["rest_days"]:
                scores.append(0.0)
                displays.append(f"休{current_rest}d(<{cfg['rest_days']}d) | R{cfg['rest_days']}d")
                continue

            scores.append(points)
            sample_str = ",".join(samples) if samples else "無樣本"
            displays.append(
                f"休{current_rest}d(≥{cfg['rest_days']}d) | 復出{comeback_n}次 冠{win_n} 位{place_n} | +{points:.2f} | {sample_str} | R{cfg['rest_days']}d"
            )

        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)


def get_available_factors():
    out = set()
    for name in dir(FactorCalculator):
        if name.startswith("_calculate_"):
            out.add(name.replace("_calculate_", "", 1))
    return out
