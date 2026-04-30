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

        m = re.search(r'第\s*([一二三四五])\s*班', s)
        if m:
            return {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5}.get(m.group(1))

        if str(s).strip() in {"1", "2", "3", "4", "5"}:
            return int(str(s).strip())

        return None

    def _parse_class_info(self, s: str):
        import re

        raw = str(s or "").strip()
        if not raw:
            return {"kind": "unknown", "level": None, "raw": raw}

        m = re.search(r'Class\s*([0-9]+)', raw, re.IGNORECASE)
        if m:
            try:
                n = int(m.group(1))
                if n in (1, 2, 3, 4, 5):
                    return {"kind": "class", "level": n, "raw": raw}
            except Exception:
                pass

        m = re.search(r'第\s*([0-9]+)\s*班', raw)
        if m:
            try:
                n = int(m.group(1))
                if n in (1, 2, 3, 4, 5):
                    return {"kind": "class", "level": n, "raw": raw}
            except Exception:
                pass

        m = re.search(r'第\s*([一二三四五])\s*班', raw)
        if m:
            n = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5}.get(m.group(1))
            if n is not None:
                return {"kind": "class", "level": n, "raw": raw}

        if raw in {"1", "2", "3", "4", "5"}:
            return {"kind": "class", "level": int(raw), "raw": raw}

        m = re.search(r'(?:Group\s*([0-9]+)|\bG\s*([0-9]+)\b)', raw, re.IGNORECASE)
        if m:
            g = m.group(1) or m.group(2)
            try:
                n = int(g)
                if n in (1, 2, 3):
                    return {"kind": "grade", "level": n, "raw": raw}
            except Exception:
                pass

        m = re.search(r'([一二三])\s*級\s*賽', raw)
        if m:
            n = {"一": 1, "二": 2, "三": 3}.get(m.group(1))
            if n is not None:
                return {"kind": "grade", "level": n, "raw": raw}

        return {"kind": "unknown", "level": None, "raw": raw}

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
            "place_weight": 0.25,
            "target_k": 4,
            "field_weight": 0.25,
            "rating_relief_cap": 20.0,
            "weight_relief_cap": 12.0,
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
                if "target_k" in v:
                    cfg["target_k"] = int(v["target_k"])
                if "field_weight" in v:
                    cfg["field_weight"] = float(v["field_weight"])
                if "rating_relief_cap" in v:
                    cfg["rating_relief_cap"] = float(v["rating_relief_cap"])
                if "weight_relief_cap" in v:
                    cfg["weight_relief_cap"] = float(v["weight_relief_cap"])
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
        if cfg["target_k"] not in (3, 4, 5):
            cfg["target_k"] = 4
        if cfg["field_weight"] < 0:
            cfg["field_weight"] = 0.0
        if cfg["field_weight"] > 1:
            cfg["field_weight"] = 1.0
        if cfg["rating_relief_cap"] <= 0:
            cfg["rating_relief_cap"] = 20.0
        if cfg["weight_relief_cap"] <= 0:
            cfg["weight_relief_cap"] = 12.0

        win_weight = 1.0 - cfg["place_weight"]
        cutoff_date = (race_date - timedelta(days=cfg["window_days"])) if cfg["window_days"] > 0 else None

        raw_scores = []
        displays = []

        cached_best_same_dist = {}
        cached_stats_same_dist = {}

        valid_r = []
        valid_w = []
        for _, row in self.df.iterrows():
            r = self._to_int(row.get("rating", 0), default=0)
            w = self._to_int(row.get("weight", 0), default=0)
            if r > 0 and w > 0:
                valid_r.append(float(r))
                valid_w.append(float(w))
        r_min = min(valid_r) if valid_r else 0.0
        r_max = max(valid_r) if valid_r else 0.0
        r_rng = (r_max - r_min) if (r_max - r_min) > 0 else 0.0
        mean_w = (sum(valid_w) / len(valid_w)) if valid_w else 0.0
        slope = 0.0
        intercept = mean_w
        if len(valid_r) >= 2 and r_rng > 0:
            mr = sum(valid_r) / len(valid_r)
            mw = sum(valid_w) / len(valid_w)
            cov = sum((valid_r[i] - mr) * (valid_w[i] - mw) for i in range(len(valid_r)))
            var = sum((x - mr) ** 2 for x in valid_r)
            if var > 0:
                slope = cov / var
                intercept = mw - slope * mr

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
                    if best_win_rating is None:
                        rec2 = (
                            self.session.query(HorseHistory.rating, HorseHistory.weight, HorseHistory.race_date, HorseHistory.rank)
                            .filter(
                                HorseHistory.horse_id == horse_id,
                                HorseHistory.distance == current_distance,
                                HorseHistory.rank.in_(tuple(range(1, int(cfg["target_k"]) + 1))),
                                HorseHistory.rating > 0,
                                HorseHistory.race_date < cutoff_dt,
                            )
                            .order_by(HorseHistory.rating.desc())
                        )
                        if cutoff_date:
                            rec2 = rec2.filter(HorseHistory.race_date >= cutoff_date)
                        rec2 = rec2.first()
                        if rec2:
                            best_win_rating = self._to_int(rec2[0], default=0) or None
                            best_win_weight = self._to_int(rec2[1], default=0) or None
                            if isinstance(rec2[2], datetime):
                                best_win_days = max((race_date - rec2[2]).days, 0)
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
                        HorseHistory.rank.in_(tuple(range(1, int(cfg["target_k"]) + 1))),
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
            eff_runs = 0.0
            if horse_id and current_distance:
                key = (horse_id, current_distance, cfg["window_days"], cfg["half_life_days"])
                if key in cached_stats_same_dist:
                    total_runs, weighted_place_rate, eff_runs = cached_stats_same_dist[key]
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
                                if rnk in tuple(range(1, int(cfg["target_k"]) + 1)):
                                    sum_place_w += w
                            weighted_place_rate = (sum_place_w / sum_w) if sum_w > 0 else 0.0
                            eff_runs = float(sum_w)
                        else:
                            places = sum(1 for rnk, _ in hist if rnk in tuple(range(1, int(cfg["target_k"]) + 1)))
                            weighted_place_rate = places / total_runs
                            eff_runs = float(total_runs)
                    cached_stats_same_dist[key] = (total_runs, weighted_place_rate, eff_runs)

            decay = 1.0
            if cfg["half_life_days"] > 0 and best_win_days is not None:
                decay = math.exp(-best_win_days / float(cfg["half_life_days"]))

            delta_rating = (best_win_rating - current_rating) if (best_win_rating is not None and current_rating) else 0
            delta_weight = (ref_w - current_weight) if (ref_w is not None and current_weight) else 0

            field_rating = 0.5
            if current_rating > 0 and r_rng > 0:
                field_rating = (float(current_rating) - float(r_min)) / float(r_rng)
            expected_w = (float(intercept) + float(slope) * float(current_rating)) if (current_rating > 0 and mean_w > 0) else float(mean_w)
            field_relief = float(expected_w) - float(current_weight or 0.0)
            if field_relief > 6.0:
                field_relief = 6.0
            if field_relief < -6.0:
                field_relief = -6.0
            field_relief_score = (field_relief + 6.0) / 12.0
            field_component = (0.7 * float(field_rating)) + (0.3 * float(field_relief_score))

            relief_component = 0.0
            if delta_rating > 0:
                relief_component += min(float(delta_rating), float(cfg["rating_relief_cap"])) / float(cfg["rating_relief_cap"])
            relief_component *= float(decay)

            weight_component = 0.0
            weight_decay = 1.0
            if cfg["half_life_days"] > 0 and ref_w_days is not None:
                weight_decay = math.exp(-ref_w_days / float(cfg["half_life_days"]))
            if delta_weight > 0:
                weight_component = (min(float(delta_weight), float(cfg["weight_relief_cap"])) / float(cfg["weight_relief_cap"])) * float(weight_decay)

            place_component = 0.0
            if weighted_place_rate is not None and (total_runs > 0 or eff_runs > 0):
                conf2 = float(eff_runs) / (float(eff_runs) + float(cfg["min_samples"] or 1)) if float(eff_runs) >= 0 else 0.0
                place_component = float(weighted_place_rate) * float(conf2)

            history_component = (0.75 * float(relief_component)) + (0.25 * float(weight_component))
            history_component = (float(win_weight) * float(history_component)) + (float(cfg["place_weight"]) * float(place_component))

            score = (float(cfg["field_weight"]) * float(field_component)) + ((1.0 - float(cfg["field_weight"])) * float(history_component))

            raw_scores.append(score)

            parts = []
            parts.append(f"W{cfg['window_days']}d")
            parts.append(f"HL{cfg['half_life_days']}d")
            parts.append(f"N{cfg['min_samples']}")
            parts.append(f"PW{cfg['place_weight']:.2f}")
            parts.append(f"K{cfg['target_k']}")
            parts.append(f"FW{cfg['field_weight']:.2f}")
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
                parts.append(f"同程Top{cfg['target_k']}率{weighted_place_rate*100:.1f}%({total_runs})")

            if cfg["half_life_days"] > 0 and best_win_days is not None:
                parts.append(f"decay{decay:.2f}")

            if current_rating > 0:
                parts.append(f"場內評分{field_rating*100:.0f}%")
            if current_weight:
                parts.append(f"場內磅差{(expected_w - float(current_weight)):+.1f}")
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
        import math
        from database.models import HorseHistory, Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        current_class_str = getattr(race, "race_class", "") if race else ""
        current_info = self._parse_class_info(current_class_str)
        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {
            "lookback_races": 8,
            "half_life_days": 45,
            "max_gap_days": 120,
            "allowed_pairs": [[3, 4], [4, 5]],
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="class_drop_signal_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "lookback_races" in v:
                    cfg["lookback_races"] = int(v["lookback_races"])
                if "half_life_days" in v:
                    cfg["half_life_days"] = int(v["half_life_days"])
                if "max_gap_days" in v:
                    cfg["max_gap_days"] = int(v["max_gap_days"])
                if "allowed_pairs" in v:
                    cfg["allowed_pairs"] = v["allowed_pairs"]
        except Exception:
            pass

        if cfg["lookback_races"] <= 0:
            cfg["lookback_races"] = 1
        if cfg["lookback_races"] > 30:
            cfg["lookback_races"] = 30
        if cfg["half_life_days"] <= 0:
            cfg["half_life_days"] = 45
        if cfg["max_gap_days"] <= 0:
            cfg["max_gap_days"] = 120
        allowed_pairs = set()
        ap = cfg.get("allowed_pairs")
        if isinstance(ap, list):
            for item in ap:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    try:
                        a = int(item[0])
                        b = int(item[1])
                        if a in (1, 2, 3, 4, 5) and b in (1, 2, 3, 4, 5):
                            allowed_pairs.add((a, b))
                    except Exception:
                        continue
                elif isinstance(item, str) and "->" in item:
                    try:
                        a, b = item.split("->", 1)
                        a = int(a.strip())
                        b = int(b.strip())
                        if a in (1, 2, 3, 4, 5) and b in (1, 2, 3, 4, 5):
                            allowed_pairs.add((a, b))
                    except Exception:
                        continue
        if not allowed_pairs:
            allowed_pairs = {(3, 4), (4, 5)}
        cfg["allowed_pairs"] = [[a, b] for a, b in sorted(allowed_pairs)]

        raw_scores = []
        displays = []

        cached_prev_class = {}
        sig = f"LB{int(cfg['lookback_races'])}|HL{int(cfg['half_life_days'])}|MG{int(cfg['max_gap_days'])}|AP{','.join([f'{a}->{b}' for a,b in sorted(allowed_pairs)])}"

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)

            prev_info = None
            prev_dt = None
            if horse_id:
                if horse_id in cached_prev_class:
                    prev_info, prev_dt = cached_prev_class[horse_id]
                else:
                    hist = (
                        self.session.query(HorseHistory.race_class, HorseHistory.race_date)
                        .filter(HorseHistory.horse_id == horse_id)
                        .filter(HorseHistory.race_date < cutoff_dt)
                        .order_by(HorseHistory.race_date.desc())
                        .limit(int(cfg["lookback_races"]))
                        .all()
                    )
                    for rc, _ in hist:
                        info = self._parse_class_info(rc or "")
                        if info.get("kind") != "unknown":
                            prev_info = info
                            prev_dt = _ if isinstance(_, datetime) else None
                            break
                    cached_prev_class[horse_id] = (prev_info, prev_dt)

            strength = 0.0
            drop_label = "無降班"
            if prev_info and current_info.get("kind") == "class" and prev_info.get("kind") == "class":
                a = int(prev_info.get("level") or 0)
                b = int(current_info.get("level") or 0)
                if (a, b) in allowed_pairs:
                    strength = 1.0
                    drop_label = f"降班{a}→{b}"

            recency = 1.0
            gap_days = None
            if prev_dt is not None and isinstance(prev_dt, datetime):
                gap_days = max((race_date - prev_dt).days, 0)
                if gap_days > int(cfg["max_gap_days"]):
                    recency = 0.0
                else:
                    recency = math.exp(-float(gap_days) / float(cfg["half_life_days"]))
            score = float(strength) * float(recency)
            raw_scores.append(score)

            parts = []
            parts.append(f"今班{current_class_str or 'N/A'}")
            if prev_info:
                parts.append(f"上次{prev_info.get('raw')}")
            else:
                parts.append("上次N/A")
            parts.append(drop_label)
            if gap_days is not None:
                parts.append(f"隔{gap_days}d")
                parts.append(f"decay{recency:.2f}")
            parts.append(f"raw{score:.2f}")
            parts.append(sig)
            displays.append(" | ".join(parts) if parts else "無數據")

        return pd.Series(raw_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 13. 場地狀況專長 (Going Specialty)
    def _calculate_going_specialty(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 14. HKJC SpeedPRO 能量分 (SpeedPRO)
    def _calculate_speedpro_energy(self):
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        from database.models import Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        if not race_id:
            return pd.Series(0.0, index=self.df.index), pd.Series("無數據", index=self.df.index)

        race = self.session.get(Race, race_id) if race_id else None
        race_no = int(getattr(race, "race_no", 0) or 0) if race else 0
        race_class = str(getattr(race, "race_class", "") or "").strip() if race else ""
        date_str = None
        race_day = None
        try:
            rd = getattr(race, "race_date", None)
            if isinstance(rd, datetime):
                race_day = rd
                date_str = rd.date().strftime("%Y/%m/%d")
        except Exception:
            date_str = None
            race_day = None

        data_map = {}
        source_key = None
        if date_str and race_no:
            cfg2 = self.session.query(SystemConfig).filter_by(key=f"speedpro_energy:{date_str}:{race_no}").first()
            if cfg2 and isinstance(cfg2.value, dict):
                data_map = cfg2.value
                source_key = f"speedpro_energy:{date_str}:{race_no}"
        if not data_map:
            cfg = self.session.query(SystemConfig).filter_by(key=f"speedpro_energy:{race_id}").first()
            data_map = cfg.value if cfg and isinstance(cfg.value, dict) else {}
            if data_map:
                source_key = f"speedpro_energy:{race_id}"
        if not isinstance(data_map, dict) or not data_map:
            if race_class and ("新馬" in race_class):
                return pd.Series(0.0, index=self.df.index), pd.Series("新馬賽無SpeedPRO", index=self.df.index)
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
                v = data_map.get(int(hn))
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

        total = n_total
        has_energy = 0
        has_status = 0
        both = 0
        for hn in sorted_hn:
            m = _get_metric(hn)
            ea = _num(m.get("energy_assess"))
            sr = _num(m.get("status_rating"))
            if ea is not None:
                has_energy += 1
            if sr is not None:
                has_status += 1
            if ea is not None and sr is not None:
                both += 1

        ready = bool(total and total >= 6 and has_energy > 0 and has_status > 0 and (both / float(total)) >= 0.6)
        if not ready:
            parts = [f"未齊全 EA{has_energy}/{total}", f"SR{has_status}/{total}", f"BOTH{both}/{total}"]
            hk_tz = ZoneInfo("Asia/Hong_Kong")
            if isinstance(race_day, datetime):
                try:
                    rd_hk = race_day
                    if rd_hk.tzinfo is None:
                        rd_hk = rd_hk.replace(tzinfo=hk_tz)
                    now_hk = datetime.now(hk_tz)
                    window_start = (
                        datetime.combine((rd_hk - timedelta(days=1)).date(), datetime.strptime("12:00", "%H:%M").time())
                        .replace(tzinfo=hk_tz)
                    )
                    if now_hk < window_start:
                        parts.append(f"未到預計發佈(≥{window_start.strftime('%m/%d %H:%M')})")
                except Exception:
                    pass
            if source_key:
                parts.append(source_key)
            return pd.Series(0.0, index=self.df.index), pd.Series("｜".join(parts), index=self.df.index)

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
                parts = [f"需{er}", f"評{sr}", f"評估{ea}", f"差{ed}", f"排{rnk}"]
                if source_key:
                    parts.append(source_key)
                parts.append("P" + ",".join(priority))
                displays.append("｜".join(parts))

        return pd.Series(raw_scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 15. 近期狀態 (Recent Form - Last 6 Runs) - 真實邏輯：加權計算過去 6 場的平均名次
    def _calculate_recent_form(self):
        from datetime import datetime
        import math
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

        cfg = {
            "mid_rank": 4.5,
            "rank_slope": 1.6,
            "dnf_rank": 14,
            "rank_cap": 20,
            "use_day_decay": True,
            "day_tau": 120.0,
            "neutral": 0.5,
            "conf_k": 2.0,
            "gap_days_neutral": 60.0,
            "gap_tau": 60.0,
            "trend_w": 0.08,
            "trend_tau": 3.0,
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="recent_form_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "mid_rank" in v:
                    cfg["mid_rank"] = float(v["mid_rank"])
                if "rank_slope" in v:
                    cfg["rank_slope"] = float(v["rank_slope"])
                if "dnf_rank" in v:
                    cfg["dnf_rank"] = int(v["dnf_rank"])
                if "rank_cap" in v:
                    cfg["rank_cap"] = int(v["rank_cap"])
                if "use_day_decay" in v:
                    cfg["use_day_decay"] = bool(v["use_day_decay"])
                if "day_tau" in v:
                    cfg["day_tau"] = float(v["day_tau"])
                if "conf_k" in v:
                    cfg["conf_k"] = float(v["conf_k"])
                if "gap_days_neutral" in v:
                    cfg["gap_days_neutral"] = float(v["gap_days_neutral"])
                if "gap_tau" in v:
                    cfg["gap_tau"] = float(v["gap_tau"])
                if "trend_w" in v:
                    cfg["trend_w"] = float(v["trend_w"])
                if "trend_tau" in v:
                    cfg["trend_tau"] = float(v["trend_tau"])
        except Exception:
            pass

        if cfg["rank_slope"] <= 0:
            cfg["rank_slope"] = 1.0
        if cfg["rank_cap"] < 1:
            cfg["rank_cap"] = 1
        if cfg["dnf_rank"] < 1:
            cfg["dnf_rank"] = 1
        if cfg["day_tau"] < 0:
            cfg["day_tau"] = 0.0
        if cfg["conf_k"] < 0:
            cfg["conf_k"] = 0.0
        if cfg["gap_days_neutral"] < 0:
            cfg["gap_days_neutral"] = 0.0
        if cfg["gap_tau"] < 0:
            cfg["gap_tau"] = 0.0
        if cfg["trend_w"] < 0:
            cfg["trend_w"] = 0.0
        if cfg["trend_tau"] <= 0:
            cfg["trend_tau"] = 1.0

        cached = {}

        def _days_ago(dt):
            if not dt:
                return None
            try:
                d = (cutoff_dt.date() - dt.date()).days
                return 0 if d < 0 else int(d)
            except Exception:
                return None

        def _rank_to_score(r):
            x = (float(r) - float(cfg["mid_rank"])) / float(cfg["rank_slope"])
            if x >= 60:
                return 0.0
            if x <= -60:
                return 1.0
            return 1.0 / (1.0 + math.exp(x))

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)
            horse_code = str(row.get("horse_code") or "").strip()
            cache_key = horse_id if horse_id else horse_code

            if cache_key and cache_key in cached:
                raw, disp = cached[cache_key]
                scores.append(raw)
                displays.append(disp)
                continue

            history = []
            if horse_id:
                history = (
                    self.session.query(HorseHistory.race_date, HorseHistory.rank)
                    .filter(HorseHistory.horse_id == horse_id, HorseHistory.race_date < cutoff_dt)
                    .order_by(HorseHistory.race_date.desc())
                    .limit(6)
                    .all()
                )
            elif horse_code:
                history = (
                    self.session.query(HorseHistory.race_date, HorseHistory.rank)
                    .join(Horse)
                    .filter(Horse.code == horse_code, HorseHistory.race_date < cutoff_dt)
                    .order_by(HorseHistory.race_date.desc())
                    .limit(6)
                    .all()
                )

            if not history:
                raw = float(cfg["neutral"])
                disp = "無近仗"
                if cache_key:
                    cached[cache_key] = (raw, disp)
                scores.append(raw)
                displays.append(disp)
                continue

            runs = [(_days_ago(dt), self._to_int(rk, default=0)) for (dt, rk) in history]
            n = len(runs)
            weights = default_weights[:n]
            denom = 0.0
            numer = 0.0
            disp_ranks = []
            eff_ranks = []

            for i, (days_ago, rk) in enumerate(runs):
                eff = rk if rk > 0 else int(cfg["dnf_rank"])
                if eff < 1:
                    eff = 1
                if eff > int(cfg["rank_cap"]):
                    eff = int(cfg["rank_cap"])

                eff_ranks.append(eff)
                disp_ranks.append("X" if rk <= 0 else str(int(rk)))

                s = _rank_to_score(eff)
                w = float(weights[i]) if i < len(weights) else 1.0
                decay = 1.0
                if cfg["use_day_decay"] and days_ago is not None and cfg["day_tau"] > 0:
                    decay = math.exp(-float(days_ago) / float(cfg["day_tau"]))
                ww = w * decay
                numer += ww * s
                denom += ww

            mean_score = float(cfg["neutral"]) if denom <= 0 else (numer / denom)
            conf = float(n) / (float(n) + float(cfg["conf_k"])) if cfg["conf_k"] > 0 else 1.0
            raw = float(cfg["neutral"]) + conf * (mean_score - float(cfg["neutral"]))

            last_days = runs[0][0]
            if last_days is not None and float(last_days) > float(cfg["gap_days_neutral"]) and cfg["gap_tau"] > 0:
                shrink = math.exp(-(float(last_days) - float(cfg["gap_days_neutral"])) / float(cfg["gap_tau"]))
                raw = float(cfg["neutral"]) + shrink * (raw - float(cfg["neutral"]))

            trend_bonus = 0.0
            trend_delta = None
            last_k = min(3, n)
            prev_k = min(3, max(0, n - last_k))
            if prev_k > 0:
                last_mean = sum(eff_ranks[:last_k]) / float(last_k)
                prev_mean = sum(eff_ranks[last_k:last_k + prev_k]) / float(prev_k)
                trend_delta = prev_mean - last_mean
                trend_bonus = float(cfg["trend_w"]) * math.tanh(float(trend_delta) / float(cfg["trend_tau"]))
                raw += trend_bonus

            if raw < 0.0:
                raw = 0.0
            if raw > 1.0:
                raw = 1.0

            recent_str = "-".join(disp_ranks)
            if trend_delta is None:
                disp = f"近仗:{recent_str}｜Top4分{mean_score:.2f}｜信心{conf:.2f}｜距上仗{last_days if last_days is not None else 'NA'}日"
            else:
                disp = f"近仗:{recent_str}｜Top4分{mean_score:.2f}｜信心{conf:.2f}｜距上仗{last_days if last_days is not None else 'NA'}日｜趨勢{trend_delta:+.1f}"

            if cache_key:
                cached[cache_key] = (raw, disp)
            scores.append(raw)
            displays.append(disp)

        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 16. 獸醫報告／休息天數 (Vet/Rest Days)
    def _calculate_vet_rest_days(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 17. 初出／長休後表現 (Debut/Long Rest)
    def _calculate_debut_long_rest(self):
        from datetime import datetime
        import math
        from database.models import HorseHistory, Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None

        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()
        cutoff_dt = self._race_cutoff_dt()

        cfg = {
            "rest_days": 90,
            "rest_tau": 60.0,
            "neutral_top4": 0.0,
            "prior_strength": 6.0,
            "prior_top4": 0.28,
            "conf_k": 3.0,
            "sample_max": 12,
            "dnf_rank": 14,
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="debut_long_rest_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "rest_days" in v:
                    cfg["rest_days"] = int(v["rest_days"])
                if "rest_tau" in v:
                    cfg["rest_tau"] = float(v["rest_tau"])
                if "neutral_top4" in v:
                    cfg["neutral_top4"] = float(v["neutral_top4"])
                if "prior_strength" in v:
                    cfg["prior_strength"] = float(v["prior_strength"])
                if "prior_top4" in v:
                    cfg["prior_top4"] = float(v["prior_top4"])
                if "conf_k" in v:
                    cfg["conf_k"] = float(v["conf_k"])
                if "sample_max" in v:
                    cfg["sample_max"] = int(v["sample_max"])
                if "dnf_rank" in v:
                    cfg["dnf_rank"] = int(v["dnf_rank"])
        except Exception:
            pass

        if cfg["rest_days"] < 0:
            cfg["rest_days"] = 0
        if cfg["rest_tau"] < 0:
            cfg["rest_tau"] = 0.0
        if cfg["neutral_top4"] < 0:
            cfg["neutral_top4"] = 0.0
        if cfg["neutral_top4"] > 1.0:
            cfg["neutral_top4"] = 1.0
        if cfg["prior_strength"] < 0:
            cfg["prior_strength"] = 0.0
        if cfg["prior_top4"] < 0:
            cfg["prior_top4"] = 0.0
        if cfg["prior_top4"] > 1.0:
            cfg["prior_top4"] = 1.0
        if cfg["conf_k"] < 0:
            cfg["conf_k"] = 0.0
        if cfg["sample_max"] < 1:
            cfg["sample_max"] = 1
        if cfg["dnf_rank"] < 1:
            cfg["dnf_rank"] = 1

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
                raw, disp = cached[horse_id]
                scores.append(raw)
                displays.append(disp)
                continue

            neutral = float(cfg["neutral_top4"])
            if neutral <= 0.0:
                n_field = len(self.df) if hasattr(self.df, "__len__") else 0
                neutral = 4.0 / float(n_field) if n_field and n_field > 0 else float(cfg["prior_top4"])
            neutral = max(0.0, min(1.0, neutral))

            hist = (
                self.session.query(HorseHistory.race_date, HorseHistory.rank)
                .filter(HorseHistory.horse_id == horse_id, HorseHistory.race_date < cutoff_dt)
                .order_by(HorseHistory.race_date.asc())
                .all()
            )

            sig = (
                f"R{int(cfg['rest_days'])}d|T{float(cfg['rest_tau']):.0f}d|"
                f"PS{float(cfg['prior_strength']):.1f}|P{float(cfg['prior_top4']):.2f}|"
                f"K{float(cfg['conf_k']):.1f}|M{int(cfg['sample_max'])}|D{int(cfg['dnf_rank'])}"
            )

            if not hist:
                raw = neutral
                disp = f"初出/無往績｜中性{neutral:.2f}｜{sig}"
                cached[horse_id] = (raw, disp)
                scores.append(raw)
                displays.append(disp)
                continue

            last_hist_date = hist[-1][0] if isinstance(hist[-1][0], datetime) else None
            current_rest = max((race_date - last_hist_date).days, 0) if last_hist_date else None

            if current_rest is None:
                raw = neutral
                disp = f"無有效日期｜中性{neutral:.2f}｜{sig}"
                cached[horse_id] = (raw, disp)
                scores.append(raw)
                displays.append(disp)
                continue

            rest_days = int(cfg["rest_days"])
            if current_rest < rest_days:
                raw = neutral
                disp = f"休{current_rest}d(<{rest_days}d)｜中性{neutral:.2f}｜{sig}"
                cached[horse_id] = (raw, disp)
                scores.append(raw)
                displays.append(disp)
                continue

            outcomes = []
            samples = []
            prev_date = None
            for dt, rnk in hist:
                if not isinstance(dt, datetime):
                    prev_date = None
                    continue
                if prev_date is not None:
                    gap = (dt - prev_date).days
                    if gap >= rest_days:
                        rk = self._to_int(rnk, default=0)
                        eff = rk if rk > 0 else int(cfg["dnf_rank"])
                        ok = 1 if (eff > 0 and eff <= 4) else 0
                        outcomes.append(ok)
                        tag = "T4" if ok else "-"
                        samples.append(f"{tag}@{int(gap)}d")
                prev_date = dt

            if len(outcomes) > int(cfg["sample_max"]):
                outcomes = outcomes[-int(cfg["sample_max"]):]
                samples = samples[-int(cfg["sample_max"]):]

            n = len(outcomes)
            ps = float(cfg["prior_strength"])
            pr = float(cfg["prior_top4"])
            if ps <= 0:
                smoothed = (sum(outcomes) / float(n)) if n > 0 else pr
            else:
                smoothed = (sum(outcomes) + ps * pr) / (float(n) + ps)

            conf = float(n) / (float(n) + float(cfg["conf_k"])) if float(cfg["conf_k"]) > 0 else 1.0
            base = neutral + conf * (smoothed - neutral)

            if float(cfg["rest_tau"]) > 0:
                shrink = math.exp(-(float(current_rest) - float(rest_days)) / float(cfg["rest_tau"]))
                raw = neutral + shrink * (base - neutral)
            else:
                raw = base

            raw = max(0.0, min(1.0, float(raw)))

            sample_str = ",".join(samples[-6:]) if samples else "無樣本"
            disp = (
                f"休{current_rest}d(≥{rest_days}d)｜復出樣本{n}｜T4率{smoothed:.2f}｜信心{conf:.2f}｜raw{raw:.2f}｜{sample_str}｜{sig}"
            )
            cached[horse_id] = (raw, disp)
            scores.append(raw)
            displays.append(disp)

        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)


def get_available_factors():
    out = set()
    for name in dir(FactorCalculator):
        if name.startswith("_calculate_"):
            out.add(name.replace("_calculate_", "", 1))
    return out
