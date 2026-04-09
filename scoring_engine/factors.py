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
        from database.models import HorseHistory, SystemConfig
        scores = []
        displays = []

        cfg = {
            "global_window": 0,
            "global_win_w": 0.7,
            "global_place_w": 0.3,
            "horse_window": 0,
            "horse_win_w": 0.7,
            "horse_place_w": 0.3,
            "global_weight": 0.5,
            "horse_weight": 0.5
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
            ).order_by(HorseHistory.race_date.desc())
            
            if cfg["global_window"] > 0:
                q_global = q_global.limit(cfg["global_window"])

            try:
                hist_global = q_global.all()
            except Exception:
                hist_global = []
                
            runs_global = len(hist_global)
            score_global = 0.0
            str_global = f"全庫不足({runs_global})"
            
            if runs_global >= 3:
                w_g = sum(1 for h in hist_global if h.rank == 1)
                p_g = sum(1 for h in hist_global if h.rank in (1, 2, 3))
                wr_g = w_g / runs_global
                pr_g = p_g / runs_global
                score_global = (wr_g * cfg["global_win_w"]) + (pr_g * cfg["global_place_w"])
                str_global = f"全({runs_global}次,勝{wr_g*100:.0f}%,位{pr_g*100:.0f}%)"

            # --- 2. 計算本駒合作 ---
            score_horse = 0.0
            str_horse = "本駒無"
            
            if horse_id:
                q_horse = self.session.query(HorseHistory).filter(
                    HorseHistory.horse_id == int(horse_id),
                    HorseHistory.jockey_name == jockey,
                    HorseHistory.trainer_name == trainer
                ).order_by(HorseHistory.race_date.desc())
                
                if cfg["horse_window"] > 0:
                    q_horse = q_horse.limit(cfg["horse_window"])

                try:
                    hist_horse = q_horse.all()
                except Exception:
                    hist_horse = []
                    
                runs_horse = len(hist_horse)
                str_horse = f"本駒不足({runs_horse})"
                
                if runs_horse >= 3:
                    w_h = sum(1 for h in hist_horse if h.rank == 1)
                    p_h = sum(1 for h in hist_horse if h.rank in (1, 2, 3))
                    wr_h = w_h / runs_horse
                    pr_h = p_h / runs_horse
                    score_horse = (wr_h * cfg["horse_win_w"]) + (pr_h * cfg["horse_place_w"])
                    str_horse = f"本({runs_horse}次,勝{wr_h*100:.0f}%,位{pr_h*100:.0f}%)"

            # 綜合計分
            bond_score = (score_global * gw) + (score_horse * hw)
            scores.append(bond_score)
            displays.append(f"{str_global} | {str_horse}")
            
        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 2. 馬匹分段時間＋完成時間 (Horse Time Perf)
    def _calculate_horse_time_perf(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 3. 投注額變動 (Odds Movement)
    def _calculate_odds_movement(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 4. 場地＋路程專長 (Venue/Dist Specialty)
    def _calculate_venue_dist_specialty(self):
        from datetime import datetime, timedelta
        from database.models import HorseHistory, Race, SystemConfig

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None

        distance = self._to_int(getattr(race, "distance", 0), default=0) if race else 0
        surface = getattr(race, "going", "") if race else ""
        if surface not in ("草地", "泥地"):
            tt = str(getattr(race, "track_type", "") if race else "")
            if ("全天候" in tt) or ("泥地" in tt):
                surface = "泥地"
            elif "草地" in tt:
                surface = "草地"

        race_date = getattr(race, "race_date", None) if race else None
        if not isinstance(race_date, datetime):
            race_date = datetime.now()

        cfg = {
            "window_days": 720,
            "min_samples": 3,
            "confidence_runs": 8,
            "win_w": 0.6,
            "place_w": 0.4,
        }
        try:
            config = self.session.query(SystemConfig).filter_by(key="venue_dist_specialty_config").first()
            if config and isinstance(config.value, dict):
                v = config.value
                if "window_days" in v:
                    cfg["window_days"] = int(v["window_days"])
                if "min_samples" in v:
                    cfg["min_samples"] = int(v["min_samples"])
                if "confidence_runs" in v:
                    cfg["confidence_runs"] = int(v["confidence_runs"])
                if "win_w" in v:
                    cfg["win_w"] = float(v["win_w"])
                if "place_w" in v:
                    cfg["place_w"] = float(v["place_w"])
        except Exception:
            pass

        if cfg["window_days"] < 0:
            cfg["window_days"] = 0
        if cfg["min_samples"] < 0:
            cfg["min_samples"] = 0
        if cfg["confidence_runs"] <= 0:
            cfg["confidence_runs"] = 1
        if cfg["win_w"] < 0:
            cfg["win_w"] = 0.0
        if cfg["place_w"] < 0:
            cfg["place_w"] = 0.0
        tw = cfg["win_w"] + cfg["place_w"]
        if tw <= 0:
            cfg["win_w"], cfg["place_w"], tw = 0.6, 0.4, 1.0
        cfg["win_w"] /= tw
        cfg["place_w"] /= tw

        cutoff_date = (race_date - timedelta(days=cfg["window_days"])) if cfg["window_days"] > 0 else None

        scores = []
        displays = []

        cached = {}

        for _, row in self.df.iterrows():
            horse_id = self._to_int(row.get("horse_id", 0), default=0)
            if not horse_id or not distance or surface not in ("草地", "泥地"):
                scores.append(0.0)
                displays.append("無數據")
                continue

            key = (horse_id, surface, distance, cfg["window_days"])
            if key in cached:
                runs, wins, places, last_days = cached[key]
            else:
                q = (
                    self.session.query(HorseHistory.rank, HorseHistory.race_date)
                    .filter(
                        HorseHistory.horse_id == horse_id,
                        HorseHistory.distance == distance,
                        HorseHistory.surface == surface,
                        HorseHistory.rank > 0
                    )
                    .order_by(HorseHistory.race_date.desc())
                )
                if cutoff_date:
                    q = q.filter(HorseHistory.race_date >= cutoff_date)
                hist = q.all()
                runs = len(hist)
                wins = sum(1 for rnk, _ in hist if rnk == 1)
                places = sum(1 for rnk, _ in hist if rnk in (1, 2, 3))
                last_days = None
                if hist and isinstance(hist[0][1], datetime):
                    last_days = max((race_date - hist[0][1]).days, 0)
                cached[key] = (runs, wins, places, last_days)

            if runs < cfg["min_samples"]:
                scores.append(0.0)
                displays.append(f"{surface}{distance}m 樣本不足({runs}<{cfg['min_samples']})")
                continue

            win_rate = wins / runs if runs else 0.0
            place_rate = places / runs if runs else 0.0

            raw = (win_rate * cfg["win_w"]) + (place_rate * cfg["place_w"])
            confidence = min(runs / float(cfg["confidence_runs"]), 1.0)
            raw *= confidence

            scores.append(raw)

            param_label = f"W{cfg['window_days']}d | N{cfg['min_samples']} | C{cfg['confidence_runs']} | WW{cfg['win_w']:.2f} | PW{cfg['place_w']:.2f}"
            last_label = f"@{last_days}d" if last_days is not None else ""
            displays.append(
                f"{param_label} | {surface}{distance}m | 勝{win_rate*100:.1f}% | 位{place_rate*100:.1f}% | n{runs} | conf{confidence:.2f}{last_label}"
            )

        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)

    # 5. 檔位偏差 (Draw Stats) - 基於當日檔位統計
    def _calculate_draw_stats(self):
        from database.models import SystemConfig, Race
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
        
        # 如果有讀取到檔位統計，則使用統計數據；否則回退到預設邏輯
        if draw_stats_dict:
            # 找出最大勝出率作為基準
            max_win_rate = max([float(item.get("win_rate", 0.0)) for item in draw_stats_dict.values()]) if draw_stats_dict else 0.0
            # 找出最大上名率作為基準 (用於防呆或輔助)
            max_place_rate = max([float(item.get("place_rate", 0.0)) for item in draw_stats_dict.values()]) if draw_stats_dict else 0.0
            
            for _, row in self.df.iterrows():
                try:
                    draw = int(row.get("draw", 0))
                except (ValueError, TypeError):
                    draw = 0
                    
                if draw in draw_stats_dict:
                    stat = draw_stats_dict[draw]
                    win_rate = float(stat.get("win_rate", 0.0))
                    place_rate = float(stat.get("place_rate", 0.0))
                    runs = stat.get("total_runs", 0)
                    
                    # 混合得分: 70% 勝率 + 30% 上名率 (如果都有最大值基準)
                    score = 0.0
                    if max_win_rate > 0:
                        score += (win_rate / max_win_rate) * 7.0
                    if max_place_rate > 0:
                        score += (place_rate / max_place_rate) * 3.0
                        
                    scores.append(score)
                    displays.append(f"第 {draw} 檔 (勝率 {win_rate}%, 上名率 {place_rate}%, 樣本 {runs})")
                else:
                    # 該檔位無統計數據
                    scores.append(0.0)
                    displays.append(f"第 {draw} 檔 (無統計數據)")
        else:
            # 預設簡單邏輯：檔位越小，分數越高 (1檔 10分, 14檔 1分)
            for _, row in self.df.iterrows():
                try:
                    draw = int(row.get("draw", 0))
                except (ValueError, TypeError):
                    draw = 0
                score = float(max(11 - draw, 1)) if draw > 0 else 0.0
                scores.append(score)
                displays.append(f"第 {draw} 檔 (未載入官方統計)")
                
        return pd.Series(scores, index=self.df.index), pd.Series(displays, index=self.df.index)

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
                            HorseHistory.rating > 0
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
                        HorseHistory.weight > 0
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
                            HorseHistory.rank > 0
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

    # 11. 配速分析 (Pace Analysis)
    def _calculate_pace_analysis(self):
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 12. 班次表現 (Class Performance)
    def _calculate_class_performance(self):
        from database.models import HorseHistory, Race

        race_id = self.df.iloc[0].get("race_id") if "race_id" in self.df.columns else None
        race_id = self._to_int(race_id, default=0)
        race = self.session.get(Race, race_id) if race_id else None
        current_class_str = getattr(race, "race_class", "") if race else ""
        current_class_num = self._parse_class_num(current_class_str)

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
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display

    # 15. 近期狀態 (Recent Form - Last 6 Runs) - 真實邏輯：加權計算過去 6 場的平均名次
    def _calculate_recent_form(self):
        from database.models import HorseHistory, Horse, SystemConfig
        scores = []
        displays = []
        
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
        raw_scores = pd.Series(np.random.rand(len(self.df)), index=self.df.index)
        display = pd.Series(["無數據"] * len(self.df), index=self.df.index)
        return raw_scores, display
